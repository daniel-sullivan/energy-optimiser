package cmd

import (
	"context"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"energy-optimiser/config"
	"energy-optimiser/influx"
	"energy-optimiser/loadmodel"
)

// loadeval is a walk-forward forecast-accuracy harness for the load model. For
// each evaluation day D it trains the load model ONLY on data BEFORE D, predicts
// day D's load, and compares the prediction against the ACTUAL measured load
// pulled from the metrics DB. Unlike `backtest` — which replays actuals through
// the optimizer and so BYPASSES the load model entirely — this measures the
// forecast itself: the overnight predicted-vs-actual kWh bias (target ≈ 1.0; the
// pre-fix model over-predicts overnight ~1.87×). It can also SWEEP the per-bucket
// half-life to pick the value that drives that bias toward 1.0.
const (
	loadEvalSlotDur          = 30 * time.Minute
	loadEvalNightFromHour    = 1 // local hour, inclusive — start of the overnight cheap off-peak window
	loadEvalNightToHour      = 5 // local hour, exclusive — end of it
	loadEvalMinNightCoverage = 0.7
	loadEvalClampHi          = 25000.0 // W — reject/clamp counter-contamination spikes
	loadEvalSlotHours        = 0.5
)

var (
	loadEvalFrom     string
	loadEvalDays     int
	loadEvalBucketHL float64
	loadEvalSweep    string
)

// loadEvalSource adapts the influx metrics client to loadmodel.DataSource so the
// model can train/predict off the same VM data spine the hub uses. (hub's own
// adapter is unexported, so we define our own tiny one here.)
type loadEvalSource struct{ c *influx.Client }

func (s loadEvalSource) QueryPower(ctx context.Context, entityID string, from, to time.Time) ([]loadmodel.Sample, error) {
	ss, err := s.c.QueryPower(ctx, entityID, from, to)
	if err != nil {
		return nil, err
	}
	out := make([]loadmodel.Sample, len(ss))
	for i, v := range ss {
		out[i] = loadmodel.Sample{Time: v.Time, Value: v.Value}
	}
	return out, nil
}

// bandResult carries a predicted-vs-actual energy comparison over a set of slots
// (a night window or a full day), plus the fraction of those slots backed by a
// real measured sample (held-forward gap-fill does NOT count as covered).
type bandResult struct {
	predKWh  float64
	actKWh   float64
	coverage float64
}

func (r bandResult) ratio() float64 {
	if r.actKWh == 0 {
		return 0
	}
	return r.predKWh / r.actKWh
}

// dayRow is one evaluation day's result.
type dayRow struct {
	date    time.Time
	night   bandResult
	day     bandResult
	skipped bool // night coverage < threshold — excluded from aggregates
}

var loadEvalCmd = &cobra.Command{
	Use:   "loadeval",
	Short: "Walk-forward forecast-accuracy harness for the load model (overnight predicted-vs-actual kWh)",
	RunE: func(cmd *cobra.Command, args []string) error {
		cfg, err := config.Parse(cfgFile)
		if err != nil {
			return err
		}
		loc := cfg.Location()
		if loc == nil {
			loc = time.UTC
		}

		days, err := loadEvalDayList(cfg, loc)
		if err != nil {
			return err
		}
		lookback := time.Duration(cfg.LoadModel.LookbackDays * 24 * float64(time.Hour))

		db, err := influx.New(cfg.InfluxDB)
		if err != nil {
			return err
		}
		defer func() { _ = db.Close() }()
		src := loadEvalSource{c: db}

		ctx, cancel := context.WithTimeout(cmd.Context(), 15*time.Minute)
		defer cancel()

		out := cmd.OutOrStdout()

		if loadEvalSweep != "" {
			hls, err := parseSweep(loadEvalSweep)
			if err != nil {
				return err
			}
			return runLoadEvalSweep(ctx, out, src, cfg, days, lookback, hls)
		}

		params := loadEvalParams(cfg, loadEvalBucketHL)
		rows, err := evalDays(ctx, src, cfg, params, days, lookback)
		if err != nil {
			return err
		}
		printLoadEvalSingle(out, params, days, lookback, rows)
		return nil
	},
}

// loadEvalDayList resolves the set of COMPLETE local days to evaluate — up to
// yesterday (today is incomplete and would query future timestamps).
func loadEvalDayList(cfg *config.Config, loc *time.Location) ([]time.Time, error) {
	now := time.Now().In(loc)
	todayStart := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, loc)

	var startDay time.Time
	if loadEvalFrom != "" {
		d, err := time.ParseInLocation("2006-01-02", loadEvalFrom, loc)
		if err != nil {
			return nil, fmt.Errorf("parse --from: %w", err)
		}
		startDay = d
	} else {
		startDay = todayStart.AddDate(0, 0, -loadEvalDays)
	}

	var days []time.Time
	for d := startDay; d.Before(todayStart); d = d.AddDate(0, 0, 1) {
		days = append(days, d)
	}
	if len(days) == 0 {
		return nil, fmt.Errorf("no complete days to evaluate (start %s is not before today %s)",
			startDay.Format("2006-01-02"), todayStart.Format("2006-01-02"))
	}
	return days, nil
}

// loadEvalParams mirrors the hub's load-model Params construction (config.LoadModel
// + Optimizer.ConfidenceThreshold), with an optional bucket-half-life override
// (bucketHL > 0) for a single tuning run.
func loadEvalParams(cfg *config.Config, bucketHL float64) loadmodel.Params {
	p := loadmodel.Params{
		SouthernHemisphere:  cfg.Weather.Latitude < 0,
		RecencyHalfLifeDays: cfg.LoadModel.RecencyHalfLifeDays,
		BucketHalfLifeDays:  cfg.LoadModel.BucketHalfLifeDays,
		ConfidenceThreshold: cfg.Optimizer.ConfidenceThreshold,
		ConservativeMargin:  cfg.LoadModel.ConservativeMargin,
	}
	if bucketHL > 0 {
		p.BucketHalfLifeDays = bucketHL
	}
	return p
}

// evalNight is the pure core: train on [dStart-lookback, dStart) (walk-forward,
// no leakage), predict [dStart, next-local-midnight) — a 23h/24h/25h span across
// DST transitions — and return predicted-vs-actual kWh for the overnight window
// and the full day, each summed over covered slots only. It takes a
// loadmodel.DataSource so tests can drive it with a synthetic in-memory source
// (no network).
func evalNight(ctx context.Context, src loadmodel.DataSource, cfg *config.Config, params loadmodel.Params, dStart time.Time, lookback time.Duration) (night, day bandResult, err error) {
	loc := cfg.Location()
	if loc == nil {
		loc = time.UTC
	}
	// Derive the day end as the next local midnight so a DST-transition day is
	// 23h or 25h, not a hard-coded 24h/48 slots. (The Japan deployment has no DST,
	// but the harness is generic.) Slots stay 30 min; the count follows the span.
	dStartLoc := dStart.In(loc)
	dEnd := dStartLoc.AddDate(0, 0, 1)
	entity := cfg.HomeAssistant.Entities.LoadPower

	model := loadmodel.New(cfg.Circuits, entity, params)
	if err = model.TrainWindow(ctx, src, dStartLoc.Add(-lookback), dStartLoc); err != nil {
		return night, day, fmt.Errorf("train %s: %w", dStartLoc.Format("2006-01-02"), err)
	}

	slotCount := int(dEnd.Sub(dStartLoc) / loadEvalSlotDur)
	slots := make([]time.Time, slotCount)
	for i := range slots {
		slots[i] = dStartLoc.Add(time.Duration(i) * loadEvalSlotDur)
	}
	predicted := model.Predict(slots) // W per slot

	actualSamples, err := src.QueryPower(ctx, entity, dStartLoc, dEnd)
	if err != nil {
		return night, day, fmt.Errorf("actual %s: %w", dStartLoc.Format("2006-01-02"), err)
	}
	actual, covered := resampleLoadEval(actualSamples, slots, loadEvalSlotDur, 0, loadEvalClampHi)

	// Sum predicted and actual kWh over COVERED slots ONLY — the two totals cover
	// the SAME real-data slots, so the bias/MAE is an apples-to-apples ratio with
	// no synthetic (hold-last / leading-zero) actuals folded in. Coverage is still
	// measured against ALL slots in the band. These totals are therefore
	// "covered-slot" energy, not whole-night energy.
	var nightSlots, nightCov, daySlots, dayCov int
	for i, s := range slots {
		daySlots++
		if covered[i] {
			dayCov++
			day.predKWh += predicted[i] * loadEvalSlotHours / 1000
			day.actKWh += actual[i] * loadEvalSlotHours / 1000
		}
		if isNightSlot(&cfg.Rates, s, loc) {
			nightSlots++
			if covered[i] {
				nightCov++
				night.predKWh += predicted[i] * loadEvalSlotHours / 1000
				night.actKWh += actual[i] * loadEvalSlotHours / 1000
			}
		}
	}
	if nightSlots > 0 {
		night.coverage = float64(nightCov) / float64(nightSlots)
	}
	if daySlots > 0 {
		day.coverage = float64(dayCov) / float64(daySlots)
	}
	return night, day, nil
}

// isNightSlot reports whether slot t falls in the overnight cheap off-peak window
// we grid-charge to cover — the EV-Night window (01:00–05:00 local). We gate on
// cfg.Rates.IsOffPeak so the window tracks the configured rate schedule, and
// restrict to the pre-noon overnight hours [01:00,05:00) so an evening off-peak
// window can't leak into the "overnight" number.
func isNightSlot(rates *config.Rates, t time.Time, loc *time.Location) bool {
	if !rates.IsOffPeak(t) {
		return false
	}
	h := t.In(loc).Hour()
	return h >= loadEvalNightFromHour && h < loadEvalNightToHour
}

// resampleLoadEval buckets samples onto the 30-min slot grid: each slot is the
// mean of samples in [slot, slot+dur), clamped to [lo, hi]; gaps hold the last
// known value forward. It also reports, per slot, whether ≥1 real sample fell in
// that bin (coverage) — a held-forward value is NOT counted as covered. This is
// the mean-in-bin + hold-last + clamp recipe from cmd/backtest.go's resampleSlots,
// with the per-slot coverage bookkeeping the eval needs. Values stay in raw W.
func resampleLoadEval(samples []loadmodel.Sample, grid []time.Time, dur time.Duration, lo, hi float64) (vals []float64, covered []bool) {
	sort.Slice(samples, func(i, j int) bool { return samples[i].Time.Before(samples[j].Time) })
	vals = make([]float64, len(grid))
	covered = make([]bool, len(grid))
	si := 0
	var last float64
	haveLast := false
	for i, gt := range grid {
		end := gt.Add(dur)
		var sum float64
		var n int
		for si < len(samples) && samples[si].Time.Before(end) {
			s := samples[si]
			si++
			if s.Time.Before(gt) {
				continue // belongs to an earlier bin already emitted; drop it
			}
			v := math.Min(hi, math.Max(lo, s.Value)) // clamp counter-contamination spikes
			sum += v
			n++
		}
		switch {
		case n > 0:
			vals[i] = sum / float64(n)
			covered[i] = true
			last = vals[i]
			haveLast = true
		case haveLast:
			vals[i] = last
		default:
			vals[i] = 0
		}
	}
	return vals, covered
}

// evalDays runs evalNight across every day, flagging days whose night coverage
// falls below the threshold as skipped (excluded from aggregates).
func evalDays(ctx context.Context, src loadmodel.DataSource, cfg *config.Config, params loadmodel.Params, days []time.Time, lookback time.Duration) ([]dayRow, error) {
	rows := make([]dayRow, 0, len(days))
	for _, d := range days {
		night, day, err := evalNight(ctx, src, cfg, params, d, lookback)
		if err != nil {
			return nil, err
		}
		rows = append(rows, dayRow{
			date:    d,
			night:   night,
			day:     day,
			skipped: night.coverage < loadEvalMinNightCoverage,
		})
	}
	return rows, nil
}

// loadEvalAggregate holds the headline accuracy numbers over the evaluated days.
type loadEvalAggregate struct {
	n         int
	nightBias float64 // Σpred_night / Σact_night — THE headline number (target ≈ 1.0)
	nightMAE  float64 // kWh/night
	nightRMSE float64
	dayBias   float64
	dayMAE    float64
	dayRMSE   float64
}

func aggregateRows(rows []dayRow) loadEvalAggregate {
	var a loadEvalAggregate
	var sumPredN, sumActN, sumAbsN, sumSqN float64
	var sumPredD, sumActD, sumAbsD, sumSqD float64
	for _, r := range rows {
		if r.skipped {
			continue
		}
		a.n++
		sumPredN += r.night.predKWh
		sumActN += r.night.actKWh
		dN := r.night.predKWh - r.night.actKWh
		sumAbsN += math.Abs(dN)
		sumSqN += dN * dN

		sumPredD += r.day.predKWh
		sumActD += r.day.actKWh
		dD := r.day.predKWh - r.day.actKWh
		sumAbsD += math.Abs(dD)
		sumSqD += dD * dD
	}
	if a.n > 0 {
		if sumActN > 0 {
			a.nightBias = sumPredN / sumActN
		}
		a.nightMAE = sumAbsN / float64(a.n)
		a.nightRMSE = math.Sqrt(sumSqN / float64(a.n))
		if sumActD > 0 {
			a.dayBias = sumPredD / sumActD
		}
		a.dayMAE = sumAbsD / float64(a.n)
		a.dayRMSE = math.Sqrt(sumSqD / float64(a.n))
	}
	return a
}

func printLoadEvalSingle(w io.Writer, params loadmodel.Params, days []time.Time, lookback time.Duration, rows []dayRow) {
	// Console output is best-effort — swallow write errors (matches the repo's
	// fmt.Printf CLI idiom while keeping an io.Writer for output redirection).
	pf := func(format string, a ...any) { _, _ = fmt.Fprintf(w, format, a...) }
	pl := func(a ...any) { _, _ = fmt.Fprintln(w, a...) }

	pf("loadeval: %d days (%s → %s), lookback %.0fd, bucket_half_life=%.1fd\n",
		len(days), days[0].Format("2006-01-02"), days[len(days)-1].Format("2006-01-02"),
		lookback.Hours()/24, params.BucketHalfLifeDays)
	pl("Walk-forward: each day trained ONLY on prior data; predicted vs actual measured load.")
	pl("pred/act kWh are summed over COVERED slots only (apples-to-apples; NOT whole-night energy).")
	pl()

	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	tf := func(format string, a ...any) { _, _ = fmt.Fprintf(tw, format, a...) }
	_, _ = fmt.Fprintln(tw, "date\tpred_night\tact_night\tratio\tpred_day\tact_day\tratio\tnight_cov")
	for _, r := range rows {
		flag := ""
		if r.skipped {
			flag = "  (skipped: night coverage < 0.70)"
		}
		tf("%s\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.0f%%%s\n",
			r.date.Format("2006-01-02"),
			r.night.predKWh, r.night.actKWh, r.night.ratio(),
			r.day.predKWh, r.day.actKWh, r.day.ratio(),
			r.night.coverage*100, flag)
	}
	_ = tw.Flush()

	agg := aggregateRows(rows)
	pf("\nAGGREGATE (%d of %d days; low-coverage days skipped):\n", agg.n, len(rows))
	if agg.n == 0 {
		pl("  no days met the coverage floor — nothing to aggregate.")
		return
	}
	pf("  overnight bias  %.3f   MAE %.2f kWh/night   RMSE %.2f   (target ≈ 1.00; pre-fix ≈ 1.87)\n",
		agg.nightBias, agg.nightMAE, agg.nightRMSE)
	pf("  full-day  bias  %.3f   MAE %.2f kWh/day     RMSE %.2f\n",
		agg.dayBias, agg.dayMAE, agg.dayRMSE)
}

func runLoadEvalSweep(ctx context.Context, w io.Writer, src loadmodel.DataSource, cfg *config.Config, days []time.Time, lookback time.Duration, hls []float64) error {
	pf := func(format string, a ...any) { _, _ = fmt.Fprintf(w, format, a...) }

	pf("loadeval SWEEP: %d days (%s → %s), lookback %.0fd, bucket_half_life ∈ %v\n\n",
		len(days), days[0].Format("2006-01-02"), days[len(days)-1].Format("2006-01-02"),
		lookback.Hours()/24, hls)

	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "bucket_half_life\tovernight_bias\tovernight_MAE\tday_bias\tdays")

	var results []sweepResult
	for _, hl := range hls {
		params := loadEvalParams(cfg, hl)
		rows, err := evalDays(ctx, src, cfg, params, days, lookback)
		if err != nil {
			return err
		}
		agg := aggregateRows(rows)
		results = append(results, sweepResult{hl: hl, agg: agg})
		_, _ = fmt.Fprintf(tw, "%.1f\t%.3f\t%.2f\t%.3f\t%d\n", hl, agg.nightBias, agg.nightMAE, agg.dayBias, agg.n)
	}
	_ = tw.Flush()

	pf("\n(pred/act kWh are summed over COVERED slots only — apples-to-apples, not whole-night energy.)\n")

	best, ok := recommendSweep(results)
	if ok {
		pf("recommend: bucket_half_life_days=%.1f (overnight_bias %.3f, MAE %.2f kWh/night)\n",
			best.hl, best.agg.nightBias, best.agg.nightMAE)
	} else {
		pf("no usable days across any candidate (all below the coverage floor) — cannot recommend a bucket_half_life.\n")
	}
	return nil
}

// sweepResult pairs a swept bucket half-life with its aggregate accuracy.
type sweepResult struct {
	hl  float64
	agg loadEvalAggregate
}

// recommendSweep picks the sweep candidate whose overnight bias is closest to 1.0
// (ties broken by lower MAE), considering ONLY candidates that produced at least
// one usable day (agg.n > 0). An n==0 aggregate has a fabricated zero bias/MAE and
// must never be recommended; when NO candidate qualifies, ok is false and the
// caller prints no recommendation.
func recommendSweep(results []sweepResult) (sweepResult, bool) {
	var usable []sweepResult
	for _, r := range results {
		if r.agg.n > 0 {
			usable = append(usable, r)
		}
	}
	return pickBestSweep(usable, func(r sweepResult) (float64, float64) {
		return math.Abs(r.agg.nightBias - 1), r.agg.nightMAE
	})
}

// pickBestSweep chooses the candidate minimizing the primary key, breaking ties
// on the secondary key. It does NOT itself filter unusable candidates — callers
// (see recommendSweep) must pre-filter (e.g. drop n==0 aggregates) before calling.
// Returns ok=false when cands is empty.
func pickBestSweep[T any](cands []T, key func(T) (primary, secondary float64)) (T, bool) {
	var best T
	var bestP, bestS float64
	found := false
	for _, c := range cands {
		p, s := key(c)
		if !found || p < bestP || (p == bestP && s < bestS) {
			best, bestP, bestS, found = c, p, s, true
		}
	}
	return best, found
}

func parseSweep(s string) ([]float64, error) {
	var out []float64
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		v, err := strconv.ParseFloat(part, 64)
		if err != nil {
			return nil, fmt.Errorf("parse --sweep value %q: %w", part, err)
		}
		if v <= 0 {
			return nil, fmt.Errorf("--sweep value must be > 0, got %v", v)
		}
		out = append(out, v)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("--sweep is empty")
	}
	return out, nil
}

func init() {
	loadEvalCmd.Flags().StringVar(&loadEvalFrom, "from", "", "explicit start date YYYY-MM-DD (default: --days ago)")
	loadEvalCmd.Flags().IntVar(&loadEvalDays, "days", 14, "number of past complete days (up to yesterday) to evaluate")
	loadEvalCmd.Flags().Float64Var(&loadEvalBucketHL, "bucket-half-life", 0, "override bucket half-life (days) for a single run (0 = use config)")
	loadEvalCmd.Flags().StringVar(&loadEvalSweep, "sweep", "", `comma-separated bucket half-lives to sweep, e.g. "5,7,10,14,21" (overrides --bucket-half-life)`)
	rootCmd.AddCommand(loadEvalCmd)
}
