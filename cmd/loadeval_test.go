package cmd

import (
	"context"
	"math"
	"testing"
	"time"

	"energy-optimiser/config"
	"energy-optimiser/loadmodel"
)

// stepSource is an in-memory loadmodel.DataSource for offline walk-forward tests.
// It emits one synthetic sample every 30 minutes across the queried [from, to)
// window (dense enough that every 30-min eval slot is covered). The value is
// `before` for timestamps < step and `after` from step onward — so a test can
// place a level step exactly at an evaluation-day boundary and prove the training
// cutoff is honored (no leakage).
type stepSource struct {
	before float64
	after  float64
	step   time.Time // zero → constant `before` everywhere
}

func (s stepSource) QueryPower(_ context.Context, _ string, from, to time.Time) ([]loadmodel.Sample, error) {
	var out []loadmodel.Sample
	for t := from; t.Before(to); t = t.Add(30 * time.Minute) {
		v := s.before
		if !s.step.IsZero() && !t.Before(s.step) {
			v = s.after
		}
		out = append(out, loadmodel.Sample{Time: t, Value: v})
	}
	return out, nil
}

// gapSource emits a constant sample every 30 minutes EXCEPT inside [gapFrom, gapTo),
// where it emits nothing — so the eval must hold the last value forward (synthetic,
// not covered) across that window. Used to prove bias/MAE ignore gap-filled slots.
type gapSource struct {
	value          float64
	gapFrom, gapTo time.Time
}

func (s gapSource) QueryPower(_ context.Context, _ string, from, to time.Time) ([]loadmodel.Sample, error) {
	var out []loadmodel.Sample
	for t := from; t.Before(to); t = t.Add(30 * time.Minute) {
		if !s.gapFrom.IsZero() && !t.Before(s.gapFrom) && t.Before(s.gapTo) {
			continue // no real sample here → forces hold-last gap-fill
		}
		out = append(out, loadmodel.Sample{Time: t, Value: s.value})
	}
	return out, nil
}

// evalTestConfig builds a minimal config for offline eval. The unexported loc
// fields on Config/Rates are left nil, so all time-of-day logic runs in UTC —
// tests use UTC instants throughout to match. The off-peak window is the
// overnight 01:00–05:00 EV-Night window.
func evalTestConfig() *config.Config {
	cfg := &config.Config{}
	cfg.HomeAssistant.Entities.LoadPower = "sensor.load_power"
	cfg.Weather.Latitude = 35.0 // northern hemisphere
	cfg.LoadModel.LookbackDays = 30
	cfg.LoadModel.RecencyHalfLifeDays = 3
	cfg.LoadModel.BucketHalfLifeDays = 10
	cfg.LoadModel.ConservativeMargin = 1.3
	cfg.Optimizer.ConfidenceThreshold = 0.3
	cfg.Rates.OffPeakRate = 10
	cfg.Rates.PeakRate = 30
	cfg.Rates.OffPeakWindows = []config.TimeWindow{
		{Start: config.TimeOfDay{Hour: 1}, End: config.TimeOfDay{Hour: 5}},
	}
	return cfg
}

func TestEvalNightConstantLoad(t *testing.T) {
	cfg := evalTestConfig()
	params := loadEvalParams(cfg, 0)
	src := stepSource{before: 1500, after: 1500} // constant everywhere

	// Evaluation day: a clean UTC midnight well after the lookback start.
	dStart := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	lookback := 30 * 24 * time.Hour

	night, day, err := evalNight(context.Background(), src, cfg, params, dStart, lookback)
	if err != nil {
		t.Fatalf("evalNight: %v", err)
	}

	// Overnight window is 01:00–05:00 = 4h = 8 slots at constant 1500 W:
	// 1500 W × 0.5 h × 8 / 1000 = 6.0 kWh predicted and actual.
	if math.Abs(night.actKWh-6.0) > 0.01 {
		t.Errorf("night actual kWh = %.3f, want ~6.0", night.actKWh)
	}
	if r := night.ratio(); math.Abs(r-1.0) > 0.05 {
		t.Errorf("night pred/act ratio = %.3f, want within 5%% of 1.0", r)
	}
	if math.Abs(day.actKWh-36.0) > 0.02 { // 1500 W × 0.5 h × 48 slots / 1000
		t.Errorf("day actual kWh = %.3f, want ~36.0", day.actKWh)
	}
	if r := day.ratio(); math.Abs(r-1.0) > 0.05 {
		t.Errorf("day pred/act ratio = %.3f, want within 5%% of 1.0", r)
	}
	if night.coverage != 1.0 {
		t.Errorf("night coverage = %.3f, want 1.0 (30-min samples cover every slot)", night.coverage)
	}
	if day.coverage != 1.0 {
		t.Errorf("day coverage = %.3f, want 1.0", day.coverage)
	}
}

func TestEvalNightWalkForwardNoLeakage(t *testing.T) {
	cfg := evalTestConfig()
	params := loadEvalParams(cfg, 0)

	dStart := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	lookback := 30 * 24 * time.Hour

	// Load STEPS UP from 1000 W to 3000 W exactly at the evaluation-day boundary.
	// Training covers [dStart-lookback, dStart) — entirely pre-step 1000 W — so a
	// leak-free predictor reflects the PRE-step level, NOT the day's actual 3000 W.
	src := stepSource{before: 1000, after: 3000, step: dStart}

	night, _, err := evalNight(context.Background(), src, cfg, params, dStart, lookback)
	if err != nil {
		t.Fatalf("evalNight: %v", err)
	}

	// Actual night at 3000 W: 3000 × 0.5 × 8 / 1000 = 12.0 kWh.
	if math.Abs(night.actKWh-12.0) > 0.02 {
		t.Errorf("night actual kWh = %.3f, want ~12.0 (post-step)", night.actKWh)
	}
	// Predicted night must reflect the PRE-step 1000 W level (~4.0 kWh), proving
	// the training cutoff excluded the step. If the cutoff leaked, the prediction
	// would be pulled toward the 3000 W post-step level.
	if math.Abs(night.predKWh-4.0) > 0.6 {
		t.Errorf("night predicted kWh = %.3f, want ~4.0 (pre-step level, no leakage)", night.predKWh)
	}
	if night.predKWh >= night.actKWh {
		t.Errorf("prediction (%.3f) should be well below actual (%.3f): leakage suspected",
			night.predKWh, night.actKWh)
	}
}

func TestResampleLoadEval(t *testing.T) {
	grid := []time.Time{
		time.Date(2026, 6, 1, 1, 0, 0, 0, time.UTC),  // slot 0: two in-bin samples -> mean
		time.Date(2026, 6, 1, 1, 30, 0, 0, time.UTC), // slot 1: no samples -> hold-last
		time.Date(2026, 6, 1, 2, 0, 0, 0, time.UTC),  // slot 2: one sample, above clamp -> clamped
	}
	samples := []loadmodel.Sample{
		{Time: time.Date(2026, 6, 1, 1, 5, 0, 0, time.UTC), Value: 1000},
		{Time: time.Date(2026, 6, 1, 1, 20, 0, 0, time.UTC), Value: 2000},
		// nothing in [01:30, 02:00)
		{Time: time.Date(2026, 6, 1, 2, 10, 0, 0, time.UTC), Value: 99999}, // clamps to 25000
	}

	vals, covered := resampleLoadEval(samples, grid, 30*time.Minute, 0, loadEvalClampHi)

	if math.Abs(vals[0]-1500) > 1e-9 { // mean of 1000, 2000
		t.Errorf("slot 0 = %.1f, want 1500 (mean-in-bin)", vals[0])
	}
	if !covered[0] {
		t.Errorf("slot 0 should be covered")
	}
	if math.Abs(vals[1]-1500) > 1e-9 { // hold-last from slot 0
		t.Errorf("slot 1 = %.1f, want 1500 (hold-last)", vals[1])
	}
	if covered[1] {
		t.Errorf("slot 1 should NOT be covered (gap-filled)")
	}
	if math.Abs(vals[2]-loadEvalClampHi) > 1e-9 { // 99999 clamped to 25000
		t.Errorf("slot 2 = %.1f, want %.1f (clamped)", vals[2], loadEvalClampHi)
	}
	if !covered[2] {
		t.Errorf("slot 2 should be covered")
	}
}

func TestResampleLoadEvalLeadingGap(t *testing.T) {
	// No samples at all before the first slot: default to 0, not covered.
	grid := []time.Time{time.Date(2026, 6, 1, 1, 0, 0, 0, time.UTC)}
	vals, covered := resampleLoadEval(nil, grid, 30*time.Minute, 0, loadEvalClampHi)
	if vals[0] != 0 {
		t.Errorf("leading gap = %.1f, want 0", vals[0])
	}
	if covered[0] {
		t.Errorf("leading gap should not be covered")
	}
}

// TestEvalNightCoveredOnly proves bias/MAE are computed over COVERED slots only:
// the 2 gap-filled (hold-last, synthetic) night slots must NOT contribute to the
// actual sum, and coverage must drop below 1.0. With a constant 2000 W source and
// a [02:00,03:00) gap, the 8-slot night window has 6 covered slots →
// 2000 W × 0.5 h × 6 / 1000 = 6.0 kWh (a bug summing all 8 held slots would give 8.0).
func TestEvalNightCoveredOnly(t *testing.T) {
	cfg := evalTestConfig()
	params := loadEvalParams(cfg, 0)

	dStart := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	lookback := 30 * 24 * time.Hour
	src := gapSource{
		value:   2000,
		gapFrom: time.Date(2026, 6, 1, 2, 0, 0, 0, time.UTC),
		gapTo:   time.Date(2026, 6, 1, 3, 0, 0, 0, time.UTC),
	}

	night, _, err := evalNight(context.Background(), src, cfg, params, dStart, lookback)
	if err != nil {
		t.Fatalf("evalNight: %v", err)
	}

	// 6 of 8 night slots carry a real sample → coverage 0.75, above the 0.70 floor.
	if math.Abs(night.coverage-0.75) > 1e-9 {
		t.Errorf("night coverage = %.3f, want 0.75 (2 of 8 slots gap-filled)", night.coverage)
	}
	// Covered-only actual: 6 covered slots, NOT all 8. The 2 hold-last slots
	// (which would push the sum to 8.0) must be excluded.
	if math.Abs(night.actKWh-6.0) > 0.02 {
		t.Errorf("night actual kWh = %.3f, want ~6.0 (covered slots only, not 8.0)", night.actKWh)
	}
	if night.actKWh > 7.0 {
		t.Errorf("night actual kWh = %.3f includes fabricated gap-fill (would be ~8.0)", night.actKWh)
	}
	// Predicted is summed over the SAME covered slots → apples-to-apples ratio ≈ 1.
	if r := night.ratio(); math.Abs(r-1.0) > 0.05 {
		t.Errorf("night pred/act ratio = %.3f, want within 5%% of 1.0 (same covered slots)", r)
	}
}

// TestRecommendSweepSkipsNoUsableDays proves an all-skipped (n==0) candidate is
// NEVER recommended — even when its fabricated (|0-1|, 0) = (1.0, 0.0) key would
// beat a real-but-poor candidate — and that with no usable candidate at all the
// runner reports no recommendation.
func TestRecommendSweepSkipsNoUsableDays(t *testing.T) {
	// hl=5 produced zero usable days; hl=7 is real but has a poor bias of 2.5
	// (key primary 1.5). The n==0 key (1.0) would win a naive min — it must not.
	results := []sweepResult{
		{hl: 5, agg: loadEvalAggregate{n: 0}},
		{hl: 7, agg: loadEvalAggregate{n: 4, nightBias: 2.5, nightMAE: 3.0}},
	}
	best, ok := recommendSweep(results)
	if !ok {
		t.Fatalf("recommendSweep: ok=false, want a real candidate recommended")
	}
	if best.hl != 7 {
		t.Errorf("recommended hl = %.1f, want 7 (n==0 candidate must be skipped)", best.hl)
	}

	// All candidates n==0 → nothing to recommend.
	allSkipped := []sweepResult{
		{hl: 5, agg: loadEvalAggregate{n: 0}},
		{hl: 7, agg: loadEvalAggregate{n: 0}},
	}
	if _, ok := recommendSweep(allSkipped); ok {
		t.Errorf("recommendSweep(all n==0): ok=true, want false (cannot recommend)")
	}

	// Sanity: among two real candidates the one closer to bias 1.0 wins.
	realVsReal := []sweepResult{
		{hl: 5, agg: loadEvalAggregate{n: 4, nightBias: 1.4, nightMAE: 2.0}},
		{hl: 7, agg: loadEvalAggregate{n: 4, nightBias: 1.05, nightMAE: 2.0}},
	}
	best, ok = recommendSweep(realVsReal)
	if !ok || best.hl != 7 {
		t.Errorf("recommendSweep(real vs real): got hl=%.1f ok=%v, want hl=7", best.hl, ok)
	}
}
