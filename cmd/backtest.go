package cmd

import (
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/spf13/cobra"

	"energy-optimiser/config"
	"energy-optimiser/hub"
	"energy-optimiser/influx"
	"energy-optimiser/optimizer"
)

var (
	backtestFrom string
	backtestDays int
)

// backtestCmd replays historical actuals through the optimizer to inspect when
// it decides to grid-charge (the low-SoC -> mains fallback) — a tuning aid for
// the SOC penalty scale and blip cost. It is a PERFECT-FORESIGHT, closed-loop
// simulation: the horizon is filled with actual future load/PV, and the
// simulated SoC follows the solver's own planned battery flow.
var backtestCmd = &cobra.Command{
	Use:   "backtest",
	Short: "Replay historical load/PV/SoC through the optimizer to inspect grid-charge decisions",
	RunE: func(cmd *cobra.Command, args []string) error {
		cfg, err := config.Parse(cfgFile)
		if err != nil {
			return err
		}
		loc := cfg.Location()

		var from time.Time
		if backtestFrom != "" {
			from, err = time.ParseInLocation("2006-01-02", backtestFrom, loc)
			if err != nil {
				return fmt.Errorf("parse --from: %w", err)
			}
		} else {
			from = time.Now().In(loc).AddDate(0, 0, -backtestDays)
		}
		to := from.AddDate(0, 0, backtestDays)
		// Never run past now: the horizon lookahead would query future timestamps
		// with no VM data, which resampleSlots carry-forwards into garbage.
		nowT := time.Now().In(loc)
		if to.After(nowT) {
			to = nowT
		}

		db, err := influx.New(cfg.InfluxDB)
		if err != nil {
			return err
		}
		defer func() { _ = db.Close() }()

		ctx, cancel := context.WithTimeout(cmd.Context(), 5*time.Minute)
		defer cancel()

		slotMins := int(cfg.Service.SlotDuration.Minutes())
		slotDur := time.Duration(slotMins) * time.Minute
		horizonSlots := int(cfg.Service.PlanningHorizon.Minutes()) / slotMins

		// Pull actuals over the range plus one horizon of lookahead (foresight),
		// clamped to now (no future data exists to carry-forward).
		qTo := to.Add(cfg.Service.PlanningHorizon.Duration)
		if qTo.After(nowT) {
			qTo = nowT
		}
		ent := cfg.HomeAssistant.Entities
		loadS, err := db.QueryPower(ctx, ent.LoadPower, from, qTo)
		if err != nil {
			return fmt.Errorf("load: %w", err)
		}
		pvS, err := db.QueryPower(ctx, ent.PVPower, from, qTo)
		if err != nil {
			return fmt.Errorf("pv: %w", err)
		}
		socS, err := db.QueryPercentage(ctx, ent.BatterySOC, from, qTo)
		if err != nil {
			return fmt.Errorf("soc: %w", err)
		}

		start := from.Truncate(slotDur)
		nSlots := int(qTo.Sub(start)/slotDur) + 1
		grid := make([]time.Time, nSlots)
		for i := range grid {
			grid[i] = start.Add(time.Duration(i) * slotDur)
		}

		loadW := resampleSlots(loadS, grid, slotDur, 1.0, 0, 25000)      // W
		pvKW := resampleSlots(pvS, grid, slotDur, 1.0/1000.0, 0, 25000)  // kW
		socFrac := resampleSlots(socS, grid, slotDur, 1.0/100.0, 0, 100) // fraction; SoC band-filter

		nTicks := nSlots - horizonSlots
		if nTicks <= 0 {
			return fmt.Errorf("range too short: need > %dh of data", horizonSlots*slotMins/60)
		}

		etaC := math.Sqrt(cfg.Battery.Efficiency)
		etaD := math.Sqrt(cfg.Battery.Efficiency)
		capKWh := cfg.Battery.CapacityKWh
		dh := float64(slotMins) / 60.0

		simSoC := socFrac[0]
		if simSoC <= 0 || simSoC > cfg.Battery.SOCMax {
			simSoC = 0.5 // glitched/absent start reading → neutral default
		}

		type event struct {
			t      time.Time
			soc    float64
			start  bool
			charge float64
			rate   float64
		}
		var log []event
		var totalGridKWh float64
		numEntries := 0
		entriesByDay := map[string]int{}
		minSoC, maxSoC := simSoC, simSoC
		prev := false

		for i := range nTicks {
			t0 := grid[i]
			// Telescope the uniform actuals window onto this tick's production grid
			// (BuildGrid): fine near slots take one uniform slot, coarse far slots
			// average the uniform slots they span (mean power). This mirrors the
			// shipped hub path so the backtest exercises the same code.
			tg := optimizer.BuildGrid(t0, cfg)
			pvSlot := backtestSolarFill(tg, t0, telescopeUniform(pvKW[i:], tg, slotMins))
			loadSlot := telescopeUniform(loadW[i:], tg, slotMins)
			in := optimizer.PrepareInput(t0, cfg, pvSlot, loadSlot, simSoC)
			sched, err := optimizer.Solve(in)
			if err != nil {
				return fmt.Errorf("solve @ %s: %w", t0.Format(time.RFC3339), err)
			}
			s0 := sched.Slots[0]

			if s0.GridCharge && !prev {
				log = append(log, event{t0, simSoC, true, s0.BatteryFlowKW, in.Rates[0]})
				entriesByDay[t0.In(loc).Format("2006-01-02")]++
				numEntries++
			} else if !s0.GridCharge && prev {
				log = append(log, event{t0, simSoC, false, 0, in.Rates[0]})
			}
			prev = s0.GridCharge

			charge := math.Max(0, s0.BatteryFlowKW)
			discharge := math.Max(0, -s0.BatteryFlowKW)
			simSoC += (etaC*charge - discharge/etaD) * dh / capKWh
			simSoC = math.Min(cfg.Battery.SOCMax, math.Max(cfg.Battery.SOCMin, simSoC))
			if s0.GridCharge {
				totalGridKWh += charge * dh
			}
			minSoC = math.Min(minSoC, simSoC)
			maxSoC = math.Max(maxSoC, simSoC)
		}

		fmt.Printf("Backtest %s → %s  (%d ticks @ %s slots, %dh horizon, perfect foresight)\n",
			from.Format("2006-01-02"), to.Format("2006-01-02"), nTicks, slotDur, horizonSlots*slotMins/60)
		fmt.Printf("Battery %.1f kWh, η=%.2f, soc_risk_weight=%.1f, blip_cost=¥%.1f\n",
			capKWh, cfg.Battery.Efficiency, cfg.Optimizer.SOCRiskWeight, cfg.Optimizer.BlipCost)
		fmt.Printf("Sim SoC: start %.0f%%, range %.0f%%–%.0f%%\n\n", socFrac[0]*100, minSoC*100, maxSoC*100)

		if len(log) == 0 {
			fmt.Println("No grid-charge decisions in this window (solar+battery carried the load).")
		} else {
			fmt.Println("Grid-charge decisions (low-SoC → mains fallback):")
			for _, e := range log {
				if e.start {
					fmt.Printf("  %s  ENTER bypass  SoC %4.0f%%  rate ¥%5.2f  charge %.1f kW\n",
						e.t.In(loc).Format("01-02 15:04"), e.soc*100, e.rate, e.charge)
				} else {
					fmt.Printf("  %s  exit  bypass   SoC %4.0f%%\n",
						e.t.In(loc).Format("01-02 15:04"), e.soc*100)
				}
			}
		}

		days := make([]string, 0, len(entriesByDay))
		for d := range entriesByDay {
			days = append(days, d)
		}
		sort.Strings(days)
		fmt.Printf("\nTotal grid-charge: %.1f kWh across %d signal blocks.\n", totalGridKWh, numEntries)
		for _, d := range days {
			if entriesByDay[d] > 1 {
				fmt.Printf("  · %s: %d grid-charge blocks (stateless per-tick re-solve fragments the signal;\n"+
					"      Phase-1 episode state + actuator bypass-hold collapse these to ≤1 real bypass/window)\n", d, entriesByDay[d])
			}
		}
		// Solar-only oracle: per day, starting from that day's actual morning SoC,
		// integrate net solar with ZERO grid and check if SoC reaches SOCMax by
		// midday. Prints PV/load magnitudes so a low hit-rate can be diagnosed as
		// data-scale vs assumption. Daniel expects ~50% of days to reach it.
		{
			fmt.Println("\nSolar-only oracle (per-day, start = day's first actual SoC, ZERO grid):")
			var total, reached int
			i := 0
			for i < nSlots {
				dayStr := grid[i].In(loc).Format("2006-01-02")
				soc := math.Min(cfg.Battery.SOCMax, math.Max(cfg.Battery.SOCMin, socFrac[i]))
				start := soc
				var pvKWh, loadKWh, peakPV float64
				hit := false
				j := i
				for j < nSlots && grid[j].In(loc).Format("2006-01-02") == dayStr {
					t := grid[j].In(loc)
					pvKWh += pvKW[j] * dh
					loadKWh += loadW[j] / 1000.0 * dh
					peakPV = math.Max(peakPV, pvKW[j])
					net := pvKW[j] - loadW[j]/1000.0
					charge, discharge := math.Max(0, net), math.Max(0, -net)
					soc += (etaC*charge - discharge/etaD) * dh / capKWh
					soc = math.Min(cfg.Battery.SOCMax, math.Max(cfg.Battery.SOCMin, soc))
					if t.Hour()*60+t.Minute() <= 12*60 && soc >= cfg.Battery.SOCMax-0.005 {
						hit = true
					}
					j++
				}
				if j-i >= 40 { // full days only (48 slots/day); skip partial ends
					total++
					mark := " "
					if hit {
						reached++
						mark = "✓"
					}
					fmt.Printf("  %s %s start %2.0f%%  PV %5.1f kWh  peak %4.1f kW  load %5.1f kWh\n",
						dayStr, mark, start*100, pvKWh, peakPV, loadKWh)
				}
				i = j
			}
			if total > 0 {
				fmt.Printf("→ %d/%d full days (%.0f%%) reach %.0f%% by midday solar-only (expect ~50%%).\n",
					reached, total, 100*float64(reached)/float64(total), cfg.Battery.SOCMax*100)
			}
		}
		return nil
	},
}

// resampleSlots buckets samples onto the slot grid by averaging, carrying the
// last known value forward across gaps. Samples outside [lo, hi] (raw units,
// before scale) are rejected — the validated SoC-smoothing recipe (band-reject +
// hold-last that reproduces a smoothed-SoC HA entity). Values ×scale.
func resampleSlots(samples []influx.Sample, grid []time.Time, slotDur time.Duration, scale, lo, hi float64) []float64 {
	sort.Slice(samples, func(i, j int) bool { return samples[i].Time.Before(samples[j].Time) })
	out := make([]float64, len(grid))
	si := 0
	var last float64
	haveLast := false
	for i, gt := range grid {
		end := gt.Add(slotDur)
		var sum float64
		var n int
		for si < len(samples) && samples[si].Time.Before(end) {
			s := samples[si]
			si++
			if s.Value < lo || s.Value > hi {
				continue // reject out-of-range garbage (spikes / counter contamination)
			}
			if !s.Time.Before(gt) {
				sum += s.Value
				n++
			}
		}
		switch {
		case n > 0:
			out[i] = (sum / float64(n)) * scale
			last = out[i]
			haveLast = true
		case haveLast:
			out[i] = last
		default:
			out[i] = 0
		}
	}
	return out
}

// backtestSolarFill routes the telescoped measured-PV actuals through the SAME
// hub.BlendSolar path production uses, so the offline replay exercises the shipped
// fill. The backtest has perfect foresight — actuals cover the whole horizon — so
// coverage extends to the grid end and no learned model/weather is supplied: the
// learned far-fill and crossfade stay dormant and the actuals pass through
// unchanged. (Production instead feeds Solcast as the near-term source and the
// learned PV model × GTI beyond its ~72h coverage.)
func backtestSolarFill(g optimizer.Grid, now time.Time, pvNear []float64) []float64 {
	if g.Len() == 0 {
		return pvNear
	}
	hasNear := make([]bool, len(pvNear))
	for i := range hasNear {
		hasNear[i] = true
	}
	coverageEnd := g.End(g.Len() - 1)
	return hub.BlendSolar(g, now, pvNear, hasNear, coverageEnd, nil, nil)
}

// telescopeUniform maps a uniform-slot power window (slotMins-wide mean-power
// samples) onto a telescoping grid: each grid slot averages the
// round(width/slotMins) uniform slots it spans (mean power, matching the hub's
// Solcast averaging). Grid slot boundaries always align to the uniform grid, so
// the per-slot counts are exact; a window exhausted at the tail contributes
// nothing to the trailing slots.
func telescopeUniform(uniform []float64, g optimizer.Grid, slotMins int) []float64 {
	out := make([]float64, g.Len())
	u := 0
	for k := range out {
		n := int(math.Round(g.Hours[k] * 60 / float64(slotMins)))
		if n < 1 {
			n = 1
		}
		var sum float64
		var c int
		for j := 0; j < n; j++ {
			if u < len(uniform) {
				sum += uniform[u]
				c++
			}
			u++
		}
		if c > 0 {
			out[k] = sum / float64(c)
		}
	}
	return out
}

func init() {
	backtestCmd.Flags().StringVar(&backtestFrom, "from", "", "start date YYYY-MM-DD (default: --days ago)")
	backtestCmd.Flags().IntVar(&backtestDays, "days", 3, "number of days to backtest")
	rootCmd.AddCommand(backtestCmd)
}
