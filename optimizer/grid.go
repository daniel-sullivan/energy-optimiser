package optimizer

import (
	"fmt"
	"math"
	"time"

	"energy-optimiser/config"
)

// Grid is a telescoping slot grid: fine SlotDuration slots from now to the near
// horizon, then coarse FarSlotDuration slots out to the planning horizon.
// Start[i] is slot i's start; Hours[i] is its width in hours. It is the single
// source of slot geometry — hub, serve, backtest, and the solver all consume it
// and never re-derive widths.
type Grid struct {
	Start []time.Time
	Hours []float64
}

// Len returns the number of slots.
func (g Grid) Len() int { return len(g.Start) }

// End returns slot i's end time.
func (g Grid) End(i int) time.Time {
	return g.Start[i].Add(hoursToDuration(g.Hours[i]))
}

// hoursToDuration converts an exact slot width (0.5, 1.0, …) to a Duration. Grid
// widths are whole minutes, so the ×60 round-trip is exact.
func hoursToDuration(h float64) time.Duration {
	return time.Duration(math.Round(h*60)) * time.Minute
}

// BuildGrid constructs the telescoping slot grid for a solve anchored at now.
//
// now is floored to SlotDuration so every caller (hub tick, PrepareInput,
// backtest) agrees on the grid. The fine (SlotDuration) region runs to
// NearHorizon; the coarse (FarSlotDuration) region runs from there to
// PlanningHorizon.
//
// Backward compatibility: if NearHorizon <= 0, NearHorizon >= PlanningHorizon, or
// FarSlotDuration <= 0, the whole horizon uses SlotDuration (uniform grid = the
// pre-telescoping behaviour).
//
// BuildGrid panics if a coarse slot straddles a tariff-rate change or off-peak
// boundary: that means FarSlotDuration is misaligned with the off-peak windows,
// a build-time configuration error the solver's per-slot tariff assumption
// cannot tolerate.
func BuildGrid(now time.Time, cfg *config.Config) Grid {
	slotDur := cfg.Service.SlotDuration.Duration
	planning := cfg.Service.PlanningHorizon.Duration
	near := cfg.Service.NearHorizon.Duration
	far := cfg.Service.FarSlotDuration.Duration

	now = now.Truncate(slotDur)
	planEnd := now.Add(planning)

	var g Grid
	appendFine := func(from, to time.Time) {
		for t := from; t.Before(to); t = t.Add(slotDur) {
			g.Start = append(g.Start, t)
			g.Hours = append(g.Hours, slotDur.Hours())
		}
	}

	telescoping := near > 0 && near < planning && far > 0
	if !telescoping {
		appendFine(now, planEnd)
		return g
	}

	// Fine region: SlotDuration slots to the near horizon, extended forward to the
	// next whole LOCAL hour so the coarse slots begin exactly on the hour and never
	// straddle a tariff-window edge (C1 boundary snap).
	fineEnd := nextWholeHour(now.Add(near), cfg.Location())
	appendFine(now, fineEnd)

	// Coarse region: FarSlotDuration slots to the planning horizon. Slots are
	// hour-aligned, so each carries a single tariff rate and off-peak flag.
	for t := fineEnd; t.Before(planEnd); t = t.Add(far) {
		end := t.Add(far)
		if r0, r1 := cfg.Rates.RateAt(t), cfg.Rates.RateAt(end.Add(-time.Minute)); r0 != r1 {
			panic(fmt.Sprintf("BuildGrid: coarse slot [%s, %s) straddles a tariff-rate change (¥%.2f→¥%.2f); far_slot_duration is misaligned with the off-peak windows",
				t.Format(time.RFC3339), end.Format(time.RFC3339), r0, r1))
		}
		if o0, o1 := cfg.Rates.IsOffPeak(t), cfg.Rates.IsOffPeak(end.Add(-time.Minute)); o0 != o1 {
			panic(fmt.Sprintf("BuildGrid: coarse slot [%s, %s) straddles an off-peak boundary; far_slot_duration is misaligned with the off-peak windows",
				t.Format(time.RFC3339), end.Format(time.RFC3339)))
		}
		g.Start = append(g.Start, t)
		g.Hours = append(g.Hours, far.Hours())
	}
	return g
}

// nextWholeHour returns t rounded up to the next whole hour in loc (Asia/Tokyo by
// default, which has no DST). A time already on the hour is returned unchanged.
func nextWholeHour(t time.Time, loc *time.Location) time.Time {
	if loc != nil {
		t = t.In(loc)
	}
	if t.Minute() == 0 && t.Second() == 0 && t.Nanosecond() == 0 {
		return t
	}
	return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, t.Location()).Add(time.Hour)
}
