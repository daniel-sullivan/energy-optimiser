package hub

import (
	"context"
	"sort"
	"time"

	"energy-optimiser/forecast"
	"energy-optimiser/influx"
	"energy-optimiser/optimizer"
	"energy-optimiser/pvmodel"
)

// Hybrid solar-fill tuning. The near horizon is Solcast truth; beyond Solcast's
// coverage the horizon is filled with the learned PV model × Open-Meteo GTI, so
// the solver never sees an all-zero far horizon (a degenerate, solve-hard case).
const (
	// solarCrossfadeWindow is the span before Solcast coverage-end over which the
	// fill blends linearly from Solcast to the learned model, so there is no level
	// step at the seam for the solver to trade against.
	solarCrossfadeWindow = 6 * time.Hour

	// solarHaircutFullLead is the lead time up to which the learned prediction is
	// trusted at face value; solarHaircutFarLead/Value is where it bottoms out.
	// Between them the haircut ramps linearly, clamped to [FarValue, 1.0].
	solarHaircutFullLead = 48 * time.Hour
	solarHaircutFarLead  = 168 * time.Hour
	solarHaircutFarValue = 0.8
)

// PVPredictor is the subset of *pvmodel.Model the far-horizon fill depends on.
// A small interface keeps the fill testable with a deterministic stub and lets
// the hub nil-guard a cold-started model cleanly.
type PVPredictor interface {
	PredictKW(t time.Time, gti map[string]float64) float64
}

// pvHistorySource adapts the metrics client to pvmodel.PVHistory, binding the PV
// power entity id. The model never depends on the influx client type directly.
type pvHistorySource struct {
	client   *influx.Client
	entityID string
}

func (s pvHistorySource) PVPower(ctx context.Context, from, to time.Time) ([]pvmodel.Sample, error) {
	raw, err := s.client.QueryPower(ctx, s.entityID, from, to)
	if err != nil {
		return nil, err
	}
	out := make([]pvmodel.Sample, len(raw))
	for i, v := range raw {
		out[i] = pvmodel.Sample{Time: v.Time, Value: v.Value}
	}
	return out, nil
}

// fillSolarSlots produces the per-slot solar kW for the whole grid: Solcast where
// it covers, the learned model × interpolated GTI (haircut-scaled) beyond, with a
// 6h crossfade at the seam. It returns nil only when there is nothing to work with
// (no Solcast AND no learned model/weather), preserving the pre-fill contract.
func fillSolarSlots(grid optimizer.Grid, now time.Time, solar *forecast.SolarForecast, weather *forecast.WeatherForecast, model PVPredictor) []float64 {
	if solar == nil && (weather == nil || model == nil) {
		return nil
	}
	n := grid.Len()
	near := make([]float64, n)
	hasNear := make([]bool, n)
	for i := range near {
		near[i], hasNear[i] = averageSolcast(solar, grid.Start[i], grid.End(i))
	}
	return BlendSolar(grid, now, near, hasNear, solcastCoverageEnd(solar, now), weather, model)
}

// BlendSolar merges a grid-aligned near-term solar vector (Solcast in production,
// measured actuals in the backtest) with the learned PV model × interpolated
// Open-Meteo GTI beyond coverageEnd. Within coverage the near-term value is truth;
// beyond it the learned prediction (lead-time haircut applied) takes over; across
// the last solarCrossfadeWindow of coverage the two blend linearly (w: 0→1). With
// no learned model or weather the learned component is 0, so the result collapses
// to near-term-only (Solcast within coverage, 0 beyond) — the pre-fill behaviour.
func BlendSolar(grid optimizer.Grid, now time.Time, near []float64, hasNear []bool, coverageEnd time.Time, weather *forecast.WeatherForecast, model PVPredictor) []float64 {
	n := grid.Len()
	out := make([]float64, n)
	learnedAvailable := model != nil && weather != nil && len(weather.Points) > 0
	crossStart := coverageEnd.Add(-solarCrossfadeWindow)

	for i := range out {
		ts := grid.Start[i]
		nearKW := 0.0
		if i < len(near) {
			nearKW = near[i]
		}
		has := i < len(hasNear) && hasNear[i]

		learnedKW := 0.0
		if learnedAvailable {
			if gti := interpolateGTI(weather.Points, ts); len(gti) > 0 {
				learnedKW = model.PredictKW(ts, gti) * solarHaircut(ts.Sub(now))
			}
		}

		switch {
		case !ts.Before(coverageEnd):
			// Beyond Solcast coverage — learned only (0 when unavailable).
			out[i] = learnedKW
		case ts.Before(crossStart):
			// Well within coverage — near-term truth. In a mid-forecast Solcast gap
			// (no covering point) fall back to the learned value rather than 0, so a
			// coverage hole doesn't read as a false zero-solar notch.
			switch {
			case has:
				out[i] = nearKW
			case learnedAvailable:
				out[i] = learnedKW
			}
		default:
			// Seam window [crossStart, coverageEnd): linear crossfade.
			base := 0.0
			if has {
				base = nearKW
			}
			if learnedAvailable {
				w := crossfadeWeight(ts, coverageEnd)
				out[i] = (1-w)*base + w*learnedKW
			} else {
				out[i] = base
			}
		}
	}
	return out
}

// averageSolcast returns the mean Solcast estimate (kW) over [start, end) and
// whether any point covered the slot — the pre-fill per-slot averaging, extracted
// so both the fill and the residual monitor share it.
func averageSolcast(solar *forecast.SolarForecast, start, end time.Time) (float64, bool) {
	if solar == nil {
		return 0, false
	}
	var sum float64
	var count int
	for _, p := range solar.Points {
		if !p.Time.Before(start) && p.Time.Before(end) {
			sum += p.EstimateKW
			count++
		}
	}
	if count == 0 {
		return 0, false
	}
	return sum / float64(count), true
}

// solcastCoverageEnd is the instant beyond which no Solcast data exists — the last
// point's period-start. With no Solcast the whole horizon is "beyond coverage"
// (coverageEnd == now), so the learned model fills everything.
func solcastCoverageEnd(solar *forecast.SolarForecast, now time.Time) time.Time {
	if solar == nil || len(solar.Points) == 0 {
		return now
	}
	return solar.Points[len(solar.Points)-1].Time
}

// solarHaircut discounts the learned prediction by lead time: 1.0 out to
// solarHaircutFullLead, ramping linearly down to solarHaircutFarValue at
// solarHaircutFarLead, clamped to [FarValue, 1.0]. Applied only at prediction
// time — never in the model's calibration.
func solarHaircut(lead time.Duration) float64 {
	if lead <= solarHaircutFullLead {
		return 1.0
	}
	if lead >= solarHaircutFarLead {
		return solarHaircutFarValue
	}
	frac := (lead - solarHaircutFullLead).Hours() / (solarHaircutFarLead - solarHaircutFullLead).Hours()
	return 1.0 + frac*(solarHaircutFarValue-1.0)
}

// crossfadeWeight is the learned-model weight (0→1) for a slot in the seam window
// [coverageEnd-solarCrossfadeWindow, coverageEnd), clamped to [0, 1].
func crossfadeWeight(ts, coverageEnd time.Time) float64 {
	start := coverageEnd.Add(-solarCrossfadeWindow)
	w := ts.Sub(start).Seconds() / solarCrossfadeWindow.Seconds()
	return min(1, max(0, w))
}

// interpolateGTI linearly interpolates the per-site GTI map to instant t from the
// hourly, chronologically-sorted GTIPoints, bracketing t between the two adjacent
// points (do NOT step-repeat). Before the first / after the last point it clamps
// to the nearest (no extrapolation). Returns nil when there are no points.
func interpolateGTI(points []forecast.GTIPoint, t time.Time) map[string]float64 {
	if len(points) == 0 {
		return nil
	}
	if !t.After(points[0].Time) {
		return points[0].GTI
	}
	last := points[len(points)-1]
	if !t.Before(last.Time) {
		return last.GTI
	}
	// First point strictly after t; the bracket is [idx-1, idx].
	idx := sort.Search(len(points), func(i int) bool { return points[i].Time.After(t) })
	lo, hi := points[idx-1], points[idx]
	span := hi.Time.Sub(lo.Time).Seconds()
	if span <= 0 {
		return lo.GTI
	}
	frac := t.Sub(lo.Time).Seconds() / span
	out := make(map[string]float64, len(lo.GTI))
	for site, loV := range lo.GTI {
		hiV, ok := hi.GTI[site]
		if !ok {
			hiV = loV
		}
		out[site] = loV + frac*(hiV-loV)
	}
	for site, hiV := range hi.GTI {
		if _, seen := out[site]; !seen {
			out[site] = hiV
		}
	}
	return out
}
