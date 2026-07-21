package hub

import (
	"math"
	"testing"
	"time"

	"energy-optimiser/forecast"
	"energy-optimiser/optimizer"
)

// stubPredictor mimics pvmodel: PredictKW = factor · ΣGTI/1000, so the learned
// component of the fill is exactly computable in the test.
type stubPredictor struct{ factor float64 }

func (s stubPredictor) PredictKW(_ time.Time, gti map[string]float64) float64 {
	var sum float64
	for _, v := range gti {
		sum += v
	}
	return s.factor * sum / 1000.0
}

// telescopingGrid builds a grid mirroring the production geometry: 30-min fine
// slots to fineHours, then 60-min coarse slots to planHours.
func telescopingGrid(now time.Time, fineHours, planHours int) optimizer.Grid {
	var g optimizer.Grid
	for t := 0; t < fineHours*60; t += 30 {
		g.Start = append(g.Start, now.Add(time.Duration(t)*time.Minute))
		g.Hours = append(g.Hours, 0.5)
	}
	for t := fineHours * 60; t < planHours*60; t += 60 {
		g.Start = append(g.Start, now.Add(time.Duration(t)*time.Minute))
		g.Hours = append(g.Hours, 1.0)
	}
	return g
}

// gtiValue is a deterministic per-hour GTI shape (W/m²) so interpolation is exact.
func gtiValue(hour int) float64 { return 100 + 10*float64(hour%24) }

// syntheticWeather builds hourly GTIPoints over [now, now+hours] for one site.
func syntheticWeather(now time.Time, hours int) *forecast.WeatherForecast {
	pts := make([]forecast.GTIPoint, 0, hours+1)
	for h := 0; h <= hours; h++ {
		pts = append(pts, forecast.GTIPoint{
			Time: now.Add(time.Duration(h) * time.Hour),
			GTI:  map[string]float64{"roof": gtiValue(h)},
		})
	}
	return &forecast.WeatherForecast{FetchedAt: now, Points: pts}
}

// syntheticSolcast builds 30-min Solcast points from now out to coverageHours.
func syntheticSolcast(now time.Time, coverageHours int, kw float64) *forecast.SolarForecast {
	var pts []forecast.SolarPoint
	for t := 0; t < coverageHours*60; t += 30 {
		pts = append(pts, forecast.SolarPoint{Time: now.Add(time.Duration(t) * time.Minute), EstimateKW: kw})
	}
	return &forecast.SolarForecast{FetchedAt: now, Points: pts}
}

func TestFillFarHorizonLearned(t *testing.T) {
	now := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	const coverageH, planH = 72, 168
	grid := telescopingGrid(now, coverageH, planH)
	solar := syntheticSolcast(now, coverageH, 3.0)
	weather := syntheticWeather(now, planH+2)
	model := stubPredictor{factor: 0.9}

	out := fillSolarSlots(grid, now, solar, weather, model)
	if len(out) != grid.Len() {
		t.Fatalf("fill length %d, want %d", len(out), grid.Len())
	}

	coverageEnd := solar.Points[len(solar.Points)-1].Time // last 30-min point
	crossStart := coverageEnd.Add(-solarCrossfadeWindow)

	var sawFar bool
	for i := range out {
		ts := grid.Start[i]
		switch {
		case ts.Before(crossStart):
			// Well within coverage: pure Solcast truth.
			if math.Abs(out[i]-3.0) > 1e-9 {
				t.Fatalf("slot %d @ %v: near-term fill %.4f, want Solcast 3.0", i, ts, out[i])
			}
		case ts.Before(coverageEnd):
			// Seam: strictly between Solcast and the learned value, monotonic.
			w := crossfadeWeight(ts, coverageEnd)
			gti := interpolateGTI(weather.Points, ts)
			learned := model.PredictKW(ts, gti) * solarHaircut(ts.Sub(now))
			want := (1-w)*3.0 + w*learned
			if math.Abs(out[i]-want) > 1e-9 {
				t.Fatalf("seam slot %d @ %v: fill %.4f, want crossfade %.4f (w=%.3f)", i, ts, out[i], want, w)
			}
		default:
			// Beyond coverage: learned × haircut, with interpolated GTI.
			gti := interpolateGTI(weather.Points, ts)
			want := model.PredictKW(ts, gti) * solarHaircut(ts.Sub(now))
			if math.Abs(out[i]-want) > 1e-9 {
				t.Fatalf("far slot %d @ %v: fill %.4f, want learned×haircut %.4f", i, ts, out[i], want)
			}
			if out[i] <= 0 {
				t.Fatalf("far slot %d: expected non-zero learned solar, got %.4f", i, out[i])
			}
			sawFar = true
		}
	}
	if !sawFar {
		t.Fatal("no beyond-coverage slots exercised the learned fill")
	}
}

// TestCrossfadeMonotonicNoStep asserts the seam has no level step: the learned
// weight ramps 0→1 and the blended value moves monotonically from the Solcast
// level toward the learned level across the window.
func TestCrossfadeMonotonicNoStep(t *testing.T) {
	now := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	const coverageH, planH = 72, 96
	grid := telescopingGrid(now, coverageH, planH)
	solar := syntheticSolcast(now, coverageH, 3.0)
	weather := syntheticWeather(now, planH+2)
	model := stubPredictor{factor: 0.9}

	out := fillSolarSlots(grid, now, solar, weather, model)
	coverageEnd := solar.Points[len(solar.Points)-1].Time
	crossStart := coverageEnd.Add(-solarCrossfadeWindow)

	var lastW float64 = -1
	for i := range out {
		ts := grid.Start[i]
		if ts.Before(crossStart) || !ts.Before(coverageEnd) {
			continue
		}
		w := crossfadeWeight(ts, coverageEnd)
		if w < lastW {
			t.Fatalf("crossfade weight not monotonic: %.3f after %.3f", w, lastW)
		}
		if w < 0 || w >= 1 {
			t.Fatalf("crossfade weight out of [0,1): %.3f", w)
		}
		lastW = w
	}
	if lastW < 0 {
		t.Fatal("no seam slots found")
	}
}

func TestHaircutBounds(t *testing.T) {
	cases := []struct {
		lead time.Duration
		want float64
	}{
		{0, 1.0},
		{48 * time.Hour, 1.0},
		{108 * time.Hour, 0.9}, // midpoint of [48h,168h] → 0.9
		{168 * time.Hour, 0.8},
		{240 * time.Hour, 0.8}, // clamped
	}
	for _, c := range cases {
		if got := solarHaircut(c.lead); math.Abs(got-c.want) > 1e-9 {
			t.Errorf("solarHaircut(%v)=%.4f, want %.4f", c.lead, got, c.want)
		}
	}
}

func TestInterpolateGTI(t *testing.T) {
	now := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	pts := []forecast.GTIPoint{
		{Time: now, GTI: map[string]float64{"a": 100, "b": 200}},
		{Time: now.Add(time.Hour), GTI: map[string]float64{"a": 200, "b": 400}},
	}
	// Half-hour in → exact midpoint (NOT step-repeat of the lower point).
	mid := interpolateGTI(pts, now.Add(30*time.Minute))
	if math.Abs(mid["a"]-150) > 1e-9 || math.Abs(mid["b"]-300) > 1e-9 {
		t.Fatalf("midpoint interpolation = %v, want a=150 b=300", mid)
	}
	// On a point → exact value.
	on := interpolateGTI(pts, now.Add(time.Hour))
	if on["a"] != 200 || on["b"] != 400 {
		t.Fatalf("on-point = %v, want a=200 b=400", on)
	}
	// Before first / after last → clamp, no extrapolation.
	before := interpolateGTI(pts, now.Add(-time.Hour))
	if before["a"] != 100 {
		t.Fatalf("pre-range clamp a=%.1f, want 100", before["a"])
	}
	after := interpolateGTI(pts, now.Add(3*time.Hour))
	if after["a"] != 200 {
		t.Fatalf("post-range clamp a=%.1f, want 200", after["a"])
	}
	if interpolateGTI(nil, now) != nil {
		t.Fatal("empty points should interpolate to nil")
	}
}

// TestFillGracefulDegradation: with no weather cache or no model, the fill is
// exactly Solcast-only (near-term value within coverage, 0 beyond) and never
// panics or produces NaN.
func TestFillGracefulDegradation(t *testing.T) {
	now := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	const coverageH, planH = 72, 168
	grid := telescopingGrid(now, coverageH, planH)
	solar := syntheticSolcast(now, coverageH, 3.0)
	coverageEnd := solar.Points[len(solar.Points)-1].Time

	check := func(name string, out []float64) {
		if len(out) != grid.Len() {
			t.Fatalf("%s: length %d, want %d", name, len(out), grid.Len())
		}
		for i := range out {
			if math.IsNaN(out[i]) || math.IsInf(out[i], 0) {
				t.Fatalf("%s: slot %d is NaN/Inf", name, i)
			}
			ts := grid.Start[i]
			want := 0.0
			if ts.Before(coverageEnd) {
				want = 3.0
			}
			if math.Abs(out[i]-want) > 1e-9 {
				t.Fatalf("%s: slot %d @ %v = %.4f, want Solcast-only %.4f", name, i, ts, out[i], want)
			}
		}
	}

	check("nil weather", fillSolarSlots(grid, now, solar, nil, stubPredictor{factor: 0.9}))
	check("nil model", fillSolarSlots(grid, now, solar, syntheticWeather(now, planH), nil))

	// No Solcast AND no learned inputs → nil (pre-fill contract preserved).
	if out := fillSolarSlots(grid, now, nil, nil, nil); out != nil {
		t.Fatalf("no-inputs fill = %v, want nil", out)
	}
}

// TestFillCoverageHoleFallsBackToLearned: a slot inside Solcast's coverage but
// with no covering Solcast point (a mid-forecast gap) must read the learned value
// rather than a false zero-solar notch, when the model + GTI are available.
func TestFillCoverageHoleFallsBackToLearned(t *testing.T) {
	now := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	const coverageH, planH = 72, 96
	grid := telescopingGrid(now, coverageH, planH)
	weather := syntheticWeather(now, planH+2)
	model := stubPredictor{factor: 0.9}

	// Solcast covers the near horizon EXCEPT a mid-forecast gap [10h, 12h).
	var pts []forecast.SolarPoint
	for tm := 0; tm < coverageH*60; tm += 30 {
		mins := time.Duration(tm) * time.Minute
		if mins >= 10*time.Hour && mins < 12*time.Hour {
			continue // coverage hole
		}
		pts = append(pts, forecast.SolarPoint{Time: now.Add(mins), EstimateKW: 3.0})
	}
	solar := &forecast.SolarForecast{FetchedAt: now, Points: pts}

	out := fillSolarSlots(grid, now, solar, weather, model)

	holeTS := now.Add(11 * time.Hour) // well within coverage, inside the gap
	idx := -1
	for i := range grid.Start {
		if grid.Start[i].Equal(holeTS) {
			idx = i
			break
		}
	}
	if idx < 0 {
		t.Fatal("no grid slot at the coverage hole")
	}
	gti := interpolateGTI(weather.Points, holeTS)
	want := model.PredictKW(holeTS, gti) * solarHaircut(holeTS.Sub(now))
	if want <= 0 {
		t.Fatal("test fixture broken: learned value should be > 0 at the hole")
	}
	if math.Abs(out[idx]-want) > 1e-9 {
		t.Fatalf("coverage-hole slot = %.4f, want learned %.4f (not a zero notch)", out[idx], want)
	}
}
func TestFillNoSolcastLearnedEverywhere(t *testing.T) {
	now := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	grid := telescopingGrid(now, 72, 96)
	weather := syntheticWeather(now, 98)
	model := stubPredictor{factor: 0.9}

	out := fillSolarSlots(grid, now, nil, weather, model)
	if out == nil {
		t.Fatal("expected learned fill, got nil")
	}
	var nonzero int
	for i := range out {
		gti := interpolateGTI(weather.Points, grid.Start[i])
		want := model.PredictKW(grid.Start[i], gti) * solarHaircut(grid.Start[i].Sub(now))
		if math.Abs(out[i]-want) > 1e-9 {
			t.Fatalf("slot %d = %.4f, want learned %.4f", i, out[i], want)
		}
		if out[i] > 0 {
			nonzero++
		}
	}
	if nonzero == 0 {
		t.Fatal("learned-everywhere fill produced all zeros")
	}
}
