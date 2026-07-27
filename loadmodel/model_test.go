package loadmodel

import (
	"context"
	"math"
	"sort"
	"testing"
	"time"
)

type mockDataSource struct {
	samples map[string][]Sample
}

// QueryPower returns only the samples inside the half-open window [from,to),
// so walk-forward training windows (TrainWindow with a past `to`) actually
// exclude out-of-window data instead of leaking the whole series.
func (m *mockDataSource) QueryPower(_ context.Context, entityID string, from, to time.Time) ([]Sample, error) {
	var out []Sample
	for _, s := range m.samples[entityID] {
		if !s.Time.Before(from) && s.Time.Before(to) {
			out = append(out, s)
		}
	}
	return out, nil
}

// percentile75 computes the p75 (linear interpolation between closest ranks)
// of vals — the OLD per-bucket SHAPE numerator, reconstructed in-test so the
// double-count regression guard can compare against the retired formula.
func percentile75(vals []float64) float64 {
	s := append([]float64(nil), vals...)
	sort.Float64s(s)
	if len(s) == 1 {
		return s[0]
	}
	rank := 0.75 * float64(len(s)-1)
	lo := int(math.Floor(rank))
	hi := int(math.Ceil(rank))
	if lo == hi {
		return s[lo]
	}
	return s[lo] + (rank-float64(lo))*(s[hi]-s[lo])
}

func TestTrainAndPredict(t *testing.T) {
	// Build the model manually with whole-house as primary.
	m := &Model{
		WholeHouse: &CircuitModel{
			Name:     "kitchen",
			EntityID: "sensor.ct_kitchen",
			Category: "fixed",
			Buckets:  make(map[bucketKey]*bucketStats),
			Default:  200,
		},
	}

	// Generate mock training data: 100W every hour for a week.
	start := time.Date(2024, time.January, 1, 0, 0, 0, 0, time.UTC)
	var samples []Sample
	for i := 0; i < 7*24; i++ {
		samples = append(samples, Sample{Time: start.Add(time.Duration(i) * time.Hour), Value: 100})
	}

	src := &mockDataSource{
		samples: map[string][]Sample{"sensor.ct_kitchen": samples},
	}

	to := start.Add(7 * 24 * time.Hour)
	if err := m.TrainWindow(context.Background(), src, start, to); err != nil {
		t.Fatal(err)
	}

	// Predict at a time whose hour bucket matches training data.
	slot := time.Date(2024, time.January, 8, 12, 0, 0, 0, time.UTC)
	pred := m.Predict([]time.Time{slot})
	if len(pred) != 1 {
		t.Fatalf("predict returned %d values, want 1", len(pred))
	}
	if pred[0] != 100 {
		t.Errorf("prediction = %v, want 100", pred[0])
	}
}

func TestConfidenceEmpty(t *testing.T) {
	m := &Model{}
	if c := m.Confidence(); c != 0 {
		t.Errorf("empty model confidence = %v, want 0", c)
	}
}

func TestConfidencePartial(t *testing.T) {
	m := &Model{
		WholeHouse: &CircuitModel{
			Name:     "test",
			Category: "fixed",
			Buckets: map[bucketKey]*bucketStats{
				{Hour: 0, DOW: -1, Season: Summer}: {Sum: 300, Count: 5},
				{Hour: 1, DOW: -1, Season: Summer}: {Sum: 300, Count: 5},
			},
			Default: 100,
		},
	}

	c := m.Confidence()
	// 2 filled out of 96 total (24 hours × 4 seasons)
	expected := 2.0 / 96.0
	if c < expected-0.001 || c > expected+0.001 {
		t.Errorf("confidence = %v, want ~%v", c, expected)
	}
}

func TestPredictFallsBackToDefault(t *testing.T) {
	m := &Model{
		WholeHouse: &CircuitModel{
			Name:     "test",
			Category: "fixed",
			Buckets:  make(map[bucketKey]*bucketStats),
			Default:  250,
		},
	}

	slot := time.Date(2024, 6, 15, 14, 0, 0, 0, time.UTC)
	pred := m.Predict([]time.Time{slot})
	if pred[0] != 250 {
		t.Errorf("fallback prediction = %v, want 250", pred[0])
	}
}

// TestColdStartFallsBackToRecentLevel checks the predictAt fallback ladder: a
// bucket below minSamples with no DOW fallback should return the recency
// LEVEL (real recent signal) rather than the hardcoded Default.
func TestColdStartFallsBackToRecentLevel(t *testing.T) {
	cm := newCircuitModel("house", "sensor.load", "fixed", 500,
		Params{RecencyHalfLifeDays: 3, BucketHalfLifeDays: 10})
	to := time.Date(2026, time.July, 22, 0, 0, 0, 0, time.UTC)

	// One sample per day at hour 6 for 10 days (fills the hour-6 bucket), all
	// at 1400W, so recentLevel ≈ 1400. Predict at hour 14, which has no bucket.
	var samples []Sample
	for d := 1; d <= 10; d++ {
		samples = append(samples, Sample{Time: to.Add(-time.Duration(d)*24*time.Hour + 6*time.Hour), Value: 1400})
	}
	cm.trainFrom(samples, to)

	got := cm.predictAt(to.Add(14 * time.Hour)) // hour 14, unseen bucket
	if math.Abs(got-cm.recentLevel) > 0.01 {
		t.Errorf("cold-start predictAt = %.1f, want recentLevel %.1f (not Default %.0f)", got, cm.recentLevel, cm.Default)
	}
	if got == cm.Default {
		t.Errorf("cold-start predictAt returned Default %.0f, want recentLevel", cm.Default)
	}
}

func TestSeasonOf(t *testing.T) {
	tests := []struct {
		month              time.Month
		southernHemisphere bool
		want               Season
	}{
		// Southern hemisphere (e.g. the config.toml example, Sydney).
		{time.January, true, Summer},
		{time.March, true, Autumn},
		{time.June, true, Winter},
		{time.September, true, Spring},
		{time.December, true, Summer},
		// Northern hemisphere (e.g. the live deployment, Kanto/Japan): flipped.
		{time.January, false, Winter},
		{time.March, false, Spring},
		{time.June, false, Summer},
		{time.July, false, Summer},
		{time.September, false, Autumn},
		{time.December, false, Winter},
	}
	for _, tt := range tests {
		d := time.Date(2024, tt.month, 15, 0, 0, 0, 0, time.UTC)
		got := SeasonOf(d, tt.southernHemisphere)
		if got != tt.want {
			t.Errorf("SeasonOf(%v, southern=%v) = %v, want %v", tt.month, tt.southernHemisphere, got, tt.want)
		}
	}
}

// TestRecencyTracksStepChange reproduces the diagnosed LEVEL bug: a flat
// 30-day mean dilutes a real step change in household baseline load. Both the
// recency-weighted LEVEL (recentLevel) and the per-bucket recency mean (the
// prediction) should track the new baseline instead of averaging it away.
func TestRecencyTracksStepChange(t *testing.T) {
	cm := newCircuitModel("house", "sensor.load", "fixed", 500,
		Params{RecencyHalfLifeDays: 3, BucketHalfLifeDays: 3})
	to := time.Date(2026, time.July, 22, 0, 0, 0, 0, time.UTC)

	// 27 days at a 1000W baseline, then a step change to 2000W for the last
	// 3 days — every hour of every day.
	var samples []Sample
	for d := 30; d >= 1; d-- {
		v := 1000.0
		if d <= 3 {
			v = 2000.0
		}
		for h := 0; h < 24; h++ {
			samples = append(samples, Sample{
				Time:  to.Add(-time.Duration(d)*24*time.Hour + time.Duration(h)*time.Hour),
				Value: v,
			})
		}
	}
	cm.trainFrom(samples, to)

	flatMean := (1000.0*27 + 2000.0*3) / 30.0
	if cm.recentLevel <= flatMean {
		t.Errorf("recentLevel = %.1f, want > flat 30-day mean %.1f (must track the step change)", cm.recentLevel, flatMean)
	}
	if cm.recentLevel < 1500 {
		t.Errorf("recentLevel = %.1f, want >= 1500 (materially closer to the new 2000W baseline than the flat mean)", cm.recentLevel)
	}

	target := to.Add(-1 * time.Hour) // within the post-step-change window
	pred := cm.predictAt(target)
	if pred <= flatMean {
		t.Errorf("predictAt(%v) = %.1f, want > flat mean %.1f (bucket recency mean must follow the step up)", target, pred, flatMean)
	}
}

// TestPerBucketRecencyMeanNoDoubleCount is the regression guard for the
// double-count bug (v0.2.10): the retired LEVEL×(p75/overallMean) formula
// multiplied a level lift by a headroom-inflated shape numerator, over-
// predicting a low-but-spiky overnight bucket. The per-bucket recency mean
// must land on the real recent overnight load, strictly below the old value.
func TestPerBucketRecencyMeanNoDoubleCount(t *testing.T) {
	cm := newCircuitModel("house", "sensor.load", "fixed", 500,
		Params{RecencyHalfLifeDays: 3, BucketHalfLifeDays: 10, ConfidenceThreshold: 0})
	day0 := time.Date(2026, time.February, 15, 0, 0, 0, 0, time.UTC) // deep winter → 30d back stays one season
	to := day0

	const overnightHour = 2
	var samples []Sample
	var overnightVals []float64
	for d := 1; d <= 30; d++ {
		for h := 0; h < 24; h++ {
			t := day0.Add(-time.Duration(d)*24*time.Hour + time.Duration(h)*time.Hour)
			var v float64
			switch h {
			case overnightHour:
				// Recent 10 days: the true ~1240W overnight load. Older 20 days:
				// a high-variance mix (400/2080) whose p75 is badly inflated.
				switch {
				case d <= 10:
					v = 1240
				case d%2 == 1:
					v = 400
				default:
					v = 2080
				}
				overnightVals = append(overnightVals, v)
			default:
				// Daytime baseline, stepped up over the last 5 days — lifts
				// overallMean and recentLevel the way the real fault did.
				v = 2000
				if d <= 5 {
					v = 3500
				}
			}
			samples = append(samples, Sample{Time: t, Value: v})
		}
	}
	cm.trainFrom(samples, to)

	k := cm.key(day0.Add(overnightHour * time.Hour))
	predicted, ok := cm.bucketPredict(k)
	if !ok {
		t.Fatalf("overnight bucket %+v not usable", k)
	}

	const actual = 1240.0
	if math.Abs(predicted-actual) > 0.15*actual {
		t.Errorf("overnight prediction = %.1f, want within 15%% of recent actual %.0f", predicted, actual)
	}

	// The retired formula, computed inline from the model's own level/mean and
	// the reconstructed p75 shape numerator.
	p75 := percentile75(overnightVals)
	old := cm.recentLevel * (p75 / cm.overallMean)
	if predicted >= old {
		t.Errorf("prediction %.1f >= old level×p75/mean %.1f — double-count not fixed", predicted, old)
	}
	if predicted > 0.8*old {
		t.Errorf("prediction %.1f not clearly below old value %.1f (p75=%.0f, recentLevel=%.0f, overallMean=%.0f)",
			predicted, old, p75, cm.recentLevel, cm.overallMean)
	}
}

// TestBucketMeanTracksShapeDrift verifies SHAPE drift (a per-bucket, not
// whole-model, change) is now tracked: overnight load dropping relative to a
// steady daytime should pull the overnight prediction down within days.
func TestBucketMeanTracksShapeDrift(t *testing.T) {
	cm := newCircuitModel("house", "sensor.load", "fixed", 500,
		Params{RecencyHalfLifeDays: 3, BucketHalfLifeDays: 5, ConfidenceThreshold: 0})
	day0 := time.Date(2026, time.February, 15, 0, 0, 0, 0, time.UTC)
	to := day0

	const overnightHour = 2
	var samples []Sample
	for d := 1; d <= 30; d++ {
		for h := 0; h < 24; h++ {
			t := day0.Add(-time.Duration(d)*24*time.Hour + time.Duration(h)*time.Hour)
			v := 2000.0 // steady daytime
			if h == overnightHour {
				v = 2000 // overnight used to match daytime...
				if d <= 7 {
					v = 1000 // ...then dropped to 1000W over the last week
				}
			}
			samples = append(samples, Sample{Time: t, Value: v})
		}
	}
	cm.trainFrom(samples, to)

	k := cm.key(day0.Add(overnightHour * time.Hour))
	predicted, ok := cm.bucketPredict(k)
	if !ok {
		t.Fatalf("overnight bucket %+v not usable", k)
	}
	if predicted >= 1500 {
		t.Errorf("overnight prediction = %.1f, want well below the old 2000W (drift not tracked)", predicted)
	}
	if predicted < 1000 {
		t.Errorf("overnight prediction = %.1f, want >= 1000 (should not overshoot below the new level)", predicted)
	}
}

// TestTrainWindowCutoff verifies TrainWindow with a past `to` excludes later
// samples (via the range-filtering data source) and decays relative to `to`.
func TestTrainWindowCutoff(t *testing.T) {
	day0 := time.Date(2026, time.February, 15, 0, 0, 0, 0, time.UTC)
	to := day0                      // cutoff, in the "past" relative to newest data
	from := day0.AddDate(0, 0, -30) // 30-day training window

	// 60 days of hourly data ending 20 days AFTER the cutoff. In-window: an
	// older half at 1000W then a recent-to-cutoff half at 1500W. Post-cutoff:
	// 5000W spikes that must NOT leak into training.
	var samples []Sample
	windowMid := to.AddDate(0, 0, -15)
	for i := 0; i < 60*24; i++ {
		t := from.Add(time.Duration(i) * time.Hour)
		var v float64
		switch {
		case t.Before(windowMid):
			v = 1000
		case t.Before(to):
			v = 1500
		default:
			v = 5000 // after the cutoff — excluded
		}
		samples = append(samples, Sample{Time: t, Value: v})
	}

	m := &Model{
		WholeHouse: newCircuitModel("house", "sensor.load", "fixed", 500,
			Params{RecencyHalfLifeDays: 3, BucketHalfLifeDays: 10}),
	}
	src := &mockDataSource{samples: map[string][]Sample{"sensor.load": samples}}
	if err := m.TrainWindow(context.Background(), src, from, to); err != nil {
		t.Fatal(err)
	}
	cm := m.WholeHouse

	// Only [from,to) samples counted → mean is (1000+1500)/2 = 1250, nowhere
	// near the 2000+ it would be if the 5000W post-cutoff spikes leaked in.
	if cm.overallMean < 1200 || cm.overallMean > 1300 {
		t.Errorf("overallMean = %.1f, want ~1250 (post-cutoff 5000W samples must be excluded)", cm.overallMean)
	}
	// Decay is measured relative to `to`: the near-cutoff 1500W half outweighs
	// the older 1000W half, so recentLevel sits above the flat window mean.
	if cm.recentLevel <= cm.overallMean {
		t.Errorf("recentLevel = %.1f, want > overallMean %.1f (decay relative to `to`)", cm.recentLevel, cm.overallMean)
	}
	if cm.recentLevel > 1500 {
		t.Errorf("recentLevel = %.1f, want <= 1500 (bounded by the newest in-window regime)", cm.recentLevel)
	}
	// A prediction stays in the trained regime, never near the excluded 5000W.
	if pred := cm.predictAt(to.Add(-24 * time.Hour)); pred >= 2000 {
		t.Errorf("prediction = %.1f, want < 2000 (5000W samples excluded)", pred)
	}
}

// TestConfidenceGatedConservativeFallback verifies Optimizer.ConfidenceThreshold
// actually gates the prediction: a thin bucket (at the minSamples floor, far
// under minConfidentSamples) should be scaled by ConservativeMargin when
// ConfidenceThreshold is set, and left alone when it isn't.
func TestConfidenceGatedConservativeFallback(t *testing.T) {
	to := time.Date(2026, time.July, 22, 6, 0, 0, 0, time.UTC)
	var samples []Sample
	for i := 0; i < minSamples; i++ {
		samples = append(samples, Sample{Time: to.Add(-time.Duration(i+1) * 24 * time.Hour), Value: 100})
	}

	ungated := newCircuitModel("house", "sensor.load", "fixed", 500,
		Params{RecencyHalfLifeDays: 0, BucketHalfLifeDays: 0, ConfidenceThreshold: 0})
	ungated.trainFrom(samples, to)

	gated := newCircuitModel("house", "sensor.load", "fixed", 500,
		Params{RecencyHalfLifeDays: 0, BucketHalfLifeDays: 0, ConfidenceThreshold: 0.9, ConservativeMargin: 1.5})
	gated.trainFrom(samples, to)

	target := to.Add(-24 * time.Hour)
	k := gated.key(target)
	if conf := gated.bucketConfidence(k); conf >= gated.confidenceThreshold {
		t.Fatalf("bucketConfidence = %.2f, want < threshold %.2f (only %d samples)", conf, gated.confidenceThreshold, minSamples)
	}

	if got := ungated.predictAt(target); math.Abs(got-100) > 0.01 {
		t.Errorf("ungated predictAt = %.2f, want ~100 (no margin — threshold disabled)", got)
	}

	want := ungated.predictAt(target) * 1.5
	got := gated.predictAt(target)
	if math.Abs(got-want) > 0.01 {
		t.Errorf("gated predictAt = %.2f, want %.2f (ungated %.2f × ConservativeMargin 1.5)", got, want, ungated.predictAt(target))
	}
}
