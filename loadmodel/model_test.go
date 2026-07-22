package loadmodel

import (
	"context"
	"math"
	"testing"
	"time"
)

type mockDataSource struct {
	samples map[string][]Sample
}

func (m *mockDataSource) QueryPower(_ context.Context, entityID string, _, _ time.Time) ([]Sample, error) {
	return m.samples[entityID], nil
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

	// Generate mock training data: 100W every hour for a summer week
	var samples []Sample
	start := time.Date(2024, time.January, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 7*24; i++ {
		t := start.Add(time.Duration(i) * time.Hour)
		samples = append(samples, Sample{Time: t, Value: 100})
	}

	src := &mockDataSource{
		samples: map[string][]Sample{
			"sensor.ct_kitchen": samples,
		},
	}

	if err := m.Train(context.Background(), src, 30*24*time.Hour); err != nil {
		t.Fatal(err)
	}

	// Predict at a time that matches training data
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

// TestRecencyTracksStepChange reproduces the diagnosed bug: a flat 30-day
// mean dilutes a real step change in household baseline load (confirmed
// live: ~1.0-1.4kW model buckets vs a ~2.3kW actual morning average after the
// baseline roughly doubled a few days prior). The recency-weighted LEVEL
// should track the new baseline instead of averaging it away.
func TestRecencyTracksStepChange(t *testing.T) {
	cm := newCircuitModel("house", "sensor.load", "fixed", 500,
		Params{RecencyHalfLifeDays: 3, Percentile: 0.75})
	to := time.Date(2026, time.July, 22, 0, 0, 0, 0, time.UTC)

	// 27 days at a 1000W baseline, then a step change to 2000W for the last
	// 3 days — every hour of every day, so every hour bucket sees the same
	// mix and the SHAPE ratio stays ~1 (isolating the LEVEL effect).
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
		t.Errorf("predictAt(%v) = %.1f, want > flat mean %.1f", target, pred, flatMean)
	}
}

// TestPercentileHeadroom verifies a right-skewed bucket (mostly a low
// baseline with occasional spikes) predicts above its arithmetic mean,
// giving the safety headroom a flat mean discards by averaging spikes away.
func TestPercentileHeadroom(t *testing.T) {
	cm := newCircuitModel("house", "sensor.load", "fixed", 500,
		Params{RecencyHalfLifeDays: 0, Percentile: 0.9}) // flat weighting isolates the percentile effect
	to := time.Date(2026, time.July, 22, 6, 0, 0, 0, time.UTC)

	var samples []Sample
	for d := 1; d <= 20; d++ {
		v := 100.0
		if d%4 == 0 { // a spike every 4th day (e.g. kettle + induction hob)
			v = 900.0
		}
		samples = append(samples, Sample{Time: to.Add(-time.Duration(d) * 24 * time.Hour), Value: v})
	}
	cm.trainFrom(samples, to)

	k := cm.key(to.Add(-24 * time.Hour))
	b, ok := cm.Buckets[k]
	if !ok {
		t.Fatalf("bucket %+v not trained", k)
	}
	mean := b.Mean()
	if b.Pct <= mean {
		t.Errorf("bucket p90 = %.1f, want > arithmetic mean %.1f (headroom from the skew)", b.Pct, mean)
	}

	pred := cm.predictAt(to.Add(-24 * time.Hour))
	if pred <= mean {
		t.Errorf("predictAt = %.1f, want > arithmetic mean %.1f", pred, mean)
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
		Params{RecencyHalfLifeDays: 0, Percentile: 0.5, ConfidenceThreshold: 0})
	ungated.trainFrom(samples, to)

	gated := newCircuitModel("house", "sensor.load", "fixed", 500,
		Params{RecencyHalfLifeDays: 0, Percentile: 0.5, ConfidenceThreshold: 0.9, ConservativeMargin: 1.5})
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
