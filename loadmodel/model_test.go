package loadmodel

import (
	"context"
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
		month time.Month
		want  Season
	}{
		{time.January, Summer},
		{time.March, Autumn},
		{time.June, Winter},
		{time.September, Spring},
		{time.December, Summer},
	}
	for _, tt := range tests {
		d := time.Date(2024, tt.month, 15, 0, 0, 0, 0, time.UTC)
		got := SeasonOf(d)
		if got != tt.want {
			t.Errorf("SeasonOf(%v) = %v, want %v", tt.month, got, tt.want)
		}
	}
}
