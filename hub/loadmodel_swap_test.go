package hub

import (
	"context"
	"sync"
	"testing"
	"time"

	"energy-optimiser/loadmodel"
)

// loadRaceSource is a fast in-memory loadmodel.DataSource: it returns a fixed
// batch of samples for the whole-house entity so building+training a fresh model
// is cheap enough to hammer in a tight loop. It stands in for the InfluxDB pull
// that buildTrainedLoadModel does in production.
type loadRaceSource struct {
	entityID string
	samples  []loadmodel.Sample
}

func (s *loadRaceSource) QueryPower(_ context.Context, entityID string, from, to time.Time) ([]loadmodel.Sample, error) {
	if entityID != s.entityID {
		return nil, nil
	}
	var out []loadmodel.Sample
	for _, v := range s.samples {
		if !v.Time.Before(from) && v.Time.Before(to) {
			out = append(out, v)
		}
	}
	return out, nil
}

// buildRaceModel constructs a FRESH, populated load model the same way
// buildTrainedLoadModel does (loadmodel.New then Train), so each swapped-in model
// owns its own Buckets map. A week of hourly samples fills the hour-of-day buckets
// past minSamples, so Confidence() actually ranges a non-empty map — reproducing
// the map-iteration-vs-write shape of the original bug, not just a nil pointer.
func buildRaceModel(t *testing.T, src *loadRaceSource) *loadmodel.Model {
	t.Helper()
	m := loadmodel.New(nil, src.entityID, loadmodel.Params{})
	from := time.Date(2026, time.July, 1, 0, 0, 0, 0, time.UTC)
	to := from.Add(7 * 24 * time.Hour)
	if err := m.TrainWindow(context.Background(), src, from, to); err != nil {
		t.Fatalf("train: %v", err)
	}
	return m
}

// TestLoadModelSwapAccessorRace races the locked read path
// (currentLoadModel().Predict / LoadConfidence, the exact paths the tick loop and
// the dashboard use) against the atomic swap (swapLoadModel, what the background
// retrain does) for thousands of iterations. Under -race this FAILS without the
// loadModelMu guard — an unlocked pointer read against an unlocked pointer write
// is a data race, and the pre-v0.2.10 in-place retrain additionally tripped
// "concurrent map iteration and map write" on the Buckets map. With the fix
// (immutable published model + RWMutex on the pointer) it is race-free.
func TestLoadModelSwapAccessorRace(t *testing.T) {
	const entity = "sensor.load_power"
	from := time.Date(2026, time.July, 1, 0, 0, 0, 0, time.UTC)
	var samples []loadmodel.Sample
	for i := 0; i < 7*24; i++ {
		samples = append(samples, loadmodel.Sample{
			Time:  from.Add(time.Duration(i) * time.Hour),
			Value: 400 + float64(i%24)*20, // a mild hour-of-day shape
		})
	}
	src := &loadRaceSource{entityID: entity, samples: samples}

	h := &Hub{}
	h.loadModel = buildRaceModel(t, src) // initial published model

	slots := []time.Time{
		time.Date(2026, time.July, 10, 8, 0, 0, 0, time.UTC),
		time.Date(2026, time.July, 10, 18, 0, 0, 0, time.UTC),
	}

	const (
		readers    = 6
		writers    = 3
		readIters  = 4000
		writeIters = 1500
	)

	var wg sync.WaitGroup
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < readIters; i++ {
				// Both production read paths: the tick's Predict and the
				// dashboard's LoadConfidence (which routes through currentLoadModel).
				_ = h.currentLoadModel().Predict(slots)
				_ = h.LoadConfidence()
			}
		}()
	}
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < writeIters; i++ {
				h.swapLoadModel(buildRaceModel(t, src))
			}
		}()
	}
	wg.Wait()

	// A published model must always be usable (never nil, never partially built).
	if got := h.LoadConfidence(); got < 0 || got > 1 {
		t.Fatalf("LoadConfidence() = %v, want within [0,1]", got)
	}
}
