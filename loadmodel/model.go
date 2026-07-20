package loadmodel

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"time"

	"energy-optimiser/config"
)

// Season represents a meteorological season (southern hemisphere).
type Season int

const (
	Spring Season = iota
	Summer
	Autumn
	Winter
)

func SeasonOf(t time.Time) Season {
	switch t.Month() {
	case time.September, time.October, time.November:
		return Spring
	case time.December, time.January, time.February:
		return Summer
	case time.March, time.April, time.May:
		return Autumn
	default:
		return Winter
	}
}

// Sample is a timestamped power reading.
type Sample struct {
	Time  time.Time
	Value float64 // watts
}

// DataSource provides historical power data for training.
type DataSource interface {
	QueryPower(ctx context.Context, entityID string, from, to time.Time) ([]Sample, error)
}

type bucketKey struct {
	Hour   int
	DOW    int // 0-6, or -1 for fixed/weather categories
	Season Season
}

type bucketStats struct {
	Sum   float64
	Count int
}

func (b bucketStats) Mean() float64 {
	if b.Count == 0 {
		return 0
	}
	return b.Sum / float64(b.Count)
}

// CircuitModel holds the learned profile for a single circuit.
type CircuitModel struct {
	Name     string
	EntityID string
	Category string // fixed, routine, weather
	Buckets  map[bucketKey]*bucketStats
	Default  float64 // conservative default (W)
}

// Model holds all circuit models and produces aggregate load predictions.
type Model struct {
	WholeHouse *CircuitModel   // primary: total house load
	Circuits   []*CircuitModel // per-circuit (v2: for deferrable splitting)
}

// New creates a load model. The whole-house load entity is always trained as
// the primary model — it's the real total load. Per-circuit models (v2) can
// refine this for deferrable load control but don't replace it.
func New(circuits []config.Circuit, wholeHouseEntity string) *Model {
	m := &Model{}

	// Always include whole-house load as the primary model
	if wholeHouseEntity != "" {
		m.WholeHouse = &CircuitModel{
			Name:     "whole_house",
			EntityID: wholeHouseEntity,
			Category: "fixed",
			Buckets:  make(map[bucketKey]*bucketStats),
			Default:  1000,
		}
	}

	// Per-circuit models (v2: for deferrable load splitting)
	for _, c := range circuits {
		if c.Category == "deferrable" {
			continue
		}
		m.Circuits = append(m.Circuits, &CircuitModel{
			Name:     c.Name,
			EntityID: c.EntityID,
			Category: c.Category,
			Buckets:  make(map[bucketKey]*bucketStats),
			Default:  200,
		})
	}
	return m
}

// Train fetches historical data from src and builds bucket profiles.
func (m *Model) Train(ctx context.Context, src DataSource, lookback time.Duration) error {
	to := time.Now()
	from := to.Add(-lookback)

	all := m.allModels()
	for _, cm := range all {
		samples, err := src.QueryPower(ctx, cm.EntityID, from, to)
		if err != nil {
			slog.Warn("training failed for circuit", "name", cm.Name, "error", err)
			continue
		}
		for _, s := range samples {
			key := cm.key(s.Time)
			b, ok := cm.Buckets[key]
			if !ok {
				b = &bucketStats{}
				cm.Buckets[key] = b
			}
			b.Sum += s.Value
			b.Count++
		}
		slog.Info("trained circuit", "name", cm.Name, "samples", len(samples),
			"confidence", fmt.Sprintf("%.1f%%", cm.confidence()*100))
	}
	return nil
}

// Predict returns predicted total load (W) for each slot time.
// Uses the whole-house model as the primary source.
func (m *Model) Predict(slots []time.Time) []float64 {
	out := make([]float64, len(slots))
	if m.WholeHouse != nil {
		for i, t := range slots {
			out[i] = m.WholeHouse.predictAt(t)
		}
	}
	return out
}

// Confidence returns 0-1 indicating data coverage for the whole-house model.
func (m *Model) Confidence() float64 {
	if m.WholeHouse == nil {
		return 0
	}
	return m.WholeHouse.confidence()
}

func (m *Model) allModels() []*CircuitModel {
	var all []*CircuitModel
	if m.WholeHouse != nil {
		all = append(all, m.WholeHouse)
	}
	all = append(all, m.Circuits...)
	return all
}

func (cm *CircuitModel) key(t time.Time) bucketKey {
	k := bucketKey{
		Hour:   t.Hour(),
		Season: SeasonOf(t),
		DOW:    -1,
	}
	if cm.Category == "routine" {
		k.DOW = int(t.Weekday())
	}
	return k
}

const minSamples = 3

func (cm *CircuitModel) predictAt(t time.Time) float64 {
	k := cm.key(t)
	if b, ok := cm.Buckets[k]; ok && b.Count >= minSamples {
		return b.Mean()
	}
	// Fallback: drop DOW for routine circuits
	if cm.Category == "routine" {
		fb := bucketKey{Hour: k.Hour, Season: k.Season, DOW: -1}
		if b, ok := cm.Buckets[fb]; ok && b.Count >= minSamples {
			return b.Mean()
		}
	}
	return cm.Default
}

func (cm *CircuitModel) confidence() float64 {
	total := 24 * 4 // hours × seasons
	if cm.Category == "routine" {
		total = 24 * 7 * 4
	}
	filled := 0
	for _, b := range cm.Buckets {
		if b.Count >= minSamples {
			filled++
		}
	}
	return math.Min(1.0, float64(filled)/float64(total))
}
