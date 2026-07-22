package loadmodel

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"time"

	"energy-optimiser/config"
)

// Season represents a meteorological season.
type Season int

const (
	Spring Season = iota
	Summer
	Autumn
	Winter
)

// SeasonOf returns the meteorological season for t. Hemisphere flips which
// calendar months map to which season (June/July/August is winter in the
// south, summer in the north).
func SeasonOf(t time.Time, southernHemisphere bool) Season {
	switch t.Month() {
	case time.September, time.October, time.November:
		if southernHemisphere {
			return Spring
		}
		return Autumn
	case time.December, time.January, time.February:
		if southernHemisphere {
			return Summer
		}
		return Winter
	case time.March, time.April, time.May:
		if southernHemisphere {
			return Autumn
		}
		return Spring
	default: // June, July, August
		if southernHemisphere {
			return Winter
		}
		return Summer
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

// bucketStats holds a bucket's arithmetic mean (Sum/Count — sample-count
// confidence, diagnostics) and its headroom percentile (Pct — the
// CircuitModel.percentile-th percentile of the bucket's samples, computed
// once at the end of training).
type bucketStats struct {
	Sum   float64
	Count int
	Pct   float64
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
	Default  float64 // conservative default (W), used only when there is no training data at all

	// overallMean is the plain (unweighted) mean of every sample seen during
	// training, across the whole lookback window — the normalizing
	// denominator for bucketStats.Pct, turning it into a relative
	// hour-of-day/season SHAPE ratio independent of the current baseline.
	overallMean float64
	// recentLevel is the recency-decay-weighted mean of every sample seen
	// during training (see recencyHalfLifeDays) — the current baseline LEVEL,
	// tracking a step change within days instead of being diluted by the full
	// lookback window the way a flat mean is.
	recentLevel float64

	southernHemisphere  bool
	recencyHalfLifeDays float64
	percentile          float64
	confidenceThreshold float64
	conservativeMargin  float64
}

// Model holds all circuit models and produces aggregate load predictions.
type Model struct {
	WholeHouse *CircuitModel   // primary: total house load
	Circuits   []*CircuitModel // per-circuit (v2: for deferrable splitting)
}

// Params bundles the tunable load-estimation knobs threaded in from
// config.LoadModel and config.Optimizer.ConfidenceThreshold. Zero values fall
// back to sane in-package defaults so a caller that skips config finalization
// (e.g. a test) still gets sensible behaviour.
type Params struct {
	// SouthernHemisphere selects the calendar→season mapping (see SeasonOf).
	SouthernHemisphere bool
	// RecencyHalfLifeDays is the exponential-decay half-life (in days) used to
	// weight training samples by age when computing the LEVEL. Default 3.
	RecencyHalfLifeDays float64
	// Percentile (0-1) is used instead of the mean for each bucket's
	// hour-of-day/season SHAPE, so peaky buckets bias predictions up rather
	// than being averaged away. Default 0.75 (p75).
	Percentile float64
	// ConfidenceThreshold: predictions for a bucket whose confidence (see
	// CircuitModel.bucketConfidence) falls below this are scaled up by
	// ConservativeMargin. 0 disables the gate.
	ConfidenceThreshold float64
	// ConservativeMargin multiplies a low-confidence bucket's prediction.
	// Default 1.3 (+30%) when the gate is active.
	ConservativeMargin float64
}

const (
	defaultRecencyHalfLifeDays = 3.0
	defaultPercentile          = 0.75
	defaultConservativeMargin  = 1.3
)

// New creates a load model. The whole-house load entity is always trained as
// the primary model — it's the real total load. Per-circuit models (v2) can
// refine this for deferrable load control but don't replace it.
func New(circuits []config.Circuit, wholeHouseEntity string, p Params) *Model {
	if p.RecencyHalfLifeDays <= 0 {
		p.RecencyHalfLifeDays = defaultRecencyHalfLifeDays
	}
	if p.Percentile <= 0 || p.Percentile > 1 {
		p.Percentile = defaultPercentile
	}
	if p.ConservativeMargin <= 0 {
		p.ConservativeMargin = defaultConservativeMargin
	}

	m := &Model{}

	// Always include whole-house load as the primary model
	if wholeHouseEntity != "" {
		m.WholeHouse = newCircuitModel("whole_house", wholeHouseEntity, "fixed", 1000, p)
	}

	// Per-circuit models (v2: for deferrable load splitting)
	for _, c := range circuits {
		if c.Category == "deferrable" {
			continue
		}
		m.Circuits = append(m.Circuits, newCircuitModel(c.Name, c.EntityID, c.Category, 200, p))
	}
	return m
}

func newCircuitModel(name, entityID, category string, dflt float64, p Params) *CircuitModel {
	return &CircuitModel{
		Name:     name,
		EntityID: entityID,
		Category: category,
		Buckets:  make(map[bucketKey]*bucketStats),
		Default:  dflt,

		southernHemisphere:  p.SouthernHemisphere,
		recencyHalfLifeDays: p.RecencyHalfLifeDays,
		percentile:          p.Percentile,
		confidenceThreshold: p.ConfidenceThreshold,
		conservativeMargin:  p.ConservativeMargin,
	}
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
		cm.trainFrom(samples, to)
		slog.Info("trained circuit", "name", cm.Name, "samples", len(samples),
			"confidence", fmt.Sprintf("%.1f%%", cm.confidence()*100),
			"recent_level_w", fmt.Sprintf("%.0f", cm.recentLevel),
			"overall_mean_w", fmt.Sprintf("%.0f", cm.overallMean))
	}
	return nil
}

// trainFrom builds this circuit's bucket profiles, wide-window mean, and
// recency-weighted level from a batch of historical samples.
func (cm *CircuitModel) trainFrom(samples []Sample, to time.Time) {
	cm.Buckets = make(map[bucketKey]*bucketStats)
	if len(samples) == 0 {
		cm.overallMean = 0
		cm.recentLevel = 0
		return
	}

	bucketValues := make(map[bucketKey][]float64)
	var rawSum, weightedSum, weightSum float64
	for _, s := range samples {
		key := cm.key(s.Time)
		b, ok := cm.Buckets[key]
		if !ok {
			b = &bucketStats{}
			cm.Buckets[key] = b
		}
		b.Sum += s.Value
		b.Count++
		bucketValues[key] = append(bucketValues[key], s.Value)

		rawSum += s.Value
		w := decayWeight(to.Sub(s.Time).Hours()/24, cm.recencyHalfLifeDays)
		weightedSum += w * s.Value
		weightSum += w
	}

	cm.overallMean = rawSum / float64(len(samples))
	if weightSum > 0 {
		cm.recentLevel = weightedSum / weightSum
	} else {
		cm.recentLevel = cm.overallMean
	}
	for key, b := range cm.Buckets {
		b.Pct = percentileOf(bucketValues[key], cm.percentile)
	}
}

// decayWeight is the exponential recency weight for a sample ageDays old,
// halving every halfLifeDays. halfLifeDays <= 0 disables decay (flat weight).
func decayWeight(ageDays, halfLifeDays float64) float64 {
	if halfLifeDays <= 0 {
		return 1
	}
	return math.Exp(-math.Ln2 * ageDays / halfLifeDays)
}

// percentileOf returns the p-th percentile (0-1) of values via linear
// interpolation between closest ranks. values is not mutated.
func percentileOf(values []float64, p float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	if len(sorted) == 1 {
		return sorted[0]
	}
	rank := p * float64(len(sorted)-1)
	lo := int(math.Floor(rank))
	hi := int(math.Ceil(rank))
	if lo == hi {
		return sorted[lo]
	}
	frac := rank - float64(lo)
	return sorted[lo] + frac*(sorted[hi]-sorted[lo])
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
		Season: SeasonOf(t, cm.southernHemisphere),
		DOW:    -1,
	}
	if cm.Category == "routine" {
		k.DOW = int(t.Weekday())
	}
	return k
}

const (
	minSamples = 3 // a bucket needs at least this many samples to be used at all

	// minConfidentSamples is the sample count at which bucketConfidence's
	// coverage term saturates at 1. Higher than minSamples: a bucket that just
	// clears the "usable at all" bar isn't yet trustworthy enough to skip the
	// conservative-margin gate.
	minConfidentSamples = 10

	// maxTrustedShift bounds the fractional divergence between recentLevel and
	// overallMean that shiftConfidence tolerates before bottoming out at 0 — a
	// 50%+ swing between the recency-weighted level and the wide-window mean
	// means the trained shape/level are actively drifting (e.g. a household
	// baseline step-change), not just noisy.
	maxTrustedShift = 0.5
)

func (cm *CircuitModel) predictAt(t time.Time) float64 {
	k := cm.key(t)
	if v, ok := cm.bucketPredict(k); ok {
		return v
	}
	// Fallback: drop DOW for routine circuits
	if cm.Category == "routine" {
		fb := bucketKey{Hour: k.Hour, Season: k.Season, DOW: -1}
		if v, ok := cm.bucketPredict(fb); ok {
			return v
		}
	}
	// Cold start / thin bucket with no fallback: prefer the recency-tracked
	// level (still informative — it saw real recent samples) over the
	// hardcoded Default, which is only reached with zero training data.
	if cm.recentLevel > 0 {
		return cm.recentLevel
	}
	return cm.Default
}

// bucketPredict returns the bucket's LEVEL × SHAPE prediction — the
// recency-weighted overall level scaled by the bucket's percentile-headroom
// ratio to the wide-window mean — stepped up by conservativeMargin when the
// bucket's confidence is below confidenceThreshold. ok is false when the
// bucket doesn't have minSamples yet, so the caller should fall back.
func (cm *CircuitModel) bucketPredict(k bucketKey) (float64, bool) {
	b, ok := cm.Buckets[k]
	if !ok || b.Count < minSamples {
		return 0, false
	}

	level := cm.recentLevel
	if level <= 0 {
		level = cm.overallMean
	}
	ratio := 1.0
	if cm.overallMean > 0 {
		ratio = b.Pct / cm.overallMean
	}
	predicted := level * ratio

	if cm.confidenceThreshold > 0 && cm.bucketConfidence(k) < cm.confidenceThreshold {
		predicted *= cm.conservativeMargin
	}
	return predicted, true
}

// bucketConfidence is the per-bucket analogue of confidence(): how much a
// single bucket's prediction should be trusted, combining sample-count
// coverage with the shift signal (both bottom out at 0; the bucket is only
// as trustworthy as the model's weakest signal).
func (cm *CircuitModel) bucketConfidence(k bucketKey) float64 {
	b, ok := cm.Buckets[k]
	if !ok {
		return 0
	}
	sampleConf := math.Min(1.0, float64(b.Count)/float64(minConfidentSamples))
	return math.Min(sampleConf, cm.shiftConfidence())
}

// shiftConfidence returns 1 when the recency-weighted LEVEL closely tracks
// the wide-window overall mean, decaying to 0 as they diverge. Plain
// sample-count confidence can't see a distribution shift: every bucket stays
// "well sampled" while the household baseline moves, since counts don't
// change — this does, because a genuine step-change pulls recentLevel away
// from overallMean.
func (cm *CircuitModel) shiftConfidence() float64 {
	if cm.overallMean <= 0 {
		return 1
	}
	shift := math.Abs(cm.recentLevel-cm.overallMean) / cm.overallMean
	return math.Max(0, 1-shift/maxTrustedShift)
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
	coverage := math.Min(1.0, float64(filled)/float64(total))
	return coverage * cm.shiftConfidence()
}
