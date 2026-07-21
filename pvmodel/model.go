// Package pvmodel is the persistent PV-response model: the years-long learning
// asset that predicts solar output from Open-Meteo tilted irradiance (GTI)
// without Solcast. It learns the transfer function
//
//	PV_pred(t) = f(bin(t)) · kWp_ref · (Σ_sites GTI_site(t)) / 1000
//
// where kWp_ref is a single self-calibrating clear-sky reference capacity and
// f(bin) is a dimensionless per-bin performance factor. Only TOTAL PV is
// measured (one metrics series), so the model is identified against the SUMMED
// per-site GTI rather than per-site — the summed plane-of-array irradiance still
// captures the combined shape of the two roof orientations.
//
// The learning core is a table of (half-month-of-year × hour-of-day) = 576 bins,
// each holding wall-clock exponentially-decayed sufficient statistics. Because a
// bin is only revisited for ~15 days a year, decay is anchored to each sample's
// timestamp (lazy, time-based) rather than a per-update factor: a mature 3-year
// bin holds ~30 effective hour-samples, and old mass fades on a ~2.5-year
// half-life. This is what lets the model keep maturing across years even though
// the underlying metrics store only retains ~60 days.
//
// All binning is done in UTC. Ingest (GTIPoint.Time is period-start UTC) and
// prediction therefore share one convention, so f(bin) captures the true diurnal
// shape regardless of the caller's zone.
package pvmodel

import (
	"context"
	"log/slog"
	"math"
	"sync"
	"time"

	"energy-optimiser/config"
)

const (
	numHalfMonths = 24
	numHours      = 24
	numBins       = numHalfMonths * numHours // 576

	// factorMin/factorMax bound f(bin) so a poisoned or thin bin cannot produce
	// an absurd prediction. coldFactor is the final fallback when neither the bin
	// nor the global aggregate has enough decayed mass — deliberately 0.8 (a
	// conservative real-world performance ratio), never 1.0 which would err
	// optimistic and under-charge the battery.
	factorMin  = 0.15
	factorMax  = 2.0
	coldFactor = 0.8

	// gtiThresholdWm2 is the summed-GTI floor (W/m²) below which an hour carries
	// no usable signal (night / near-zero irradiance) and is excluded.
	gtiThresholdWm2 = 20.0

	// kWpRefPercentile picks the clear-sky reference capacity from the upper tail
	// of PV/(ΣGTI/1000): robust to outliers, close to the best-case (~nameplate)
	// output while f(bin) then carries the typical performance fraction.
	kWpRefPercentile = 0.95
)

// Sample is a timestamped measured-power reading (watts). The consumer (W4)
// adapts its metrics client to PVHistory so Model never depends on the client
// type directly.
type Sample struct {
	Time  time.Time
	Value float64 // watts
}

// PVHistory reads measured total-PV power (watts) over a half-open [from, to)
// range. The real implementation wraps the metrics client and binds the PV
// entity id; tests supply a fake.
type PVHistory interface {
	PVPower(ctx context.Context, from, to time.Time) ([]Sample, error)
}

// binStats holds one bin's wall-clock-decayed sufficient statistics. SumGTI is
// Σ(ΣGTI/1000) — the irradiance-energy proxy in kW-per-kWp units — so
// SumPV/SumGTI is the ratio-of-sums with kWp_ref cancelled (see factorFor).
type binStats struct {
	SumPV      float64   `json:"sum_pv"`      // Σ decayed measured PV (kW)
	SumGTI     float64   `json:"sum_gti"`     // Σ decayed ΣGTI/1000 (kW per kWp)
	NEff       float64   `json:"n_eff"`       // decayed effective sample count
	LastUpdate time.Time `json:"last_update"` // decay clock anchor for this bin
}

// add applies lazy wall-clock decay to the existing mass (by 2^(−Δt/halfLife),
// Δt = at − LastUpdate) and then folds in one new unit-weight sample at time at.
// Anchoring to the sample time (not a fixed per-update factor) is essential: a
// bin skipped for ~350 days must decay by that full gap, not by one step.
func (b *binStats) add(pv, gti float64, at time.Time, halfLifeDays float64) {
	if !b.LastUpdate.IsZero() {
		if dtDays := at.Sub(b.LastUpdate).Hours() / 24; dtDays > 0 {
			f := math.Exp2(-dtDays / halfLifeDays)
			b.SumPV *= f
			b.SumGTI *= f
			b.NEff *= f
		}
	}
	b.SumPV += pv
	b.SumGTI += gti
	b.NEff += 1
	b.LastUpdate = at
}

// Model is the persistent PV-response model. An internal RWMutex guards the
// learned state (bins, global, kWp_ref, raw): Update takes the write lock, while
// PredictKW/Maturity take the read lock — so the tick thread can read predictions
// while a background refresh writes.
type Model struct {
	dataDir         string
	halfLifeDays    float64
	calibrationDays int
	minSamples      float64
	maxPVKW         float64 // physical ceiling on PredictKW (kW); 0 = unbounded

	mu              sync.RWMutex
	kWpRef          float64  // clear-sky reference capacity (kWp)
	lastIngestedDay day      // watermark: last COMPLETE day folded in (UTC)
	global          binStats // decayed aggregate over all bins (fallback source)
	bins            [numBins]binStats
	raw             []RawRecord // in-memory raw hourly aggregates (kWp_ref window + refit)
}

// New loads the model from cfg.DataDir, or cold-starts cleanly if the state file
// is missing or corrupt (logged, never a panic). It returns an error only when
// the data directory itself cannot be prepared.
func New(cfg config.PVModel) (*Model, error) {
	m := &Model{
		dataDir:         cfg.DataDir,
		halfLifeDays:    cfg.HalfLifeDays,
		calibrationDays: cfg.CalibrationDays,
		minSamples:      cfg.MinSamples,
		maxPVKW:         cfg.MaxPVKW,
	}
	if err := ensureDir(cfg.DataDir); err != nil {
		return nil, err
	}
	m.load()
	m.raw = m.loadRaw()
	return m, nil
}

// PredictKW returns modeled PV (kW) for the hour starting at t given that hour's
// per-site GTI map, as f(bin(t))·kWp_ref·(Σgti)/1000. The bin's factor is used
// when the bin is mature, else the global decayed ratio, else the cold factor.
// The result is clamped to [0, maxPVKW] when maxPVKW > 0: a physical ceiling so a
// kWp_ref inflated by a sustained PV-sensor fault cannot yield an absurd forecast.
func (m *Model) PredictKW(t time.Time, gti map[string]float64) float64 {
	sumGTI := 0.0
	for _, v := range gti {
		sumGTI += v
	}
	m.mu.RLock()
	kw := m.factorAt(t) * (m.kWpRef * sumGTI / 1000.0)
	m.mu.RUnlock()
	if kw < 0 {
		kw = 0
	}
	if m.maxPVKW > 0 && kw > m.maxPVKW {
		kw = m.maxPVKW
	}
	return kw
}

// Maturity reports 0..1 trust for the bin at t from its decayed effective sample
// count: NEff/(NEff+minSamples), reaching 0.5 at minSamples. W4 uses this to
// blend Solcast toward the learned model as bins mature.
func (m *Model) Maturity(t time.Time) float64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	b := &m.bins[binIndex(t)]
	if b.NEff <= 0 {
		return 0
	}
	return b.NEff / (b.NEff + m.minSamples)
}

// factorAt resolves f for the bin at t: the bin's clamped ratio when it holds
// enough decayed mass, else the global decayed ratio, else the cold factor.
// The caller must hold m.mu (read or write): it reads bins/global/kWpRef.
func (m *Model) factorAt(t time.Time) float64 {
	if b := &m.bins[binIndex(t)]; b.NEff >= m.minSamples {
		if f, ok := factorFor(b, m.kWpRef); ok {
			return f
		}
	}
	if f, ok := factorFor(&m.global, m.kWpRef); ok && m.global.NEff >= m.minSamples {
		return f
	}
	return coldFactor
}

// factorFor derives the dimensionless performance factor for a bin. Storing
// SumGTI WITHOUT kWp_ref means SumPV/SumGTI is the direct PV-to-irradiance
// ratio-of-sums; dividing by kWp_ref yields f, and PredictKW multiplies kWp_ref
// straight back — so kWp_ref cancels for an in-range bin and prediction is
// robust to kWp_ref drift between updates. The clamp is the only place kWp_ref
// bites, which is exactly the runaway-bin case it must bound.
func factorFor(b *binStats, kWpRef float64) (float64, bool) {
	if kWpRef <= 0 || b.SumGTI <= 0 {
		return 0, false
	}
	return clampFactor(b.SumPV / (kWpRef * b.SumGTI)), true
}

func clampFactor(f float64) float64 {
	switch {
	case f < factorMin:
		return factorMin
	case f > factorMax:
		return factorMax
	default:
		return f
	}
}

// binIndex maps an instant to its (half-month, hour) bin in [0, numBins). Days
// 1–15 are the first half of a month, 16+ the second.
func binIndex(t time.Time) int {
	u := t.UTC()
	half := (int(u.Month()) - 1) * 2
	if u.Day() >= 16 {
		half++
	}
	return half*numHours + u.Hour()
}

// ingest folds one validated hourly aggregate into its bin and the global
// aggregate, both decayed to the sample's own timestamp.
func (m *Model) ingest(r RawRecord) {
	pv := r.PVMeanKW
	gti := r.GTISum / 1000.0
	m.bins[binIndex(r.Hour)].add(pv, gti, r.Hour, m.halfLifeDays)
	m.global.add(pv, gti, r.Hour, m.halfLifeDays)
}

// coldStartDay is the first day ingested when there is no watermark yet: the
// calibration window ending at yesterday.
func (m *Model) coldStartDay(now time.Time) day {
	return dayOf(now).add(-m.calibrationDays)
}

func (m *Model) logColdStart(reason string, err error) {
	slog.Warn("pvmodel: cold start", "reason", reason, "error", err)
}
