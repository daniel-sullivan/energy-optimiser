package pvmodel

import (
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	"energy-optimiser/forecast"
)

const (
	// minDaytimeHours is the fewest daytime (ΣGTI ≥ threshold) hours a day must
	// have to carry any usable signal.
	minDaytimeHours = 2
	// coverageFrac is the minimum fraction of daytime hours that must carry at
	// least one PV sample; below it the day is treated as a metrics gap.
	coverageFrac = 0.5
	// pvDropoutFloorKW: if peak measured PV across daytime hours is below this
	// while irradiance is clearly present, the PV sensor dropped out (flatlined
	// zero) and the day is excluded so it cannot drag the ratio toward zero.
	pvDropoutFloorKW = 0.05
)

// hourAgg accumulates PV samples falling in one hour.
type hourAgg struct {
	sum   float64 // watts
	count int
}

func (h hourAgg) meanKW() float64 {
	if h.count == 0 {
		return 0
	}
	return (h.sum / float64(h.count)) / 1000.0
}

// Update performs a watermark-idempotent ingest of every COMPLETE past day in
// (lastIngestedDay, yesterday], recomputes kWp_ref over the rolling calibration
// window, folds validated hourly aggregates into the decayed bins, appends the
// raw aggregates, and persists atomically. Re-running with the same now and the
// same history is an exact no-op — the watermark makes re-ingest of a day
// impossible, so a crash/restart never double-counts. now is supplied by the
// caller so the method is deterministic under test.
func (m *Model) Update(ctx context.Context, pv PVHistory, weather *forecast.WeatherForecast, now time.Time) error {
	if weather == nil {
		return fmt.Errorf("pvmodel: nil weather forecast")
	}
	yesterday := dayOf(now).add(-1)

	firstDay := m.lastIngestedDay.add(1)
	if m.lastIngestedDay.zero() {
		firstDay = m.coldStartDay(now)
	}
	if firstDay.after(yesterday) {
		return nil // nothing new to ingest — idempotent no-op
	}

	from := firstDay.time()
	to := yesterday.add(1).time() // half-open: exclusive end at midnight after yesterday
	samples, err := pv.PVPower(ctx, from, to)
	if err != nil {
		return fmt.Errorf("pvmodel: reading PV history: %w", err)
	}

	gtiByHour := indexGTI(weather)
	pvByHour := aggregateHourly(samples)

	// The network read above is lock-free (only Update mutates the watermark, and
	// Updates are single-flighted by the caller); the write lock guards the shared
	// learned state against concurrent tick-thread PredictKW/Maturity reads.
	m.mu.Lock()
	defer m.mu.Unlock()

	var newRecords []RawRecord
	for d := firstDay; !d.after(yesterday); d = d.add(1) {
		recs, ok := dayRecords(d, gtiByHour, pvByHour, m.maxPVKW)
		if ok {
			for _, r := range recs {
				m.ingest(r)
				m.raw = append(m.raw, r)
				newRecords = append(newRecords, r)
			}
		}
		// The watermark advances past bad days too, so a poisoned day is skipped
		// once and never retried — retrying could re-poison on later runs.
	}
	m.lastIngestedDay = yesterday

	if ref, ok := computeKWpRef(m.raw, now.AddDate(0, 0, -m.calibrationDays), now); ok {
		m.kWpRef = ref
	}

	// Persist state (incl. the advanced watermark) BEFORE appending raw: a crash
	// between the two then leaves the watermark advanced, so the just-ingested days
	// won't re-ingest and re-append on restart (which would duplicate raw records
	// and bias the kWp_ref percentile). The raw log trails the watermark by at most
	// one crash-interrupted batch — a lost record is far cheaper than a duplicated one.
	if err := m.persist(); err != nil {
		return err
	}
	return m.appendRaw(newRecords)
}

// indexGTI folds a forecast into hour-start-UTC → summed per-site GTI (W/m²).
func indexGTI(weather *forecast.WeatherForecast) map[int64]float64 {
	out := make(map[int64]float64, len(weather.Points))
	for _, p := range weather.Points {
		hour := p.Time.UTC().Truncate(time.Hour)
		sum := 0.0
		for _, v := range p.GTI {
			sum += v
		}
		out[hour.Unix()] = sum
	}
	return out
}

// aggregateHourly buckets raw PV samples into hour-start-UTC → mean/count.
func aggregateHourly(samples []Sample) map[int64]hourAgg {
	out := make(map[int64]hourAgg)
	for _, s := range samples {
		key := s.Time.UTC().Truncate(time.Hour).Unix()
		a := out[key]
		a.sum += s.Value
		a.count++
		out[key] = a
	}
	return out
}

// dayRecords builds the validated hourly aggregates for one UTC day, or reports
// the day invalid (ok=false) when it fails ingest-validation: too little
// daylight signal, a metrics gap, a flatlined PV sensor while irradiance was
// present, or a sustained HIGH fault (peak PV above the physical ceiling maxPVKW,
// e.g. a W↔kW unit relabel or stuck-high reading). Only daytime hours
// (ΣGTI ≥ threshold) that carry a PV sample produce records; night/near-zero
// hours carry no signal and are dropped. maxPVKW ≤ 0 disables the high-side reject.
func dayRecords(d day, gtiByHour map[int64]float64, pvByHour map[int64]hourAgg, maxPVKW float64) ([]RawRecord, bool) {
	base := d.time()
	var daytime int
	var covered int
	var peakPV float64
	var recs []RawRecord

	for h := 0; h < numHours; h++ {
		ts := base.Add(time.Duration(h) * time.Hour)
		gti := gtiByHour[ts.Unix()]
		if gti < gtiThresholdWm2 {
			continue
		}
		daytime++
		agg, has := pvByHour[ts.Unix()]
		if !has || agg.count == 0 {
			continue
		}
		covered++
		pvKW := agg.meanKW()
		if pvKW > peakPV {
			peakPV = pvKW
		}
		recs = append(recs, RawRecord{Hour: ts, PVMeanKW: pvKW, GTISum: gti, Samples: agg.count})
	}

	if daytime < minDaytimeHours {
		return nil, false // not enough daylight to learn from
	}
	if float64(covered) < coverageFrac*float64(daytime) {
		return nil, false // metrics gap over the daytime window
	}
	if peakPV < pvDropoutFloorKW {
		return nil, false // PV sensor flatlined while the sun was up
	}
	if maxPVKW > 0 && peakPV > maxPVKW {
		return nil, false // sustained HIGH fault (unit relabel / stuck-high) — keep it out of kWp_ref
	}
	return recs, true
}

// computeKWpRef sets kWp_ref to the upper-percentile of PV/(ΣGTI/1000) over the
// half-open [from, to) window. Using a high percentile yields the clear-sky
// (best-case, ~nameplate) reference capacity while f(bin) then carries the
// typical performance fraction. Returns ok=false when the window holds no usable
// samples, so the caller keeps the previous reference.
func computeKWpRef(raw []RawRecord, from, to time.Time) (float64, bool) {
	var ratios []float64
	for _, r := range raw {
		if r.Hour.Before(from) || !r.Hour.Before(to) {
			continue
		}
		if r.GTISum < gtiThresholdWm2 {
			continue
		}
		ratios = append(ratios, r.PVMeanKW/(r.GTISum/1000.0))
	}
	if len(ratios) == 0 {
		return 0, false
	}
	return percentile(ratios, kWpRefPercentile), true
}

// percentile returns the p-quantile (0..1) via nearest-rank on a sorted copy.
func percentile(vals []float64, p float64) float64 {
	s := append([]float64(nil), vals...)
	sort.Float64s(s)
	idx := int(math.Ceil(p * float64(len(s)-1)))
	if idx < 0 {
		idx = 0
	}
	if idx >= len(s) {
		idx = len(s) - 1
	}
	return s[idx]
}
