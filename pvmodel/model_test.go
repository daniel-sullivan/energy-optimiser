package pvmodel

import (
	"context"
	"math"
	"sync"
	"testing"
	"time"

	"energy-optimiser/config"
	"energy-optimiser/forecast"
)

// fakeHistory is a deterministic PVHistory: it returns the pre-baked samples
// whose Time falls in the requested [from, to) range.
type fakeHistory struct {
	samples []Sample
	calls   int
}

func (f *fakeHistory) PVPower(_ context.Context, from, to time.Time) ([]Sample, error) {
	f.calls++
	var out []Sample
	for _, s := range f.samples {
		if !s.Time.Before(from) && s.Time.Before(to) {
			out = append(out, s)
		}
	}
	return out, nil
}

func testConfig(dir string) config.PVModel {
	return config.PVModel{DataDir: dir, HalfLifeDays: 900, CalibrationDays: 21, MinSamples: 12}
}

// bellGTI is a crude clear-sky diurnal shape (W/m² per site) peaking at UTC noon.
func bellGTI(hour int) float64 {
	x := float64(hour) - 12.0
	v := 1000.0 * math.Exp(-x*x/18.0)
	if v < 1 {
		return 0
	}
	return v
}

// perf models the true per-hour performance fraction: it peaks near 1.0 around
// solar noon and sags off-peak, so the daily mean is ~0.8 while the upper tail
// (kWp_ref's percentile) sits near nameplate.
func perf(hour int) float64 {
	x := float64(hour) - 12.0
	return 0.72 + 0.28*math.Exp(-x*x/40.0)
}

const (
	testKWp   = 6.0 // planted nameplate capacity (kWp)
	testSites = 2   // two orientations summed
)

// syntheticData builds a weather forecast and matching PV history over `days`
// complete UTC days ending the day before `now`. Each daytime hour gets a PV
// reading of perf·kWp·ΣGTI/1000 (in watts). dropoutDay, if in range, is
// flatlined to zero PV while irradiance is present.
func syntheticData(now time.Time, days int, dropoutDay day) (*forecast.WeatherForecast, *fakeHistory) {
	var points []forecast.GTIPoint
	var samples []Sample
	first := dayOf(now).add(-days)
	yesterday := dayOf(now).add(-1)
	for d := first; !d.after(yesterday); d = d.add(1) {
		base := d.time()
		for h := 0; h < 24; h++ {
			ts := base.Add(time.Duration(h) * time.Hour)
			g := bellGTI(h)
			// Per-site GTI; the model sums them. Both sites share the shape here.
			points = append(points, forecast.GTIPoint{
				Time: ts,
				GTI:  map[string]float64{"east": g, "west": g},
			})
			sumGTI := g * testSites
			if sumGTI < gtiThresholdWm2 {
				continue
			}
			pvWatts := perf(h) * testKWp * sumGTI / 1000.0 * 1000.0
			if d == dropoutDay {
				pvWatts = 0 // sensor dropout
			}
			// Four 15-minute samples per hour, identical value -> mean unchanged.
			for q := 0; q < 4; q++ {
				samples = append(samples, Sample{Time: ts.Add(time.Duration(q*15) * time.Minute), Value: pvWatts})
			}
		}
	}
	return &forecast.WeatherForecast{FetchedAt: now, Points: points}, &fakeHistory{samples: samples}
}

// --- Lazy wall-clock decay: the make-or-break test ---------------------------

func TestLazyWallClockDecay(t *testing.T) {
	var b binStats
	const halfLife = 900.0
	t0 := time.Date(2020, 1, 1, 12, 0, 0, 0, time.UTC)
	b.add(1.0, 1.0, t0, halfLife)
	if b.SumPV != 1.0 || b.NEff != 1.0 {
		t.Fatalf("after first add: SumPV=%v NEff=%v, want 1,1", b.SumPV, b.NEff)
	}

	t1 := t0.AddDate(0, 0, 365) // exactly one year later
	b.add(2.0, 1.0, t1, halfLife)

	decay := math.Exp2(-365.0 / halfLife) // ≈ 0.754
	wantPV := 1.0*decay + 2.0
	wantN := 1.0*decay + 1.0
	if math.Abs(b.SumPV-wantPV) > 1e-9 {
		t.Errorf("SumPV=%v, want %v (old mass must decay by 2^(-365/900))", b.SumPV, wantPV)
	}
	if math.Abs(b.NEff-wantN) > 1e-9 {
		t.Errorf("NEff=%v, want %v", b.NEff, wantN)
	}
	// Guardrails: not undecayed (3.0) and not fully gone (2.0).
	if math.Abs(b.SumPV-3.0) < 1e-6 {
		t.Errorf("old mass was NOT decayed at all")
	}
	if math.Abs(b.SumPV-2.0) < 1e-6 {
		t.Errorf("old mass fully vanished — over-decayed")
	}
	if !b.LastUpdate.Equal(t1) {
		t.Errorf("LastUpdate=%v, want %v", b.LastUpdate, t1)
	}
}

// --- Persistence round-trip ---------------------------------------------------

func TestPersistenceRoundTrip(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2024, 6, 15, 3, 0, 0, 0, time.UTC)

	m, err := New(testConfig(dir))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	weather, hist := syntheticData(now, 21, day{})
	if err := m.Update(context.Background(), hist, weather, now); err != nil {
		t.Fatalf("Update: %v", err)
	}
	if m.kWpRef <= 0 {
		t.Fatalf("kWpRef not calibrated: %v", m.kWpRef)
	}

	m2, err := New(testConfig(dir))
	if err != nil {
		t.Fatalf("reload New: %v", err)
	}
	if m2.kWpRef != m.kWpRef {
		t.Errorf("kWpRef not restored: got %v want %v", m2.kWpRef, m.kWpRef)
	}
	if m2.lastIngestedDay != m.lastIngestedDay {
		t.Errorf("watermark not restored: got %+v want %+v", m2.lastIngestedDay, m.lastIngestedDay)
	}
	if !binStatsEqual(m2.global, m.global) {
		t.Errorf("global not restored: got %+v want %+v", m2.global, m.global)
	}
	for i := range m.bins {
		if !binStatsEqual(m2.bins[i], m.bins[i]) {
			t.Fatalf("bin %d not restored: got %+v want %+v", i, m2.bins[i], m.bins[i])
		}
	}
}

// --- Watermark idempotence ----------------------------------------------------

func TestWatermarkIdempotence(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2024, 6, 15, 3, 0, 0, 0, time.UTC)

	m, err := New(testConfig(dir))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	weather, hist := syntheticData(now, 21, day{})

	if err := m.Update(context.Background(), hist, weather, now); err != nil {
		t.Fatalf("first Update: %v", err)
	}
	firstBins := m.bins
	firstGlobal := m.global
	firstRef := m.kWpRef
	firstRaw := len(m.raw)
	callsAfterFirst := hist.calls

	if err := m.Update(context.Background(), hist, weather, now); err != nil {
		t.Fatalf("second Update: %v", err)
	}
	if hist.calls != callsAfterFirst {
		t.Errorf("second Update queried history again (%d calls) — not a no-op", hist.calls-callsAfterFirst)
	}
	if len(m.raw) != firstRaw {
		t.Errorf("second Update appended raw records: %d -> %d", firstRaw, len(m.raw))
	}
	if m.kWpRef != firstRef {
		t.Errorf("kWpRef changed on no-op Update: %v -> %v", firstRef, m.kWpRef)
	}
	if !binStatsEqual(m.global, firstGlobal) {
		t.Errorf("global changed on no-op Update")
	}
	for i := range m.bins {
		if !binStatsEqual(m.bins[i], firstBins[i]) {
			t.Fatalf("bin %d double-counted on re-Update: %+v -> %+v", i, firstBins[i], m.bins[i])
		}
	}
}

// --- Calibration + dropout exclusion -----------------------------------------

func TestCalibration(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2024, 6, 15, 3, 0, 0, 0, time.UTC)
	dropout := dayOf(now).add(-5)

	m, err := New(testConfig(dir))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	weather, hist := syntheticData(now, 21, dropout)
	if err := m.Update(context.Background(), hist, weather, now); err != nil {
		t.Fatalf("Update: %v", err)
	}

	// kWp_ref ≈ planted nameplate (upper tail of the performance curve ~1.0·kWp).
	if math.Abs(m.kWpRef-testKWp) > 0.15*testKWp {
		t.Errorf("kWpRef=%.3f, want ≈%.1f", m.kWpRef, testKWp)
	}

	// A midday bin's factor ≈ perf at that hour (near 1.0 at noon).
	noon := time.Date(2024, 6, 10, 12, 0, 0, 0, time.UTC)
	fNoon := m.factorAt(noon)
	if math.Abs(fNoon-perf(12)) > 0.1 {
		t.Errorf("noon factor=%.3f, want ≈%.3f", fNoon, perf(12))
	}

	// The dropout day must have been excluded: no raw record lands on it.
	for _, r := range m.raw {
		if dayOf(r.Hour) == dropout {
			t.Fatalf("dropout day %+v was ingested (raw record at %v)", dropout, r.Hour)
		}
	}
	// And a good neighbouring day IS present.
	good := dayOf(now).add(-6)
	found := false
	for _, r := range m.raw {
		if dayOf(r.Hour) == good {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("good day %+v missing from raw history", good)
	}
}

// --- Prediction + fallback ----------------------------------------------------

func TestPredictKWAndFallback(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2024, 6, 15, 3, 0, 0, 0, time.UTC)

	m, err := New(testConfig(dir))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	weather, hist := syntheticData(now, 21, day{})
	if err := m.Update(context.Background(), hist, weather, now); err != nil {
		t.Fatalf("Update: %v", err)
	}

	// A trained midday bin: prediction ≈ perf·kWp·ΣGTI/1000.
	noon := time.Date(2024, 6, 10, 12, 0, 0, 0, time.UTC)
	g := bellGTI(12)
	gti := map[string]float64{"east": g, "west": g}
	sumGTI := g * testSites
	wantKW := perf(12) * testKWp * sumGTI / 1000.0
	gotKW := m.PredictKW(noon, gti)
	if math.Abs(gotKW-wantKW) > 0.1*wantKW {
		t.Errorf("PredictKW=%.3f kW, want ≈%.3f kW", gotKW, wantKW)
	}

	// Maturity of the trained noon bin should be well above zero.
	if mat := m.Maturity(noon); mat <= 0 {
		t.Errorf("trained bin maturity=%.3f, want >0", mat)
	}

	// Unseen bin (a half-month with no data, e.g. December) falls back to the
	// global decayed ratio rather than any bin factor.
	dec := time.Date(2024, 12, 10, 12, 0, 0, 0, time.UTC)
	if m.bins[binIndex(dec)].NEff != 0 {
		t.Fatalf("expected December bin to be empty")
	}
	globalFactor, ok := factorFor(&m.global, m.kWpRef)
	if !ok {
		t.Fatalf("global factor unavailable")
	}
	if math.Abs(m.factorAt(dec)-globalFactor) > 1e-9 {
		t.Errorf("unseen-bin factor=%.4f, want global %.4f", m.factorAt(dec), globalFactor)
	}
	// Maturity of the unseen bin is zero.
	if mat := m.Maturity(dec); mat != 0 {
		t.Errorf("unseen bin maturity=%.3f, want 0", mat)
	}
}

// --- Cold factor tier (empty bin AND empty global) ---------------------------

func TestColdFactor(t *testing.T) {
	m := &Model{minSamples: 12, kWpRef: 5.0} // no bins, no global mass
	t0 := time.Date(2024, 6, 10, 12, 0, 0, 0, time.UTC)
	if f := m.factorAt(t0); f != coldFactor {
		t.Errorf("cold factor=%.3f, want %.3f", f, coldFactor)
	}
}

// --- Cold start from missing/corrupt state -----------------------------------

func TestColdStartMissingAndCorrupt(t *testing.T) {
	dir := t.TempDir()
	m, err := New(testConfig(dir))
	if err != nil {
		t.Fatalf("New on empty dir: %v", err)
	}
	if !m.lastIngestedDay.zero() || m.kWpRef != 0 {
		t.Errorf("expected empty cold-start model, got watermark=%+v kWpRef=%v", m.lastIngestedDay, m.kWpRef)
	}

	// Corrupt the state file; New must still cold-start cleanly, not error/panic.
	if err := atomicWrite(m.statePath(), []byte("{not json")); err != nil {
		t.Fatalf("write corrupt state: %v", err)
	}
	m2, err := New(testConfig(dir))
	if err != nil {
		t.Fatalf("New on corrupt state: %v", err)
	}
	if !m2.lastIngestedDay.zero() {
		t.Errorf("corrupt state should cold-start empty, got %+v", m2.lastIngestedDay)
	}
}

// binStatsEqual compares two bins, using time.Equal for the decay-clock anchor
// (JSON round-trips wall time but reflect.DeepEqual is brittle on time.Time).
func binStatsEqual(a, b binStats) bool {
	return a.SumPV == b.SumPV && a.SumGTI == b.SumGTI && a.NEff == b.NEff && a.LastUpdate.Equal(b.LastUpdate)
}

// --- M1: physical PV ceiling ---------------------------------------------------

// TestPredictKWClampsToMaxPV: an inflated kWp_ref (a sustained PV-sensor fault
// runaway) drives PredictKW to absurd kW; a maxPVKW ceiling clamps it, and
// maxPVKW=0 leaves it unbounded.
func TestPredictKWClampsToMaxPV(t *testing.T) {
	m := &Model{minSamples: 12, kWpRef: 1000.0, maxPVKW: 25.0} // absurd kWp_ref, no bins → cold factor 0.8
	t0 := time.Date(2024, 6, 10, 12, 0, 0, 0, time.UTC)
	gti := map[string]float64{"east": 900, "west": 900} // ΣGTI = 1800 → unclamped 0.8·1000·1.8 = 1440 kW

	if got := m.PredictKW(t0, gti); got != 25.0 {
		t.Errorf("clamped PredictKW = %.3f kW, want 25.0 (ceiling)", got)
	}
	m.maxPVKW = 0 // unbounded
	if got := m.PredictKW(t0, gti); got <= 25.0 {
		t.Errorf("unbounded PredictKW = %.3f kW, want >25 (no clamp)", got)
	}
}

// TestDayRecordsRejectsHighFault: a day whose peak measured PV exceeds the
// physical ceiling (e.g. a W↔kW unit relabel) is dropped from ingest, so it can't
// contaminate kWp_ref; a ceiling of 0 disables the reject; a normal day passes.
func TestDayRecordsRejectsHighFault(t *testing.T) {
	d := dayOf(time.Date(2024, 6, 10, 0, 0, 0, 0, time.UTC))
	base := d.time()
	gtiByHour := map[int64]float64{}
	pvByHour := map[int64]hourAgg{}
	for h := 8; h <= 16; h++ {
		ts := base.Add(time.Duration(h) * time.Hour)
		gtiByHour[ts.Unix()] = 800                          // clear daytime irradiance
		pvByHour[ts.Unix()] = hourAgg{sum: 60000, count: 1} // 60 kW peak — a stuck-high/relabel fault
	}

	if _, ok := dayRecords(d, gtiByHour, pvByHour, 0); !ok {
		t.Fatal("day should be valid with the ceiling disabled")
	}
	if _, ok := dayRecords(d, gtiByHour, pvByHour, 25); ok {
		t.Error("60 kW-peak day should be rejected under a 25 kW ceiling")
	}

	for h := 8; h <= 16; h++ {
		ts := base.Add(time.Duration(h) * time.Hour)
		pvByHour[ts.Unix()] = hourAgg{sum: 8000, count: 1} // 8 kW — normal
	}
	if _, ok := dayRecords(d, gtiByHour, pvByHour, 25); !ok {
		t.Error("normal 8 kW-peak day should pass the 25 kW ceiling")
	}
}

// --- M2: concurrent read/write safety (relies on -race) -----------------------

// TestConcurrentUpdateAndPredict runs Update (writer) against PredictKW/Maturity
// (readers) concurrently; with the RWMutex it must be race-free under -race.
func TestConcurrentUpdateAndPredict(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2024, 6, 15, 3, 0, 0, 0, time.UTC)
	m, err := New(testConfig(dir))
	if err != nil {
		t.Fatal(err)
	}
	weather, hist := syntheticData(now, 21, day{})

	noon := time.Date(2024, 6, 10, 12, 0, 0, 0, time.UTC)
	g := bellGTI(12)
	gti := map[string]float64{"east": g, "west": g}

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_ = m.PredictKW(noon, gti)
					_ = m.Maturity(noon)
				}
			}
		}()
	}
	if err := m.Update(context.Background(), hist, weather, now); err != nil {
		t.Errorf("Update: %v", err)
	}
	close(stop)
	wg.Wait()
}
