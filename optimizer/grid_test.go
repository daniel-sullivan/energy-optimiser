package optimizer

import (
	"math"
	"os"
	"testing"
	"time"

	_ "time/tzdata" // hermetic tz database for Asia/Tokyo

	"energy-optimiser/config"
)

// parseConfig writes toml to a temp file and parses it (running finalize so the
// time zone and telescoping validation apply), failing the test on error.
func parseConfig(t *testing.T, toml string) *config.Config {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "cfg-*.toml")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(toml); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	cfg, err := config.Parse(f.Name())
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}
	return cfg
}

// telescopingTOML is a small telescoping config: a 6h horizon with a 2h fine
// region (30-min slots) telescoping to 60-min far slots, off-peak all night so a
// single run spans the near→far seam. JST (Asia/Tokyo, no DST) is the default.
const telescopingTOML = `
[service]
poll_interval = "5m"
planning_horizon = "6h"
near_horizon = "2h"
slot_duration = "30m"
far_slot_duration = "60m"

[battery]
capacity_kwh = 9.6
max_charge_kw = 5.0
max_discharge_kw = 5.0
soc_min = 0.20
soc_max = 1.0
efficiency = 0.95

[rates]
currency = "JPY"
peak_rate = 40.0
off_peak_rate = 5.0
feed_in_rate = 1.0

[[rates.off_peak_windows]]
start = "00:00"
end = "07:00"
rate = 5.0

[optimizer]
soc_risk_weight = 10.0
`

func jst(t *testing.T) *time.Location {
	t.Helper()
	loc, err := time.LoadLocation("Asia/Tokyo")
	if err != nil {
		t.Fatal(err)
	}
	return loc
}

// TestUniformParity is the refactor oracle: a uniform (non-telescoping) grid must
// reproduce the pre-refactor schedule bit-for-bit. Golden values were captured
// from the solver before the telescoping change. Two cases: a no-grid-charge
// solve and a grid-charging solve.
func TestUniformParity(t *testing.T) {
	t.Run("no_grid_charge", func(t *testing.T) {
		now := time.Date(2024, 1, 15, 3, 0, 0, 0, time.UTC)
		in := &Input{
			Now: now, NumSlots: 4, SlotMinutes: 30,
			SolarKW: []float64{0, 0, 3, 3}, LoadKW: []float64{1, 1, 1, 1},
			Rates: []float64{0.12, 0.12, 0.35, 0.35}, IsOffPeak: []bool{true, true, false, false},
			CurrentSOC: 0.5,
			Battery:    config.Battery{CapacityKWh: 9.6, MaxChargeKW: 5.0, MaxDischargeKW: 5.0, SOCMin: 0.20, SOCMax: 1.0, Efficiency: 0.95},
			FeedInRate: 0.05, PeakRate: 0.35, SOCRiskWeight: 2.0, MinChargeKW: 1.0, BlipCost: 5.0,
		}
		fillUniform(in)
		s, err := Solve(in)
		if err != nil {
			t.Fatal(err)
		}
		wantObj := -0.0185507058599295
		if math.Abs(s.ObjectiveValue-wantObj) > 1e-9 {
			t.Errorf("objective = %.15g, want %.15g (parity broken)", s.ObjectiveValue, wantObj)
		}
		wantGC := []bool{false, false, false, false}
		wantSOC := []float64{0.5, 0.446563627495565, 0.5, 0.3}
		for i := range s.Slots {
			if s.Slots[i].GridCharge != wantGC[i] {
				t.Errorf("slot %d GridCharge = %v, want %v", i, s.Slots[i].GridCharge, wantGC[i])
			}
			if math.Abs(s.Slots[i].SOC-wantSOC[i]) > 1e-9 {
				t.Errorf("slot %d SOC = %.15g, want %.15g", i, s.Slots[i].SOC, wantSOC[i])
			}
		}
	})

	t.Run("grid_charge", func(t *testing.T) {
		const T = 8
		rates := make([]float64, T)
		off := make([]bool, T)
		solar := make([]float64, T)
		load := make([]float64, T)
		for i := range T {
			load[i] = 1.5
			if i >= 1 && i <= 6 {
				off[i], rates[i] = true, 10.0
			} else {
				rates[i] = 40.0
			}
		}
		in := &Input{
			Now: time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC), NumSlots: T, SlotMinutes: 30,
			SolarKW: solar, LoadKW: load, Rates: rates, IsOffPeak: off, CurrentSOC: 0.25,
			Battery:  config.Battery{CapacityKWh: 9.6, MaxChargeKW: 5, MaxDischargeKW: 5, SOCMin: 0.20, SOCMax: 1.0, Efficiency: 0.95},
			PeakRate: 40.0, SOCRiskWeight: 2.0, MinChargeKW: 1.0, BlipCost: 5.0,
		}
		fillUniform(in)
		s, err := Solve(in)
		if err != nil {
			t.Fatal(err)
		}
		wantObj := 122.822558845972
		if math.Abs(s.ObjectiveValue-wantObj) > 1e-9 {
			t.Errorf("objective = %.15g, want %.15g (parity broken)", s.ObjectiveValue, wantObj)
		}
		wantGC := []bool{false, true, true, false, false, false, false, false}
		wantSOC := []float64{0.2, 0.453822769396067, 0.50458732327528, 0.5, 0.5, 0.5, 0.419845441243347, 0.339690882486695}
		for i := range s.Slots {
			if s.Slots[i].GridCharge != wantGC[i] {
				t.Errorf("slot %d GridCharge = %v, want %v", i, s.Slots[i].GridCharge, wantGC[i])
			}
			if math.Abs(s.Slots[i].SOC-wantSOC[i]) > 1e-9 {
				t.Errorf("slot %d SOC = %.15g, want %.15g", i, s.Slots[i].SOC, wantSOC[i])
			}
		}
	})
}

// TestBuildGridUniformShape: a config with no near_horizon telescopes to nothing —
// every slot is SlotDuration wide (the backward-compat path).
func TestBuildGridUniformShape(t *testing.T) {
	cfg := parseConfig(t, `
[service]
planning_horizon = "2h"
slot_duration = "30m"
[battery]
capacity_kwh = 9.6
[rates]
peak_rate = 40.0
off_peak_rate = 5.0
feed_in_rate = 1.0
`)
	g := BuildGrid(time.Now(), cfg)
	if g.Len() != 4 {
		t.Fatalf("Len = %d, want 4", g.Len())
	}
	for i, h := range g.Hours {
		if h != 0.5 {
			t.Errorf("slot %d width = %v h, want 0.5 (uniform)", i, h)
		}
	}
}

// TestC1HourSnap: with now on a :30 boundary the fine region extends forward to
// the next whole LOCAL hour, so the first far slot starts on the hour.
func TestC1HourSnap(t *testing.T) {
	cfg := parseConfig(t, telescopingTOML)
	loc := jst(t)
	now := time.Date(2024, 1, 15, 1, 30, 0, 0, loc) // :30 boundary
	g := BuildGrid(now, cfg)

	// Locate the first far (1.0h) slot.
	firstFar := -1
	for i, h := range g.Hours {
		if h == 1.0 {
			firstFar = i
			break
		}
	}
	if firstFar < 0 {
		t.Fatal("no far slot found")
	}
	fs := g.Start[firstFar].In(loc)
	if fs.Minute() != 0 || fs.Second() != 0 {
		t.Errorf("first far slot starts at %s, want a whole local hour", fs.Format("15:04:05"))
	}
	// The last fine slot must end exactly where the first far slot begins.
	if !g.End(firstFar - 1).Equal(g.Start[firstFar]) {
		t.Errorf("fine region ends %s, far begins %s — not contiguous", g.End(firstFar-1), g.Start[firstFar])
	}
	// near_horizon = 2h from 01:30 → 03:30, snapped up to 04:00.
	if want := time.Date(2024, 1, 15, 4, 0, 0, 0, loc); !fs.Equal(want) {
		t.Errorf("first far slot = %s, want %s (snap 03:30 → 04:00)", fs, want)
	}
}

// TestFarSlotTariffUniform: every far slot carries a single tariff rate and
// off-peak flag over its whole width (the build-time invariant BuildGrid asserts).
func TestFarSlotTariffUniform(t *testing.T) {
	cfg := parseConfig(t, telescopingTOML)
	loc := jst(t)
	now := time.Date(2024, 1, 15, 0, 0, 0, 0, loc)
	g := BuildGrid(now, cfg)
	for i := range g.Start {
		if g.Hours[i] != 1.0 {
			continue
		}
		start, end := g.Start[i], g.End(i)
		if cfg.Rates.RateAt(start) != cfg.Rates.RateAt(end.Add(-time.Minute)) {
			t.Errorf("far slot %d [%s,%s) straddles a rate change", i, start, end)
		}
		if cfg.Rates.IsOffPeak(start) != cfg.Rates.IsOffPeak(end.Add(-time.Minute)) {
			t.Errorf("far slot %d [%s,%s) straddles an off-peak boundary", i, start, end)
		}
	}
}

// TestBuildGridPanicsOnMisalignedFarSlot: a far slot that would straddle a tariff
// boundary (off-peak window ending mid-hour) must fail loudly. This is the
// last-resort invariant behind config.validateTelescopingGrid's fail-fast — the
// config is built directly here because Parse now rejects such a config at load.
func TestBuildGridPanicsOnMisalignedFarSlot(t *testing.T) {
	cfg := &config.Config{
		Service: config.Service{
			PlanningHorizon: config.Duration{Duration: 6 * time.Hour},
			NearHorizon:     config.Duration{Duration: 2 * time.Hour},
			SlotDuration:    config.Duration{Duration: 30 * time.Minute},
			FarSlotDuration: config.Duration{Duration: 60 * time.Minute},
		},
		Rates: config.Rates{
			PeakRate:    40.0,
			OffPeakRate: 5.0,
			OffPeakWindows: []config.TimeWindow{
				{Start: config.TimeOfDay{Hour: 0, Minute: 0}, End: config.TimeOfDay{Hour: 3, Minute: 30}, Rate: 5.0},
			},
		},
	}
	// UTC (cfg has no loc) so the 03:30 edge is read in the same zone as the grid:
	// far slot [03:00,04:00) straddles it.
	now := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
	defer func() {
		if recover() == nil {
			t.Error("expected panic on a far slot straddling the 03:30 tariff edge")
		}
	}()
	BuildGrid(now, cfg)
}

// TestMixedWidthEnergyAccounting: on a telescoping grid the SOC recursion must
// use each slot's own width — a 60-min far slot moves SOC by power·1h, a 30-min
// near slot by power·0.5h. Verify the identity from the returned schedule.
func TestMixedWidthEnergyAccounting(t *testing.T) {
	cfg := parseConfig(t, telescopingTOML)
	loc := jst(t)
	now := time.Date(2024, 1, 15, 1, 0, 0, 0, loc)

	solarKW := make([]float64, 32)
	loadW := make([]float64, 32)
	for i := range loadW {
		loadW[i] = 2000 // 2 kW, no solar → battery + grid must cover it
	}
	in := PrepareInput(now, cfg, solarKW, loadW, 0.20)
	s, err := Solve(in)
	if err != nil {
		t.Fatal(err)
	}

	sawFar := false
	etaC := math.Sqrt(cfg.Battery.Efficiency)
	etaD := math.Sqrt(cfg.Battery.Efficiency)
	cap := cfg.Battery.CapacityKWh
	prev := math.Min(cfg.Battery.SOCMax, math.Max(0, 0.20))
	for i := range s.Slots {
		sl := s.Slots[i]
		if sl.DurationH == 1.0 {
			sawFar = true
		}
		charge := math.Max(0, sl.BatteryFlowKW)
		discharge := math.Max(0, -sl.BatteryFlowKW)
		wantDelta := (etaC*charge - discharge/etaD) * sl.DurationH / cap
		if got := sl.SOC - prev; math.Abs(got-wantDelta) > 1e-9 {
			t.Errorf("slot %d (%.1fh): ΔSOC = %.12g, want %.12g (energy = power·%.1fh)",
				i, sl.DurationH, got, wantDelta, sl.DurationH)
		}
		prev = sl.SOC
	}
	if !sawFar {
		t.Fatal("no far (1.0h) slot exercised")
	}
}

// TestBlipBudgetAcrossSeam: an off-peak run spanning the near→far (30→60) seam is
// a single contiguous index run, so the ≤1-entry bypass budget covers it — at
// most one grid-charge entry across the whole run.
func TestBlipBudgetAcrossSeam(t *testing.T) {
	cfg := parseConfig(t, telescopingTOML)
	loc := jst(t)
	now := time.Date(2024, 1, 15, 1, 0, 0, 0, loc) // whole hour → fine region ends at 03:00 exactly

	solarKW := make([]float64, 32)
	loadW := make([]float64, 32)
	for i := range loadW {
		loadW[i] = 2000
	}
	in := PrepareInput(now, cfg, solarKW, loadW, 0.20)

	// Confirm the run genuinely spans the seam: a 30-min slot and a 60-min slot are
	// both off-peak and adjacent.
	seam := -1
	for i := 0; i+1 < in.NumSlots; i++ {
		if in.SlotHours[i] == 0.5 && in.SlotHours[i+1] == 1.0 && in.IsOffPeak[i] && in.IsOffPeak[i+1] {
			seam = i
		}
	}
	if seam < 0 {
		t.Fatal("no off-peak run spanning the 30→60 seam in this fixture")
	}

	s, err := Solve(in)
	if err != nil {
		t.Fatal(err)
	}
	entries, prev := 0, false
	for _, sl := range s.Slots {
		if sl.GridCharge && !prev {
			entries++
		}
		prev = sl.GridCharge
	}
	if entries > 1 {
		t.Errorf("got %d bypass entries across the seam-spanning run, want ≤1", entries)
	}
	t.Logf("bypass entries across seam = %d", entries)
}
