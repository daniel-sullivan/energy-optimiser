package optimizer

import (
	"testing"
	"time"

	"energy-optimiser/config"
)

// fillUniform populates SlotStart/SlotHours for a uniform grid of NumSlots slots
// of SlotMinutes width from Now — the shape PrepareInput produces for a
// non-telescoping config. Tests that construct Input directly use this so the
// solver's per-slot Δh terms have their widths.
func fillUniform(in *Input) {
	dur := time.Duration(in.SlotMinutes) * time.Minute
	in.SlotStart = make([]time.Time, in.NumSlots)
	in.SlotHours = make([]float64, in.NumSlots)
	for i := range in.NumSlots {
		in.SlotStart[i] = in.Now.Add(time.Duration(i) * dur)
		in.SlotHours[i] = dur.Hours()
	}
}

func TestSolveBasic(t *testing.T) {
	// Small 4-slot problem: 2h horizon, 30-min slots
	// Slots 0-1: off-peak, slots 2-3: peak
	// Solar: 0, 0, 3kW, 3kW
	// Load: 1kW constant
	// Battery starts at 50% SOC

	now := time.Date(2024, 1, 15, 3, 0, 0, 0, time.UTC) // 3 AM

	input := &Input{
		Now:         now,
		NumSlots:    4,
		SlotMinutes: 30,
		SolarKW:     []float64{0, 0, 3, 3},
		LoadKW:      []float64{1, 1, 1, 1},
		Rates:       []float64{0.12, 0.12, 0.35, 0.35},
		IsOffPeak:   []bool{true, true, false, false},
		CurrentSOC:  0.5,
		Battery: config.Battery{
			CapacityKWh:    9.6,
			MaxChargeKW:    5.0,
			MaxDischargeKW: 5.0,
			SOCMin:         0.20,
			SOCMax:         1.0,
			Efficiency:     0.95,
		},
		FeedInRate:    0.05,
		PeakRate:      0.35,
		SOCRiskWeight: 2.0,
		MinChargeKW:   1.0,
		BlipCost:      5.0,
	}
	fillUniform(input)

	sched, err := Solve(input)
	if err != nil {
		t.Fatal(err)
	}

	if len(sched.Slots) != 4 {
		t.Fatalf("got %d slots, want 4", len(sched.Slots))
	}

	// Verify basic properties
	for i, slot := range sched.Slots {
		// SOC should be within bounds
		if slot.SOC < input.Battery.SOCMin-0.001 || slot.SOC > input.Battery.SOCMax+0.001 {
			t.Errorf("slot %d: SOC %.3f out of bounds [%.2f, %.2f]",
				i, slot.SOC, input.Battery.SOCMin, input.Battery.SOCMax)
		}
		// Grid import should be non-negative
		if slot.GridImportKW < -0.001 {
			t.Errorf("slot %d: negative grid import %.3f", i, slot.GridImportKW)
		}
		// Grid export should be non-negative
		if slot.GridExportKW < -0.001 {
			t.Errorf("slot %d: negative grid export %.3f", i, slot.GridExportKW)
		}
	}

	// Peak slots should not have grid charge
	for i := 2; i < 4; i++ {
		if sched.Slots[i].GridCharge {
			t.Errorf("slot %d (peak): grid_charge should be false", i)
		}
	}

	// With solar surplus in slots 2-3, expect some export or battery charging
	totalExport := sched.Slots[2].GridExportKW + sched.Slots[3].GridExportKW
	totalCharge := sched.Slots[2].BatteryFlowKW + sched.Slots[3].BatteryFlowKW
	if totalExport < 0.01 && totalCharge < 0.01 {
		t.Error("expected some export or battery charging during solar slots")
	}

	t.Logf("objective: $%.4f", sched.ObjectiveValue)
	for i, s := range sched.Slots {
		t.Logf("slot %d: grid_charge=%v flow=%.2fkW import=%.2fkW export=%.2fkW soc=%.3f",
			i, s.GridCharge, s.BatteryFlowKW, s.GridImportKW, s.GridExportKW, s.SOC)
	}
}

func TestContiguityAndNoDischargeInBypass(t *testing.T) {
	// One 6-slot off-peak run (cheap) flanked by expensive peak; low start SOC so
	// grid-charging to cover peak load is attractive.
	const T = 8
	rates := make([]float64, T)
	off := make([]bool, T)
	solar := make([]float64, T)
	load := make([]float64, T)
	for i := range T {
		load[i] = 1.5
		if i >= 1 && i <= 6 {
			off[i] = true
			rates[i] = 10.0 // cheap off-peak
		} else {
			rates[i] = 40.0 // expensive peak
		}
	}
	in := &Input{
		Now:         time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
		NumSlots:    T,
		SlotMinutes: 30,
		SolarKW:     solar,
		LoadKW:      load,
		Rates:       rates,
		IsOffPeak:   off,
		CurrentSOC:  0.25,
		Battery: config.Battery{
			CapacityKWh: 9.6, MaxChargeKW: 5, MaxDischargeKW: 5,
			SOCMin: 0.20, SOCMax: 1.0, Efficiency: 0.95,
		},
		PeakRate: 40.0, SOCRiskWeight: 2.0, MinChargeKW: 1.0, BlipCost: 5.0,
	}
	fillUniform(in)
	sched, err := Solve(in)
	if err != nil {
		t.Fatal(err)
	}

	// Contiguity: at most one bypass entry across the single off-peak run.
	entries := 0
	prev := false
	for _, s := range sched.Slots {
		if s.GridCharge && !prev {
			entries++
		}
		prev = s.GridCharge
	}
	if entries > 1 {
		t.Errorf("got %d bypass entries, want ≤1 (contiguity)", entries)
	}

	// No discharge while in bypass, and min-charge honored when permitted.
	for i, s := range sched.Slots {
		if s.GridCharge {
			if s.BatteryFlowKW < -1e-6 {
				t.Errorf("slot %d: discharging (%.3f kW) while grid-charging", i, s.BatteryFlowKW)
			}
			if s.BatteryFlowKW < in.MinChargeKW-1e-6 {
				t.Errorf("slot %d: charge %.3f kW below min-charge %.1f", i, s.BatteryFlowKW, in.MinChargeKW)
			}
		}
	}
	t.Logf("bypass entries=%d", entries)
}

func TestPeakSlotsNeverGridCharge(t *testing.T) {
	// All-peak horizon, low SOC, heavy load: grid-charging would help, but every
	// slot is peak so grid_charge must stay locked off. Regression for the
	// GLP_BV-clobbers-FX-0 bug (SetColKind must precede SetColBnds).
	const T = 6
	rates := make([]float64, T)
	off := make([]bool, T)
	solar := make([]float64, T)
	load := make([]float64, T)
	for i := range T {
		rates[i] = 40.0
		load[i] = 3.0
	}
	in := &Input{
		Now: time.Date(2024, 1, 15, 14, 0, 0, 0, time.UTC), NumSlots: T, SlotMinutes: 30,
		SolarKW: solar, LoadKW: load, Rates: rates, IsOffPeak: off, CurrentSOC: 0.25,
		Battery:  config.Battery{CapacityKWh: 9.6, MaxChargeKW: 5, MaxDischargeKW: 5, SOCMin: 0.20, SOCMax: 1.0, Efficiency: 0.95},
		PeakRate: 40.0, SOCRiskWeight: 2.0, MinChargeKW: 1.0, BlipCost: 5.0,
	}
	fillUniform(in)
	sched, err := Solve(in)
	if err != nil {
		t.Fatal(err)
	}
	for i, s := range sched.Slots {
		if s.GridCharge {
			t.Errorf("slot %d (peak): grid_charge must be locked off, got true", i)
		}
	}
}

func TestGlitchedSOCAboveMaxDoesNotCrash(t *testing.T) {
	// A sensor glitch (SOC far above SOCMax) must not make the solve infeasible.
	const T = 4
	in := &Input{
		Now: time.Date(2024, 1, 15, 2, 0, 0, 0, time.UTC), NumSlots: T, SlotMinutes: 30,
		SolarKW: make([]float64, T), LoadKW: []float64{1, 1, 1, 1},
		Rates: []float64{21, 21, 21, 21}, IsOffPeak: []bool{true, true, true, true},
		CurrentSOC: 18.5, // 1850% glitch
		Battery:    config.Battery{CapacityKWh: 9.6, MaxChargeKW: 5, MaxDischargeKW: 5, SOCMin: 0.20, SOCMax: 1.0, Efficiency: 0.95},
		PeakRate:   32.78, SOCRiskWeight: 2.0, MinChargeKW: 1.0, BlipCost: 5.0,
	}
	fillUniform(in)
	sched, err := Solve(in)
	if err != nil {
		t.Fatalf("solve must not be infeasible on a glitched SOC reading: %v", err)
	}
	if sched.Slots[0].SOC > in.Battery.SOCMax+1e-6 {
		t.Errorf("SOC not clamped: %.3f", sched.Slots[0].SOC)
	}
}

func TestScheduleCurrentSlot(t *testing.T) {
	now := time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC)
	sched := &Schedule{
		Slots: []Slot{
			{Start: now},
			{Start: now.Add(30 * time.Minute)},
			{Start: now.Add(60 * time.Minute)},
		},
	}

	// Exactly at slot 0 start
	slot := sched.CurrentSlot(now)
	if slot == nil || slot.Start != now {
		t.Error("expected slot 0")
	}

	// 15 min into slot 0
	slot = sched.CurrentSlot(now.Add(15 * time.Minute))
	if slot == nil || slot.Start != now {
		t.Error("expected slot 0 at +15min")
	}

	// 45 min (slot 1)
	slot = sched.CurrentSlot(now.Add(45 * time.Minute))
	if slot == nil || slot.Start != now.Add(30*time.Minute) {
		t.Error("expected slot 1 at +45min")
	}

	// Before all slots
	slot = sched.CurrentSlot(now.Add(-1 * time.Hour))
	if slot != nil {
		t.Error("expected nil for time before all slots")
	}
}

func TestPrepareInput(t *testing.T) {
	cfg := &config.Config{
		Service: config.Service{
			SlotDuration:    config.Duration{Duration: 30 * time.Minute},
			PlanningHorizon: config.Duration{Duration: 2 * time.Hour},
		},
		Battery: config.Battery{
			CapacityKWh: 9.6,
			SOCMin:      0.2,
			SOCMax:      1.0,
		},
		Rates: config.Rates{
			PeakRate:    0.35,
			OffPeakRate: 0.12,
			FeedInRate:  0.05,
			OffPeakWindows: []config.TimeWindow{
				{Start: config.TimeOfDay{Hour: 1}, End: config.TimeOfDay{Hour: 5}},
			},
		},
		Optimizer: config.Optimizer{SOCRiskWeight: 2.0},
	}

	now := time.Date(2024, 1, 15, 2, 0, 0, 0, time.UTC) // 2 AM, off-peak
	solar := []float64{0, 0, 0, 0}
	load := []float64{1000, 1000, 1000, 1000} // 1000W

	input := PrepareInput(now, cfg, solar, load, 0.8)

	if input.NumSlots != 4 {
		t.Errorf("NumSlots = %d, want 4", input.NumSlots)
	}
	if input.LoadKW[0] != 1.0 {
		t.Errorf("LoadKW[0] = %v, want 1.0 (converted from W)", input.LoadKW[0])
	}
	// 2:00 AM is off-peak
	if !input.IsOffPeak[0] {
		t.Error("slot 0 (2:00 AM) should be off-peak")
	}
	// 3:30 AM is still off-peak
	if !input.IsOffPeak[3] {
		t.Error("slot 3 (3:30 AM) should be off-peak")
	}
}

// TestGridImportCapEnforced verifies MaxGridImportKW is a hard per-slot bound.
// The scenario is deliberately built so the *unbounded* optimum wants to draw
// well above the cap in at least one slot (charge the battery hard in the short
// cheap off-peak window to cover a long expensive peak), then re-solves the same
// problem with a low cap and asserts no slot exceeds it.
func TestGridImportCapEnforced(t *testing.T) {
	now := time.Date(2024, 1, 15, 3, 0, 0, 0, time.UTC) // 3 AM, off-peak

	build := func(cap float64) *Input {
		in := &Input{
			Now:         now,
			NumSlots:    6,
			SlotMinutes: 30,
			SolarKW:     []float64{0, 0, 0, 0, 0, 0},
			LoadKW:      []float64{1, 1, 5, 5, 5, 5},
			Rates:       []float64{0.10, 0.10, 0.40, 0.40, 0.40, 0.40},
			IsOffPeak:   []bool{true, true, false, false, false, false},
			CurrentSOC:  0.40,
			Battery: config.Battery{
				CapacityKWh:    20.0,
				MaxChargeKW:    5.0,
				MaxDischargeKW: 8.0,
				SOCMin:         0.10,
				SOCMax:         1.0,
				Efficiency:     0.95,
			},
			FeedInRate:      0.05,
			PeakRate:        0.40,
			SOCRiskWeight:   2.0,
			MinChargeKW:     1.0,
			BlipCost:        5.0,
			MaxGridImportKW: cap,
		}
		fillUniform(in)
		return in
	}

	const capKW = 3.0
	const tol = 1e-6

	// Unbounded (cap = 0 = no limit): confirm the scenario genuinely wants to
	// draw above capKW somewhere, otherwise the cap test below is vacuous.
	unbounded, err := Solve(build(0))
	if err != nil {
		t.Fatalf("unbounded solve: %v", err)
	}
	var maxUnbounded float64
	for _, s := range unbounded.Slots {
		if s.GridImportKW > maxUnbounded {
			maxUnbounded = s.GridImportKW
		}
	}
	if maxUnbounded <= capKW {
		t.Fatalf("scenario is vacuous: unbounded peak import %.3f kW <= cap %.1f kW; "+
			"cap would never bind", maxUnbounded, capKW)
	}

	// Capped: no slot may exceed the cap.
	capped, err := Solve(build(capKW))
	if err != nil {
		t.Fatalf("capped solve: %v", err)
	}
	for i, s := range capped.Slots {
		if s.GridImportKW > capKW+tol {
			t.Errorf("slot %d: grid import %.4f kW exceeds cap %.1f kW", i, s.GridImportKW, capKW)
		}
	}
	t.Logf("unbounded peak import %.2f kW -> capped peak import respects %.1f kW", maxUnbounded, capKW)
}

// TestGridImportCapSpreadsDeepDeficit verifies that when the required off-peak
// charge energy exceeds what a single capped slot can deliver, the solver
// spreads charging across multiple slots instead of spiking one — the whole
// point of the cap. Peak load (5 kW) exceeds the 3 kW import cap, so the battery
// MUST supply the difference during peak; covering it needs more stored energy
// than one off-peak slot at the cap can add, forcing multi-slot charging.
func TestGridImportCapSpreadsDeepDeficit(t *testing.T) {
	now := time.Date(2024, 1, 15, 1, 0, 0, 0, time.UTC) // 1 AM, off-peak

	const T = 10
	solar := make([]float64, T)
	load := make([]float64, T)
	rates := make([]float64, T)
	off := make([]bool, T)
	for i := range T {
		if i < 6 { // slots 0-5 off-peak
			load[i] = 1
			rates[i] = 0.10
			off[i] = true
		} else { // slots 6-9 deep peak
			load[i] = 5
			rates[i] = 0.40
			off[i] = false
		}
	}

	const capKW = 3.0
	const tol = 1e-6

	in := &Input{
		Now:         now,
		NumSlots:    T,
		SlotMinutes: 30,
		SolarKW:     solar,
		LoadKW:      load,
		Rates:       rates,
		IsOffPeak:   off,
		CurrentSOC:  0.20,
		Battery: config.Battery{
			CapacityKWh:    20.0,
			MaxChargeKW:    5.0,
			MaxDischargeKW: 5.0,
			SOCMin:         0.10,
			SOCMax:         1.0,
			Efficiency:     0.95,
		},
		FeedInRate:      0.0,
		PeakRate:        0.40,
		SOCRiskWeight:   2.0,
		MinChargeKW:     1.0,
		BlipCost:        5.0,
		MaxGridImportKW: capKW,
	}
	fillUniform(in)

	sched, err := Solve(in)
	if err != nil {
		t.Fatalf("solve: %v", err)
	}

	// Invariant: no slot exceeds the cap.
	for i, s := range sched.Slots {
		if s.GridImportKW > capKW+tol {
			t.Errorf("slot %d: grid import %.4f kW exceeds cap %.1f kW", i, s.GridImportKW, capKW)
		}
	}

	// Spread: charging must occur in at least two distinct off-peak slots, since
	// no single capped slot can add enough energy to cover the peak deficit.
	chargingSlots := 0
	var offPeakImportKWh float64
	for i := range 6 {
		s := sched.Slots[i]
		offPeakImportKWh += s.GridImportKW * in.SlotHours[i]
		if s.GridCharge && s.BatteryFlowKW > 0.1 {
			chargingSlots++
		}
	}
	if chargingSlots < 2 {
		t.Errorf("expected charging spread across >=2 off-peak slots, got %d", chargingSlots)
	}
	// One slot at the cap over 30 min delivers at most capKW*0.5 kWh of import;
	// exceeding that proves the import was necessarily spread.
	if offPeakImportKWh <= capKW*0.5+tol {
		t.Errorf("off-peak import %.3f kWh fits in a single capped slot; expected spread", offPeakImportKWh)
	}
	t.Logf("charging spread across %d off-peak slots, %.2f kWh off-peak import", chargingSlots, offPeakImportKWh)
}

// TestGridImportCapStaysFeasibleWhenLoadForcesOverImport is the degradation test
// for the soft cap: when real load alone exceeds MaxGridImportKW and the battery
// is floored (so it cannot supply the difference), the solve must NOT go
// infeasible. It must return a valid plan whose import reflects the forced
// over-draw. A hard cap would fail here and make hub.tick early-return (serving a
// stale plan and never re-commanding the actuator or alerting).
func TestGridImportCapStaysFeasibleWhenLoadForcesOverImport(t *testing.T) {
	now := time.Date(2024, 1, 15, 18, 0, 0, 0, time.UTC) // 6 PM, peak

	const T = 4
	const capKW = 10.0
	const socMin = 0.20

	in := &Input{
		Now:         now,
		NumSlots:    T,
		SlotMinutes: 30,
		SolarKW:     []float64{0, 0, 0, 0},
		LoadKW:      []float64{15, 3, 3, 3}, // slot 0 load exceeds the cap
		Rates:       []float64{0.40, 0.40, 0.40, 0.40},
		IsOffPeak:   []bool{false, false, false, false},
		CurrentSOC:  socMin, // floored: battery cannot discharge to cover the spike
		Battery: config.Battery{
			CapacityKWh:    10.0,
			MaxChargeKW:    5.0,
			MaxDischargeKW: 5.0,
			SOCMin:         socMin,
			SOCMax:         1.0,
			Efficiency:     0.95,
		},
		FeedInRate:      0.0,
		PeakRate:        0.40,
		SOCRiskWeight:   2.0,
		MinChargeKW:     1.0,
		BlipCost:        5.0,
		MaxGridImportKW: capKW,
	}
	fillUniform(in)

	sched, err := Solve(in)
	if err != nil {
		t.Fatalf("soft cap must keep the solve feasible when load forces over-import, got: %v", err)
	}
	// The forced slot must carry the real load as import (~15 kW), above the cap —
	// proving the model acknowledged reality rather than failing.
	if got := sched.Slots[0].GridImportKW; got < in.LoadKW[0]-1e-3 {
		t.Errorf("slot 0: grid import %.3f kW, want >= load %.1f kW (forced over-import)", got, in.LoadKW[0])
	}
	if sched.Slots[0].GridImportKW <= capKW {
		t.Errorf("slot 0: grid import %.3f kW should exceed the cap %.1f kW under forced load", sched.Slots[0].GridImportKW, capKW)
	}
	// Non-forced slots (load 3 kW < cap) must still respect the cap.
	for i := 1; i < T; i++ {
		if sched.Slots[i].GridImportKW > capKW+1e-6 {
			t.Errorf("slot %d: grid import %.3f kW exceeds cap %.1f kW despite load within cap", i, sched.Slots[i].GridImportKW, capKW)
		}
	}
	t.Logf("forced slot import %.2f kW (cap %.1f) — feasible, over-cap acknowledged", sched.Slots[0].GridImportKW, capKW)
}
