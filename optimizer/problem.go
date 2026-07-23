package optimizer

import (
	"math"
	"time"

	"energy-optimiser/config"
)

// Input contains everything needed to build and solve the optimization problem.
type Input struct {
	Now         time.Time
	NumSlots    int
	SlotMinutes int // width of the first (fine) slot; display only — per-slot math uses SlotHours

	SlotStart []time.Time // slot start times (telescoping grid; len == NumSlots)
	SlotHours []float64   // slot widths in hours (variable; drives all per-slot energy/cost math)

	SolarKW   []float64 // predicted solar per slot (kW)
	LoadKW    []float64 // predicted load per slot (kW)
	Rates     []float64 // electricity rate per slot (¥/kWh)
	IsOffPeak []bool    // off-peak flag per slot

	CurrentSOC    float64
	Battery       config.Battery
	FeedInRate    float64 // ¥/kWh export revenue (0 = curtailment, no revenue)
	PeakRate      float64 // ¥/kWh, used to scale SOC penalties into currency units
	SOCRiskWeight float64
	MinChargeKW   float64 // a grid-charge permit must yield >= this
	BlipCost      float64 // objective penalty per bypass entry (currency)
	// MaxGridImportKW is the per-slot grid-import cap (kW; the electrical
	// connection limit). It is enforced as a SOFT bound: import above it is
	// heavily penalised so the optimiser never plans to exceed it, but real load
	// that forces import over the cap keeps the model feasible rather than failing.
	// ≤0 means unbounded (no cap) — used by tests that build Input directly;
	// PrepareInput always carries config.finalize's safe default.
	MaxGridImportKW float64
}

// PrepareInput builds solver input from config, forecasts, and current state.
// The telescoping slot grid comes from BuildGrid, which floors now to the slot
// boundary so plans stay stable across re-solves and tariff windows align to
// slots exactly. Solar and load are expected already aligned to that grid
// (hub/backtest build them from the same BuildGrid); they are padded/truncated
// to the grid length as a guard.
func PrepareInput(
	now time.Time,
	cfg *config.Config,
	solarKW []float64,
	loadW []float64,
	currentSOC float64,
) *Input {
	grid := BuildGrid(now, cfg)
	numSlots := grid.Len()
	start := grid.Start[0]

	solar := padSlice(solarKW, numSlots)
	load := make([]float64, numSlots)
	for i := range numSlots {
		if i < len(loadW) {
			load[i] = loadW[i] / 1000.0 // W → kW
		}
	}

	rates := make([]float64, numSlots)
	offPeak := make([]bool, numSlots)
	for i := range numSlots {
		t := grid.Start[i]
		rates[i] = cfg.Rates.RateAt(t)
		offPeak[i] = cfg.Rates.IsOffPeak(t)
	}

	return &Input{
		Now:             start,
		NumSlots:        numSlots,
		SlotMinutes:     int(cfg.Service.SlotDuration.Minutes()),
		SlotStart:       grid.Start,
		SlotHours:       grid.Hours,
		SolarKW:         solar,
		LoadKW:          load,
		Rates:           rates,
		IsOffPeak:       offPeak,
		CurrentSOC:      currentSOC,
		Battery:         cfg.Battery,
		FeedInRate:      cfg.Rates.FeedInRate,
		PeakRate:        cfg.Rates.PeakRate,
		SOCRiskWeight:   cfg.Optimizer.SOCRiskWeight,
		MinChargeKW:     cfg.Optimizer.MinChargeKW,
		BlipCost:        cfg.Optimizer.BlipCost,
		MaxGridImportKW: cfg.Optimizer.MaxGridImportKW,
	}
}

// solarSurplus returns max(0, solar - load) per slot.
func solarSurplus(solar, load []float64) []float64 {
	out := make([]float64, len(solar))
	for i := range solar {
		out[i] = math.Max(0, solar[i]-load[i])
	}
	return out
}

// slotRun is a contiguous span of off-peak slots [start, end).
type slotRun struct {
	start int
	end   int
}

// offPeakRuns segments the horizon into maximal contiguous off-peak runs — one
// bypass-entry budget (Σstart ≤ 1) is enforced per run.
func offPeakRuns(offPeak []bool) []slotRun {
	var runs []slotRun
	i := 0
	for i < len(offPeak) {
		if !offPeak[i] {
			i++
			continue
		}
		j := i
		for j < len(offPeak) && offPeak[j] {
			j++
		}
		runs = append(runs, slotRun{start: i, end: j})
		i = j
	}
	return runs
}

func padSlice(s []float64, n int) []float64 {
	out := make([]float64, n)
	copy(out, s)
	return out
}
