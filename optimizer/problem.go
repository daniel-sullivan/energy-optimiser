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
	SlotMinutes int

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
}

// PrepareInput builds solver input from config, forecasts, and current state.
// now is floored to the slot boundary so plans stay stable across re-solves and
// tariff windows align to slots exactly.
func PrepareInput(
	now time.Time,
	cfg *config.Config,
	solarKW []float64,
	loadW []float64,
	currentSOC float64,
) *Input {
	slotMins := int(cfg.Service.SlotDuration.Minutes())
	numSlots := int(cfg.Service.PlanningHorizon.Minutes()) / slotMins

	now = now.Truncate(time.Duration(slotMins) * time.Minute)

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
		t := now.Add(time.Duration(i) * time.Duration(slotMins) * time.Minute)
		rates[i] = cfg.Rates.RateAt(t)
		offPeak[i] = cfg.Rates.IsOffPeak(t)
	}

	return &Input{
		Now:           now,
		NumSlots:      numSlots,
		SlotMinutes:   slotMins,
		SolarKW:       solar,
		LoadKW:        load,
		Rates:         rates,
		IsOffPeak:     offPeak,
		CurrentSOC:    currentSOC,
		Battery:       cfg.Battery,
		FeedInRate:    cfg.Rates.FeedInRate,
		PeakRate:      cfg.Rates.PeakRate,
		SOCRiskWeight: cfg.Optimizer.SOCRiskWeight,
		MinChargeKW:   cfg.Optimizer.MinChargeKW,
		BlipCost:      cfg.Optimizer.BlipCost,
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
