package optimizer

import (
	"fmt"
	"math"
	"time"

	milp "github.com/daniel-sullivan/go-milp"
)

// solveTimeLimit is a wall-clock safety bound on the branch-and-bound search.
// The energy shape is tight and solves in milliseconds; this only guards against
// a pathological input, in which case Solve returns an error rather than a
// truncated (possibly sub-optimal) schedule.
const solveTimeLimit = 30 * time.Second

// SOC penalty thresholds (fraction of capacity).
const (
	socThreshLow  = 0.5
	socThreshMed  = 0.3
	socThreshHigh = 0.2
)

// SOC penalty weights relative to SOCRiskWeight.
const (
	penWeightLow  = 0.5
	penWeightMed  = 2.0
	penWeightHigh = 10.0
)

// Solve builds the MILP problem and returns the optimal schedule.
//
// The model is pure Go (go-milp): no cgo, no process-global solver state, and
// m.Solve does not mutate the built model, so no locking or OS-thread pinning is
// required.
func Solve(in *Input) (*Schedule, error) {
	T := in.NumSlots
	// Per-slot widths (hours) drive every energy/cost term: a telescoping grid has
	// 30-min near slots and 60-min far slots, so a scalar slot width would double-
	// or halve-count energy and mis-scale the SOC penalties in the far horizon.
	h := in.SlotHours
	// Split round-trip efficiency into symmetric charge/discharge factors so
	// charging gains less and discharging loses more (η_c·η_d = round-trip η).
	etaC := math.Sqrt(in.Battery.Efficiency)
	etaD := math.Sqrt(in.Battery.Efficiency)
	capKWh := in.Battery.CapacityKWh
	surplus := solarSurplus(in.SolarKW, in.LoadKW)

	// Clamp the reported SOC into [0, SOCMax]: a sensor glitch (e.g. 1850%) must
	// not fix soc[0] above SOCMax and make every solve infeasible. The low guard
	// (socMin ≤ current) keeps a genuinely-low battery from being infeasible too.
	cur := math.Min(in.Battery.SOCMax, math.Max(0, in.CurrentSOC))
	socMin := math.Min(in.Battery.SOCMin, cur)

	// SOC penalties are scaled into currency (¥) via the peak rate so battery
	// protection is comparable to grid-import cost under JPY.
	penScale := in.PeakRate
	if penScale <= 0 {
		penScale = 1
	}

	m := milp.NewModel()

	// ---------- Variables ----------
	gridCharge := make([]milp.Var, T)
	charge := make([]milp.Var, T)
	discharge := make([]milp.Var, T)
	gridImport := make([]milp.Var, T)
	gridExport := make([]milp.Var, T)
	start := make([]milp.Var, T)
	penLow := make([]milp.Var, T)
	penMed := make([]milp.Var, T)
	penHigh := make([]milp.Var, T)
	// soc has T+1 entries: soc[0] is the (clamped) current SOC, soc[t+1] is the
	// SOC at the end of slot t.
	soc := make([]milp.Var, T+1)

	// soc[0] fixed to the (clamped) current SOC.
	soc[0] = m.AddContinuous(cur, cur)
	for t := 1; t <= T; t++ {
		soc[t] = m.AddContinuous(socMin, in.Battery.SOCMax)
	}

	for t := range T {
		// grid_charge[t]: binary permit to charge from grid (enters bypass).
		// Peak slots are fixed to 0 (a continuous var pinned to [0,0]), matching
		// the GLPK version that forced peak grid_charge off.
		if in.IsOffPeak[t] {
			gridCharge[t] = m.AddBinary()
		} else {
			gridCharge[t] = m.AddContinuous(0, 0)
		}
		charge[t] = m.AddContinuous(0, in.Battery.MaxChargeKW)
		discharge[t] = m.AddContinuous(0, in.Battery.MaxDischargeKW)
		gridImport[t] = m.AddContinuous(0, milp.Inf)
		gridExport[t] = m.AddContinuous(0, milp.Inf)
		start[t] = m.AddContinuous(0, 1)
		penLow[t] = m.AddContinuous(0, milp.Inf)
		penMed[t] = m.AddContinuous(0, milp.Inf)
		penHigh[t] = m.AddContinuous(0, milp.Inf)
	}

	// ---------- Objective ----------
	// grid_import cost, −grid_export revenue, blip cost per bypass entry, and the
	// three SOC-protection penalties (scaled to currency). The tariff terms are
	// energy (¥/kWh · kW · h), weighted by the slot width h[t]. The SOC penalties
	// are a per-slot discomfort tuned on the fine grid; they are weighted by the
	// slot width relative to a fine slot (penScaleH = h[t]/fineH), so a far 60-min
	// slot costs exactly what two 30-min slots would and a uniform 30-min grid
	// reproduces the pre-telescoping penalty weight unchanged. BlipCost is
	// per-entry (width-free).
	fineH := float64(in.SlotMinutes) / 60.0
	if fineH <= 0 {
		fineH = h[0]
	}
	var obj []milp.Term
	for t := range T {
		penScaleH := h[t] / fineH
		obj = append(obj,
			milp.Term{Var: gridImport[t], Coef: in.Rates[t] * h[t]},
			milp.Term{Var: gridExport[t], Coef: -in.FeedInRate * h[t]},
			milp.Term{Var: start[t], Coef: in.BlipCost},
			milp.Term{Var: penLow[t], Coef: in.SOCRiskWeight * penWeightLow * penScale * penScaleH},
			milp.Term{Var: penMed[t], Coef: in.SOCRiskWeight * penWeightMed * penScale * penScaleH},
			milp.Term{Var: penHigh[t], Coef: in.SOCRiskWeight * penWeightHigh * penScale * penScaleH},
		)
	}
	m.SetObjective(milp.Minimize, obj)

	// ---------- Constraints ----------
	for t := range T {
		// 1. Energy balance: import + discharge - charge - export = load - solar
		m.AddConstraint([]milp.Term{
			{Var: gridImport[t], Coef: 1},
			{Var: discharge[t], Coef: 1},
			{Var: charge[t], Coef: -1},
			{Var: gridExport[t], Coef: -1},
		}, milp.EqualTo, in.LoadKW[t]-in.SolarKW[t])

		// 2. SOC tracking: soc[t+1] - soc[t] - (η_c·charge - discharge/η_d)·Δh/cap = 0
		m.AddConstraint([]milp.Term{
			{Var: soc[t+1], Coef: 1},
			{Var: soc[t], Coef: -1},
			{Var: charge[t], Coef: -etaC * h[t] / capKWh},
			{Var: discharge[t], Coef: h[t] / (etaD * capKWh)},
		}, milp.EqualTo, 0)

		// 3. Charge limit: charge - grid_charge·(max_charge - surplus) ≤ surplus
		//    grid_charge=0 → charge ≤ surplus (solar only); =1 → charge ≤ max_charge
		m.AddConstraint([]milp.Term{
			{Var: charge[t], Coef: 1},
			{Var: gridCharge[t], Coef: -(in.Battery.MaxChargeKW - surplus[t])},
		}, milp.LessEq, surplus[t])

		// 4. Min-charge link: charge - min_charge·grid_charge ≥ 0
		//    a permit must produce real charging (kills the empty-bypass degeneracy)
		m.AddConstraint([]milp.Term{
			{Var: charge[t], Coef: 1},
			{Var: gridCharge[t], Coef: -in.MinChargeKW},
		}, milp.GreaterEq, 0)

		// 5. No discharge while permitted: discharge + max_discharge·grid_charge ≤ max_discharge
		//    (battery cannot discharge to the house while in utility bypass)
		m.AddConstraint([]milp.Term{
			{Var: discharge[t], Coef: 1},
			{Var: gridCharge[t], Coef: in.Battery.MaxDischargeKW},
		}, milp.LessEq, in.Battery.MaxDischargeKW)

		// 6. Start link: start[t] ≥ grid_charge[t] - grid_charge[t-1] (t=0: gc[-1]=0)
		startTerms := []milp.Term{
			{Var: start[t], Coef: 1},
			{Var: gridCharge[t], Coef: -1},
		}
		if t > 0 {
			startTerms = append(startTerms, milp.Term{Var: gridCharge[t-1], Coef: 1})
		}
		m.AddConstraint(startTerms, milp.GreaterEq, 0)

		// 7. pen_low + soc[t+1] ≥ 0.5
		m.AddConstraint([]milp.Term{
			{Var: penLow[t], Coef: 1},
			{Var: soc[t+1], Coef: 1},
		}, milp.GreaterEq, socThreshLow)

		// 8. pen_med + soc[t+1] ≥ 0.3
		m.AddConstraint([]milp.Term{
			{Var: penMed[t], Coef: 1},
			{Var: soc[t+1], Coef: 1},
		}, milp.GreaterEq, socThreshMed)

		// 9. pen_high + soc[t+1] ≥ 0.2
		m.AddConstraint([]milp.Term{
			{Var: penHigh[t], Coef: 1},
			{Var: soc[t+1], Coef: 1},
		}, milp.GreaterEq, socThreshHigh)
	}

	// Contiguity: at most one bypass entry per off-peak window run.
	for _, r := range offPeakRuns(in.IsOffPeak) {
		terms := make([]milp.Term, 0, r.end-r.start)
		for t := r.start; t < r.end; t++ {
			terms = append(terms, milp.Term{Var: start[t], Coef: 1})
		}
		m.AddConstraint(terms, milp.LessEq, 1)
	}

	// ---------- Solve ----------
	opts := milp.DefaultOptions()
	opts.TimeLimit = solveTimeLimit
	sol, err := m.SolveWith(opts)
	if err != nil {
		return nil, fmt.Errorf("MILP solve: %w", err)
	}
	if sol.Status != milp.Optimal {
		return nil, fmt.Errorf("MILP solve: %s", sol.Status)
	}

	// ---------- Extract schedule ----------
	sched := &Schedule{
		ObjectiveValue: sol.Objective,
		Slots:          make([]Slot, T),
	}
	for t := range T {
		slotStart := in.SlotStart[t]
		sched.Slots[t] = Slot{
			Start:         slotStart,
			End:           slotStart.Add(hoursToDuration(h[t])),
			DurationH:     h[t],
			GridCharge:    sol.Value(gridCharge[t]) > 0.5,
			BatteryFlowKW: sol.Value(charge[t]) - sol.Value(discharge[t]),
			GridImportKW:  sol.Value(gridImport[t]),
			GridExportKW:  sol.Value(gridExport[t]),
			SOC:           sol.Value(soc[t+1]),
			SolarKW:       in.SolarKW[t],
			LoadKW:        in.LoadKW[t],
		}
	}
	return sched, nil
}
