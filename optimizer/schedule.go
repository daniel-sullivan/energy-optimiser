package optimizer

import "time"

// Schedule is the full optimization result.
type Schedule struct {
	ObjectiveValue float64
	Slots          []Slot
}

// Slot is a single time slot in the schedule.
type Slot struct {
	Start         time.Time
	End           time.Time // slot end (Start + DurationH); telescoping grids have variable widths
	DurationH     float64   // slot width in hours (0.5 near-horizon, 1.0 far-horizon)
	GridCharge    bool      // actively charging battery from grid
	BatteryFlowKW float64   // positive=charge, negative=discharge
	GridImportKW  float64   // power drawn from grid
	GridExportKW  float64   // power exported to grid
	SOC           float64   // battery SOC at end of slot (0-1)
	SolarKW       float64   // forecast solar for this slot
	LoadKW        float64   // forecast load for this slot
}

// CurrentSlot returns the slot containing the given time, or nil.
func (s *Schedule) CurrentSlot(now time.Time) *Slot {
	if s == nil || len(s.Slots) == 0 {
		return nil
	}
	for i := len(s.Slots) - 1; i >= 0; i-- {
		if !now.Before(s.Slots[i].Start) {
			return &s.Slots[i]
		}
	}
	return nil
}

// SlotAt returns the slot starting at or after the given time.
func (s *Schedule) SlotAt(t time.Time) *Slot {
	for i := range s.Slots {
		if !s.Slots[i].Start.After(t) && (i+1 >= len(s.Slots) || s.Slots[i+1].Start.After(t)) {
			return &s.Slots[i]
		}
	}
	return nil
}

// ChargeBlockFrom returns the SoC goal and end time of the contiguous grid-charge
// block beginning at slot cur. Per-window contiguity means grid-charge slots form a
// single run, so the goal is the SoC at the end of the last consecutive GridCharge
// slot and the end time is that slot's End. If cur is nil, not found, or not
// charging, it falls back to cur's own SOC/End. The actuator uses these to commit
// to a block and stop cleanly at its goal rather than toggling on solver wobble.
func (s *Schedule) ChargeBlockFrom(cur *Slot) (targetSOC float64, blockEnd time.Time) {
	if cur == nil {
		return 0, time.Time{}
	}
	targetSOC, blockEnd = cur.SOC, cur.End
	if s == nil {
		return targetSOC, blockEnd
	}
	i := -1
	for k := range s.Slots {
		if s.Slots[k].Start.Equal(cur.Start) {
			i = k
			break
		}
	}
	if i < 0 {
		return targetSOC, blockEnd
	}
	for j := i; j < len(s.Slots) && s.Slots[j].GridCharge; j++ {
		targetSOC = s.Slots[j].SOC
		blockEnd = s.Slots[j].End
	}
	return targetSOC, blockEnd
}
