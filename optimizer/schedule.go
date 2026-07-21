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
