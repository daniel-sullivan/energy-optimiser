package serve

import (
	"fmt"
	"math"
	"strings"
	"time"

	"energy-optimiser/optimizer"
)

// DashboardView is the fully-derived view model. Both the full-page render and
// the SSE partials consume it, so on-screen numbers and HA's MQTT sensors agree
// (they share TimeRemaining / FormatHours / RationaleFor).
type DashboardView struct {
	Confidence   int    // load-model confidence, whole %
	ConfidenceHi bool   // >= 70%: full multi-day planning
	ModelNote    string // one-line confidence caption
	LastTick     string // wall-clock of the last completed tick
	HasData      bool
	Stale        bool // live HA feed is frozen/disconnected — numbers are not current

	Decision       Decision
	Tiles          []Tile
	ChargeGauge    Gauge
	DischargeGauge Gauge
	Ribbon         Ribbon
	Forecast       Forecast
	Events         []EventVM
	Accuracy       AccuracyView
}

// Decision is the current action the optimiser is taking, in the operator's
// words — the page's thesis.
type Decision struct {
	Action    string // "GRID CHARGE" / "DISCHARGE" / "ON SOLAR" / "HOLD" / "STANDBY"
	Accent    string // css accent: charge / discharge / solar / idle
	Rationale string // RationaleFor(...) — forecast trough + next charge
	NextLabel string // "next: discharge 17:30" (or "")
}

// Tile is one live instrument readout.
type Tile struct {
	Key    string // change-flash / data attr
	Label  string
	Value  string // main number (monospaced)
	Unit   string
	Sub    string // small caption under the value
	Accent string // charge / discharge / solar / ink
}

// Gauge is a donut time-remaining glance card (charge-to-full / discharge-to-empty).
type Gauge struct {
	Title  string
	Big    string  // FormatHours(...) or em-dash when not applicable
	Sub    string  // "at +2.1 kW" / "battery 63% of usable"
	Frac   float64 // fill 0..1 = SoC within the usable band
	Dash   float64 // dasharray length = Frac * Circ
	Circ   float64 // ring circumference
	Accent string  // charge / discharge
	Active bool     // this direction is the one currently happening
}

// Ribbon is the 72-hour tariff ribbon — the signature element. Geometry is
// precomputed in a viewBox of W x H units (1 unit per slot horizontally,
// 0..100 = SoC 0..100% vertically); the SVG scales to full width.
type Ribbon struct {
	W, H     float64
	SocArea  string // filled area under the SoC line
	SocLine  string // SoC stroke
	Bands    []RibbonBand
	OffPeak  []RibbonRect
	NowFrac  float64 // 0..1 position of "now" across the horizon
	NowShown bool
	BandTopY float64 // y of SOCMax guide
	BandBotY float64 // y of SOCMin guide
	MaxLabel string
	MinLabel string
	Marks    []Mark
	Cols     []SlotHover
	Empty    bool
}

// RibbonBand is a merged run of charge/discharge slots drawn as a rail.
type RibbonBand struct {
	X, W float64
	Kind string // charge / discharge
}

// RibbonRect is a merged off-peak (cheap tariff) shaded band.
type RibbonRect struct {
	X, W float64
}

// Mark is a time-axis label positioned by fraction across the horizon.
type Mark struct {
	Frac  float64
	Label string
	IsDay bool
}

// SlotHover is a transparent hover column over the ribbon carrying a tooltip.
type SlotHover struct {
	Frac  float64
	W     float64
	When  string
	SOC   string
	Batt  string
	Load  string
	Solar string
	Grid  string
	Kind  string
}

// Forecast is the solar sparkline panel.
type Forecast struct {
	W, H      float64
	Area      string
	Line      string
	Peak      string // "peak 6.4 kW"
	PeakWhen  string // "tomorrow 12:30"
	TotalKWh  string // integrated over the horizon
	Empty     bool
}

// EventVM is one upcoming planned state change.
type EventVM struct {
	Time string
	Kind string // charge / discharge / idle
	Desc string
}

const gaugeCirc = 2 * math.Pi * 54 // r=54 donut

func (s *Server) buildView() *DashboardView {
	now := time.Now().In(s.loc)
	st := s.provider.CurrentState()
	sched := s.provider.Schedule()
	conf := s.provider.LoadConfidence()

	socFrac := st["battery_soc"] / 100.0
	powerKW := st["battery_power"] / 1000.0
	pvKW := st["pv_power"] / 1000.0
	gridKW := st["grid_power"] / 1000.0
	loadKW := st["load_power"] / 1000.0

	v := &DashboardView{
		Confidence:   int(math.Round(conf * 100)),
		ConfidenceHi: conf >= 0.7,
		ModelNote:    modelNote(conf),
		HasData:      sched != nil && len(sched.Slots) > 0,
	}
	if lt := s.provider.LastTick(); !lt.IsZero() {
		v.LastTick = lt.In(s.loc).Format("15:04:05")
	} else {
		v.LastTick = "--:--:--"
	}
	v.Stale = s.provider.DataStale()

	v.Tiles = buildTiles(st, socFrac, powerKW, pvKW, gridKW, loadKW)
	v.ChargeGauge, v.DischargeGauge = s.buildGauges(socFrac, powerKW)
	v.Decision = s.buildDecision(now, socFrac, sched)
	v.Ribbon = s.buildRibbon(now, sched)
	v.Forecast = s.buildForecast(sched)
	v.Events = s.buildEvents(now, sched)
	v.Accuracy = s.buildAccuracy(s.provider.Accuracy())
	return v
}

func modelNote(conf float64) string {
	switch {
	case conf < 0.3:
		return "Learning — conservative load estimates from recent history"
	case conf < 0.7:
		return "Building profiles — solar forecasts driving charge decisions"
	default:
		return "Full multi-day planning active"
	}
}

func buildTiles(st map[string]float64, socFrac, powerKW, pvKW, gridKW, loadKW float64) []Tile {
	battSub, battAccent := "idle", "idle"
	switch {
	case powerKW > 0.05:
		battSub, battAccent = "charging", "charge"
	case powerKW < -0.05:
		battSub, battAccent = "discharging", "discharge"
	}
	gridSub := "balanced"
	switch {
	case gridKW > 0.05:
		gridSub = "importing"
	case gridKW < -0.05:
		gridSub = "exporting"
	}
	socAccent := "solar"
	if socFrac < 0.25 {
		socAccent = "discharge"
	}
	return []Tile{
		{Key: "soc", Label: "State of Charge", Value: fmt.Sprintf("%.0f", st["battery_soc"]), Unit: "%", Sub: "battery level", Accent: socAccent},
		{Key: "pv", Label: "Solar", Value: fmt.Sprintf("%.2f", pvKW), Unit: "kW", Sub: pvSub(pvKW), Accent: "solar"},
		{Key: "load", Label: "Load", Value: fmt.Sprintf("%.2f", loadKW), Unit: "kW", Sub: "site demand", Accent: "ink"},
		{Key: "grid", Label: "Grid", Value: fmt.Sprintf("%+.2f", gridKW), Unit: "kW", Sub: gridSub, Accent: "charge"},
		{Key: "batt", Label: "Battery", Value: fmt.Sprintf("%+.2f", powerKW), Unit: "kW", Sub: battSub, Accent: battAccent},
	}
}

func pvSub(kw float64) string {
	if kw > 0.05 {
		return "generating"
	}
	return "dark"
}

func (s *Server) buildGauges(socFrac, powerKW float64) (charge, discharge Gauge) {
	band := s.battery.SOCMax - s.battery.SOCMin
	frac := 0.0
	if band > 0 {
		frac = (socFrac - s.battery.SOCMin) / band
	}
	frac = math.Max(0, math.Min(1, frac))

	chargeH, dischargeH := TimeRemaining(s.battery, socFrac, powerKW)
	charging := powerKW > 0.05
	discharging := powerKW < -0.05

	charge = Gauge{
		Title: "Charge to full", Frac: frac, Dash: frac * gaugeCirc, Circ: gaugeCirc,
		Accent: "charge", Active: charging,
	}
	if charging && chargeH != nil {
		charge.Big = FormatHours(*chargeH)
		charge.Sub = fmt.Sprintf("at %+.1f kW", powerKW)
	} else {
		charge.Big = "—"
		charge.Sub = fmt.Sprintf("full at %.0f%%", s.battery.SOCMax*100)
	}

	discharge = Gauge{
		Title: "Discharge to empty", Frac: frac, Dash: frac * gaugeCirc, Circ: gaugeCirc,
		Accent: "discharge", Active: discharging,
	}
	if discharging && dischargeH != nil {
		discharge.Big = FormatHours(*dischargeH)
		discharge.Sub = fmt.Sprintf("at %.1f kW", -powerKW)
	} else {
		discharge.Big = "—"
		discharge.Sub = fmt.Sprintf("reserve to %.0f%%", s.battery.SOCMin*100)
	}
	return charge, discharge
}

func (s *Server) buildDecision(now time.Time, socFrac float64, sched *optimizer.Schedule) Decision {
	d := Decision{
		Action: "STANDBY", Accent: "idle",
		Rationale: strings.ReplaceAll(RationaleFor(now, socFrac, sched), "->", "→"),
	}
	if sched == nil || len(sched.Slots) == 0 {
		return d
	}
	cur := sched.CurrentSlot(now)
	if cur == nil {
		return d
	}
	d.Action, d.Accent = actionFor(cur)

	curKind := kindOf(cur)
	for i := range sched.Slots {
		sl := &sched.Slots[i]
		if !sl.Start.After(now) {
			continue
		}
		if kindOf(sl) != curKind {
			d.NextLabel = fmt.Sprintf("next: %s at %s", kindVerb(sl), sl.Start.In(s.loc).Format("Mon 15:04"))
			break
		}
	}
	return d
}

func actionFor(sl *optimizer.Slot) (string, string) {
	switch {
	case sl.GridCharge:
		return "GRID CHARGE", "charge"
	case sl.BatteryFlowKW < -0.1:
		return "DISCHARGE", "discharge"
	case sl.SolarKW > 0.1:
		return "ON SOLAR", "solar"
	default:
		return "HOLD", "idle"
	}
}

func kindOf(sl *optimizer.Slot) string {
	switch {
	case sl.GridCharge:
		return "charge"
	case sl.BatteryFlowKW < -0.1:
		return "discharge"
	default:
		return "idle"
	}
}

func kindVerb(sl *optimizer.Slot) string {
	switch kindOf(sl) {
	case "charge":
		return "grid charge"
	case "discharge":
		return "discharge"
	default:
		return "hold"
	}
}

func (s *Server) buildRibbon(now time.Time, sched *optimizer.Schedule) Ribbon {
	if sched == nil || len(sched.Slots) == 0 {
		return Ribbon{Empty: true}
	}
	n := len(sched.Slots)
	r := Ribbon{
		W: float64(n), H: 100,
		BandTopY: 100 - s.battery.SOCMax*100,
		BandBotY: 100 - s.battery.SOCMin*100,
		MaxLabel: fmt.Sprintf("%.0f%%", s.battery.SOCMax*100),
		MinLabel: fmt.Sprintf("%.0f%%", s.battery.SOCMin*100),
	}

	var line, area strings.Builder
	for i := range sched.Slots {
		x := float64(i) + 0.5
		y := 100 - clamp01(sched.Slots[i].SOC)*100
		if i == 0 {
			fmt.Fprintf(&line, "M %.2f %.2f", x, y)
			fmt.Fprintf(&area, "M %.2f %.2f", x, y)
		} else {
			fmt.Fprintf(&line, " L %.2f %.2f", x, y)
			fmt.Fprintf(&area, " L %.2f %.2f", x, y)
		}
	}
	lastX := float64(n-1) + 0.5
	fmt.Fprintf(&area, " L %.2f 100 L 0.5 100 Z", lastX)
	r.SocLine = line.String()
	r.SocArea = area.String()

	// Merge runs of charge/discharge into rails, and off-peak slots into bands.
	flushBand := func(start, end int, kind string) {
		if kind == "" || kind == "idle" {
			return
		}
		r.Bands = append(r.Bands, RibbonBand{X: float64(start), W: float64(end - start), Kind: kind})
	}
	runKind, runStart := "", 0
	for i := range sched.Slots {
		k := kindOf(&sched.Slots[i])
		if k != runKind {
			flushBand(runStart, i, runKind)
			runKind, runStart = k, i
		}
	}
	flushBand(runStart, n, runKind)

	offStart, inOff := 0, false
	for i := range sched.Slots {
		off := s.rates.IsOffPeak(sched.Slots[i].Start)
		if off && !inOff {
			offStart, inOff = i, true
		} else if !off && inOff {
			r.OffPeak = append(r.OffPeak, RibbonRect{X: float64(offStart), W: float64(i - offStart)})
			inOff = false
		}
	}
	if inOff {
		r.OffPeak = append(r.OffPeak, RibbonRect{X: float64(offStart), W: float64(n - offStart)})
	}

	horizon := sched.Slots[n-1].End.Sub(sched.Slots[0].Start)
	if el := now.Sub(sched.Slots[0].Start); el >= 0 && horizon > 0 && el <= horizon {
		r.NowFrac = float64(el) / float64(horizon)
		r.NowShown = true
	}

	for i := range sched.Slots {
		sl := &sched.Slots[i]
		if sl.Start.Minute() == 0 && sl.Start.Hour()%6 == 0 {
			isDay := sl.Start.Hour() == 0
			label := sl.Start.In(s.loc).Format("15:04")
			if isDay {
				label = sl.Start.In(s.loc).Format("Mon")
			}
			r.Marks = append(r.Marks, Mark{Frac: float64(i) / float64(n), Label: label, IsDay: isDay})
		}
		r.Cols = append(r.Cols, SlotHover{
			Frac:  float64(i) / float64(n),
			W:     1.0 / float64(n),
			When:  sl.Start.In(s.loc).Format("Mon 15:04"),
			SOC:   fmt.Sprintf("%.0f%%", sl.SOC*100),
			Batt:  fmt.Sprintf("%+.1f kW", sl.BatteryFlowKW),
			Load:  fmt.Sprintf("%.1f kW", sl.LoadKW),
			Solar: fmt.Sprintf("%.1f kW", sl.SolarKW),
			Grid:  fmt.Sprintf("%.1f kW", sl.GridImportKW),
			Kind:  kindOf(sl),
		})
	}
	return r
}

func (s *Server) buildForecast(sched *optimizer.Schedule) Forecast {
	if sched == nil || len(sched.Slots) == 0 {
		return Forecast{Empty: true}
	}
	n := len(sched.Slots)
	var peak float64
	var peakIdx int
	var totalKWh float64
	for i := range sched.Slots {
		sk := sched.Slots[i].SolarKW
		totalKWh += sk * sched.Slots[i].DurationH // variable slot width (telescoping grid)
		if sk > peak {
			peak, peakIdx = sk, i
		}
	}
	f := Forecast{W: float64(n), H: 100, Empty: peak <= 0}
	if peak <= 0 {
		f.Peak = "no solar in horizon"
		return f
	}
	var line, area strings.Builder
	for i := range sched.Slots {
		x := float64(i) + 0.5
		y := 100 - (sched.Slots[i].SolarKW/peak)*96 - 2
		if i == 0 {
			fmt.Fprintf(&line, "M %.2f %.2f", x, y)
			fmt.Fprintf(&area, "M %.2f %.2f", x, y)
		} else {
			fmt.Fprintf(&line, " L %.2f %.2f", x, y)
			fmt.Fprintf(&area, " L %.2f %.2f", x, y)
		}
	}
	fmt.Fprintf(&area, " L %.2f 100 L 0.5 100 Z", float64(n-1)+0.5)
	f.Line = line.String()
	f.Area = area.String()
	f.Peak = fmt.Sprintf("peak %.1f kW", peak)
	f.PeakWhen = sched.Slots[peakIdx].Start.In(s.loc).Format("Mon 15:04")
	horizonH := sched.Slots[n-1].End.Sub(sched.Slots[0].Start).Hours()
	f.TotalKWh = fmt.Sprintf("%.0f kWh planned over %.0f h", totalKWh, horizonH)
	return f
}

func (s *Server) buildEvents(now time.Time, sched *optimizer.Schedule) []EventVM {
	if sched == nil || len(sched.Slots) == 0 {
		return nil
	}
	var out []EventVM
	prev := ""
	for i := range sched.Slots {
		sl := &sched.Slots[i]
		k := kindOf(sl)
		if i == 0 {
			prev = k
			continue
		}
		if k == prev {
			continue
		}
		prev = k
		if sl.Start.Before(now) {
			continue
		}
		desc := ""
		switch k {
		case "charge":
			desc = fmt.Sprintf("Start grid charge (SoC %.0f%%)", sl.SOC*100)
		case "discharge":
			desc = fmt.Sprintf("Start discharge (%.1f kW)", -sl.BatteryFlowKW)
		default:
			desc = fmt.Sprintf("Hold / solar (SoC %.0f%%)", sl.SOC*100)
		}
		out = append(out, EventVM{Time: sl.Start.In(s.loc).Format("Mon 15:04"), Kind: k, Desc: desc})
		if len(out) >= 8 {
			break
		}
	}
	return out
}

func clamp01(v float64) float64 {
	return math.Max(0, math.Min(1, v))
}
