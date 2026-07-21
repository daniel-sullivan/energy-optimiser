package serve

import (
	"fmt"
	"math"
	"strings"
	"time"
)

// AccuracyPoint is one elapsed slot's predicted-vs-actual across the three
// tracked signals. Every value carries a Has* flag so the panel renders honest
// gaps (a missing prediction, or an actual not yet resolved) instead of drawing
// a zero. The hub's recorder is the single producer; the dashboard is the only
// consumer.
type AccuracyPoint struct {
	Slot time.Time `json:"slot"` // slot start

	// Solar (kW): the two independent predictions overlaid against measured PV.
	SolcastKW   float64 `json:"solcast_kw,omitempty"`
	HasSolcast  bool    `json:"has_solcast,omitempty"`
	ModelKW     float64 `json:"model_kw,omitempty"`
	HasModel    bool    `json:"has_model,omitempty"`
	ActualPVKW  float64 `json:"actual_pv_kw,omitempty"`
	HasActualPV bool    `json:"has_actual_pv,omitempty"`

	// Battery SoC (fraction 0-1): the optimiser's planned end-of-slot SoC vs the
	// SoC that actually happened.
	PlannedSOC    float64 `json:"planned_soc,omitempty"`
	HasPlannedSOC bool    `json:"has_planned_soc,omitempty"`
	ActualSOC     float64 `json:"actual_soc,omitempty"`
	HasActualSOC  bool    `json:"has_actual_soc,omitempty"`

	// Grid (kW, signed: + import / − export): planned net grid vs measured.
	PlannedGridKW  float64 `json:"planned_grid_kw,omitempty"`
	HasPlannedGrid bool    `json:"has_planned_grid,omitempty"`
	ActualGridKW   float64 `json:"actual_grid_kw,omitempty"`
	HasActualGrid  bool    `json:"has_actual_grid,omitempty"`
}

// AccuracySnapshot is the rolling-window read model the dashboard consumes,
// produced by the hub's accuracy recorder. Points are sorted by Slot ascending.
type AccuracySnapshot struct {
	Points   []AccuracyPoint
	WindowH  float64 // rolling-window length (hours) — for the caption
	SlotMin  float64 // slot resolution (minutes) — for the caption
}

// --- view model (derived geometry the template renders) ---

// AccuracyView is the fully-derived forecast-accuracy panel. The three compact
// overlaid charts share the same slot domain (x-axis); each draws its
// prediction(s) dashed and the measured actual solid.
type AccuracyView struct {
	Collecting  bool   // no elapsed slots recorded yet — panel shows a collecting note
	WindowLabel string // "last 7 days · 30-min slots"
	Charts      []AccChart
	Marks       []AccMark // shared x-axis time ticks
}

// AccChart is one signal's overlay (solar / SoC / grid).
type AccChart struct {
	Key     string // solar / soc / grid
	Title   string
	Empty   bool     // no data for this signal yet
	Pending bool     // predictions exist but no measured actual has landed
	Note    string   // empty/pending caption
	W, H    float64  // viewBox
	ZeroY   *float64 // baseline y for the signed (grid) chart; nil otherwise
	YTop    string   // top axis label (e.g. "6.4 kW" / "100%")
	YBot    string   // bottom axis label (e.g. "0" / "−3.0 kW")
	Series  []AccSeries
	Stats   []AccStat
}

// AccSeries is one line in a chart: predicted (dashed) or actual (solid).
type AccSeries struct {
	Label  string // "Measured" / "Solcast" / "Expected (weather)" / "Planned"
	Accent string // css accent token: solar / charge / discharge / model
	Dashed bool
	Path   string // SVG path; broken into sub-paths across gaps
}

// AccStat is one error summary (per predictor) over the paired points.
type AccStat struct {
	Label string // "Solcast" / "Expected (weather)" / "Planned"
	Value string // "MAE 0.42 · RMSE 0.61 kW  (n=48)"
}

// AccMark is one x-axis time tick, positioned by fraction across the window.
type AccMark struct {
	Frac  float64
	Label string
	IsDay bool
}

const accVBW, accVBH = 100.0, 100.0

func (s *Server) buildAccuracy(snap AccuracySnapshot) AccuracyView {
	v := AccuracyView{
		WindowLabel: fmt.Sprintf("last %.0f days · %.0f-min slots",
			math.Round(snap.WindowH/24), snap.SlotMin),
	}
	pts := snap.Points
	if len(pts) == 0 {
		v.Collecting = true
		return v
	}
	n := len(pts)
	v.Marks = s.accMarks(pts)
	v.Charts = []AccChart{
		s.buildSolarChart(pts, n),
		s.buildSOCChart(pts, n),
		s.buildGridChart(pts, n),
	}
	return v
}

// xAt maps a slot index to its viewBox x (slot-centred, like the forecast).
func xAt(i, n int) float64 { return (float64(i) + 0.5) / float64(n) * accVBW }

// seriesPath renders a broken line: a fresh sub-path (M) starts after every gap
// where the value is absent, so unresolved actuals read as gaps, never zeros.
func seriesPath(pts []AccuracyPoint, n int, value func(AccuracyPoint) (float64, bool), y func(float64) float64) string {
	var b strings.Builder
	pen := false
	for i := range pts {
		val, ok := value(pts[i])
		if !ok {
			pen = false
			continue
		}
		x, yy := xAt(i, n), y(val)
		if pen {
			fmt.Fprintf(&b, " L %.2f %.2f", x, yy)
		} else {
			fmt.Fprintf(&b, "M %.2f %.2f", x, yy)
			pen = true
		}
	}
	return b.String()
}

func (s *Server) buildSolarChart(pts []AccuracyPoint, n int) AccChart {
	solcast := func(p AccuracyPoint) (float64, bool) { return p.SolcastKW, p.HasSolcast }
	model := func(p AccuracyPoint) (float64, bool) { return p.ModelKW, p.HasModel }
	actual := func(p AccuracyPoint) (float64, bool) { return p.ActualPVKW, p.HasActualPV }

	peak := 0.0
	for i := range pts {
		for _, f := range []func(AccuracyPoint) (float64, bool){solcast, model, actual} {
			if val, ok := f(pts[i]); ok && val > peak {
				peak = val
			}
		}
	}
	c := AccChart{Key: "solar", Title: "Solar", W: accVBW, H: accVBH}
	if peak <= 0 {
		c.Empty, c.Note = true, "No solar forecast or generation recorded yet."
		return c
	}
	y := linearY(0, peak)
	c.YTop, c.YBot = fmt.Sprintf("%.1f kW", peak), "0"
	c.Series = []AccSeries{
		{Label: "Solcast", Accent: "solar", Dashed: true, Path: seriesPath(pts, n, solcast, y)},
		{Label: "Expected (weather)", Accent: "model", Dashed: true, Path: seriesPath(pts, n, model, y)},
		{Label: "Measured", Accent: "solar", Path: seriesPath(pts, n, actual, y)},
	}
	c.Stats = []AccStat{
		errStat("Solcast", pts, solcast, actual, "kW", 1),
		errStat("Expected (weather)", pts, model, actual, "kW", 1),
	}
	c.Pending, c.Note = pendingState(c.Stats, "Measured PV pending — accuracy fills in as slots elapse.")
	return c
}

func (s *Server) buildSOCChart(pts []AccuracyPoint, n int) AccChart {
	planned := func(p AccuracyPoint) (float64, bool) { return p.PlannedSOC * 100, p.HasPlannedSOC }
	actual := func(p AccuracyPoint) (float64, bool) { return p.ActualSOC * 100, p.HasActualSOC }

	c := AccChart{Key: "soc", Title: "Battery SoC", W: accVBW, H: accVBH}
	anyPlan, anyActual := anyPresent(pts, planned), anyPresent(pts, actual)
	if !anyPlan && !anyActual {
		c.Empty, c.Note = true, "No planned or measured SoC recorded yet."
		return c
	}
	y := linearY(0, 100) // SoC is always 0–100%
	c.YTop, c.YBot = "100%", "0%"
	c.Series = []AccSeries{
		{Label: "Planned", Accent: "charge", Dashed: true, Path: seriesPath(pts, n, planned, y)},
		{Label: "Measured", Accent: "charge", Path: seriesPath(pts, n, actual, y)},
	}
	c.Stats = []AccStat{errStat("Planned", pts, planned, actual, "%", 1)}
	c.Pending, c.Note = pendingState(c.Stats, "Measured SoC pending — accuracy fills in as slots elapse.")
	return c
}

func (s *Server) buildGridChart(pts []AccuracyPoint, n int) AccChart {
	planned := func(p AccuracyPoint) (float64, bool) { return p.PlannedGridKW, p.HasPlannedGrid }
	actual := func(p AccuracyPoint) (float64, bool) { return p.ActualGridKW, p.HasActualGrid }

	c := AccChart{Key: "grid", Title: "Grid", W: accVBW, H: accVBH}
	anyPlan, anyActual := anyPresent(pts, planned), anyPresent(pts, actual)
	if !anyPlan && !anyActual {
		c.Empty, c.Note = true, "No planned or measured grid flow recorded yet."
		return c
	}
	// Symmetric domain around zero so import (+) and export (−) read correctly.
	amax := 0.5
	for i := range pts {
		for _, f := range []func(AccuracyPoint) (float64, bool){planned, actual} {
			if val, ok := f(pts[i]); ok {
				amax = math.Max(amax, math.Abs(val))
			}
		}
	}
	y := linearY(-amax, amax)
	zero := y(0)
	c.ZeroY = &zero
	c.YTop, c.YBot = fmt.Sprintf("+%.1f kW", amax), fmt.Sprintf("−%.1f kW", amax)
	c.Series = []AccSeries{
		{Label: "Planned", Accent: "discharge", Dashed: true, Path: seriesPath(pts, n, planned, y)},
		{Label: "Measured", Accent: "discharge", Path: seriesPath(pts, n, actual, y)},
	}
	c.Stats = []AccStat{errStat("Planned", pts, planned, actual, "kW", 1)}
	c.Pending, c.Note = pendingState(c.Stats, "Measured grid pending — accuracy fills in as slots elapse.")
	return c
}

// linearY returns a value→y mapper over [lo, hi] into the padded viewBox (higher
// value = higher on screen = smaller y). A degenerate range pins to the middle.
func linearY(lo, hi float64) func(float64) float64 {
	const pad = 3.0
	span := hi - lo
	if span <= 0 {
		return func(float64) float64 { return accVBH / 2 }
	}
	return func(val float64) float64 {
		frac := (val - lo) / span
		return accVBH - pad - frac*(accVBH-2*pad)
	}
}

// anyPresent reports whether any point carries the given value.
func anyPresent(pts []AccuracyPoint, value func(AccuracyPoint) (float64, bool)) bool {
	for i := range pts {
		if _, ok := value(pts[i]); ok {
			return true
		}
	}
	return false
}

// errStat computes MAE + RMSE of pred vs actual over the paired points.
func errStat(label string, pts []AccuracyPoint, pred, actual func(AccuracyPoint) (float64, bool), unit string, dp int) AccStat {
	var sumAbs, sumSq float64
	var n int
	for i := range pts {
		pv, okP := pred(pts[i])
		av, okA := actual(pts[i])
		if !okP || !okA {
			continue
		}
		d := pv - av
		sumAbs += math.Abs(d)
		sumSq += d * d
		n++
	}
	if n == 0 {
		return AccStat{Label: label, Value: "awaiting measured data"}
	}
	mae := sumAbs / float64(n)
	rmse := math.Sqrt(sumSq / float64(n))
	return AccStat{
		Label: label,
		Value: fmt.Sprintf("MAE %.*f · RMSE %.*f %s  (n=%d)", dp, mae, dp, rmse, unit, n),
	}
}

// pendingState flags a chart whose predictions are drawn but no paired actual
// has landed (every stat is still awaiting data), so it can show a note.
func pendingState(stats []AccStat, note string) (bool, string) {
	for _, st := range stats {
		if !strings.HasPrefix(st.Value, "awaiting") {
			return false, ""
		}
	}
	return true, note
}

// accMarks places up to a handful of time ticks across the window: a day label
// at local midnight, otherwise the hour at 12:00 boundaries.
func (s *Server) accMarks(pts []AccuracyPoint) []AccMark {
	n := len(pts)
	var marks []AccMark
	for i := range pts {
		t := pts[i].Slot.In(s.loc)
		if t.Minute() != 0 {
			continue
		}
		isDay := t.Hour() == 0
		if !isDay && t.Hour()%12 != 0 {
			continue
		}
		label := t.Format("15:04")
		if isDay {
			label = t.Format("Mon")
		}
		marks = append(marks, AccMark{Frac: (float64(i) + 0.5) / float64(n), Label: label, IsDay: isDay})
	}
	return marks
}
