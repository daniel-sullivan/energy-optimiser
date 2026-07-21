package serve

import (
	"bytes"
	"html/template"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"energy-optimiser/config"
	"energy-optimiser/optimizer"
)

func TestSeriesPathBreaksAtGaps(t *testing.T) {
	pts := []AccuracyPoint{
		{HasActualPV: true, ActualPVKW: 1},
		{}, // gap — no actual
		{HasActualPV: true, ActualPVKW: 2},
		{HasActualPV: true, ActualPVKW: 3},
	}
	value := func(p AccuracyPoint) (float64, bool) { return p.ActualPVKW, p.HasActualPV }
	path := seriesPath(pts, len(pts), value, func(v float64) float64 { return v })

	// Two runs (before and after the gap) → two move commands.
	if got := strings.Count(path, "M"); got != 2 {
		t.Errorf("want 2 sub-paths across the gap, got %d in %q", got, path)
	}
}

func TestErrStatComputesMaeRmse(t *testing.T) {
	pts := []AccuracyPoint{
		{HasSolcast: true, SolcastKW: 2, HasActualPV: true, ActualPVKW: 1}, // err +1
		{HasSolcast: true, SolcastKW: 1, HasActualPV: true, ActualPVKW: 4}, // err -3
	}
	pred := func(p AccuracyPoint) (float64, bool) { return p.SolcastKW, p.HasSolcast }
	actual := func(p AccuracyPoint) (float64, bool) { return p.ActualPVKW, p.HasActualPV }
	st := errStat("Solcast", pts, pred, actual, "kW", 1)

	// MAE = (1+3)/2 = 2.0 ; RMSE = sqrt((1+9)/2) = sqrt(5) ≈ 2.2
	if !strings.Contains(st.Value, "MAE 2.0") || !strings.Contains(st.Value, "RMSE 2.2") {
		t.Errorf("unexpected stat: %q", st.Value)
	}
	if !strings.Contains(st.Value, "n=2") {
		t.Errorf("missing sample count: %q", st.Value)
	}
}

func TestErrStatAwaitsWithoutPairs(t *testing.T) {
	pts := []AccuracyPoint{{HasSolcast: true, SolcastKW: 2}} // prediction but no actual
	pred := func(p AccuracyPoint) (float64, bool) { return p.SolcastKW, p.HasSolcast }
	actual := func(p AccuracyPoint) (float64, bool) { return p.ActualPVKW, p.HasActualPV }
	st := errStat("Solcast", pts, pred, actual, "kW", 1)
	if !strings.HasPrefix(st.Value, "awaiting") {
		t.Errorf("want awaiting-data note, got %q", st.Value)
	}
}

func TestBuildAccuracyCollectingWhenEmpty(t *testing.T) {
	s := &Server{loc: time.UTC}
	v := s.buildAccuracy(AccuracySnapshot{WindowH: 168, SlotMin: 30})
	if !v.Collecting {
		t.Error("empty snapshot should render the collecting state")
	}
}

// TestAccuracyTemplateRenders parses the real template set (as Server.New does)
// and executes the accuracy partial against a populated view exercising the
// SVG paths, legend, per-chart stats, an empty chart, a zero baseline, and the
// x-axis marks — a guard that the template stays in sync with the view model.
func TestAccuracyTemplateRenders(t *testing.T) {
	funcMap := template.FuncMap{"mulf": func(a, b float64) float64 { return a * b }}
	tmpl := template.Must(template.New("").Funcs(funcMap).ParseFS(templateFS, "templates/*.html"))

	zero := 50.0
	v := &DashboardView{Accuracy: AccuracyView{
		WindowLabel: "last 7 days · 30-min slots",
		Charts: []AccChart{
			{Key: "solar", Title: "Solar", W: 100, H: 100, YTop: "6.0 kW", YBot: "0",
				Series: []AccSeries{
					{Label: "Solcast", Accent: "solar", Dashed: true, Path: "M 0 10 L 50 20"},
					{Label: "Measured", Accent: "solar", Path: "M 0 12 L 50 22"},
				},
				Stats: []AccStat{{Label: "Solcast", Value: "MAE 0.4 · RMSE 0.6 kW  (n=12)"}}},
			{Key: "grid", Title: "Grid", W: 100, H: 100, ZeroY: &zero, YTop: "+3.0 kW", YBot: "−3.0 kW",
				Pending: true, Note: "Measured grid pending — accuracy fills in as slots elapse.",
				Series: []AccSeries{{Label: "Planned", Accent: "discharge", Dashed: true, Path: "M 0 40"}},
				Stats:  []AccStat{{Label: "Planned", Value: "awaiting measured data"}}},
			{Key: "soc", Title: "Battery SoC", Empty: true, Note: "No planned or measured SoC recorded yet."},
		},
		Marks: []AccMark{{Frac: 0.25, Label: "Mon", IsDay: true}, {Frac: 0.75, Label: "12:00"}},
	}}

	var buf bytes.Buffer
	if err := tmpl.ExecuteTemplate(&buf, "accuracy", v); err != nil {
		t.Fatalf("accuracy template failed to render: %v", err)
	}
	out := buf.String()
	for _, want := range []string{"acc-grid", "M 0 10 L 50 20", "acc-zero", "awaiting measured data", "No planned or measured SoC", "acc-tick"} {
		if !strings.Contains(out, want) {
			t.Errorf("rendered accuracy panel missing %q", want)
		}
	}
}

// fakeProvider is a minimal StateProvider for full-page render tests.
type fakeProvider struct{ snap AccuracySnapshot }

func (f fakeProvider) Schedule() *optimizer.Schedule    { return nil }
func (f fakeProvider) CurrentState() map[string]float64 { return map[string]float64{} }
func (f fakeProvider) LoadConfidence() float64          { return 0.5 }
func (f fakeProvider) LastTick() time.Time              { return time.Now() }
func (f fakeProvider) DataStale() bool                  { return false }
func (f fakeProvider) Accuracy() AccuracySnapshot       { return f.snap }
func (f fakeProvider) Subscribe() *Subscriber           { return &Subscriber{C: make(chan struct{}, 1)} }
func (f fakeProvider) Unsubscribe(*Subscriber)          {}

// TestDashboardRoutePrintsAccuracyPanel renders the whole page through the real
// handler + parsed template set and asserts the accuracy panel is wired in.
func TestDashboardRoutePrintsAccuracyPanel(t *testing.T) {
	funcMap := template.FuncMap{"mulf": func(a, b float64) float64 { return a * b }}
	tmpl := template.Must(template.New("").Funcs(funcMap).ParseFS(templateFS, "templates/*.html"))

	slot := time.Date(2026, 7, 20, 10, 0, 0, 0, time.UTC)
	snap := AccuracySnapshot{
		WindowH: 168, SlotMin: 30,
		Points: []AccuracyPoint{{
			Slot:       slot,
			HasSolcast: true, SolcastKW: 3.0,
			HasActualPV: true, ActualPVKW: 2.6,
			HasPlannedSOC: true, PlannedSOC: 0.6, HasActualSOC: true, ActualSOC: 0.58,
			HasPlannedGrid: true, PlannedGridKW: -0.4, HasActualGrid: true, ActualGridKW: -0.5,
		}},
	}
	s := &Server{
		provider: fakeProvider{snap: snap},
		battery:  config.Battery{SOCMin: 0.2, SOCMax: 0.9, CapacityKWh: 10},
		loc:      time.UTC,
		slotDur:  30 * time.Minute,
		tmpl:     tmpl,
	}

	rr := httptest.NewRecorder()
	s.handleDashboard(rr, httptest.NewRequest(http.MethodGet, "/", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("dashboard status = %d", rr.Code)
	}
	body := rr.Body.String()
	for _, want := range []string{"Forecast accuracy", `id="accuracy"`, "acc-grid", "--model:#b58cff"} {
		if !strings.Contains(body, want) {
			t.Errorf("dashboard missing %q", want)
		}
	}
}
