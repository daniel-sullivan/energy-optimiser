package serve

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"html/template"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"energy-optimiser/config"
	"energy-optimiser/optimizer"
)

//go:embed templates/*
var templateFS embed.FS

// Subscriber receives a signal each time the hub completes a tick. The channel
// carries no payload — on wake the SSE handler re-reads the provider (the single
// source of truth) and re-renders. Mirrors srne-solar-controller's fan-out.
type Subscriber struct {
	C chan struct{}
}

// StateProvider is the interface the dashboard needs from the hub: the current
// plan, live measured state, model confidence, the last tick time, and a
// tick-driven pub/sub for pushing schedule/decision updates to SSE clients.
type StateProvider interface {
	Schedule() *optimizer.Schedule
	CurrentState() map[string]float64
	LoadConfidence() float64
	LastTick() time.Time
	DataStale() bool
	Subscribe() *Subscriber
	Unsubscribe(sub *Subscriber)
}

// Server serves the web dashboard.
type Server struct {
	provider StateProvider
	battery  config.Battery
	rates    config.Rates
	loc      *time.Location
	slotDur  time.Duration
	port     int
	tmpl     *template.Template
}

// New builds the dashboard server. It reads the battery band, tariff windows,
// time zone, and slot duration from cfg so the UI's derived numbers (time
// remaining, off-peak shading, the schedule grid) match the optimiser and the
// MQTT sensors exactly.
func New(provider StateProvider, cfg *config.Config) *Server {
	funcMap := template.FuncMap{
		"mulf": func(a, b float64) float64 { return a * b },
	}
	tmpl := template.Must(
		template.New("").Funcs(funcMap).ParseFS(templateFS, "templates/*.html"),
	)
	slot := cfg.Service.SlotDuration.Duration
	if slot <= 0 {
		slot = 30 * time.Minute
	}
	return &Server{
		provider: provider,
		battery:  cfg.Battery,
		rates:    cfg.Rates,
		loc:      cfg.Location(),
		slotDur:  slot,
		port:     cfg.Service.WebPort,
		tmpl:     tmpl,
	}
}

func (s *Server) Start(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", s.handleDashboard)
	mux.HandleFunc("GET /api/state", s.handleState)
	mux.HandleFunc("GET /api/schedule", s.handleSchedule)
	mux.HandleFunc("GET /api/decision", s.handleDecision)
	mux.HandleFunc("GET /sse", s.handleSSE)

	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", s.port),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		<-ctx.Done()
		_ = srv.Shutdown(context.Background())
	}()

	slog.Info("dashboard", "url", fmt.Sprintf("http://localhost:%d", s.port))
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		return err
	}
	return nil
}

func (s *Server) handleDashboard(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := s.tmpl.ExecuteTemplate(w, "layout", s.buildView()); err != nil {
		slog.Error("template render", "error", err)
		http.Error(w, "render error", http.StatusInternalServerError)
	}
}

func (s *Server) handleState(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(s.provider.CurrentState())
}

func (s *Server) handleSchedule(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(s.provider.Schedule())
}

// handleDecision returns the derived, human-facing decision snapshot (action,
// rationale, gauges) as JSON — the same numbers the tiles show.
func (s *Server) handleDecision(w http.ResponseWriter, _ *http.Request) {
	v := s.buildView()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"action":             v.Decision.Action,
		"rationale":          v.Decision.Rationale,
		"next":               v.Decision.NextLabel,
		"charge_remaining":   v.ChargeGauge.Big,
		"discharge_remaining": v.DischargeGauge.Big,
		"confidence":         v.Confidence,
		"last_tick":          v.LastTick,
	})
}

// handleSSE streams HTML partials for htmx sse-swap. Two cadences drive it: the
// hub's tick signal (a full refresh when the plan changes) and a short heartbeat
// (live telemetry — SoC/PV/grid/load and the derived gauges — re-read from HA's
// continuously-updated state cache between ticks).
func (s *Server) handleSSE(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	flusher.Flush()

	sub := s.provider.Subscribe()
	defer s.provider.Unsubscribe(sub)

	heartbeat := time.NewTicker(3 * time.Second)
	defer heartbeat.Stop()

	ctx := r.Context()
	s.emitFull(w, flusher) // paint immediately on connect

	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-sub.C:
			if !ok {
				return
			}
			s.emitFull(w, flusher)
		case <-heartbeat.C:
			s.emitTelemetry(w, flusher)
		}
	}
}

// emitFull renders every region — used on connect and on each tick, when the
// plan (ribbon, forecast, events) may have changed.
func (s *Server) emitFull(w http.ResponseWriter, flusher http.Flusher) {
	v := s.buildView()
	for _, name := range []string{"meta", "decision", "tiles", "gauges", "ribbon", "forecast", "events"} {
		s.writePartial(w, name, v)
	}
	flusher.Flush()
}

// emitTelemetry renders only the fast-moving regions between ticks.
func (s *Server) emitTelemetry(w http.ResponseWriter, flusher http.Flusher) {
	v := s.buildView()
	for _, name := range []string{"meta", "decision", "tiles", "gauges"} {
		s.writePartial(w, name, v)
	}
	flusher.Flush()
}

func (s *Server) writePartial(w http.ResponseWriter, name string, v *DashboardView) {
	var buf bytes.Buffer
	if err := s.tmpl.ExecuteTemplate(&buf, name, v); err != nil {
		slog.Error("partial render", "name", name, "error", err)
		return
	}
	writeSSEEvent(w, name, buf.String())
}

// writeSSEEvent writes one SSE event, splitting multi-line HTML per the spec.
func writeSSEEvent(w http.ResponseWriter, event, data string) {
	_, _ = fmt.Fprintf(w, "event: %s\n", event)
	for _, line := range strings.Split(data, "\n") {
		_, _ = fmt.Fprintf(w, "data: %s\n", line)
	}
	_, _ = fmt.Fprint(w, "\n")
}
