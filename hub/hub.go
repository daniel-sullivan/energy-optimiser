package hub

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"energy-optimiser/actuator"
	"energy-optimiser/config"
	"energy-optimiser/forecast"
	"energy-optimiser/ha"
	"energy-optimiser/influx"
	"energy-optimiser/loadmodel"
	"energy-optimiser/optimizer"
	"energy-optimiser/serve"
)

// Hub is the central coordinator that runs the 5-minute tick loop.
type Hub struct {
	cfg         *config.Config
	influx      *influx.Client // nil if unavailable
	solcast     *forecast.SolcastClient
	weather     *forecast.WeatherClient
	loadModel   *loadmodel.Model
	ha          *ha.Client
	actuator    *actuator.Actuator
	decisionPub *serve.DecisionPublisher
	server      *serve.Server
	dryRun      bool
	observe     bool

	mu          sync.RWMutex
	schedule    *optimizer.Schedule
	lastTick    time.Time
	lastRec     string
	subscribers map[*serve.Subscriber]struct{}
}

func New(cfg *config.Config, dryRun bool) (*Hub, error) {
	h := &Hub{
		cfg:         cfg,
		solcast:     forecast.NewSolcast(cfg.Solcast),
		weather:     forecast.NewWeather(cfg.Weather),
		loadModel:   loadmodel.New(cfg.Circuits, cfg.HomeAssistant.Entities.LoadPower),
		ha:          ha.New(cfg.HomeAssistant),
		dryRun:      dryRun,
		observe:     cfg.Observe,
		subscribers: make(map[*serve.Subscriber]struct{}),
	}

	// InfluxDB — non-fatal in dry-run, load model falls back to defaults
	db, err := influx.New(cfg.InfluxDB)
	if err != nil {
		if dryRun {
			slog.Warn("influxdb unavailable, using default load model", "error", err)
		} else {
			return nil, fmt.Errorf("influx: %w", err)
		}
	}
	h.influx = db

	// Actuator — skips MQTT connection in dry-run
	act, err := actuator.New(cfg.MQTT, h.ha, dryRun)
	if err != nil {
		if h.influx != nil {
			_ = h.influx.Close()
		}
		return nil, fmt.Errorf("actuator: %w", err)
	}
	h.actuator = act

	// Decision publisher — skips MQTT connection in dry-run, same as the actuator.
	// Its own device (cfg.MQTT.DecisionDeviceID) is separate from the actuator's
	// srne_system target: this publishes energy-optimiser's own plan/state, it
	// doesn't command the inverter.
	decisionPub, err := serve.NewDecisionPublisher(cfg.MQTT, dryRun)
	if err != nil {
		h.actuator.Close()
		if h.influx != nil {
			_ = h.influx.Close()
		}
		return nil, fmt.Errorf("decision publisher: %w", err)
	}
	h.decisionPub = decisionPub

	h.server = serve.New(h, cfg)
	return h, nil
}

func (h *Hub) Close() {
	if h.decisionPub != nil {
		h.decisionPub.Close()
	}
	if h.actuator != nil {
		h.actuator.Close()
	}
	_ = h.ha.Close()
	if h.influx != nil {
		_ = h.influx.Close()
	}
}

func (h *Hub) Run(ctx context.Context) error {
	// Connect to Home Assistant
	if err := h.ha.Connect(ctx); err != nil {
		return fmt.Errorf("ha connect: %w", err)
	}
	if err := h.ha.SubscribeEvents(ctx); err != nil {
		return fmt.Errorf("ha subscribe: %w", err)
	}

	// Connect the decision publisher and announce its HA-discovery entities.
	// Non-fatal: a broker outage shouldn't block the optimizer loop (paho
	// retries in the background once connected; here we just try once at
	// startup and log on failure).
	if err := h.decisionPub.Connect(); err != nil {
		slog.Warn("decision publisher mqtt connect failed", "error", err)
	} else {
		h.decisionPub.PublishDiscovery()
	}

	// Train load model (skip if no InfluxDB)
	if h.influx != nil {
		slog.Info("training load model")
		if err := h.loadModel.Train(ctx, &loadDataSource{h.influx}, 30*24*time.Hour); err != nil {
			slog.Warn("load model training incomplete", "error", err)
		}
		slog.Info("load model ready", "confidence", fmt.Sprintf("%.2f", h.loadModel.Confidence()))
	} else {
		slog.Info("load model using defaults (no InfluxDB)")
	}

	// Start web server
	go func() {
		if err := h.server.Start(ctx); err != nil {
			slog.Error("web server", "error", err)
		}
	}()

	// Initial solar forecast (non-fatal)
	if cfg := h.cfg.Solcast; cfg.APIKey != "" {
		if _, err := h.solcast.Fetch(ctx); err != nil {
			slog.Warn("initial solar forecast failed", "error", err)
		}
	} else {
		slog.Info("solcast API key not configured, skipping solar forecast")
	}

	// Tick loop
	ticker := time.NewTicker(h.cfg.Service.PollInterval.Duration)
	defer ticker.Stop()

	h.tick(ctx)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			h.tick(ctx)
		}
	}
}

func (h *Hub) tick(ctx context.Context) {
	// Floor to the slot boundary so solar/load vectors, tariff windows, and the
	// schedule grid all align (PrepareInput floors too; keep them consistent).
	now := time.Now().In(h.cfg.Location()).Truncate(h.cfg.Service.SlotDuration.Duration)
	slog.Info("tick", "time", now.Format(time.TimeOnly))

	if h.cfg.Solcast.APIKey != "" {
		h.maybeRefreshSolar(ctx, now)
	}

	currentSOC := h.ha.StateFloat(h.cfg.HomeAssistant.Entities.BatterySOC) / 100.0
	if currentSOC == 0 {
		slog.Warn("battery SOC reading is 0 — HA entity may not be reporting yet",
			"entity", h.cfg.HomeAssistant.Entities.BatterySOC)
		currentSOC = 0.5 // safe default
	}

	solarKW := h.solarForecastSlots(now)
	loadW := h.loadModel.Predict(h.slotTimes(now))

	input := optimizer.PrepareInput(now, h.cfg, solarKW, loadW, currentSOC)
	sched, err := optimizer.Solve(input)
	if err != nil {
		slog.Error("optimizer failed", "error", err)
		return
	}
	h.mu.Lock()
	h.schedule = sched
	h.mu.Unlock()

	slot := sched.CurrentSlot(now)
	if slot != nil {
		if h.observe {
			slog.Info("observe mode — not actuating", "would_grid_charge", slot.GridCharge)
			h.notifyRecommendation(ctx, now, sched, currentSOC)
		} else if err := h.actuator.SetGridCharge(slot.GridCharge); err != nil {
			slog.Error("set grid charge", "error", err)
		}
	}

	h.recordDecision(ctx, now, slot)
	h.publishDecision(now, slot, sched, currentSOC)

	var flowKW, importKW float64
	if slot != nil {
		flowKW, importKW = slot.BatteryFlowKW, slot.GridImportKW
	}
	slog.Info("tick complete",
		"soc", fmt.Sprintf("%.0f%%", currentSOC*100),
		"grid_charge", slot != nil && slot.GridCharge,
		"battery_flow_kw", fmt.Sprintf("%.1f", flowKW),
		"grid_import_kw", fmt.Sprintf("%.1f", importKW),
		"objective", fmt.Sprintf("¥%.1f", sched.ObjectiveValue),
	)

	h.mu.Lock()
	h.lastTick = time.Now()
	h.mu.Unlock()
	h.broadcast()
}

// notifyRecommendation sends an HA notification when the optimizer newly
// recommends a grid-charge window (observe mode), so the plan is visible and
// reviewable before live actuation is enabled. Only positive recommendations
// notify — "nothing to do" stays silent.
func (h *Hub) notifyRecommendation(ctx context.Context, now time.Time, sched *optimizer.Schedule, soc float64) {
	var chargeSlot *optimizer.Slot
	for i := range sched.Slots {
		s := &sched.Slots[i]
		if !s.Start.Before(now) && s.GridCharge {
			chargeSlot = s
			break
		}
	}
	if chargeSlot == nil {
		h.lastRec = "" // nothing to recommend; a future charge notifies as new
		return
	}
	msg := fmt.Sprintf("I'd recommend grid-charging at %s (battery now %.0f%%).",
		chargeSlot.Start.In(h.cfg.Location()).Format("15:04 Mon"), soc*100)
	if msg == h.lastRec {
		return // already notified this recommendation
	}
	h.lastRec = msg
	if err := h.ha.CallService(ctx, "persistent_notification", "create", map[string]any{
		"title":           "Energy Optimiser",
		"message":         msg,
		"notification_id": "energy_optimiser_recommendation",
	}); err != nil {
		slog.Warn("recommendation notify failed", "error", err)
	}
}

func (h *Hub) maybeRefreshSolar(ctx context.Context, now time.Time) {
	cached := h.solcast.Cached()
	if cached == nil {
		return
	}
	for _, pt := range h.cfg.Solcast.PollTimes {
		target := time.Date(now.Year(), now.Month(), now.Day(),
			pt.Hour, pt.Minute, 0, 0, now.Location())
		if now.After(target) && cached.FetchedAt.Before(target) {
			slog.Info("refreshing solar forecast")
			if _, err := h.solcast.Fetch(ctx); err != nil {
				slog.Warn("solar refresh failed", "error", err)
			}
			return
		}
	}
}

func (h *Hub) solarForecastSlots(now time.Time) []float64 {
	cached := h.solcast.Cached()
	if cached == nil {
		return nil
	}
	slotMins := int(h.cfg.Service.SlotDuration.Minutes())
	numSlots := int(h.cfg.Service.PlanningHorizon.Minutes()) / slotMins
	out := make([]float64, numSlots)

	for i := range out {
		slotStart := now.Add(time.Duration(i) * time.Duration(slotMins) * time.Minute)
		slotEnd := slotStart.Add(time.Duration(slotMins) * time.Minute)
		// Average all forecast points overlapping the slot (mean power), so a
		// slot spanning multiple Solcast periods isn't truncated to the first.
		var sum float64
		var n int
		for _, p := range cached.Points {
			if !p.Time.Before(slotStart) && p.Time.Before(slotEnd) {
				sum += p.EstimateKW
				n++
			}
		}
		if n > 0 {
			out[i] = sum / float64(n)
		}
	}
	return out
}

func (h *Hub) slotTimes(now time.Time) []time.Time {
	slotMins := int(h.cfg.Service.SlotDuration.Minutes())
	numSlots := int(h.cfg.Service.PlanningHorizon.Minutes()) / slotMins
	out := make([]time.Time, numSlots)
	for i := range out {
		out[i] = now.Add(time.Duration(i) * time.Duration(slotMins) * time.Minute)
	}
	return out
}

func (h *Hub) recordDecision(ctx context.Context, now time.Time, slot *optimizer.Slot) {
	if slot == nil || h.influx == nil {
		return
	}
	if h.dryRun {
		return // don't write decisions in dry-run
	}
	_ = h.influx.WritePoints(ctx, "optimizer_decision", []influx.Point{{
		Time: now,
		Tags: map[string]string{},
		Fields: map[string]any{
			"grid_charge":     slot.GridCharge,
			"battery_flow_kw": slot.BatteryFlowKW,
			"grid_import_kw":  slot.GridImportKW,
			"grid_export_kw":  slot.GridExportKW,
			"soc":             slot.SOC,
		},
	}})
}

// publishDecision pushes the current schedule's decision (grid-charge plan,
// planned flows, objective, a human-readable rationale) plus derived
// charge/discharge time-remaining to MQTT as HA-discovery entity state.
// PublishState itself handles the dry-run / not-connected no-op.
func (h *Hub) publishDecision(now time.Time, slot *optimizer.Slot, sched *optimizer.Schedule, currentSOC float64) {
	powerKW := h.ha.StateFloat(h.cfg.HomeAssistant.Entities.BatteryPower) / 1000.0
	chargeH, dischargeH := serve.TimeRemaining(h.cfg.Battery, currentSOC, powerKW)

	state := serve.DecisionState{
		Rationale: serve.RationaleFor(now, currentSOC, sched),
	}
	if slot != nil {
		state.GridCharge = slot.GridCharge
		state.BatteryFlowKW = slot.BatteryFlowKW
		state.GridImportKW = slot.GridImportKW
		state.SOCTargetPct = slot.SOC * 100
	}
	if sched != nil {
		state.ObjectiveValue = sched.ObjectiveValue
	}
	if chargeH != nil {
		state.ChargeRemainingH = chargeH
		state.ChargeRemainingFmt = serve.FormatHours(*chargeH)
	}
	if dischargeH != nil {
		state.DischargeRemainingH = dischargeH
		state.DischargeRemainingFmt = serve.FormatHours(*dischargeH)
	}

	h.decisionPub.PublishState(state)
}

// --- serve.StateProvider implementation ---

func (h *Hub) Schedule() *optimizer.Schedule {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.schedule
}

func (h *Hub) LoadConfidence() float64 { return h.loadModel.Confidence() }

// LastTick returns the wall-clock time the most recent tick completed.
func (h *Hub) LastTick() time.Time {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.lastTick
}

// Subscribe registers a new SSE client for tick notifications.
func (h *Hub) Subscribe() *serve.Subscriber {
	sub := &serve.Subscriber{C: make(chan struct{}, 1)}
	h.mu.Lock()
	h.subscribers[sub] = struct{}{}
	h.mu.Unlock()
	return sub
}

// Unsubscribe removes an SSE client and closes its channel.
func (h *Hub) Unsubscribe(sub *serve.Subscriber) {
	h.mu.Lock()
	if _, ok := h.subscribers[sub]; ok {
		delete(h.subscribers, sub)
		close(sub.C)
	}
	h.mu.Unlock()
}

// broadcast wakes every subscriber (non-blocking; a client already holding a
// pending signal simply coalesces). Held under the same lock as Unsubscribe so
// a send can never race a close.
func (h *Hub) broadcast() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for sub := range h.subscribers {
		select {
		case sub.C <- struct{}{}:
		default:
		}
	}
}

func (h *Hub) CurrentState() map[string]float64 {
	return map[string]float64{
		"battery_soc":   h.ha.StateFloat(h.cfg.HomeAssistant.Entities.BatterySOC),
		"pv_power":      h.ha.StateFloat(h.cfg.HomeAssistant.Entities.PVPower),
		"grid_power":    h.ha.StateFloat(h.cfg.HomeAssistant.Entities.GridPower),
		"load_power":    h.ha.StateFloat(h.cfg.HomeAssistant.Entities.LoadPower),
		"battery_power": h.ha.StateFloat(h.cfg.HomeAssistant.Entities.BatteryPower),
	}
}

// DataStale reports whether the live HA feed has frozen — the websocket is down
// or no entity has refreshed recently (the freshest entities update every few
// seconds, so >2 min means a dead feed). Surfaced in the dashboard so stale
// numbers cannot masquerade as live.
func (h *Hub) DataStale() bool {
	if !h.ha.Connected() {
		return true
	}
	nu := h.ha.NewestUpdate()
	return nu.IsZero() || time.Since(nu) > 2*time.Minute
}

// --- loadmodel.DataSource adapter ---

type loadDataSource struct {
	client *influx.Client
}

func (s *loadDataSource) QueryPower(ctx context.Context, entityID string, from, to time.Time) ([]loadmodel.Sample, error) {
	samples, err := s.client.QueryPower(ctx, entityID, from, to)
	if err != nil {
		return nil, err
	}
	out := make([]loadmodel.Sample, len(samples))
	for i, v := range samples {
		out[i] = loadmodel.Sample{Time: v.Time, Value: v.Value}
	}
	return out, nil
}
