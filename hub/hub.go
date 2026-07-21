package hub

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"runtime/debug"
	"sync"
	"time"

	"energy-optimiser/actuator"
	"energy-optimiser/alert"
	"energy-optimiser/config"
	"energy-optimiser/forecast"
	"energy-optimiser/ha"
	"energy-optimiser/influx"
	"energy-optimiser/loadmodel"
	"energy-optimiser/optimizer"
	"energy-optimiser/pvmodel"
	"energy-optimiser/serve"
)

// Refresh cadences for the background forecast sources.
const (
	// solarRetryInterval bounds retries of the Solcast fetch while its cache is
	// still nil (a failed/never-run initial fetch), so a startup failure recovers
	// without hammering the API.
	solarRetryInterval = 10 * time.Minute
	// weatherRefreshInterval is the normal Open-Meteo GTI refresh cadence; the GTI
	// forecast moves slowly, so a few times a day is ample.
	weatherRefreshInterval = 6 * time.Hour
	// weatherRetryInterval bounds retries while the weather cache is still nil.
	weatherRetryInterval = 10 * time.Minute
	// solarResidualLogInterval rate-limits the learned-vs-Solcast RMSE log line.
	solarResidualLogInterval = 1 * time.Hour
)

// Hub is the central coordinator that runs the 5-minute tick loop.
type Hub struct {
	cfg         *config.Config
	influx      *influx.Client // nil if unavailable
	solcast     *forecast.SolcastClient
	weather     *forecast.WeatherClient
	pvModel     *pvmodel.Model // nil if the persistent model could not be prepared
	loadModel   *loadmodel.Model
	ha          *ha.Client
	actuator    *actuator.Actuator
	notifier    *alert.Notifier
	decisionPub *serve.DecisionPublisher
	server      *serve.Server
	accuracy    *accuracyRecorder
	dryRun      bool
	mode        actuator.Mode

	mu          sync.RWMutex
	schedule    *optimizer.Schedule
	lastTick    time.Time
	subscribers map[*serve.Subscriber]struct{}

	// Refresh/log bookkeeping — touched only on the single-threaded tick path.
	lastSolcastAttempt time.Time
	lastWeatherAttempt time.Time
	lastResidualLog    time.Time

	// refreshMu guards the single-flight weather+model refresh: a slow Open-Meteo
	// fetch + fsync'd pvModel.Update runs on a background goroutine, and a new one
	// must not launch while the previous is still running.
	refreshMu      sync.Mutex
	refreshRunning bool

	// accResolveMu single-flights the background accuracy actual-resolution so a
	// slow metrics lookup never overlaps itself across ticks.
	accResolveMu      sync.Mutex
	accResolveRunning bool
}

func New(cfg *config.Config, dryRun bool) (*Hub, error) {
	mode, err := actuator.ResolveMode(cfg.Mode, cfg.Observe, dryRun)
	if err != nil {
		return nil, fmt.Errorf("actuator mode: %w", err)
	}
	h := &Hub{
		cfg:         cfg,
		solcast:     forecast.NewSolcast(cfg.Solcast),
		weather:     forecast.NewWeather(cfg.Weather),
		loadModel:   loadmodel.New(cfg.Circuits, cfg.HomeAssistant.Entities.LoadPower),
		ha:          ha.New(cfg.HomeAssistant),
		dryRun:      dryRun,
		mode:        mode,
		subscribers: make(map[*serve.Subscriber]struct{}),
	}
	h.notifier = alert.NewNotifier(cfg)

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

	// Persistent PV-response model (far-horizon fill). Non-fatal: on error the
	// optimiser still runs, the fill degrades to Solcast-only (0 beyond coverage).
	if pm, err := pvmodel.New(cfg.PVModel); err != nil {
		slog.Warn("pv model unavailable — far-horizon solar fill disabled", "error", err)
	} else {
		h.pvModel = pm
	}

	// Actuator — Phase-1 blip-safe grid-charge control. Defaults to observe
	// (no inverter writes); live actuation requires an explicit mode = "live".
	act, err := actuator.New(cfg.ActuatorHW, cfg.Battery, &cfg.Rates, h.ha, mode)
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

	// Forecast-accuracy recorder — observe-safe: reads the metrics store (PV/grid
	// actuals) and the HA state cache (SoC), writes only its own rolling-window
	// JSON under DataDir. History accrues from deploy; nothing is backfilled.
	var accActuals actualsSource
	if h.influx != nil {
		accActuals = influxActuals{client: h.influx}
	}
	h.accuracy = newAccuracyRecorder(
		cfg.PVModel.DataDir,
		cfg.Service.SlotDuration.Duration,
		accActuals,
		cfg.HomeAssistant.Entities.PVPower,
		cfg.HomeAssistant.Entities.GridPower,
	)

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

	// Reconcile the actuator against the live inverter state and start its
	// write-owning goroutine + watchdog before the first tick. Non-fatal: a
	// reconcile write error is logged; the watchdog will retry.
	if err := h.actuator.Start(ctx); err != nil {
		slog.Warn("actuator startup reconcile", "error", err)
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
		h.lastSolcastAttempt = time.Now()
		if _, err := h.solcast.Fetch(ctx); err != nil {
			slog.Warn("initial solar forecast failed", "error", err)
		}
	} else {
		slog.Info("solcast API key not configured, skipping solar forecast")
	}

	// Initial weather (GTI) fetch + PV model warm-up (non-fatal), kicked off in the
	// background so the first tick isn't blocked by a slow Open-Meteo fetch. The
	// far-horizon fill degrades to Solcast-only until the model warms — acceptable.
	h.startRefreshWeatherAndModel(ctx, time.Now().In(h.cfg.Location()))

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
	// Defense in depth: a single bad tick (e.g. a config-driven BuildGrid
	// invariant panic) must never kill the daemon — recover, log with the stack,
	// and skip this tick so the next one runs.
	defer func() {
		if r := recover(); r != nil {
			slog.Error("tick panicked — skipping this tick", "panic", r, "stack", string(debug.Stack()))
		}
	}()

	// Floor to the slot boundary so solar/load vectors, tariff windows, and the
	// schedule grid all align (PrepareInput floors too; keep them consistent).
	now := time.Now().In(h.cfg.Location()).Truncate(h.cfg.Service.SlotDuration.Duration)
	slog.Info("tick", "time", now.Format(time.TimeOnly), "actuator_mode", h.mode)

	if h.cfg.Solcast.APIKey != "" {
		h.maybeRefreshSolar(ctx, now)
	}
	h.maybeRefreshWeather(ctx, now)

	currentSOC := h.ha.StateFloat(h.cfg.HomeAssistant.Entities.BatterySOC) / 100.0
	socKnown := currentSOC > 0
	if !socKnown {
		slog.Warn("battery SOC reading is 0/unknown — HA entity may not be reporting yet; "+
			"solving on an assumed SOC for the dashboard but NOT actuating",
			"entity", h.cfg.HomeAssistant.Entities.BatterySOC)
		currentSOC = 0.5 // assumed value for the schedule/dashboard only, never actuated on
	}

	// Build the telescoping slot grid once and thread it through the forecast and
	// load vectors; PrepareInput rebuilds the same deterministic grid internally.
	grid := optimizer.BuildGrid(now, h.cfg)

	solarKW := h.solarForecastSlots(grid, now)
	loadW := h.loadModel.Predict(grid.Start)

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
		plan := h.buildChargePlan(now, slot, socKnown)
		if err := h.actuator.SetChargePlan(ctx, plan); err != nil {
			slog.Error("actuator set charge plan", "error", err)
		}
	}
	// Advisory decision/risk notifications run in every mode (they never actuate).
	h.notifier.Evaluate(ctx, now, sched, currentSOC)

	h.recordDecision(ctx, now, slot)
	h.publishDecision(now, slot, sched, currentSOC)
	h.recordAccuracy(ctx, now, slot, currentSOC, socKnown)

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

// buildChargePlan derives the actuator's desired grid-charge state from the
// current slot. It commands the GRID SHARE — the grid kW needed after expected
// PV self-charge — never gross battery flow, and only ever inside an active
// off-peak window. When the SOC is unknown it never initiates charging (the
// solve ran on an assumed SOC purely for the dashboard).
func (h *Hub) buildChargePlan(now time.Time, slot *optimizer.Slot, socKnown bool) actuator.ChargePlan {
	win, off := h.cfg.Rates.ActiveWindow(now)
	charging := socKnown && slot.GridCharge && off
	var gridKW float64
	if charging {
		batteryChargeKW := math.Max(0, slot.BatteryFlowKW) // positive = charging
		pvSurplus := math.Max(0, slot.SolarKW-slot.LoadKW) // PV available to self-charge
		gridKW = math.Max(0, batteryChargeKW-pvSurplus)
	}
	return actuator.ChargePlan{Charging: charging, GridKW: gridKW, Window: win}
}

// since the cache was last fetched. A NIL cache (the initial fetch failed, or has
// not run) must TRIGGER a rate-limited retry — not suppress fetching for the whole
// process life — so a single startup failure is recoverable (M3 fix).
func (h *Hub) maybeRefreshSolar(ctx context.Context, now time.Time) {
	cached := h.solcast.Cached()
	if cached == nil {
		if h.lastSolcastAttempt.IsZero() || now.Sub(h.lastSolcastAttempt) >= solarRetryInterval {
			h.lastSolcastAttempt = now
			slog.Info("retrying initial solar forecast")
			if _, err := h.solcast.Fetch(ctx); err != nil {
				slog.Warn("solar refresh failed", "error", err)
			}
		}
		return
	}
	for _, pt := range h.cfg.Solcast.PollTimes {
		target := time.Date(now.Year(), now.Month(), now.Day(),
			pt.Hour, pt.Minute, 0, 0, now.Location())
		if now.After(target) && cached.FetchedAt.Before(target) {
			h.lastSolcastAttempt = now
			slog.Info("refreshing solar forecast")
			if _, err := h.solcast.Fetch(ctx); err != nil {
				slog.Warn("solar refresh failed", "error", err)
			}
			return
		}
	}
}

// maybeRefreshWeather refreshes the Open-Meteo GTI forecast on a periodic cadence,
// and — mirroring the M3 fix — retries on a backoff while the cache is still nil
// rather than suppressing fetching after a failed first attempt. A successful
// fetch also folds the completed past days into the PV model.
func (h *Hub) maybeRefreshWeather(ctx context.Context, now time.Time) {
	if h.weather == nil || len(h.cfg.Solcast.Sites) == 0 {
		return
	}
	cached := h.weather.Cached()
	var due bool
	if cached == nil {
		due = h.lastWeatherAttempt.IsZero() || now.Sub(h.lastWeatherAttempt) >= weatherRetryInterval
	} else {
		due = now.Sub(cached.FetchedAt) >= weatherRefreshInterval
	}
	if due {
		h.startRefreshWeatherAndModel(ctx, now)
	}
}

// startRefreshWeatherAndModel runs refreshWeatherAndModel on a background
// goroutine, single-flighted: if a refresh is still running it is a no-op. This
// keeps the slow Open-Meteo fetch (N sites @30s timeout) and the fsync'd
// pvModel.Update off the tick thread, so a slow forecast source never stalls the
// solve/decision/broadcast. lastWeatherAttempt is set here on the tick thread
// (the only writer) so the tick-path due-check stays race-free.
func (h *Hub) startRefreshWeatherAndModel(ctx context.Context, now time.Time) {
	if h.weather == nil || len(h.cfg.Solcast.Sites) == 0 {
		return
	}
	h.refreshMu.Lock()
	if h.refreshRunning {
		h.refreshMu.Unlock()
		return
	}
	h.refreshRunning = true
	h.refreshMu.Unlock()
	h.lastWeatherAttempt = now

	go func() {
		defer func() {
			h.refreshMu.Lock()
			h.refreshRunning = false
			h.refreshMu.Unlock()
		}()
		h.refreshWeatherAndModel(ctx, now)
	}()
}

// refreshWeatherAndModel fetches the GTI forecast and, on success, runs the
// watermark-idempotent PV-model ingest (calibration from past GTI vs measured PV).
// All steps are non-fatal: the optimiser runs without the far-horizon fill. Runs
// on the background refresh goroutine (see startRefreshWeatherAndModel); the
// pvModel's own RWMutex serialises its Update against tick-thread PredictKW reads.
func (h *Hub) refreshWeatherAndModel(ctx context.Context, now time.Time) {
	if h.weather == nil || len(h.cfg.Solcast.Sites) == 0 {
		return
	}
	wf, err := h.weather.Fetch(ctx, h.cfg.Solcast.Sites)
	if err != nil {
		slog.Warn("weather (GTI) refresh failed", "error", err)
		return
	}
	slog.Info("weather forecast refreshed", "points", len(wf.Points))
	if h.pvModel == nil || h.influx == nil {
		return
	}
	if err := h.pvModel.Update(ctx, h.pvHistory(), wf, now); err != nil {
		slog.Warn("pv model ingest failed", "error", err)
		return
	}
	slog.Info("pv model updated", "maturity_now", fmt.Sprintf("%.2f", h.pvModel.Maturity(now)))
}

// pvHistory binds the metrics client to the PV entity as a pvmodel.PVHistory.
// Only called when h.influx != nil.
func (h *Hub) pvHistory() pvmodel.PVHistory {
	return pvHistorySource{client: h.influx, entityID: h.cfg.HomeAssistant.Entities.PVPower}
}

// solarForecastSlots produces the per-slot solar kW: Solcast within its coverage,
// the learned PV model × Open-Meteo GTI beyond, with a 6h crossfade at the seam.
func (h *Hub) solarForecastSlots(grid optimizer.Grid, now time.Time) []float64 {
	solar := h.solcast.Cached()
	var weather *forecast.WeatherForecast
	if h.weather != nil {
		weather = h.weather.Cached()
	}
	var model PVPredictor
	if h.pvModel != nil {
		model = h.pvModel
	}
	h.logSolarResidual(now, grid, solar, weather, model)
	return fillSolarSlots(grid, now, solar, weather, model)
}

// logSolarResidual logs, at most hourly, the RMSE between the learned model and
// Solcast over their overlap (Solcast-covered slots the model can also predict),
// so far-horizon accuracy is observable over time. The learned side is compared
// WITHOUT the lead-time haircut — this measures calibration, not the applied fill.
func (h *Hub) logSolarResidual(now time.Time, grid optimizer.Grid, solar *forecast.SolarForecast, weather *forecast.WeatherForecast, model PVPredictor) {
	if solar == nil || weather == nil || model == nil {
		return
	}
	if !h.lastResidualLog.IsZero() && now.Sub(h.lastResidualLog) < solarResidualLogInterval {
		return
	}
	coverageEnd := solcastCoverageEnd(solar, now)
	var sumSq float64
	var overlap int
	for i := range grid.Start {
		ts := grid.Start[i]
		if !ts.Before(coverageEnd) {
			break
		}
		sol, ok := averageSolcast(solar, ts, grid.End(i))
		if !ok {
			continue
		}
		gti := interpolateGTI(weather.Points, ts)
		if len(gti) == 0 {
			continue
		}
		d := model.PredictKW(ts, gti) - sol
		sumSq += d * d
		overlap++
	}
	if overlap == 0 {
		return
	}
	h.lastResidualLog = now
	slog.Info("solar model residual vs solcast",
		"rmse_kw", fmt.Sprintf("%.3f", math.Sqrt(sumSq/float64(overlap))),
		"overlap_slots", overlap)
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

// recordAccuracy captures this slot's predictions (Solcast + learned model for
// solar, planned SoC + planned net grid from the current schedule slot) and the
// live measured SoC, then kicks off background resolution of the PV/grid actuals
// from the metrics store. Observe-safe: read-only inputs, writes only its own
// rolling-window store.
func (h *Hub) recordAccuracy(ctx context.Context, now time.Time, slot *optimizer.Slot, currentSOC float64, socKnown bool) {
	solcastKW, hasSolcast, modelKW, hasModel := h.solarPredictionFor(now)
	in := accuracyTick{
		Now:        now,
		SolcastKW:  solcastKW,
		HasSolcast: hasSolcast,
		ModelKW:    modelKW,
		HasModel:   hasModel,
	}
	if socKnown {
		in.MeasuredSOC = currentSOC
	}
	if slot != nil {
		in.HasPlan = true
		in.PlannedSOC = slot.SOC
		in.PlannedGrid = slot.GridImportKW - slot.GridExportKW
	}
	h.accuracy.Record(in)
	h.startResolveAccuracy(ctx, now)
}

// solarPredictionFor returns the raw Solcast and learned-model solar predictions
// (kW) for the slot starting at slotStart — the same face-value model prediction
// logSolarResidual measures (no lead-time haircut), so the panel shows calibration
// accuracy, not the applied fill.
func (h *Hub) solarPredictionFor(slotStart time.Time) (solcastKW float64, hasSolcast bool, modelKW float64, hasModel bool) {
	slotEnd := slotStart.Add(h.cfg.Service.SlotDuration.Duration)
	solcastKW, hasSolcast = averageSolcast(h.solcast.Cached(), slotStart, slotEnd)
	if h.pvModel != nil && h.weather != nil {
		if w := h.weather.Cached(); w != nil {
			if gti := interpolateGTI(w.Points, slotStart); len(gti) > 0 {
				modelKW = h.pvModel.PredictKW(slotStart, gti)
				hasModel = true
			}
		}
	}
	return solcastKW, hasSolcast, modelKW, hasModel
}

// startResolveAccuracy runs the recorder's metrics-backed actual resolution on a
// background goroutine, single-flighted so a slow lookup never overlaps itself or
// stalls the tick.
func (h *Hub) startResolveAccuracy(ctx context.Context, now time.Time) {
	h.accResolveMu.Lock()
	if h.accResolveRunning {
		h.accResolveMu.Unlock()
		return
	}
	h.accResolveRunning = true
	h.accResolveMu.Unlock()

	go func() {
		defer func() {
			h.accResolveMu.Lock()
			h.accResolveRunning = false
			h.accResolveMu.Unlock()
		}()
		h.accuracy.ResolveActuals(ctx, now)
	}()
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

// Accuracy returns the rolling predicted-vs-actual window for the dashboard panel.
func (h *Hub) Accuracy() serve.AccuracySnapshot { return h.accuracy.Snapshot() }

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
