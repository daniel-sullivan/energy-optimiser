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
	// loadRetrainInterval is how often the load model is retrained from history so
	// its level/mean/confidence track a settling load instead of freezing at boot.
	loadRetrainInterval = 24 * time.Hour
	// loadRetrainTimeout bounds the background retrain's multi-week Influx pull so
	// a slow/hung history query can't leave the retrain goroutine (and its
	// single-flight guard) running indefinitely. The tick loop is unaffected either
	// way because the retrain is off-thread; this just caps a stuck query.
	loadRetrainTimeout = 3 * time.Minute
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

	// loadModelMu guards the h.loadModel POINTER only. A published
	// *loadmodel.Model is immutable — never mutated after a swap — so the model's
	// own Predict/Confidence methods need no internal locking; the ONLY shared
	// mutable state is the pointer, which this RWMutex protects. The retrain
	// builds a FRESH model off the tick goroutine and swaps the pointer under the
	// write lock; every reader (tick Predict, LoadConfidence) goes through
	// currentLoadModel under the read lock.
	loadModelMu sync.RWMutex

	// Refresh/log bookkeeping — touched only on the single-threaded tick path.
	lastSolcastAttempt time.Time
	lastWeatherAttempt time.Time
	lastResidualLog    time.Time

	// refreshMu guards the single-flight weather+model refresh: a slow Open-Meteo
	// fetch + fsync'd pvModel.Update runs on a background goroutine, and a new one
	// must not launch while the previous is still running.
	refreshMu      sync.Mutex
	refreshRunning bool

	// loadTrainMu single-flights the background load-model retrain (loadTrainRunning)
	// and guards lastLoadTrain, which the retrain goroutine writes on a successful
	// swap and the tick reads to decide whether a retrain is due.
	loadTrainMu      sync.Mutex
	loadTrainRunning bool
	lastLoadTrain    time.Time

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
		ha:          ha.New(cfg.HomeAssistant),
		dryRun:      dryRun,
		mode:        mode,
		subscribers: make(map[*serve.Subscriber]struct{}),
	}
	// Initial (untrained) model published before any reader exists; Run trains a
	// fresh one and swaps it in before starting the web server.
	h.loadModel = h.newLoadModel()
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

	// Actuator — timed-charge grid-charge control. Defaults to observe (no
	// inverter writes); live actuation requires an explicit mode = "live".
	act, err := actuator.New(cfg.ActuatorHW, &cfg.Rates, h.ha, mode)
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

	// Train load model (skip if no InfluxDB). Synchronous and BEFORE the web
	// server starts, so the first tick has a real trained model and no reader
	// exists yet. Builds a fresh model and publishes it under the load-model lock,
	// same as the periodic retrain — the initial untrained model from New is only
	// a placeholder.
	if h.influx != nil {
		now := time.Now().In(h.cfg.Location())
		if m, err := h.buildTrainedLoadModel(ctx); err != nil {
			slog.Warn("load model training incomplete — keeping default model", "error", err)
		} else {
			h.swapLoadModel(m)
			h.loadTrainMu.Lock()
			h.lastLoadTrain = now
			h.loadTrainMu.Unlock()
			slog.Info("load model ready", "confidence", fmt.Sprintf("%.2f", m.Confidence()))
		}
	} else {
		slog.Info("load model using defaults (no InfluxDB)")
	}

	// Start web server
	go func() {
		if err := h.server.Start(ctx); err != nil {
			slog.Error("web server", "error", err)
		}
	}()

	// Initial solar forecast (non-fatal). h.solcast was constructed with any
	// persisted cache from a prior run already loaded (see forecast.NewSolcast),
	// so route the startup fetch through maybeRefreshSolar rather than always
	// fetching: a restart that lands within the poll-time freshness window
	// reuses that cache and spends zero Solcast API calls — important since the
	// free tier's daily request cap is easily exhausted by a handful of
	// restarts. A genuinely stale or missing cache still fetches immediately,
	// same as before.
	if cfg := h.cfg.Solcast; cfg.APIKey != "" {
		h.maybeRefreshSolar(ctx, time.Now().In(h.cfg.Location()))
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
	h.maybeRetrainLoadModel(ctx, now)

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
	// currentLoadModel (not h.loadModel directly): the retrain swaps the pointer
	// from a background goroutine, so even the tick's own read must take the lock.
	loadW := h.currentLoadModel().Predict(grid.Start)

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
		plan := h.buildChargePlan(now, sched, slot, currentSOC, socKnown)
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
// PV self-charge — never gross battery flow, and only when the current slot is an
// active off-peak window. When the SOC is unknown it never initiates charging
// (the solve ran on an assumed SOC purely for the dashboard). The actuator
// re-derives the off-peak window it programs from its own tariff config, so the
// plan carries only whether-to-charge and the grid kW.
func (h *Hub) buildChargePlan(now time.Time, sched *optimizer.Schedule, slot *optimizer.Slot, currentSOC float64, socKnown bool) actuator.ChargePlan {
	charging := socKnown && slot.GridCharge && h.cfg.Rates.IsOffPeak(now)
	plan := actuator.ChargePlan{Charging: charging, CurrentSOC: -1}
	if socKnown {
		plan.CurrentSOC = currentSOC
	}
	if charging {
		batteryChargeKW := math.Max(0, slot.BatteryFlowKW) // positive = charging
		pvSurplus := math.Max(0, slot.SolarKW-slot.LoadKW) // PV available to self-charge
		plan.GridKW = math.Max(0, batteryChargeKW-pvSurplus)
		// Block context so the actuator commits to the whole charge run and stops
		// at its SoC goal rather than toggling on the solver's per-tick wobble.
		plan.TargetSOC, plan.BlockEnd = sched.ChargeBlockFrom(slot)
	}
	return plan
}

// maybeRefreshSolar fetches a new solar forecast only when one of the
// configured poll_times has passed and the cache is still older than that
// target — i.e. it gates on how long it's been (in wall-clock, poll_times
// terms) since the cache was last fetched. A NIL cache (the initial fetch failed, or has
// not run) must TRIGGER a rate-limited retry — not suppress fetching for the whole
// process life — so a single startup failure is recoverable (M3 fix).
//
// This is also the entry point Run uses for the STARTUP fetch (not just the
// tick loop): h.solcast is constructed with any forecast persisted by a prior
// process already loaded into its cache (forecast.NewSolcast reads the cache
// file at CacheDir), so on a restart that lands within the poll_times window
// `cached` here is that persisted forecast — still "fresh" by the same test
// above — and the poll-time loop returns without calling Fetch at all. That's
// what makes a restart free: no cache age special-case is needed, the normal
// poll_times gate already treats a reloaded cache identically to one fetched
// earlier in the same process.
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

// newLoadModel constructs a fresh, untrained load model from the current config.
// Shared by New (the startup placeholder) and buildTrainedLoadModel (the trained
// startup + retrain models) so the Params wiring lives in exactly one place.
func (h *Hub) newLoadModel() *loadmodel.Model {
	return loadmodel.New(h.cfg.Circuits, h.cfg.HomeAssistant.Entities.LoadPower, loadmodel.Params{
		SouthernHemisphere:  h.cfg.Weather.Latitude < 0,
		RecencyHalfLifeDays: h.cfg.LoadModel.RecencyHalfLifeDays,
		BucketHalfLifeDays:  h.cfg.LoadModel.BucketHalfLifeDays,
		ConfidenceThreshold: h.cfg.Optimizer.ConfidenceThreshold,
		ConservativeMargin:  h.cfg.LoadModel.ConservativeMargin,
	})
}

// buildTrainedLoadModel constructs a FRESH load model and trains it from InfluxDB
// history over the configured lookback. It never mutates the live model — the
// caller publishes the result via swapLoadModel only on success, preserving the
// invariant that a published model is immutable. Returns an error so the caller
// keeps the existing model rather than publish a partially-built one. Callable
// off the tick goroutine; the ctx timeout is the caller's responsibility.
func (h *Hub) buildTrainedLoadModel(ctx context.Context) (*loadmodel.Model, error) {
	m := h.newLoadModel()
	lookback := time.Duration(h.cfg.LoadModel.LookbackDays*24) * time.Hour
	if err := m.Train(ctx, &loadDataSource{h.influx}, lookback); err != nil {
		return nil, err
	}
	return m, nil
}

// currentLoadModel returns the currently published load model under the read
// lock. EVERY read of h.loadModel MUST route through here — including the tick's
// own Predict call — because the retrain builds the replacement on a separate
// goroutine and swaps the pointer concurrently (see loadModelMu).
func (h *Hub) currentLoadModel() *loadmodel.Model {
	h.loadModelMu.RLock()
	defer h.loadModelMu.RUnlock()
	return h.loadModel
}

// swapLoadModel atomically publishes m as the live load model under the write
// lock. The previous model is immutable and simply drops out of reference.
func (h *Hub) swapLoadModel(m *loadmodel.Model) {
	h.loadModelMu.Lock()
	h.loadModel = m
	h.loadModelMu.Unlock()
}

// maybeRetrainLoadModel retrains the load model roughly once per day so
// recentLevel / overallMean / confidence track a settling load level instead of
// freezing at the last restart (the "stuck low confidence" failure that kept the
// conservative margin latched and over-forecast the overnight deficit).
//
// The retrain builds a FRESH model on a background goroutine (single-flighted via
// loadTrainRunning) with a bounded-time Influx pull and atomically swaps it in on
// success. This keeps the slow multi-week history query and the model rebuild OFF
// the tick goroutine — so a slow query never stalls the solve — and off any live
// model a dashboard reader is holding, so it can never race Confidence()/Predict.
// lastLoadTrain is recorded only AFTER a successful swap, so a failed or timed-out
// retrain retries on the next tick rather than waiting a full day.
func (h *Hub) maybeRetrainLoadModel(ctx context.Context, now time.Time) {
	if h.influx == nil {
		return
	}
	h.loadTrainMu.Lock()
	due := h.lastLoadTrain.IsZero() || now.Sub(h.lastLoadTrain) >= loadRetrainInterval
	if h.loadTrainRunning || !due {
		h.loadTrainMu.Unlock()
		return
	}
	h.loadTrainRunning = true
	h.loadTrainMu.Unlock()

	go func() {
		defer func() {
			h.loadTrainMu.Lock()
			h.loadTrainRunning = false
			h.loadTrainMu.Unlock()
		}()

		tctx, cancel := context.WithTimeout(ctx, loadRetrainTimeout)
		defer cancel()

		slog.Info("retraining load model")
		m, err := h.buildTrainedLoadModel(tctx)
		if err != nil {
			slog.Warn("load model retrain failed — keeping existing model", "error", err)
			return // lastLoadTrain unchanged → retries next tick
		}
		h.swapLoadModel(m)
		h.loadTrainMu.Lock()
		h.lastLoadTrain = now
		h.loadTrainMu.Unlock()
		slog.Info("load model retrained", "confidence", fmt.Sprintf("%.2f", m.Confidence()))
	}()
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

func (h *Hub) LoadConfidence() float64 { return h.currentLoadModel().Confidence() }

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
