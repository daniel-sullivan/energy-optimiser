package actuator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"energy-optimiser/config"
)

// adjustEpsilon suppresses a mains-charge-current write when the new target is
// within this many amps of the current setpoint (avoids churning the number
// entity for negligible changes).
const adjustEpsilon = 0.5

// minPlausibleVoltageV rejects an implausibly low fresh pack-voltage reading
// (e.g. a glitched sensor) that would otherwise inflate the kW→A conversion
// toward the current ceiling. Below this, kW→A falls back to nominal voltage.
const minPlausibleVoltageV = 40.0

var errClosed = errors.New("actuator: closed")

// haClient is the slice of the Home Assistant client the actuator needs. The
// real *ha.Client satisfies it; tests supply a recording fake.
type haClient interface {
	CallServiceAck(ctx context.Context, domain, service string, data map[string]any) error
	State(entityID string) string
	StateFloat(entityID string) float64
	Attributes(entityID string) map[string]any
	Fresh(entityID string, within time.Duration) bool
}

// ChargePlan is the policy's desired grid-charge state for the current tick. The
// hub computes the GRID SHARE (grid kW after expected PV) and the active
// off-peak window; the actuator owns the kW→A conversion and the blip-safe
// bypass state machine.
type ChargePlan struct {
	Charging bool
	GridKW   float64
	Window   config.OffPeakOccurrence
}

// episode is the persisted per-window bypass state. It is owned exclusively by
// the single actuator goroutine (loop); no other goroutine touches it.
type episode struct {
	windowID string
	inBypass bool
	amps     float64
	entered  bool // an enter-blip has been spent in this window
	exited   bool // an exit-blip has been spent in this window
}

// Actuator is the Phase-1 blip-safe grid-charge actuator. Every inverter write —
// policy and watchdog alike — is funnelled through one goroutine via a command
// channel, so the UTI-then-amps enter sequence is atomic with respect to the
// watchdog and the per-window blip budget is enforced single-threaded.
type Actuator struct {
	cfg     config.ActuatorHW
	battery config.Battery
	rates   *config.Rates
	ha      haClient
	mode    Mode

	cmds   chan command
	closed chan struct{}
	wg     sync.WaitGroup

	startOnce   sync.Once
	closeOnce   sync.Once
	startedFlag atomic.Bool

	// now is the clock, injectable in tests.
	now func() time.Time

	// Goroutine-owned (loop only):
	ep            episode
	boundaryTimer *time.Timer
}

type cmdKind int

const (
	cmdPlan cmdKind = iota
	cmdBoundary
	cmdWatchdog
	cmdSafe
)

type command struct {
	kind     cmdKind
	plan     ChargePlan
	windowID string // cmdBoundary
	reason   string // cmdSafe
	force    bool   // cmdSafe: write even if bypass not currently suspected
	done     chan error
}

// New constructs the actuator. It performs no I/O and starts no goroutines;
// call Start (after HA is connected) to reconcile and begin control.
func New(cfg config.ActuatorHW, battery config.Battery, rates *config.Rates, ha haClient, mode Mode) (*Actuator, error) {
	if rates == nil {
		return nil, errors.New("actuator: nil rates")
	}
	if ha == nil {
		return nil, errors.New("actuator: nil ha client")
	}
	a := &Actuator{
		cfg:     cfg,
		battery: battery,
		rates:   rates,
		ha:      ha,
		mode:    mode,
		cmds:    make(chan command, 8),
		closed:  make(chan struct{}),
		now:     time.Now,
	}
	slog.Info("actuator: initialised",
		"mode", mode,
		"output_priority", cfg.OutputPrioritySelect,
		"mains_current", cfg.MainsChargeCurrentNumber,
		"num_units", cfg.NumUnits)
	return a, nil
}

// Start reconciles against the live inverter state, then launches the single
// write-owning goroutine and the independent watchdog ticker. Idempotent. Must
// be called after the HA client has connected and loaded states.
func (a *Actuator) Start(ctx context.Context) error {
	var startErr error
	a.startOnce.Do(func() {
		a.loadPersist()
		// Reconcile runs inline before the loop goroutine starts, so ep access
		// stays single-threaded. Any boundary timer it arms enqueues onto the
		// buffered command channel and is drained once the loop is up.
		startErr = a.reconcile(ctx)
		a.startedFlag.Store(true)
		a.wg.Add(2)
		go a.loop()
		go a.watchdogTicker(ctx)
	})
	return startErr
}

// SetChargePlan submits the policy's desired grid-charge state and blocks until
// the resulting transition (if any) has been applied. In observe/watchdog mode
// this only logs the intent.
func (a *Actuator) SetChargePlan(ctx context.Context, plan ChargePlan) error {
	return a.submit(ctx, command{kind: cmdPlan, plan: plan})
}

// Close force-safes the inverter (battery-priority + zero grid charge, if bypass
// is suspected) then stops all goroutines. Idempotent.
func (a *Actuator) Close() {
	a.closeOnce.Do(func() {
		if a.startedFlag.Load() {
			ctx, cancel := context.WithTimeout(context.Background(), a.cfg.WriteTimeout.Duration+time.Second)
			if err := a.submit(ctx, command{kind: cmdSafe, reason: "shutdown"}); err != nil {
				slog.Warn("actuator: shutdown safe failed", "error", err)
			}
			cancel()
		}
		close(a.closed)
		a.wg.Wait()
		a.stopBoundaryTimer()
	})
}

// submit enqueues a command and waits for its result.
func (a *Actuator) submit(ctx context.Context, c command) error {
	c.done = make(chan error, 1)
	select {
	case a.cmds <- c:
	case <-ctx.Done():
		return ctx.Err()
	case <-a.closed:
		return errClosed
	}
	select {
	case err := <-c.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-a.closed:
		return errClosed
	}
}

// submitAsync enqueues a command without waiting (used by the timer/watchdog
// goroutines). It never blocks past close.
func (a *Actuator) submitAsync(c command) {
	select {
	case a.cmds <- c:
	case <-a.closed:
	}
}

// loop is the sole owner of episode state and inverter writes.
func (a *Actuator) loop() {
	defer a.wg.Done()
	for {
		select {
		case <-a.closed:
			return
		case c := <-a.cmds:
			err := a.handle(c)
			if c.done != nil {
				c.done <- err
			}
		}
	}
}

func (a *Actuator) handle(c command) (err error) {
	// Defense in depth: a panic in any transition (e.g. a nil deref reaching the HA
	// client) must NEVER strand the inverter in bypass with no actor to safe it, nor
	// kill the write-owning goroutine. Recover, log with the stack, best-effort force
	// SBU, and return an error so the caller (if any) sees the failure; the loop then
	// continues serving subsequent commands.
	defer func() {
		if r := recover(); r != nil {
			stack := string(debug.Stack())
			slog.Error("actuator: PANIC in command handler — attempting fail-safe",
				"kind", c.kind, "panic", r, "stack", stack)
			a.safeAfterPanic()
			err = fmt.Errorf("actuator: recovered panic in command kind %d: %v", c.kind, r)
		}
	}()
	switch c.kind {
	case cmdPlan:
		return a.handlePlan(c.plan)
	case cmdBoundary:
		return a.handleBoundary(c.windowID)
	case cmdWatchdog:
		return a.handleWatchdog()
	case cmdSafe:
		return a.doSafe(c.reason, c.force)
	default:
		return fmt.Errorf("actuator: unknown command kind %d", c.kind)
	}
}

// safeAfterPanic force-safes the inverter after a recovered panic, itself guarded
// against a further panic (the panic source may be the HA client, which doSafe
// also touches) so recovery can never re-crash the goroutine.
func (a *Actuator) safeAfterPanic() {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("actuator: fail-safe after panic also panicked", "panic", r)
		}
	}()
	if err := a.doSafe("panic recovery", true); err != nil {
		slog.Error("actuator: fail-safe after panic failed", "error", err)
	}
}

// --- policy state machine ---

func (a *Actuator) handlePlan(plan ChargePlan) error {
	if !a.mode.actuates() {
		slog.Info("actuator: policy (no-op in current mode)",
			"mode", a.mode, "charging", plan.Charging,
			"grid_kw", round1(plan.GridKW), "window", plan.Window.ID)
		return nil
	}

	// Window rollover resets the per-window blip budget.
	if plan.Window.ID != "" && plan.Window.ID != a.ep.windowID {
		if a.ep.inBypass {
			// Defensive: the boundary timer should have exited the prior window.
			if err := a.doExit("window rollover with open bypass"); err != nil {
				return err
			}
		}
		a.setEpisode(episode{windowID: plan.Window.ID})
	}

	if !plan.Charging {
		if a.ep.inBypass {
			// Stop charging but STAY in bypass (loads keep running on cheap grid);
			// the exit blip is spent only at the window boundary.
			return a.doStop()
		}
		return nil
	}

	if plan.Window.ID == "" {
		slog.Warn("actuator: charge requested with no active off-peak window — ignoring")
		return nil
	}

	amps := a.kwToAmps(plan.GridKW)
	if a.ep.inBypass {
		return a.doAdjust(amps) // blip-free
	}
	if a.ep.entered {
		slog.Warn("actuator: refusing second bypass enter this window (blip budget spent)",
			"window", a.ep.windowID)
		return nil
	}
	return a.doEnter(amps, plan.Window)
}

func (a *Actuator) handleBoundary(windowID string) error {
	// Boundary exit is a safing-class action (prevents staying in bypass past the
	// window), so it is permitted whenever any write is allowed.
	if !a.mode.mayWrite() {
		return nil
	}
	if a.ep.inBypass && a.ep.windowID == windowID && !a.ep.exited {
		return a.doExit("window-end boundary")
	}
	return nil
}

// --- transitions (each writes to the inverter) ---

// doEnter enters utility bypass (one blip) then sets the charge current.
//
// C1 (blip-safety invariant): the enter-blip is spent on the SERVICE ACK, not on
// the state read-back. Home Assistant's `result` frame acks when the select
// service coroutine runs (the MQTT command is published) — the SRNE select
// entity's state_changed echo can lag seconds behind. If we waited for the echo
// and it timed out, a write that PHYSICALLY entered UTI would look failed, the
// budget would not be consumed, and the next tick would re-enter → unbounded
// blips. So once CallServiceAck reports acceptance we treat the change as having
// happened; the read-back is advisory only.
func (a *Actuator) doEnter(amps float64, win config.OffPeakOccurrence) error {
	// L7: never spend an enter-blip to charge nothing. If the grid share rounds to
	// ~0 A (instantaneous PV surplus covers the plan), stay out of bypass.
	if round1(amps) <= 0 {
		slog.Info("actuator: skip ENTER — commanded grid charge rounds to zero (no blip)",
			"window", win.ID)
		return nil
	}
	// Per-window enter hysteresis: once an enter has been ack-accepted for this
	// window the budget is spent; never issue a second output_priority enter, even
	// if episode state looks inconsistent. (handlePlan also guards this; keep both.)
	if a.ep.entered {
		slog.Warn("actuator: refusing bypass enter — enter budget already spent this window",
			"window", win.ID)
		return nil
	}
	slog.Info("actuator: ENTER bypass (blip)", "window", win.ID, "amps", round1(amps))
	if err := a.ackPriority(a.cfg.UtilityOption); err != nil {
		// The service was NOT accepted → no blip occurred, budget NOT spent. Do not
		// mark entered and do NOT fail-safe (that would blip on an assumption); the
		// next tick simply retries the enter.
		slog.Warn("actuator: ENTER priority write not accepted — will retry next tick", "error", err)
		return err
	}
	// Ack accepted: the enter-blip is spent NOW, before (and independent of) any
	// read-back. Persist immediately so a crash between here and the amps write
	// cannot lose the record and re-enter on restart.
	a.ep.inBypass = true
	a.ep.entered = true
	a.armBoundary(win)
	a.persist()

	// Advisory read-back: a mismatch/timeout is logged but must NOT reverse state
	// or trigger a safe (the blip already happened; reversing causes a second one).
	if err := a.verifyPriority(a.cfg.UtilityOption); err != nil {
		slog.Warn("actuator: ENTER read-back did not confirm within read_back_timeout "+
			"(advisory only — blip already spent, staying entered)", "error", err)
	}

	if err := a.writeAmps(amps); err != nil {
		// In bypass but amps unconfirmed — the next tick's adjust retries. Do NOT
		// exit (would waste the exit blip).
		a.ep.amps = 0
		a.persist()
		return err
	}
	a.ep.amps = amps
	a.persist()
	return nil
}

// doAdjust changes the charge current only (no blip).
func (a *Actuator) doAdjust(amps float64) error {
	if math.Abs(amps-a.ep.amps) < adjustEpsilon {
		return nil
	}
	slog.Info("actuator: adjust charge current (no blip)", "amps", round1(amps))
	if err := a.writeAmps(amps); err != nil {
		return err
	}
	a.ep.amps = amps
	a.persist()
	return nil
}

// doStop sets the charge current to zero but stays in bypass (no blip).
func (a *Actuator) doStop() error {
	if a.ep.amps == 0 {
		return nil
	}
	slog.Info("actuator: stop charging (amps=0, staying in bypass)")
	if err := a.writeAmps(0); err != nil {
		return err
	}
	a.ep.amps = 0
	a.persist()
	return nil
}

// doExit sets amps to zero then exits bypass to battery-priority (one blip).
func (a *Actuator) doExit(reason string) error {
	slog.Info("actuator: EXIT bypass (blip)", "reason", reason, "window", a.ep.windowID)
	var errs []error
	if err := a.writeAmps(0); err != nil {
		errs = append(errs, err)
	}
	if err := a.writePriority(a.cfg.BatteryOption); err != nil {
		errs = append(errs, err)
	}
	a.ep.inBypass = false
	a.ep.amps = 0
	a.ep.exited = true
	a.stopBoundaryTimer()
	a.persist()
	return errors.Join(errs...)
}

// doSafe forces battery-priority + zero charge. force writes unconditionally;
// otherwise it writes only when bypass is suspected (last-known UTI, our episode
// says in-bypass, or the state is unknown/stale). No-op (log only) in observe.
func (a *Actuator) doSafe(reason string, force bool) error {
	if !a.mode.mayWrite() {
		slog.Info("actuator: would fail-safe (no-op in current mode)", "reason", reason, "mode", a.mode)
		return nil
	}
	priority := a.ha.State(a.cfg.OutputPrioritySelect)
	suspect := force || a.ep.inBypass ||
		priority == a.cfg.UtilityOption ||
		priority == "" || priority == "unknown" || priority == "unavailable"
	if !suspect {
		a.ep.inBypass = false
		return nil
	}
	slog.Warn("actuator: FAIL-SAFE", "reason", reason, "priority", priority, "mode", a.mode)
	var errs []error
	if err := a.writeAmps(0); err != nil {
		errs = append(errs, err)
	}
	if err := a.writePriority(a.cfg.BatteryOption); err != nil {
		errs = append(errs, err)
	}
	a.ep.inBypass = false
	a.ep.amps = 0
	a.ep.exited = true
	a.stopBoundaryTimer()
	a.persist()
	return errors.Join(errs...)
}

// --- watchdog ---

func (a *Actuator) watchdogTicker(ctx context.Context) {
	defer a.wg.Done()
	t := time.NewTicker(a.cfg.WatchdogInterval.Duration)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-a.closed:
			return
		case <-t.C:
			a.submitAsync(command{kind: cmdWatchdog})
		}
	}
}

// handleWatchdog is the never-stuck-in-bypass safety net. It forces safe when the
// inverter is in bypass outside every off-peak window, or when the state is
// stale/unknown outside a window and bypass may be active (fail-safe).
func (a *Actuator) handleWatchdog() error {
	now := a.now()
	_, inWindow := a.rates.ActiveWindow(now)
	priority := a.ha.State(a.cfg.OutputPrioritySelect)
	stale := !a.ha.Fresh(a.cfg.OutputPrioritySelect, a.cfg.StateStale.Duration) ||
		priority == "" || priority == "unknown" || priority == "unavailable"

	stuckBypass := !inWindow && priority == a.cfg.UtilityOption
	// H2: fail-safe on ANY stale feed outside a window — NOT gated on ep.inBypass.
	// ep.inBypass can be false while the hardware is physically UTI (a failed or
	// ambiguous enter), so a stale feed must never leave the inverter stranded. The
	// doSafe(force=false) below still only WRITES when bypass is actually suspected
	// (priority reads UTI/unknown/stale), so a genuinely-SBU inverter is not blipped.
	failSafe := !inWindow && stale

	if !stuckBypass && !failSafe {
		return nil
	}
	if !a.mode.mayWrite() {
		slog.Warn("actuator: watchdog would fail-safe (no-op in current mode)",
			"priority", priority, "in_window", inWindow, "stale", stale, "mode", a.mode)
		return nil
	}
	reason := "watchdog: inverter in bypass outside off-peak window"
	if failSafe {
		reason = "watchdog: state stale/unknown outside window with bypass suspected"
	}
	return a.doSafe(reason, false)
}

// --- startup reconciliation ---

func (a *Actuator) reconcile(ctx context.Context) error {
	a.verifyOptions()

	now := a.now()
	priority := a.ha.State(a.cfg.OutputPrioritySelect)
	win, inWindow := a.rates.ActiveWindow(now)

	if priority != a.cfg.UtilityOption {
		// Not in bypass. Reflect reality and keep the per-window budget only if the
		// persisted state belongs to the current window (prevents a mid-window
		// restart from re-spending an enter-blip); otherwise clear it.
		a.ep.inBypass = false
		if !inWindow || a.ep.windowID != win.ID {
			a.ep = episode{}
		}
		slog.Info("actuator: startup reconcile — not in bypass",
			"priority", priority, "in_window", inWindow, "mode", a.mode)
		a.persist()
		return nil
	}

	// priority == UTI (bypass).
	if inWindow {
		// Adopt an open episode: the enter-blip is already spent, so we must NOT
		// re-enter. Preserve persisted counters if they match this window.
		if a.ep.windowID != win.ID {
			a.ep = episode{windowID: win.ID}
		}
		a.ep.inBypass = true
		a.ep.entered = true
		a.ep.amps = a.ha.StateFloat(a.cfg.MainsChargeCurrentNumber)
		slog.Warn("actuator: startup reconcile — adopting open bypass episode",
			"window", win.ID, "amps", a.ep.amps, "mode", a.mode)
		if a.mode.mayWrite() {
			a.armBoundary(win)
		}
		a.persist()
		return nil
	}

	// UTI outside every window — unsafe; fail safe immediately.
	slog.Warn("actuator: startup reconcile — UTI outside off-peak window; failing safe", "mode", a.mode)
	_ = ctx
	return a.doSafe("startup: UTI outside off-peak window", true)
}

// verifyOptions logs a warning if the configured UTI/SBU option strings are not
// among the select entity's advertised options (best-effort).
func (a *Actuator) verifyOptions() {
	attrs := a.ha.Attributes(a.cfg.OutputPrioritySelect)
	raw, ok := attrs["options"]
	if !ok {
		return
	}
	list, ok := raw.([]any)
	if !ok {
		return
	}
	have := make([]string, 0, len(list))
	for _, o := range list {
		if s, ok := o.(string); ok {
			have = append(have, s)
		}
	}
	var hasU, hasB bool
	for _, s := range have {
		if s == a.cfg.UtilityOption {
			hasU = true
		}
		if s == a.cfg.BatteryOption {
			hasB = true
		}
	}
	if !hasU || !hasB {
		slog.Warn("actuator: configured output_priority options not found on entity",
			"utility", a.cfg.UtilityOption, "battery", a.cfg.BatteryOption, "available", have)
	}
}

// --- boundary timer ---

// armBoundary schedules the window-end exit via an explicit timer (independent of
// the 5-minute tick, which could otherwise miss the boundary).
func (a *Actuator) armBoundary(win config.OffPeakOccurrence) {
	a.stopBoundaryTimer()
	d := win.End.Sub(a.now())
	if d < 0 {
		d = 0
	}
	id := win.ID
	a.boundaryTimer = time.AfterFunc(d, func() {
		a.submitAsync(command{kind: cmdBoundary, windowID: id})
	})
}

func (a *Actuator) stopBoundaryTimer() {
	if a.boundaryTimer != nil {
		a.boundaryTimer.Stop()
		a.boundaryTimer = nil
	}
}

// --- inverter writes (ack + read-back) ---

// ackPriority issues the output_priority write and waits ONLY for Home
// Assistant's correlated service ack (the `result` frame). The ack is the
// authoritative "command accepted" signal; the echoed state read-back is a
// separate, advisory step (see verifyPriority / doEnter C1).
func (a *Actuator) ackPriority(option string) error {
	ctx, cancel := context.WithTimeout(context.Background(), a.cfg.WriteTimeout.Duration)
	defer cancel()
	if err := a.ha.CallServiceAck(ctx, "select", "select_option", map[string]any{
		"entity_id": a.cfg.OutputPrioritySelect,
		"option":    option,
	}); err != nil {
		return fmt.Errorf("set output_priority=%s: %w", option, err)
	}
	return nil
}

// verifyPriority polls the state cache until the priority entity echoes option or
// the configured read-back timeout elapses. ADVISORY: a non-nil result means the
// echo lagged, not that the write failed — callers log it but must not reverse
// state (the blip already happened).
func (a *Actuator) verifyPriority(option string) error {
	return a.verifyState(a.cfg.OutputPrioritySelect, option)
}

// writePriority sets output_priority (ack-confirmed) then does an advisory
// read-back. Returns an error only when the service ack itself fails; a lagging
// or missing state echo is logged, never propagated (used by exit/safe, where the
// SBU direction is the safe one and a slow echo must not be reported as failure).
func (a *Actuator) writePriority(option string) error {
	if err := a.ackPriority(option); err != nil {
		return err
	}
	if err := a.verifyPriority(option); err != nil {
		slog.Warn("actuator: output_priority read-back did not confirm (advisory)",
			"option", option, "error", err)
	}
	return nil
}

// writeAmps sets the per-unit mains charge current, confirmed via the ack frame.
func (a *Actuator) writeAmps(amps float64) error {
	ctx, cancel := context.WithTimeout(context.Background(), a.cfg.WriteTimeout.Duration)
	defer cancel()
	val := round1(amps)
	if err := a.ha.CallServiceAck(ctx, "number", "set_value", map[string]any{
		"entity_id": a.cfg.MainsChargeCurrentNumber,
		"value":     val,
	}); err != nil {
		return fmt.Errorf("set mains_charge_current=%.1f: %w", val, err)
	}
	return nil
}

// verifyState polls the state cache until the entity reports want, or the
// configured (advisory) read-back timeout elapses. It bounds on REAL elapsed
// time (not the injectable clock) so it terminates deterministically even under a
// frozen test clock — the poll tracks real device state-propagation latency.
func (a *Actuator) verifyState(entityID, want string) error {
	deadline := time.Now().Add(a.cfg.ReadBackTimeout.Duration)
	for {
		if got := a.ha.State(entityID); got == want {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("read-back %s: want %q, have %q", entityID, want, a.ha.State(entityID))
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// --- kW → per-unit amps ---

// kwToAmps converts a grid-share kW target to a per-unit charge current. The
// commanded gridKW is first clamped to the AGGREGATE pack ceiling (MaxChargeKW)
// so a high per-unit amp headroom can never exceed pack acceptance; the per-unit
// MaxChargeCurrentA then bounds the result a second time. Uses the live pack
// voltage when fresh AND plausible, nominal otherwise.
func (a *Actuator) kwToAmps(gridKW float64) float64 {
	if gridKW <= 0 {
		return 0
	}
	// M6: aggregate-kW ceiling BEFORE the per-unit conversion.
	if a.cfg.MaxChargeKW > 0 && gridKW > a.cfg.MaxChargeKW {
		gridKW = a.cfg.MaxChargeKW
	}
	v := a.battery.NominalVoltageV
	if a.ha.Fresh(a.cfg.BatteryVoltageSensor, a.cfg.VoltageStale.Duration) {
		// M6: a fresh but implausibly-low reading (glitched sensor) would inflate
		// amps toward the ceiling — treat anything below minPlausibleVoltageV as
		// use-nominal.
		if live := a.ha.StateFloat(a.cfg.BatteryVoltageSensor); live >= minPlausibleVoltageV {
			v = live
		}
	}
	if v <= 0 {
		v = 52.8 // ultimate fallback (16S LiFePO4 operating median)
	}
	n := a.cfg.NumUnits
	if n <= 0 {
		n = 1
	}
	amps := gridKW * 1000.0 / (v * float64(n))
	if amps < 0 {
		amps = 0
	}
	if amps > a.cfg.MaxChargeCurrentA {
		amps = a.cfg.MaxChargeCurrentA
	}
	return amps
}

// --- persistence ---

type persistState struct {
	WindowID string  `json:"window_id"`
	Entered  bool    `json:"entered"`
	Exited   bool    `json:"exited"`
	InBypass bool    `json:"in_bypass"`
	Amps     float64 `json:"amps"`
}

func (a *Actuator) persistPath() string {
	return filepath.Join(a.cfg.StateDir, "actuator_episode.json")
}

// setEpisode replaces the episode and persists.
func (a *Actuator) setEpisode(ep episode) {
	a.ep = ep
	a.persist()
}

func (a *Actuator) persist() {
	if a.cfg.StateDir == "" {
		return
	}
	data, err := json.Marshal(persistState{
		WindowID: a.ep.windowID,
		Entered:  a.ep.entered,
		Exited:   a.ep.exited,
		InBypass: a.ep.inBypass,
		Amps:     a.ep.amps,
	})
	if err != nil {
		return
	}
	if err := atomicWriteFile(a.persistPath(), data, 0o644); err != nil {
		slog.Warn("actuator: persist episode failed", "error", err)
	}
}

// atomicWriteFile writes data to a temp file in the SAME directory, fsyncs it,
// then renames it over path. A crash mid-write therefore never truncates or tears
// the blip-budget record — loadPersist either sees the prior valid file or the
// fully-written new one, so "entered+exited" can never be mistaken for "never
// entered" (which would re-spend an enter-blip in the same window on restart).
func atomicWriteFile(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".actuator-episode-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpName)
		}
	}()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Chmod(tmpName, perm); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	cleanup = false
	return nil
}

func (a *Actuator) loadPersist() {
	if a.cfg.StateDir == "" {
		return
	}
	data, err := os.ReadFile(a.persistPath())
	if err != nil {
		return
	}
	var ps persistState
	if err := json.Unmarshal(data, &ps); err != nil {
		return
	}
	a.ep = episode{
		windowID: ps.WindowID,
		inBypass: ps.InBypass,
		amps:     ps.Amps,
		entered:  ps.Entered,
		exited:   ps.Exited,
	}
}

func round1(v float64) float64 { return math.Round(v*10) / 10 }
