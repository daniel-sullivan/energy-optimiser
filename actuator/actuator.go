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

// HA service plumbing. The actuator commands the SRNE inverter through three
// standard HA entity types: a switch (timed-charge enable), a number (per-unit
// charge current), and text entities (the "HH:MM" charge-window bounds).
const (
	switchDomain = "switch"
	numberDomain = "number"
	textDomain   = "text"

	switchOn  = "on"
	switchOff = "off"
)

// Write-spacing / confirmation constants. The SRNE controller DROPS rapid-fire
// consecutive writes to these registers (proven live): a second write issued
// while it is still processing the previous one is silently ignored. Every
// inverter write is therefore (a) spaced from the previous one and (b) confirmed
// by a state-cache read-back, retried until it lands. Unlike the retired bypass
// model, re-issuing is HARMLESS here — enabling an already-enabled switch or
// re-setting a value is idempotent and is NOT a power blip — so we can genuinely
// retry-until-confirmed with no double-spend hazard.
const (
	// writeSpacing separates any two consecutive inverter writes.
	writeSpacing = 1500 * time.Millisecond
	// writeConfirmPollInterval is the read-back poll cadence while awaiting a
	// write to be echoed by the (slow) SRNE state feed.
	writeConfirmPollInterval = 1 * time.Second
	// writeConfirmTimeout bounds the read-back poll when ActuatorHW.ReadBackTimeout
	// is unset. Generous: the SRNE echo/apply lag for these entities is ~20-40s, so
	// this must comfortably exceed it or every legitimate write churns retries.
	writeConfirmTimeout = 45 * time.Second
	// maxWriteRetries is the number of spaced re-issues (after the first attempt)
	// before a write is reported unconfirmed.
	maxWriteRetries = 2
)

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
// hub decides WHETHER to charge (SOC known, optimiser permits, and now is inside
// an off-peak window) and the GRID SHARE in kW; the actuator owns the kW→A
// conversion, the timed-charge window/current/enable sequencing, and the
// off-peak hardware rail. The actuator re-derives the active off-peak window from
// its own tariff config, so a window is never taken on trust from the caller.
type ChargePlan struct {
	Charging bool
	GridKW   float64
}

// chargeState is the actuator's commanded intent, owned exclusively by the single
// loop goroutine. charging reflects our last CONFIRMED timed-charge-enable; amps
// is the last confirmed per-unit current setpoint. The charge windows are a
// STATIC rail (mirrored from the off-peak config), not per-charge state, so they
// are not tracked here.
type chargeState struct {
	charging bool
	amps     float64
}

// Actuator drives grid charging via the SRNE TIMED-CHARGE mechanism. Every
// inverter write — policy, watchdog, shutdown — is funnelled through one
// goroutine via a command channel, so the window/current/enable sequence is
// atomic with respect to the watchdog's out-of-window backstop.
//
// Safety model (inverted from the retired bypass actuator): the hazard is timed
// charge left ENABLED when it shouldn't be (unwanted/expensive grid charge, e.g.
// a crashed daemon leaving it on). DISABLING is always the safe direction, so it
// is issued freely and retried. The invariant is: timed_charge is OFF whenever
// the actuator is not actively commanding a charge. Two backstops reinforce it —
// the inverter's charge windows (a hardware rail: charging can only occur inside
// a programmed off-peak window) and an out-of-band HA dead-man automation that
// disables timed charge if the daemon's MQTT availability goes offline.
type Actuator struct {
	cfg     config.ActuatorHW
	battery config.Battery
	rates   *config.Rates
	ha      haClient
	mode    Mode

	cmds   chan command
	closed chan struct{}
	wg     sync.WaitGroup

	startOnce sync.Once
	closeOnce sync.Once

	// now is the policy clock, injectable in tests. Write spacing/confirmation
	// uses real wall-clock (device latency is physical), never this clock.
	now func() time.Time

	// Write-spacing/confirm knobs. Default to the package consts / cfg; tests
	// override them to keep the suite fast (real device latency is seconds).
	confirmTimeout time.Duration
	spacing        time.Duration
	confirmPoll    time.Duration

	// Loop-owned (single goroutine):
	st chargeState
	// duringShutdown makes pause/awaitConfirmed use non-abortable waits for the
	// final fail-safe, so the shutdown disable write is never dropped.
	duringShutdown bool
}

type cmdKind int

const (
	cmdPlan cmdKind = iota
	cmdWatchdog
	cmdSafe
)

type command struct {
	kind   cmdKind
	plan   ChargePlan
	reason string // cmdSafe
	done   chan error
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

		confirmTimeout: cfg.ReadBackTimeout.Duration,
		spacing:        writeSpacing,
		confirmPoll:    writeConfirmPollInterval,
	}
	if a.confirmTimeout <= 0 {
		a.confirmTimeout = writeConfirmTimeout
	}
	slog.Info("actuator: initialised",
		"mode", mode,
		"timed_charge_switch", cfg.TimedChargeSwitch,
		"mains_current", cfg.MainsChargeCurrentNumber,
		"num_units", cfg.NumUnits,
		"charge_window_slots", len(cfg.ChargeWindows))
	return a, nil
}

// Start reconciles against the live inverter state, then launches the single
// write-owning goroutine and the independent watchdog ticker. Idempotent. Must
// be called after the HA client has connected and loaded states.
func (a *Actuator) Start(ctx context.Context) error {
	var err error
	a.startOnce.Do(func() {
		a.loadPersist()
		// Reconcile runs inline before the loop goroutine starts, so state access
		// stays single-threaded.
		err = a.reconcile(ctx)
		a.wg.Add(2)
		go a.loop()
		go a.watchdogTicker(ctx)
	})
	return err
}

// SetChargePlan submits the policy's desired grid-charge state and blocks until
// the resulting writes have been applied. In observe/watchdog mode this only
// logs the intent.
func (a *Actuator) SetChargePlan(ctx context.Context, plan ChargePlan) error {
	return a.submit(ctx, command{kind: cmdPlan, plan: plan})
}

// Close disables timed charge (the guaranteed-safe state) then stops all
// goroutines. Idempotent. The fail-safe is run DETERMINISTICALLY by the loop
// itself (shutdownSafe), not submitted as a racing cmdSafe: closing a.closed makes
// the loop drive the disable then exit. If Start was never called the loop does
// not exist and there is nothing to safe.
func (a *Actuator) Close() {
	a.closeOnce.Do(func() {
		close(a.closed)
		a.wg.Wait()
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

// submitAsync enqueues a command without waiting (used by the watchdog
// goroutine). It never blocks past close.
func (a *Actuator) submitAsync(c command) {
	select {
	case a.cmds <- c:
	case <-a.closed:
	}
}

// loop is the sole owner of chargeState and inverter writes. Commands (a policy
// plan, a watchdog check, a safe) are handled one at a time; each is bounded by a
// few spaced, confirmed writes. Because timed charge is enabled LAST in a start
// sequence, the dangerous state (switch on) exists only after a command completes,
// so a following safing command always runs promptly between commands — no
// mid-write preemption machinery is required.
func (a *Actuator) loop() {
	defer a.wg.Done()
	for {
		// Give shutdown priority: if closed is already signalled, safe and exit
		// deterministically rather than risk the select picking another ready case.
		select {
		case <-a.closed:
			a.shutdownSafe()
			return
		default:
		}
		select {
		case <-a.closed:
			a.shutdownSafe()
			return
		case c := <-a.cmds:
			err := a.handle(c)
			if c.done != nil {
				c.done <- err
			}
		}
	}
}

// shutdownSafe is the deterministic fail-safe run by the loop before it exits on
// close: it disables timed charge and zeroes the current with non-abortable
// write-spacing (duringShutdown), so the disable is never dropped. Idempotent —
// a no-op when already stopped, and a no-op entirely in observe mode.
func (a *Actuator) shutdownSafe() {
	a.duringShutdown = true
	if err := a.stopCharge("shutdown"); err != nil {
		slog.Warn("actuator: shutdown fail-safe (disable timed charge) failed", "error", err)
	}
}

func (a *Actuator) handle(c command) (err error) {
	// Defense in depth: a panic in any transition (e.g. a nil deref reaching the HA
	// client) must NEVER leave grid charging running with no actor to stop it, nor
	// kill the write-owning goroutine. Recover, log with the stack, best-effort
	// disable, and return an error so the caller (if any) sees the failure; the loop
	// then continues serving subsequent commands.
	defer func() {
		if r := recover(); r != nil {
			stack := string(debug.Stack())
			slog.Error("actuator: PANIC in command handler — attempting fail-safe (disable timed charge)",
				"kind", c.kind, "panic", r, "stack", stack)
			a.safeAfterPanic()
			err = fmt.Errorf("actuator: recovered panic in command kind %d: %v", c.kind, r)
		}
	}()
	switch c.kind {
	case cmdPlan:
		return a.handlePlan(c.plan)
	case cmdWatchdog:
		return a.handleWatchdog()
	case cmdSafe:
		return a.stopCharge(c.reason)
	default:
		return fmt.Errorf("actuator: unknown command kind %d", c.kind)
	}
}

// safeAfterPanic disables timed charge after a recovered panic, itself guarded
// against a further panic (the panic source may be the HA client, which stopCharge
// also touches) so recovery can never re-crash the goroutine.
func (a *Actuator) safeAfterPanic() {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("actuator: fail-safe after panic also panicked", "panic", r)
		}
	}()
	if err := a.stopCharge("panic recovery"); err != nil {
		slog.Error("actuator: fail-safe after panic failed", "error", err)
	}
}

// --- policy ---

func (a *Actuator) handlePlan(plan ChargePlan) error {
	if !a.mode.actuates() {
		slog.Info("actuator: policy (no-op in current mode)",
			"mode", a.mode, "charging", plan.Charging, "grid_kw", round1(plan.GridKW))
		return nil
	}

	// Deriving `off` from our own tariff config (not the caller) is a layer of the
	// off-peak rail — a charge is never commanded outside off-peak hours. The static
	// window rail (see ensureWindowsMirrorOffPeak) and the enable switch, off
	// outside off-peak, are the other two layers.
	_, off := a.rates.ActiveWindow(a.now())
	if !plan.Charging || !off {
		return a.stopCharge("policy: no grid charge scheduled")
	}

	amps := a.kwToAmps(plan.GridKW)
	if round1(amps) <= 0 {
		// Instantaneous PV surplus covers the plan — nothing to draw from grid.
		slog.Info("actuator: grid charge rounds to zero — ensuring timed charge off")
		return a.stopCharge("policy: grid charge rounds to zero")
	}

	return a.startCharge(amps)
}

// startCharge establishes (or continues) grid charging. The charge windows are a
// STATIC rail mirrored from the off-peak config, so a start never rewrites windows
// per-charge — it only ensures the rail is in place (idempotent), sets the current,
// and enables the global timed-charge switch LAST, so charging only ever begins
// once correctly configured. Continuing while already charging is just a cheap
// live current adjust (the switch is already on).
func (a *Actuator) startCharge(amps float64) error {
	if a.st.charging {
		return a.adjustCurrent(amps)
	}

	slog.Info("actuator: START grid charge", "amps", round1(amps))

	// Ensure the static off-peak window rail (idempotent — usually a no-op after
	// reconcile). If it does not confirm, do NOT enable: the switch stays off
	// (nothing charges) and the next tick retries.
	wrote, err := a.ensureWindowsMirrorOffPeak()
	if err != nil {
		return fmt.Errorf("ensure charge windows (timed charge NOT enabled): %w", err)
	}
	if wrote {
		a.pause()
	}
	if err := a.setCurrent(amps); err != nil {
		return fmt.Errorf("set charge current (timed charge NOT enabled): %w", err)
	}
	a.pause()
	// Enable LAST.
	if err := a.setTimedCharge(true); err != nil {
		// Enable unconfirmed — the switch may be off, so record NOT charging and let
		// the next tick retry the (idempotent) start.
		a.st = chargeState{amps: round1(amps)}
		a.persist()
		return fmt.Errorf("enable timed charge: %w", err)
	}
	a.st = chargeState{charging: true, amps: round1(amps)}
	a.persist()
	return nil
}

// adjustCurrent changes the live per-unit charge rate while timed charge is
// already enabled in the current window. No switch/window writes; epsilon-
// suppressed for negligible changes. On a failed write it leaves st.amps at the
// last confirmed value so the next tick re-sends rather than epsilon-suppressing
// a rate that never landed.
func (a *Actuator) adjustCurrent(amps float64) error {
	target := round1(amps)
	if math.Abs(target-a.st.amps) < adjustEpsilon {
		return nil
	}
	slog.Info("actuator: adjust charge current", "amps", target)
	if err := a.setCurrent(target); err != nil {
		return err
	}
	a.st.amps = target
	a.persist()
	return nil
}

// stopCharge drives the guaranteed-safe state: timed charge OFF, then current 0.
// Disable FIRST (this is what actually halts grid charging), THEN zero the
// current. Gated on mayWrite so the watchdog/reconcile/shutdown fail-safe may
// call it in watchdog mode too. Idempotent: no writes when already confirmed off.
//
// st is cleared to not-charging after the writes are issued, even if the disable
// read-back did not confirm. This is safe by eventual consistency: setTimedCharge
// already retries-until-confirmed, and if it still failed, the idempotent guard
// here (switch not off ⇒ re-issue) plus the watchdog's out-of-window disable
// re-drive it on the next tick — while a stale st=charging would wrongly suppress
// exactly that retry.
func (a *Actuator) stopCharge(reason string) error {
	if !a.mode.mayWrite() {
		slog.Info("actuator: would stop grid charge (no-op in current mode)", "reason", reason, "mode", a.mode)
		return nil
	}
	if !a.st.charging && a.ha.State(a.cfg.TimedChargeSwitch) == switchOff {
		return nil // already stopped and confirmed off — nothing to write
	}
	slog.Info("actuator: STOP grid charge (timed charge off, current 0)", "reason", reason)
	var errs []error
	// Disable FIRST.
	if err := a.setTimedCharge(false); err != nil {
		errs = append(errs, err)
	}
	a.pause()
	if err := a.setCurrent(0); err != nil {
		errs = append(errs, err)
	}
	a.st = chargeState{}
	a.persist()
	return errors.Join(errs...)
}

// windowWithinOffPeak is the hardware-rail guard on what is written to a
// charge-window slot: it confirms the interval lies wholly within a single
// configured off-peak window, so a peak interval can never be programmed as a
// charge window. An inset off-peak window satisfies this by construction; the
// check defends against a self-inconsistent config or an over-large inset.
func (a *Actuator) windowWithinOffPeak(w config.TimeWindow) bool {
	endMinus := todAddMinutes(w.End, -1)
	for _, off := range a.rates.OffPeakWindows {
		if off.Contains(w.Start) && off.Contains(endMinus) {
			return true
		}
	}
	return false
}

// midnightHHMM is the "HH:MM" the inverter may misread as a wrap / zero-length
// window; a mirrored bound landing on it is skipped.
const midnightHHMM = "00:00"

// insetWindow shrinks w by inset at both ends: [start+inset, end−inset]. ok is
// false when the window is shorter than 2×inset (the inset would collapse or
// invert it). Handles a window that wraps past midnight.
func insetWindow(w config.TimeWindow, inset time.Duration) (config.TimeWindow, bool) {
	insetMin := int(inset / time.Minute)
	if insetMin <= 0 {
		return w, true
	}
	if windowMinutes(w) <= 2*insetMin {
		return config.TimeWindow{}, false
	}
	return config.TimeWindow{
		Start: todAddMinutes(w.Start, insetMin),
		End:   todAddMinutes(w.End, -insetMin),
		Rate:  w.Rate,
	}, true
}

// windowMinutes is the length of w in minutes, treating end ≤ start as wrapping
// past midnight.
func windowMinutes(w config.TimeWindow) int {
	d := (w.End.Hour*60 + w.End.Minute) - (w.Start.Hour*60 + w.Start.Minute)
	if d <= 0 {
		d += 24 * 60
	}
	return d
}

// todAddMinutes returns the time-of-day mins after t (mins may be negative),
// wrapping within a 24h day.
func todAddMinutes(t config.TimeOfDay, mins int) config.TimeOfDay {
	m := ((t.Hour*60+t.Minute+mins)%(24*60) + 24*60) % (24 * 60)
	return config.TimeOfDay{Hour: m / 60, Minute: m % 60}
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

// handleWatchdog re-asserts the static window rail (self-healing a mid-life
// scramble) and is the OUT-OF-WINDOW backstop for the new hazard: timed charge
// left enabled outside every off-peak window (which would recur into future
// windows, or persist a crashed daemon's charge). Inside a window the per-tick
// policy path governs enable/disable, so the watchdog only re-asserts windows
// there. Outside every window, timed charge must be OFF; if it reads on — or the
// feed is stale/unknown so we cannot confirm it is off — disable it. Disabling is
// always harmless, so there is no risk of an unwanted write.
func (a *Actuator) handleWatchdog() error {
	var errs []error
	// Re-assert the static off-peak window rail idempotently (self-heals a mid-life
	// window scramble). Cheap: it reads each slot and writes only on a mismatch.
	if a.mode.mayWrite() {
		if _, err := a.ensureWindowsMirrorOffPeak(); err != nil {
			errs = append(errs, err)
		}
	}

	if _, inWindow := a.rates.ActiveWindow(a.now()); inWindow {
		return errors.Join(errs...)
	}
	state := a.ha.State(a.cfg.TimedChargeSwitch)
	stale := !a.ha.Fresh(a.cfg.TimedChargeSwitch, a.cfg.StateStale.Duration) ||
		state == "" || state == "unknown" || state == "unavailable"
	if state != switchOn && !stale {
		return errors.Join(errs...) // confirmed off outside every window — nothing to do
	}
	if !a.mode.mayWrite() {
		slog.Warn("actuator: watchdog would disable timed charge (no-op in current mode)",
			"switch", state, "stale", stale, "mode", a.mode)
		return errors.Join(errs...)
	}
	reason := "watchdog: timed charge enabled outside off-peak window"
	if stale {
		reason = "watchdog: timed-charge feed stale/unknown outside off-peak window"
	}
	if err := a.stopCharge(reason); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// --- startup reconciliation ---

// reconcile brings the inverter to the intended startup state. It (a) establishes
// the static off-peak window rail (idempotent) and (b) ensures timed charge is
// OFF. Per the safety model, at startup the actuator is NOT yet commanding a
// charge (no fresh optimiser decision exists), so the intended switch state is
// off; the first tick re-enables within one poll interval if the fresh solve wants
// grid charge. Toggling timed charge is not a power blip, so this costs at most a
// few minutes of deferred charging on restart, in exchange for never charging on
// stale intent.
func (a *Actuator) reconcile(ctx context.Context) error {
	_ = ctx
	a.verifyEntities()
	a.st = chargeState{}
	if !a.mode.mayWrite() {
		slog.Info("actuator: startup reconcile (no-op in current mode)",
			"mode", a.mode, "timed_charge", a.ha.State(a.cfg.TimedChargeSwitch))
		return nil
	}
	slog.Info("actuator: startup reconcile — mirroring off-peak window rail, ensuring timed charge OFF",
		"timed_charge", a.ha.State(a.cfg.TimedChargeSwitch), "mode", a.mode)
	var errs []error
	// Establish the static off-peak window rail first, then ensure the switch is off.
	wrote, err := a.ensureWindowsMirrorOffPeak()
	if err != nil {
		errs = append(errs, err)
	}
	if wrote {
		a.pause()
	}
	if err := a.stopCharge("startup reconcile"); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// verifyEntities logs a best-effort warning for any configured actuation entity
// that has no state yet (typo or a not-yet-available entity).
func (a *Actuator) verifyEntities() {
	ids := append([]string{a.cfg.TimedChargeSwitch, a.cfg.MainsChargeCurrentNumber}, a.windowEntityIDs()...)
	for _, id := range ids {
		if id == "" {
			slog.Warn("actuator: a configured actuation entity ID is empty")
			continue
		}
		if a.ha.State(id) == "" {
			slog.Warn("actuator: configured entity has no state yet (may be unavailable)", "entity", id)
		}
	}
}

// --- inverter writes (spaced, confirmed, retried) ---

// callAck issues one HA service call bounded by WriteTimeout.
func (a *Actuator) callAck(domain, service string, data map[string]any) error {
	ctx, cancel := context.WithTimeout(context.Background(), a.cfg.WriteTimeout.Duration)
	defer cancel()
	return a.ha.CallServiceAck(ctx, domain, service, data)
}

// writeConfirmed issues a write then polls the read-back until confirmed, retrying
// (spaced) up to maxWriteRetries times to defeat the SRNE's dropped-write
// behaviour. Re-issuing is safe here: these writes are idempotent and NOT power
// blips, so retry-until-confirmed carries no double-spend hazard.
func (a *Actuator) writeConfirmed(desc string, write func() error, confirmed func() bool) error {
	var err error
	for attempt := 0; attempt <= maxWriteRetries; attempt++ {
		if attempt > 0 {
			slog.Warn("actuator: write not confirmed — retrying (spaced)", "write", desc, "attempt", attempt)
			a.pause()
		}
		if err = write(); err != nil {
			continue
		}
		if a.awaitConfirmed(confirmed) {
			return nil
		}
		err = fmt.Errorf("actuator: %s not confirmed within %s", desc, a.confirmTimeout)
	}
	return err
}

// setTimedCharge enables/disables the grid timed-charge switch, confirmed via
// read-back.
func (a *Actuator) setTimedCharge(on bool) error {
	service, want := "turn_off", switchOff
	if on {
		service, want = "turn_on", switchOn
	}
	return a.writeConfirmed("timed_charge="+want,
		func() error {
			return a.callAck(switchDomain, service, map[string]any{"entity_id": a.cfg.TimedChargeSwitch})
		},
		func() bool { return a.ha.State(a.cfg.TimedChargeSwitch) == want },
	)
}

// setCurrent sets the per-unit mains charge current (A), confirmed via read-back.
func (a *Actuator) setCurrent(amps float64) error {
	val := round1(amps)
	return a.writeConfirmed(fmt.Sprintf("mains_charge_current=%.1f", val),
		func() error {
			return a.callAck(numberDomain, "set_value", map[string]any{
				"entity_id": a.cfg.MainsChargeCurrentNumber,
				"value":     val,
			})
		},
		func() bool {
			return math.Abs(a.ha.StateFloat(a.cfg.MainsChargeCurrentNumber)-val) < adjustEpsilon
		},
	)
}

// setWindowText writes one charge-window bound ("HH:MM"), confirmed via read-back.
func (a *Actuator) setWindowText(entityID, hhmm string) error {
	return a.writeConfirmed(fmt.Sprintf("%s=%s", entityID, hhmm),
		func() error {
			return a.callAck(textDomain, "set_value", map[string]any{
				"entity_id": entityID,
				"value":     hhmm,
			})
		},
		func() bool { return a.ha.State(entityID) == hhmm },
	)
}

// ensureWindowsMirrorOffPeak programs the inverter's charge-window slots to
// STATICALLY mirror the configured off-peak periods (slot i ← off-peak window i),
// inset by WindowInset at both ends — the hardware rail. Idempotent: a slot bound
// is written only when its read-back differs from the intended value, so a steady
// state issues no writes and there is no churn. Slots WITHOUT a corresponding
// off-peak window are left UNTOUCHED (never zeroed): the global enable switch, off
// outside off-peak, governs whether charging happens. A slot is SKIPPED (logged)
// when the off-peak window is shorter than 2×inset, when the inset interval is not
// wholly off-peak (windowWithinOffPeak guard), or when a mirrored bound would land
// on 00:00 (which the inverter may misread as a wrap / zero-length window).
// Returns whether any write was issued (so the caller can space a following write)
// plus any error.
func (a *Actuator) ensureWindowsMirrorOffPeak() (bool, error) {
	windows := a.rates.OffPeakWindows
	if len(windows) > len(a.cfg.ChargeWindows) {
		slog.Warn("actuator: more off-peak windows than inverter charge-window slots — "+
			"the excess periods have no charge window and cannot be grid-charged",
			"off_peak_windows", len(windows), "slots", len(a.cfg.ChargeWindows))
	}

	inset := a.cfg.WindowInset.Duration
	var errs []error
	wrote := false
	for i, slot := range a.cfg.ChargeWindows {
		if i >= len(windows) {
			break // no off-peak period for this slot — leave it untouched
		}
		w := windows[i]

		insetW, ok := insetWindow(w, inset)
		if !ok {
			slog.Warn("actuator: off-peak window shorter than 2×inset — skipping charge-window slot",
				"slot", i+1, "start", w.Start, "end", w.End, "inset", inset)
			continue
		}
		if !a.windowWithinOffPeak(insetW) {
			slog.Error("actuator: RAIL — refusing to program a charge-window slot with a "+
				"non-off-peak interval", "slot", i+1, "start", insetW.Start, "end", insetW.End)
			continue
		}
		startHHMM, endHHMM := insetW.Start.String(), insetW.End.String()
		if startHHMM == midnightHHMM || endHHMM == midnightHHMM {
			slog.Warn("actuator: mirrored charge window would write a 00:00 (midnight) bound — "+
				"skipping charge-window slot to avoid a wrap/zero-length misread",
				"slot", i+1, "start", startHHMM, "end", endHHMM)
			continue
		}

		for _, wr := range [...]struct{ entity, want string }{
			{slot.Start, startHHMM},
			{slot.End, endHHMM},
		} {
			if wr.entity == "" || a.ha.State(wr.entity) == wr.want {
				continue // unset entity, or already correct — no write, no spacing
			}
			if wrote {
				a.pause()
			}
			if err := a.setWindowText(wr.entity, wr.want); err != nil {
				errs = append(errs, err)
			}
			wrote = true
		}
	}
	return wrote, errors.Join(errs...)
}

func (a *Actuator) windowEntityIDs() []string {
	ids := make([]string, 0, len(a.cfg.ChargeWindows)*2)
	for _, slot := range a.cfg.ChargeWindows {
		ids = append(ids, slot.Start, slot.End)
	}
	return ids
}

// awaitConfirmed polls confirmed() until true or the confirm timeout elapses. It
// bounds on REAL wall-clock (device echo latency is physical, not the test clock)
// and returns promptly on close — except during the shutdown fail-safe, where the
// disable must be allowed to confirm.
func (a *Actuator) awaitConfirmed(confirmed func() bool) bool {
	deadline := time.Now().Add(a.confirmTimeout)
	for {
		if confirmed() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		if a.duringShutdown {
			time.Sleep(a.confirmPoll)
			continue
		}
		t := time.NewTimer(a.confirmPoll)
		select {
		case <-t.C:
		case <-a.closed:
			t.Stop()
			return confirmed()
		}
	}
}

// pause spaces two consecutive inverter writes (the SRNE "space writes"
// requirement). The loop goroutine is the sole writer so blocking it briefly is
// safe (async watchdog commands buffer). It returns early on a.closed so a normal
// transition's spacing does not wedge shutdown — EXCEPT on the shutdown path
// (duringShutdown), where the full spacing is honoured so the final disable write
// is never dropped.
func (a *Actuator) pause() {
	if a.spacing <= 0 {
		return
	}
	if a.duringShutdown {
		time.Sleep(a.spacing)
		return
	}
	t := time.NewTimer(a.spacing)
	defer t.Stop()
	select {
	case <-t.C:
	case <-a.closed:
	}
}

// --- kW → per-unit amps ---

// kwToAmps converts a grid-share kW target to a per-unit charge current. The
// commanded gridKW is first clamped to the AGGREGATE pack ceiling (MaxChargeKW)
// so a high per-unit amp headroom can never exceed pack acceptance; the per-unit
// MaxChargeCurrentA then bounds the result a second time. Uses the live pack
// voltage when fresh AND plausible, nominal otherwise. mains_charge_current is
// per-inverter, so the two parallel units double the delivered current — the
// division by NumUnits is what makes the confirmed-live 6 kW draw correct.
func (a *Actuator) kwToAmps(gridKW float64) float64 {
	if gridKW <= 0 {
		return 0
	}
	if a.cfg.MaxChargeKW > 0 && gridKW > a.cfg.MaxChargeKW {
		gridKW = a.cfg.MaxChargeKW
	}
	v := a.battery.NominalVoltageV
	if a.ha.Fresh(a.cfg.BatteryVoltageSensor, a.cfg.VoltageStale.Duration) {
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

// persistState records the actuator's charge intent across restarts. It is
// INFORMATIONAL, not safety-critical: reconcile disables timed charge on every
// start regardless, so a lost/corrupt file only affects logging, never safety.
type persistState struct {
	Charging bool    `json:"charging"`
	Amps     float64 `json:"amps"`
}

func (a *Actuator) persistPath() string {
	return filepath.Join(a.cfg.StateDir, "actuator_charge.json")
}

func (a *Actuator) persist() {
	if a.cfg.StateDir == "" {
		return
	}
	data, err := json.Marshal(persistState{
		Charging: a.st.charging,
		Amps:     a.st.amps,
	})
	if err != nil {
		return
	}
	if err := atomicWriteFile(a.persistPath(), data, 0o644); err != nil {
		slog.Warn("actuator: persist charge state failed", "error", err)
	}
}

// atomicWriteFile writes data to a temp file in the SAME directory, fsyncs it,
// then renames it over path, so a crash mid-write never leaves a torn file.
func atomicWriteFile(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".actuator-charge-*.tmp")
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
	a.st = chargeState{
		charging: ps.Charging,
		amps:     ps.Amps,
	}
}

func round1(v float64) float64 { return math.Round(v*10) / 10 }
