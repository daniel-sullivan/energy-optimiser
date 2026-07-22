package actuator

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"energy-optimiser/config"
)

// --- test doubles ---

type recordedCall struct {
	domain  string
	service string
	entity  string
	value   any // set_value payload (string for text, float64 for number)
}

// fakeHA records service calls and reflects successful writes into its state
// cache, so the actuator's read-back confirmation passes. It never touches real
// hardware.
type fakeHA struct {
	mu      sync.Mutex
	states  map[string]string
	attrs   map[string]map[string]any
	fresh   map[string]bool // per-entity; absent ⇒ freshOK
	freshOK bool            // default when entity absent from `fresh`
	ackErr  error           // if set, CallServiceAck returns it (no reflect)
	calls   []recordedCall

	// dropOnce[entity] = N: the next N writes to that entity are acked but NOT
	// reflected into the state cache — modelling the SRNE dropping a rapid-fire
	// consecutive write. Confirmation then fails until a retry lands.
	dropOnce map[string]int

	// panicNextCall makes the next CallServiceAck panic once (then clears),
	// modelling a fault reaching the HA client mid-transition (panic recovery).
	panicNextCall bool
}

func newFakeHA() *fakeHA {
	return &fakeHA{
		states:   map[string]string{},
		attrs:    map[string]map[string]any{},
		fresh:    map[string]bool{},
		freshOK:  true,
		dropOnce: map[string]int{},
	}
}

func (f *fakeHA) CallServiceAck(_ context.Context, domain, service string, data map[string]any) error {
	f.mu.Lock()
	if f.panicNextCall {
		f.panicNextCall = false
		f.mu.Unlock()
		panic("fakeHA: injected panic in CallServiceAck")
	}
	defer f.mu.Unlock()
	entity, _ := data["entity_id"].(string)
	f.calls = append(f.calls, recordedCall{domain: domain, service: service, entity: entity, value: data["value"]})
	if f.ackErr != nil {
		return f.ackErr
	}
	if n := f.dropOnce[entity]; n > 0 {
		f.dropOnce[entity] = n - 1
		return nil // acked but not reflected (dropped)
	}
	switch service {
	case "turn_on":
		f.states[entity] = switchOn
	case "turn_off":
		f.states[entity] = switchOff
	case "set_value":
		switch v := data["value"].(type) {
		case float64:
			f.states[entity] = strconv.FormatFloat(v, 'f', -1, 64)
		case string:
			f.states[entity] = v
		}
	}
	return nil
}

func (f *fakeHA) State(entityID string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.states[entityID]
}

func (f *fakeHA) StateFloat(entityID string) float64 {
	v, _ := strconv.ParseFloat(f.State(entityID), 64)
	return v
}

func (f *fakeHA) Attributes(entityID string) map[string]any {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.attrs[entityID]
}

func (f *fakeHA) Fresh(entityID string, _ time.Duration) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if v, ok := f.fresh[entityID]; ok {
		return v
	}
	return f.freshOK
}

func (f *fakeHA) setState(entityID, state string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.states[entityID] = state
}

// --- recorded-call queries ---

func (f *fakeHA) snapshot() []recordedCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]recordedCall(nil), f.calls...)
}

func (f *fakeHA) totalCalls() int { return len(f.snapshot()) }

// countService counts calls with the given service (across all entities).
func (f *fakeHA) countService(service string) int {
	n := 0
	for _, c := range f.snapshot() {
		if c.service == service {
			n++
		}
	}
	return n
}

// countSet counts set_value writes to a specific entity.
func (f *fakeHA) countSet(entity string) int {
	n := 0
	for _, c := range f.snapshot() {
		if c.service == "set_value" && c.entity == entity {
			n++
		}
	}
	return n
}

// lastIndexOf returns the index of the last matching call, or -1.
func (f *fakeHA) lastIndexOf(match func(recordedCall) bool) int {
	idx := -1
	for i, c := range f.snapshot() {
		if match(c) {
			idx = i
		}
	}
	return idx
}

func isSwitch(service string) func(recordedCall) bool {
	return func(c recordedCall) bool { return c.service == service }
}

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = t
}

// --- fixtures ---

const testTOML = `
time_zone = "Asia/Tokyo"
[rates]
peak_rate = 30
off_peak_rate = 10
feed_in_rate = 0
[[rates.off_peak_windows]]
start = "01:00"
end = "05:00"
[[rates.off_peak_windows]]
start = "11:00"
end = "13:00"
`

func parseRates(t *testing.T, tomlStr string) *config.Rates {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "c.toml")
	if err := os.WriteFile(path, []byte(tomlStr), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Parse(path)
	if err != nil {
		t.Fatal(err)
	}
	return &cfg.Rates
}

func testRates(t *testing.T) *config.Rates {
	t.Helper()
	return parseRates(t, testTOML)
}

const (
	switchEntity = "switch.timed_charge"
	ampsEntity   = "number.mains_a"
	voltEntity   = "sensor.batt_v"
	w1s          = "text.w1_start"
	w1e          = "text.w1_end"
	w2s          = "text.w2_start"
	w2e          = "text.w2_end"
	w3s          = "text.w3_start"
	w3e          = "text.w3_end"
)

func testCfg(t *testing.T) config.ActuatorHW {
	return config.ActuatorHW{
		TimedChargeSwitch:        switchEntity,
		MainsChargeCurrentNumber: ampsEntity,
		BatteryVoltageSensor:     voltEntity,
		ChargeWindows: []config.ChargeWindowEntities{
			{Start: w1s, End: w1e},
			{Start: w2s, End: w2e},
			{Start: w3s, End: w3e},
		},
		NumUnits:          2,
		MaxChargeCurrentA: 100,
		StateDir:          t.TempDir(),
		WindowInset:       config.Duration{Duration: 5 * time.Minute},
		WatchdogInterval:  config.Duration{Duration: time.Hour}, // won't fire mid-test
		WriteTimeout:      config.Duration{Duration: time.Second},
		ReadBackTimeout:   config.Duration{Duration: 150 * time.Millisecond},
		VoltageStale:      config.Duration{Duration: 5 * time.Minute},
		StateStale:        config.Duration{Duration: 5 * time.Minute},
	}
}

// seedWindows pre-populates the charge-window text entities with the mirrored,
// INSET off-peak values (01:00-05:00 → 01:05-04:55, 11:00-13:00 → 11:05-12:55),
// modelling the steady state after the rail has been established — so reconcile's
// idempotent mirror is a no-op and tests observe a clean write count. Slot 3 has
// no configured off-peak window, so it stays empty.
func seedWindows(f *fakeHA) {
	f.setState(w1s, "01:05")
	f.setState(w1e, "04:55")
	f.setState(w2s, "11:05")
	f.setState(w2e, "12:55")
}

// inWindow is 02:00 Tokyo (inside 01:00-05:00); outWindow is 08:00 (peak);
// window2 is 11:30 (inside 11:00-13:00).
var (
	tokyo, _  = time.LoadLocation("Asia/Tokyo")
	inWindow  = time.Date(2026, 1, 15, 2, 0, 0, 0, tokyo)
	outWindow = time.Date(2026, 1, 15, 8, 0, 0, 0, tokyo)
	window2   = time.Date(2026, 1, 15, 11, 30, 0, 0, tokyo)
)

// startWith wires an actuator to a pre-configured fake (seed windows / switch /
// drops on it BEFORE calling), with an injectable clock and fast write knobs.
func startWith(t *testing.T, fake *fakeHA, mode Mode, now time.Time) (*Actuator, *fakeClock) {
	t.Helper()
	clock := &fakeClock{t: now}
	a, err := New(testCfg(t), config.Battery{NominalVoltageV: 52.8}, testRates(t), fake, mode)
	if err != nil {
		t.Fatal(err)
	}
	a.now = clock.now
	fastSettle(a)
	if err := a.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(a.Close)
	return a, clock
}

// newActuator builds a started actuator in the steady state (window rail already
// mirrored). initialSwitch seeds the timed-charge switch state seen by reconcile.
func newActuator(t *testing.T, mode Mode, now time.Time, initialSwitch string) (*Actuator, *fakeHA, *fakeClock) {
	t.Helper()
	fake := newFakeHA()
	seedWindows(fake)
	if initialSwitch != "" {
		fake.setState(switchEntity, initialSwitch)
	}
	a, clock := startWith(t, fake, mode, now)
	return a, fake, clock
}

// rawActuator builds an unstarted actuator (no goroutines) for directly exercising
// loop-owned methods like ensureWindowsMirrorOffPeak on the test goroutine.
func rawActuator(t *testing.T, fake *fakeHA, rates *config.Rates, cfg config.ActuatorHW) *Actuator {
	t.Helper()
	a, err := New(cfg, config.Battery{NominalVoltageV: 52.8}, rates, fake, ModeLive)
	if err != nil {
		t.Fatal(err)
	}
	fastSettle(a)
	return a
}

// fastSettle shrinks the write-spacing/confirm knobs so the suite exercises the
// spacing/retry paths without real multi-second sleeps (production defaults are
// seconds).
func fastSettle(a *Actuator) {
	a.confirmTimeout = 200 * time.Millisecond
	a.spacing = time.Millisecond
	a.confirmPoll = 5 * time.Millisecond
}

func mustPlan(t *testing.T, a *Actuator, plan ChargePlan) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := a.SetChargePlan(ctx, plan); err != nil {
		t.Fatalf("SetChargePlan: %v", err)
	}
}

func submitSync(t *testing.T, a *Actuator, c command) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := a.submit(ctx, c); err != nil {
		t.Fatalf("submit %d: %v", c.kind, err)
	}
}

// --- the static off-peak window rail ---

// TestReconcileMirrorsOffPeakWindows proves reconcile programs each charge-window
// slot to STATICALLY mirror the configured off-peak period INSET by WindowInset
// (5m: 01:00-05:00 → 01:05-04:55, 11:00-13:00 → 11:05-12:55), leaves a slot with
// no corresponding window UNTOUCHED (never zeroed), and retries a dropped write.
func TestReconcileMirrorsOffPeakWindows(t *testing.T) {
	fake := newFakeHA()
	fake.setState(switchEntity, switchOff)
	fake.mu.Lock()
	fake.dropOnce[w1s] = 1 // first window-1-start write dropped → must retry
	fake.mu.Unlock()

	startWith(t, fake, ModeLive, inWindow)

	if fake.State(w1s) != "01:05" || fake.State(w1e) != "04:55" {
		t.Fatalf("slot 1 must mirror inset 01:05-04:55, got %q-%q", fake.State(w1s), fake.State(w1e))
	}
	if fake.State(w2s) != "11:05" || fake.State(w2e) != "12:55" {
		t.Fatalf("slot 2 must mirror inset 11:05-12:55, got %q-%q", fake.State(w2s), fake.State(w2e))
	}
	// Slot 3 has no configured off-peak window → left untouched (never zeroed).
	if fake.State(w3s) != "" || fake.State(w3e) != "" {
		t.Fatalf("slot 3 must be left untouched, got %q-%q", fake.State(w3s), fake.State(w3e))
	}
	if fake.countSet(w1s) < 2 {
		t.Fatalf("dropped window write must be retried (>=2 writes), got %d", fake.countSet(w1s))
	}
}

// TestSteadyStateStartupNoWrites proves that when the windows already mirror
// off-peak and the switch is off, startup issues zero writes (idempotent rail).
func TestSteadyStateStartupNoWrites(t *testing.T) {
	_, fake, _ := newActuator(t, ModeLive, inWindow, switchOff)
	if got := fake.totalCalls(); got != 0 {
		t.Fatalf("steady-state startup must be zero-write, got %d", got)
	}
}

// --- start: current + enable-last, no window rewriting ---

// TestStartSetsCurrentEnablesLast proves a fresh charge sets the per-unit current
// and enables timed charge LAST (after the current), and does NOT rewrite the
// static window rail.
func TestStartSetsCurrentEnablesLast(t *testing.T) {
	a, fake, _ := newActuator(t, ModeLive, inWindow, switchOff)

	mustPlan(t, a, ChargePlan{Charging: true, GridKW: 5})

	if v := fake.StateFloat(ampsEntity); v <= 0 {
		t.Fatalf("mains charge current must be set nonzero, got %v", v)
	}
	if fake.State(switchEntity) != switchOn {
		t.Fatal("timed charge must be enabled after a start")
	}
	enableIdx := fake.lastIndexOf(isSwitch("turn_on"))
	if enableIdx != fake.totalCalls()-1 {
		t.Fatalf("enable must be the LAST write; enable at %d of %d calls", enableIdx, fake.totalCalls())
	}
	currentIdx := fake.lastIndexOf(func(c recordedCall) bool { return c.service == "set_value" && c.entity == ampsEntity })
	if enableIdx <= currentIdx {
		t.Fatalf("enable (idx %d) must follow the current write (%d)", enableIdx, currentIdx)
	}
	for _, e := range []string{w1s, w1e, w2s, w2e, w3s, w3e} {
		if fake.countSet(e) != 0 {
			t.Fatalf("start must not rewrite the static window rail; %s written %d times", e, fake.countSet(e))
		}
	}
	if !a.st.charging {
		t.Fatal("state must record charging after a confirmed enable")
	}
}

// TestSecondWindowChargesWithoutRewritingWindows proves charging in the second
// off-peak window works off the pre-mirrored rail (global switch), with NO window
// writes — the whole point of the static-mirror model.
func TestSecondWindowChargesWithoutRewritingWindows(t *testing.T) {
	a, fake, _ := newActuator(t, ModeLive, window2, switchOff)
	mustPlan(t, a, ChargePlan{Charging: true, GridKW: 5})

	if fake.State(switchEntity) != switchOn {
		t.Fatal("must charge in the second off-peak window")
	}
	if v := fake.StateFloat(ampsEntity); v <= 0 {
		t.Fatalf("current must be set in window 2, got %v", v)
	}
	for _, e := range []string{w1s, w1e, w2s, w2e} {
		if fake.countSet(e) != 0 {
			t.Fatalf("charging in window 2 must not rewrite the static windows; %s written", e)
		}
	}
}

// TestAdjustsAreCheap proves a start followed by rate changes enables timed charge
// exactly ONCE (no re-enable) and never touches the windows.
func TestAdjustsAreCheap(t *testing.T) {
	a, fake, _ := newActuator(t, ModeLive, inWindow, switchOff)

	mustPlan(t, a, ChargePlan{Charging: true, GridKW: 6})
	for _, kw := range []float64{5, 4, 3, 2, 1} {
		mustPlan(t, a, ChargePlan{Charging: true, GridKW: kw})
	}
	if got := fake.countService("turn_on"); got != 1 {
		t.Fatalf("start + 5 adjusts must enable timed charge once, got %d", got)
	}
	for _, e := range []string{w1s, w1e, w2s, w2e, w3s, w3e} {
		if fake.countSet(e) != 0 {
			t.Fatalf("adjusts must not touch the windows; %s written", e)
		}
	}
	if v := fake.StateFloat(ampsEntity); v <= 0 {
		t.Fatalf("current must remain set during adjusts, got %v", v)
	}
}

// --- stop: disable-first, clears state, never touches windows ---

// TestStopDisablesFirstThenZerosCurrent proves stopping disables the switch
// BEFORE zeroing the current, clears the charge state, and never touches windows.
func TestStopDisablesFirstThenZerosCurrent(t *testing.T) {
	a, fake, _ := newActuator(t, ModeLive, inWindow, switchOff)
	mustPlan(t, a, ChargePlan{Charging: true, GridKW: 5}) // charging
	if !a.st.charging {
		t.Fatal("precondition: must be charging")
	}
	before := fake.totalCalls()

	mustPlan(t, a, ChargePlan{Charging: false}) // stop

	disableIdx := fake.lastIndexOf(isSwitch("turn_off"))
	zeroIdx := fake.lastIndexOf(func(c recordedCall) bool {
		v, _ := c.value.(float64)
		return c.service == "set_value" && c.entity == ampsEntity && v == 0
	})
	if disableIdx < before {
		t.Fatal("stop must issue a turn_off")
	}
	if zeroIdx < before || disableIdx >= zeroIdx {
		t.Fatalf("disable (idx %d) must precede zero-current (idx %d)", disableIdx, zeroIdx)
	}
	if fake.State(switchEntity) != switchOff {
		t.Fatal("stop must leave timed charge off")
	}
	if fake.StateFloat(ampsEntity) != 0 {
		t.Fatal("stop must zero the current")
	}
	for _, e := range []string{w1s, w1e, w2s, w2e, w3s, w3e} {
		if fake.countSet(e) != 0 {
			t.Fatalf("stop must NEVER touch the windows; %s written", e)
		}
	}
	if a.st.charging {
		t.Fatal("stop must clear the charging state")
	}
}

// TestStopWhenIdleIsNoOp proves a not-charging plan while already stopped issues
// no writes (idempotent).
func TestStopWhenIdleIsNoOp(t *testing.T) {
	a, fake, _ := newActuator(t, ModeLive, inWindow, switchOff)
	mustPlan(t, a, ChargePlan{Charging: false})
	if got := fake.totalCalls(); got != 0 {
		t.Fatalf("idle stop must issue zero writes, got %d", got)
	}
}

// --- write spacing + retry-until-confirmed ---

// TestDroppedCurrentAndSwitchRetried proves a dropped current / enable write is
// retried (spaced) within the same command and lands.
func TestDroppedCurrentAndSwitchRetried(t *testing.T) {
	a, fake, _ := newActuator(t, ModeLive, inWindow, switchOff)
	fake.mu.Lock()
	fake.dropOnce[ampsEntity] = 1
	fake.dropOnce[switchEntity] = 1
	fake.mu.Unlock()

	mustPlan(t, a, ChargePlan{Charging: true, GridKW: 5})

	if v := fake.StateFloat(ampsEntity); v <= 0 {
		t.Fatalf("dropped current write must be retried and land; got %v", v)
	}
	if fake.State(switchEntity) != switchOn {
		t.Fatal("dropped enable write must be retried and land")
	}
	if fake.countSet(ampsEntity) < 2 {
		t.Fatalf("dropped current write must be retried at least once, got %d", fake.countSet(ampsEntity))
	}
	if got := fake.countService("turn_on"); got < 2 {
		t.Fatalf("dropped enable must be retried at least once, got %d turn_on", got)
	}
}

// TestUnconfirmedEnableLeavesNotCharging proves that when the enable never
// confirms, the actuator does NOT record charging, so the next tick retries.
func TestUnconfirmedEnableLeavesNotCharging(t *testing.T) {
	a, fake, _ := newActuator(t, ModeLive, inWindow, switchOff)
	fake.mu.Lock()
	fake.dropOnce[switchEntity] = 1000 // enable never reflects
	fake.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := a.SetChargePlan(ctx, ChargePlan{Charging: true, GridKW: 5}); err == nil {
		t.Fatal("an unconfirmed enable must surface an error")
	}
	if a.st.charging {
		t.Fatal("unconfirmed enable must not record charging (so the next tick retries)")
	}
	// Current was still set (safe — switch is off, nothing charges).
	if v := fake.StateFloat(ampsEntity); v <= 0 {
		t.Fatal("current must still be set even when the enable fails")
	}
}

// --- mode gating ---

func TestObserveModeWritesNothing(t *testing.T) {
	a, fake, clock := newActuator(t, ModeObserve, inWindow, switchOff)
	mustPlan(t, a, ChargePlan{Charging: true, GridKW: 5})
	mustPlan(t, a, ChargePlan{Charging: false})

	// Even a watchdog trigger must not write in observe mode.
	clock.set(outWindow)
	fake.setState(switchEntity, switchOn)
	submitSync(t, a, command{kind: cmdWatchdog})

	if got := fake.totalCalls(); got != 0 {
		t.Fatalf("observe mode must issue zero writes, got %d", got)
	}
}

func TestWatchdogOnlyModeOnlySafes(t *testing.T) {
	a, fake, clock := newActuator(t, ModeWatchdogOnly, inWindow, switchOff)

	// Policy must not actuate (never initiates charging).
	mustPlan(t, a, ChargePlan{Charging: true, GridKW: 5})
	if got := fake.countService("turn_on"); got != 0 {
		t.Fatalf("watchdog-only must not enable timed charge, got %d turn_on", got)
	}
	// But the fail-safe path may write: timed charge left on outside a window.
	clock.set(outWindow)
	fake.setState(switchEntity, switchOn)
	submitSync(t, a, command{kind: cmdWatchdog})
	if fake.State(switchEntity) != switchOff {
		t.Fatal("watchdog-only must still disable timed charge outside a window")
	}
}

// --- watchdog: out-of-window backstop ---

func TestWatchdogDisablesTimedChargeOutsideWindow(t *testing.T) {
	a, fake, clock := newActuator(t, ModeLive, inWindow, switchOff)
	clock.set(outWindow)
	fake.setState(switchEntity, switchOn)

	submitSync(t, a, command{kind: cmdWatchdog})
	if fake.State(switchEntity) != switchOff {
		t.Fatal("watchdog must disable timed charge enabled outside a window")
	}
}

func TestWatchdogDisablesOnStaleFeedOutsideWindow(t *testing.T) {
	a, fake, clock := newActuator(t, ModeLive, inWindow, switchOff)
	clock.set(outWindow)
	fake.mu.Lock()
	fake.fresh[switchEntity] = false
	fake.states[switchEntity] = "unknown"
	fake.mu.Unlock()

	submitSync(t, a, command{kind: cmdWatchdog})
	if fake.State(switchEntity) != switchOff {
		t.Fatalf("stale feed outside a window must disable timed charge; got %q", fake.State(switchEntity))
	}
}

func TestWatchdogQuietWhenOffOutsideWindow(t *testing.T) {
	a, fake, clock := newActuator(t, ModeLive, inWindow, switchOff)
	clock.set(outWindow) // outside a window, switch already off
	before := fake.totalCalls()
	submitSync(t, a, command{kind: cmdWatchdog})
	if got := fake.totalCalls(); got != before {
		t.Fatalf("watchdog must be quiet when off outside a window; writes %d→%d", before, got)
	}
}

func TestWatchdogQuietInWindow(t *testing.T) {
	a, fake, _ := newActuator(t, ModeLive, inWindow, switchOff)
	mustPlan(t, a, ChargePlan{Charging: true, GridKW: 5}) // legitimately charging in-window
	before := fake.totalCalls()

	submitSync(t, a, command{kind: cmdWatchdog}) // in-window: policy governs, watchdog stays quiet
	if got := fake.totalCalls(); got != before {
		t.Fatalf("watchdog must not act in-window; writes %d→%d", before, got)
	}
	if fake.State(switchEntity) != switchOn {
		t.Fatal("watchdog must not disable a legitimate in-window charge")
	}
}

// TestWatchdogReassertsWindowMirror proves the watchdog idempotently re-asserts
// the static window rail, self-healing a mid-life window scramble.
func TestWatchdogReassertsWindowMirror(t *testing.T) {
	a, fake, _ := newActuator(t, ModeLive, inWindow, switchOff)
	fake.setState(w1s, "09:00") // a mid-life window corruption

	submitSync(t, a, command{kind: cmdWatchdog})

	if got := fake.State(w1s); got != "01:05" {
		t.Fatalf("watchdog must re-assert the mirrored window; w1s=%q want 01:05", got)
	}
}

// --- Close disables timed charge ---

func TestCloseDisablesTimedCharge(t *testing.T) {
	a, fake, _ := newActuator(t, ModeLive, inWindow, switchOff)
	mustPlan(t, a, ChargePlan{Charging: true, GridKW: 5}) // charging

	a.Close()
	if fake.State(switchEntity) != switchOff {
		t.Fatal("Close must disable timed charge")
	}
	if fake.StateFloat(ampsEntity) != 0 {
		t.Fatal("Close must zero the current")
	}
}

// --- startup reconciliation ---

func TestReconcileDisablesTimedChargeFoundOn(t *testing.T) {
	// Switch found ON at startup (a crashed-daemon leftover) → reconcile disables.
	_, fake, _ := newActuator(t, ModeLive, inWindow, switchOn)
	if fake.State(switchEntity) != switchOff {
		t.Fatal("startup reconcile must disable a timed charge found enabled")
	}
	if got := fake.countService("turn_off"); got < 1 {
		t.Fatalf("reconcile must issue a disable when found on, got %d turn_off", got)
	}
}

func TestReconcileObserveModeDoesNotWrite(t *testing.T) {
	_, fake, _ := newActuator(t, ModeObserve, inWindow, switchOn)
	if got := fake.totalCalls(); got != 0 {
		t.Fatalf("observe-mode reconcile must not write even if found on, got %d", got)
	}
}

// --- kW → amps ---

func TestKwToAmps(t *testing.T) {
	fake := newFakeHA()
	a, err := New(testCfg(t), config.Battery{NominalVoltageV: 52.8}, testRates(t), fake, ModeLive)
	if err != nil {
		t.Fatal(err)
	}

	// Fresh live voltage is used (per-unit = kW*1000/(V*numUnits), numUnits=2).
	fake.setState(voltEntity, "50.0")
	fake.fresh[voltEntity] = true
	if got := a.kwToAmps(5.28); !approx(got, 5280/(50.0*2)) {
		t.Fatalf("live-voltage amps: got %.3f", got)
	}

	// Stale voltage falls back to nominal (52.8).
	fake.fresh[voltEntity] = false
	if got := a.kwToAmps(5.28); !approx(got, 5280/(52.8*2)) {
		t.Fatalf("nominal-fallback amps: got %.3f", got)
	}

	// Clamp to the per-unit ceiling.
	fake.fresh[voltEntity] = false
	if got := a.kwToAmps(1000); got != a.cfg.MaxChargeCurrentA {
		t.Fatalf("clamp: got %.3f want %.1f", got, a.cfg.MaxChargeCurrentA)
	}

	// Zero/negative → zero.
	if got := a.kwToAmps(0); got != 0 {
		t.Fatalf("zero kW must be 0 A, got %.3f", got)
	}
}

func approx(a, b float64) bool {
	d := a - b
	if d < 0 {
		d = -d
	}
	return d < 1e-6
}

// TestKwToAmpsAggregateCeilingAndLowVoltage checks the aggregate-kW clamp applies
// BEFORE the kW→A conversion, and that an implausibly-low fresh voltage falls back
// to nominal instead of inflating amps.
func TestKwToAmpsAggregateCeilingAndLowVoltage(t *testing.T) {
	fake := newFakeHA()
	cfg := testCfg(t)
	cfg.MaxChargeKW = 8
	a, err := New(cfg, config.Battery{NominalVoltageV: 52.8}, testRates(t), fake, ModeLive)
	if err != nil {
		t.Fatal(err)
	}

	fake.setState(voltEntity, "52.8")
	fake.fresh[voltEntity] = true
	got := a.kwToAmps(20) // clamps to 8 kW first → 8000/(52.8*2) ≈ 75.8 A, under the 100 A per-unit ceiling
	want := 8 * 1000.0 / (52.8 * 2)
	if !approx(got, want) {
		t.Fatalf("aggregate ceiling: got %.3f want %.3f", got, want)
	}

	fake.setState(voltEntity, "5.0") // glitched-low fresh voltage → use nominal
	fake.fresh[voltEntity] = true
	got = a.kwToAmps(5)
	want = 5 * 1000.0 / (52.8 * 2)
	if !approx(got, want) {
		t.Fatalf("low-voltage guard: got %.3f want %.3f", got, want)
	}
}

// --- zero grid charge ---

// TestZeroGridChargeEnsuresOff asserts a charge plan whose grid share rounds to
// ~0 A (PV surplus covers it) does NOT enable timed charge.
func TestZeroGridChargeEnsuresOff(t *testing.T) {
	a, fake, _ := newActuator(t, ModeLive, inWindow, switchOff)
	mustPlan(t, a, ChargePlan{Charging: true, GridKW: 0})
	if got := fake.countService("turn_on"); got != 0 {
		t.Fatalf("zero grid-charge must not enable timed charge; got %d turn_on", got)
	}
	if a.st.charging {
		t.Fatal("zero grid-charge must not record charging")
	}
}

// --- the off-peak hardware rail ---

// TestWindowWithinOffPeakRail unit-tests the rail guard on what is written to a
// charge-window slot: a configured off-peak window passes; a peak-spanning or a
// fully-peak window fails.
func TestWindowWithinOffPeakRail(t *testing.T) {
	a, _, _ := newActuator(t, ModeLive, inWindow, switchOff)

	if !a.windowWithinOffPeak(config.TimeWindow{Start: config.TimeOfDay{Hour: 1}, End: config.TimeOfDay{Hour: 5}}) {
		t.Fatal("a configured off-peak window must pass the rail")
	}
	if a.windowWithinOffPeak(config.TimeWindow{Start: config.TimeOfDay{Hour: 2}, End: config.TimeOfDay{Hour: 8}}) {
		t.Fatal("a peak-spanning window (02:00-08:00) must FAIL the rail")
	}
	if a.windowWithinOffPeak(config.TimeWindow{Start: config.TimeOfDay{Hour: 6}, End: config.TimeOfDay{Hour: 7}}) {
		t.Fatal("a fully-peak window (06:00-07:00) must FAIL the rail")
	}
}

// TestInsetWindow unit-tests the window-inset helper: normal shrink at both ends,
// rejection of a window shorter than 2×inset, and zero-inset passthrough.
func TestInsetWindow(t *testing.T) {
	tw := func(sh, sm, eh, em int) config.TimeWindow {
		return config.TimeWindow{Start: config.TimeOfDay{Hour: sh, Minute: sm}, End: config.TimeOfDay{Hour: eh, Minute: em}}
	}
	if got, ok := insetWindow(tw(1, 0, 5, 0), 5*time.Minute); !ok ||
		got.Start != (config.TimeOfDay{Hour: 1, Minute: 5}) || got.End != (config.TimeOfDay{Hour: 4, Minute: 55}) {
		t.Fatalf("inset 01:00-05:00 → 01:05-04:55, got %v-%v ok=%v", got.Start, got.End, ok)
	}
	if got, ok := insetWindow(tw(11, 0, 13, 0), 5*time.Minute); !ok ||
		got.Start != (config.TimeOfDay{Hour: 11, Minute: 5}) || got.End != (config.TimeOfDay{Hour: 12, Minute: 55}) {
		t.Fatalf("inset 11:00-13:00 → 11:05-12:55, got %v-%v ok=%v", got.Start, got.End, ok)
	}
	if _, ok := insetWindow(tw(2, 0, 2, 8), 5*time.Minute); ok {
		t.Fatal("an 8-min window with 5-min inset must be rejected (< 2×inset)")
	}
	if got, ok := insetWindow(tw(1, 0, 5, 0), 0); !ok || got.Start != (config.TimeOfDay{Hour: 1}) {
		t.Fatalf("zero inset must pass through unchanged, got %v ok=%v", got, ok)
	}
}

// TestMirrorSkipsShortWindow proves an off-peak window shorter than 2×inset is
// skipped (not written) rather than programmed with a collapsed/inverted window.
func TestMirrorSkipsShortWindow(t *testing.T) {
	const shortTOML = `
time_zone = "Asia/Tokyo"
[rates]
peak_rate = 30
off_peak_rate = 10
feed_in_rate = 0
[[rates.off_peak_windows]]
start = "02:00"
end = "02:08"
`
	fake := newFakeHA()
	a := rawActuator(t, fake, parseRates(t, shortTOML), testCfg(t))
	wrote, err := a.ensureWindowsMirrorOffPeak()
	if err != nil {
		t.Fatalf("mirror should not error, got %v", err)
	}
	if wrote {
		t.Fatal("a sub-2×inset window must be skipped (no write)")
	}
	if fake.State(w1s) != "" || fake.State(w1e) != "" {
		t.Fatalf("short window slot must be left untouched, got %q-%q", fake.State(w1s), fake.State(w1e))
	}
}

// TestMirrorSkipsMidnightBound proves a mirrored window whose inset bound lands on
// 00:00 (which the inverter may misread as a wrap/zero window) is skipped.
func TestMirrorSkipsMidnightBound(t *testing.T) {
	// 23:55-04:00 inset by 5m → start 00:00 (midnight) → must skip.
	const midnightTOML = `
time_zone = "Asia/Tokyo"
[rates]
peak_rate = 30
off_peak_rate = 10
feed_in_rate = 0
[[rates.off_peak_windows]]
start = "23:55"
end = "04:00"
`
	fake := newFakeHA()
	a := rawActuator(t, fake, parseRates(t, midnightTOML), testCfg(t))
	wrote, err := a.ensureWindowsMirrorOffPeak()
	if err != nil {
		t.Fatalf("mirror should not error, got %v", err)
	}
	if wrote {
		t.Fatal("a window with a 00:00 mirrored bound must be skipped (no write)")
	}
	if fake.State(w1s) != "" || fake.State(w1e) != "" {
		t.Fatalf("midnight-bound slot must be left untouched, got %q-%q", fake.State(w1s), fake.State(w1e))
	}
}

// TestNoChargeAtPeakEnsuresOff proves that a charging plan arriving while the
// clock is at peak (a buggy caller) never enables timed charge — the actuator
// derives off-peak from its own tariff config.
func TestNoChargeAtPeakEnsuresOff(t *testing.T) {
	a, fake, _ := newActuator(t, ModeLive, outWindow, switchOff) // clock at peak (08:00)
	mustPlan(t, a, ChargePlan{Charging: true, GridKW: 5})
	if got := fake.countService("turn_on"); got != 0 {
		t.Fatalf("charge at peak must not enable timed charge; got %d turn_on", got)
	}
	if a.st.charging {
		t.Fatal("charge at peak must not record charging")
	}
}

// --- panic recovery ---

// TestPanicInTransitionRecoveredAndSafes injects a panic mid-start; the handler
// must recover, best-effort disable timed charge, return an error, and keep the
// goroutine alive for later commands.
func TestPanicInTransitionRecoveredAndSafes(t *testing.T) {
	a, fake, _ := newActuator(t, ModeLive, inWindow, switchOff)

	fake.mu.Lock()
	fake.panicNextCall = true // the first write panics
	fake.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := a.SetChargePlan(ctx, ChargePlan{Charging: true, GridKW: 5}); err == nil {
		t.Fatal("a recovered panic must surface as an error to the caller")
	}
	if fake.State(switchEntity) != switchOff {
		t.Fatalf("panic recovery must disable timed charge; switch=%q", fake.State(switchEntity))
	}
	if a.st.charging {
		t.Fatal("panic recovery must leave charging cleared")
	}
	// The goroutine survived: a subsequent command is still served.
	submitSync(t, a, command{kind: cmdWatchdog})
}

// --- persistence ---

// TestPersistAtomicAndCorruptColdStart verifies (a) a corrupt persisted file
// cold-starts cleanly and (b) persist leaves no torn temp files and writes valid
// JSON via temp+rename.
func TestPersistAtomicAndCorruptColdStart(t *testing.T) {
	dir := t.TempDir()
	cfg := testCfg(t)
	cfg.StateDir = dir
	path := filepath.Join(dir, "actuator_charge.json")

	if err := os.WriteFile(path, []byte("{ this is not valid json"), 0o644); err != nil {
		t.Fatal(err)
	}
	fake := newFakeHA()
	seedWindows(fake)
	fake.setState(switchEntity, switchOff)
	a, err := New(cfg, config.Battery{NominalVoltageV: 52.8}, testRates(t), fake, ModeLive)
	if err != nil {
		t.Fatal(err)
	}
	a.now = func() time.Time { return inWindow }
	fastSettle(a)
	if err := a.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(a.Close)
	if a.st.charging {
		t.Fatalf("corrupt persisted file must cold-start clean, got %+v", a.st)
	}

	mustPlan(t, a, ChargePlan{Charging: true, GridKW: 5})

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".actuator-charge-") {
			t.Fatalf("atomic write must not leave a temp file, found %s", e.Name())
		}
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var ps persistState
	if err := json.Unmarshal(data, &ps); err != nil {
		t.Fatalf("persisted file must be valid JSON (not torn): %v", err)
	}
	if !ps.Charging {
		t.Fatalf("persisted state after a start must record charging, got %+v", ps)
	}
}

// TestEnterWriteFailureSurfaces proves a failing ack surfaces as an error and does
// not record charging.
func TestEnterWriteFailureSurfaces(t *testing.T) {
	a, fake, _ := newActuator(t, ModeLive, inWindow, switchOff)
	fake.ackErr = errors.New("boom") // every write now fails ack

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := a.SetChargePlan(ctx, ChargePlan{Charging: true, GridKW: 5}); err == nil {
		t.Fatal("expected start to error when writes fail")
	}
	if a.st.charging {
		t.Fatal("failed start must not record charging")
	}
}
