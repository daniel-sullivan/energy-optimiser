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
	domain      string
	service     string
	entity      string
	option      string
	value       float64
	prioAtWrite string // for set_value: output_priority state visible at write time
}

// fakeHA records service calls and reflects successful writes into its state
// cache, so the actuator's read-back verification passes. It never touches real
// hardware.
type fakeHA struct {
	mu      sync.Mutex
	states  map[string]string
	attrs   map[string]map[string]any
	fresh   map[string]bool // per-entity; absent ⇒ fresh
	freshOK bool            // default when entity absent from `fresh`
	ackErr  error           // if set, CallServiceAck returns it (no reflect)
	calls   []recordedCall

	// decoupleSelect models HA's `result` ack landing BEFORE the SRNE select
	// entity echoes its new state: select_option acks successfully but the state
	// cache is NOT updated, so the actuator's advisory read-back times out even
	// though the write physically landed (the C1 double-spend hazard).
	decoupleSelect bool

	// selectEchoDelay models the SLOW SRNE state feed: a select_option write is
	// acked immediately but only becomes visible in the state cache after this
	// wall-clock delay. Used to prove the enter waits for UTI to APPLY before the
	// amps write. Zero ⇒ immediate echo (unless decoupleSelect).
	selectEchoDelay time.Duration
	pendingSelEnt   string
	pendingSelVal   string
	pendingSelAt    time.Time

	// dropFirstAmps models the SRNE dropping a rapid-fire consecutive write: the
	// first set_value (amps) is acked but NOT reflected into the state cache.
	dropFirstAmps bool
	sawFirstAmps  bool

	// panicNextCall makes the next CallServiceAck panic once (then clears),
	// modelling a fault reaching the HA client mid-transition (H3 recovery).
	panicNextCall bool
}

func newFakeHA() *fakeHA {
	return &fakeHA{
		states:  map[string]string{},
		attrs:   map[string]map[string]any{},
		fresh:   map[string]bool{},
		freshOK: true,
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
	c := recordedCall{domain: domain, service: service}
	if e, ok := data["entity_id"].(string); ok {
		c.entity = e
	}
	if o, ok := data["option"].(string); ok {
		c.option = o
	}
	if v, ok := data["value"].(float64); ok {
		c.value = v
	}
	if service == "set_value" {
		c.prioAtWrite = f.states[prioEntity]
	}
	f.calls = append(f.calls, c)
	if f.ackErr != nil {
		return f.ackErr
	}
	// Reflect the write so read-back sees it — modelling the SRNE's echo behaviour.
	switch service {
	case "select_option":
		switch {
		case f.selectEchoDelay > 0:
			// Slow feed: becomes visible only after selectEchoDelay (see State).
			f.pendingSelEnt = c.entity
			f.pendingSelVal = c.option
			f.pendingSelAt = time.Now().Add(f.selectEchoDelay)
		case f.decoupleSelect:
			// Never echoes.
		default:
			f.states[c.entity] = c.option
		}
	case "set_value":
		if f.dropFirstAmps && !f.sawFirstAmps {
			f.sawFirstAmps = true // dropped: acked but not reflected
		} else {
			f.states[c.entity] = strconv.FormatFloat(c.value, 'f', -1, 64)
		}
	}
	return nil
}

func (f *fakeHA) State(entityID string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	// Promote a delayed select echo once its wall-clock deadline has passed.
	if f.pendingSelEnt == entityID && !f.pendingSelAt.IsZero() && !time.Now().Before(f.pendingSelAt) {
		f.states[entityID] = f.pendingSelVal
		f.pendingSelAt = time.Time{}
		f.pendingSelEnt = ""
	}
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

// selectCalls counts output_priority writes — each is one power blip.
func (f *fakeHA) selectCalls(entity string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, c := range f.calls {
		if c.service == "select_option" && c.entity == entity {
			n++
		}
	}
	return n
}

func (f *fakeHA) lastSelect(entity string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	last := ""
	for _, c := range f.calls {
		if c.service == "select_option" && c.entity == entity {
			last = c.option
		}
	}
	return last
}

func (f *fakeHA) totalCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

// ampsWriteCount counts set_value writes to the amps entity (each a retry/adjust).
func (f *fakeHA) ampsWriteCount(entity string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, c := range f.calls {
		if c.service == "set_value" && c.entity == entity {
			n++
		}
	}
	return n
}

// firstAmpsWritePrio returns the output_priority state visible at the moment the
// FIRST amps write was issued — used to prove the enter gated the amps write on
// UTI having applied.
func (f *fakeHA) firstAmpsWritePrio(entity string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range f.calls {
		if c.service == "set_value" && c.entity == entity {
			return c.prioAtWrite
		}
	}
	return ""
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

func testRates(t *testing.T) *config.Rates {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "c.toml")
	if err := os.WriteFile(path, []byte(testTOML), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Parse(path)
	if err != nil {
		t.Fatal(err)
	}
	return &cfg.Rates
}

const (
	prioEntity = "select.out_prio"
	ampsEntity = "number.mains_a"
	voltEntity = "sensor.batt_v"
)

func testCfg(t *testing.T) config.ActuatorHW {
	return config.ActuatorHW{
		OutputPrioritySelect:     prioEntity,
		MainsChargeCurrentNumber: ampsEntity,
		BatteryVoltageSensor:     voltEntity,
		UtilityOption:            "UTI",
		BatteryOption:            "SBU",
		NumUnits:                 2,
		MaxChargeCurrentA:        100,
		StateDir:                 t.TempDir(),
		WatchdogInterval:         config.Duration{Duration: time.Hour}, // won't fire mid-test
		WriteTimeout:             config.Duration{Duration: time.Second},
		ReadBackTimeout:          config.Duration{Duration: 150 * time.Millisecond}, // short: advisory read-back
		VoltageStale:             config.Duration{Duration: 5 * time.Minute},
		StateStale:               config.Duration{Duration: 5 * time.Minute},
	}
}

// inWindow is 02:00 Tokyo (inside 01:00-05:00); outWindow is 08:00 (peak).
var (
	tokyo, _  = time.LoadLocation("Asia/Tokyo")
	inWindow  = time.Date(2026, 1, 15, 2, 0, 0, 0, tokyo)
	outWindow = time.Date(2026, 1, 15, 8, 0, 0, 0, tokyo)
)

// newActuator builds a started actuator wired to a fake, with an injectable
// clock. initialPriority seeds the inverter state seen by startup reconcile.
func newActuator(t *testing.T, mode Mode, now time.Time, initialPriority string) (*Actuator, *fakeHA, *fakeClock) {
	t.Helper()
	fake := newFakeHA()
	if initialPriority != "" {
		fake.setState(prioEntity, initialPriority)
	}
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
	return a, fake, clock
}

// fastSettle shrinks the write-spacing knobs so the suite exercises the settle/
// retry paths without real multi-second sleeps (production defaults are seconds).
func fastSettle(a *Actuator) {
	a.settleTimeout = 200 * time.Millisecond
	a.settleDelay = time.Millisecond
	a.settlePoll = 5 * time.Millisecond
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

// --- state machine / blip budget ---

func TestEnterAdjustStopExit_TwoBlips(t *testing.T) {
	a, fake, _ := newActuator(t, ModeLive, inWindow, "SBU")
	win, ok := a.rates.ActiveWindow(inWindow)
	if !ok {
		t.Fatal("expected in-window")
	}

	mustPlan(t, a, ChargePlan{Charging: true, GridKW: 5, Window: win}) // enter (blip)
	mustPlan(t, a, ChargePlan{Charging: true, GridKW: 3, Window: win}) // adjust (no blip)
	mustPlan(t, a, ChargePlan{Charging: true, GridKW: 4, Window: win}) // adjust (no blip)
	mustPlan(t, a, ChargePlan{Charging: false, Window: win})           // stop (no blip, stays bypass)
	submitSync(t, a, command{kind: cmdBoundary, windowID: win.ID})     // exit (blip)

	if got := fake.selectCalls(prioEntity); got != 2 {
		t.Fatalf("want exactly 2 output_priority blips, got %d", got)
	}
	if last := fake.lastSelect(prioEntity); last != "SBU" {
		t.Fatalf("want final priority SBU, got %q", last)
	}
}

func TestAdjustsAreBlipFree(t *testing.T) {
	a, fake, _ := newActuator(t, ModeLive, inWindow, "SBU")
	win, _ := a.rates.ActiveWindow(inWindow)

	mustPlan(t, a, ChargePlan{Charging: true, GridKW: 6, Window: win}) // enter
	for _, kw := range []float64{5, 4, 3, 2, 1} {
		mustPlan(t, a, ChargePlan{Charging: true, GridKW: kw, Window: win})
	}
	if got := fake.selectCalls(prioEntity); got != 1 {
		t.Fatalf("enter + 5 adjusts should be 1 blip, got %d", got)
	}
}

func TestSecondEnterRefused(t *testing.T) {
	a, fake, _ := newActuator(t, ModeLive, inWindow, "SBU")
	win, _ := a.rates.ActiveWindow(inWindow)

	mustPlan(t, a, ChargePlan{Charging: true, GridKW: 5, Window: win}) // enter (blip 1)
	// Simulate a mid-window fail-safe (e.g. a watchdog trip) that exits bypass
	// but leaves the enter budget spent.
	submitSync(t, a, command{kind: cmdSafe, reason: "test", force: true}) // exit (blip 2)

	// A fresh charge request in the SAME window must be refused (budget spent),
	// producing no third blip.
	mustPlan(t, a, ChargePlan{Charging: true, GridKW: 5, Window: win})
	if got := fake.selectCalls(prioEntity); got != 2 {
		t.Fatalf("second enter must be refused; want 2 blips, got %d", got)
	}
}

func TestNewWindowResetsBudget(t *testing.T) {
	a, fake, clock := newActuator(t, ModeLive, inWindow, "SBU")
	win1, _ := a.rates.ActiveWindow(inWindow)
	mustPlan(t, a, ChargePlan{Charging: true, GridKW: 5, Window: win1}) // enter win1
	submitSync(t, a, command{kind: cmdBoundary, windowID: win1.ID})     // exit win1 (2 blips)

	// Advance to the next off-peak window (11:00) — a new occurrence, fresh budget.
	next := time.Date(2026, 1, 15, 11, 30, 0, 0, tokyo)
	clock.set(next)
	win2, ok := a.rates.ActiveWindow(next)
	if !ok || win2.ID == win1.ID {
		t.Fatalf("expected a distinct second window, got ok=%v id=%s", ok, win2.ID)
	}
	mustPlan(t, a, ChargePlan{Charging: true, GridKW: 5, Window: win2}) // enter win2 (blip 3)
	if got := fake.selectCalls(prioEntity); got != 3 {
		t.Fatalf("want 3 blips across two windows, got %d", got)
	}
}

// --- boundary timer ---

func TestBoundaryExitKeyedToWindow(t *testing.T) {
	a, fake, _ := newActuator(t, ModeLive, inWindow, "SBU")
	win, _ := a.rates.ActiveWindow(inWindow)
	mustPlan(t, a, ChargePlan{Charging: true, GridKW: 5, Window: win}) // enter

	// A boundary for a DIFFERENT window must not exit.
	submitSync(t, a, command{kind: cmdBoundary, windowID: "some-other-window"})
	if got := fake.selectCalls(prioEntity); got != 1 {
		t.Fatalf("mismatched boundary must not exit; want 1 blip, got %d", got)
	}
	// The matching boundary exits.
	submitSync(t, a, command{kind: cmdBoundary, windowID: win.ID})
	if got := fake.selectCalls(prioEntity); got != 2 {
		t.Fatalf("matching boundary must exit; want 2 blips, got %d", got)
	}
	if fake.lastSelect(prioEntity) != "SBU" {
		t.Fatal("boundary exit must set SBU")
	}
}

// --- watchdog ---

func TestWatchdogForcesSafeWhenStuckOutsideWindow(t *testing.T) {
	a, fake, clock := newActuator(t, ModeLive, inWindow, "SBU")
	// Move outside every off-peak window and pretend the inverter is stuck in UTI.
	clock.set(outWindow)
	fake.setState(prioEntity, "UTI")

	submitSync(t, a, command{kind: cmdWatchdog})
	if fake.lastSelect(prioEntity) != "SBU" {
		t.Fatal("watchdog must force SBU when UTI outside a window")
	}
}

func TestWatchdogFailSafeOnStaleState(t *testing.T) {
	a, fake, clock := newActuator(t, ModeLive, inWindow, "SBU")
	// Enter bypass inside the window.
	win, _ := a.rates.ActiveWindow(inWindow)
	mustPlan(t, a, ChargePlan{Charging: true, GridKW: 5, Window: win})
	before := fake.selectCalls(prioEntity)

	// Now outside the window, feed goes stale (priority unknown) while our episode
	// still believes we may be in bypass → fail-safe.
	clock.set(outWindow)
	fake.mu.Lock()
	fake.fresh[prioEntity] = false
	fake.states[prioEntity] = "unknown"
	fake.mu.Unlock()

	submitSync(t, a, command{kind: cmdWatchdog})
	if got := fake.selectCalls(prioEntity); got != before+1 || fake.lastSelect(prioEntity) != "SBU" {
		t.Fatalf("stale state outside window must fail-safe to SBU; blips %d→%d last=%s",
			before, got, fake.lastSelect(prioEntity))
	}
}

func TestWatchdogQuietWhenHealthyInWindow(t *testing.T) {
	a, fake, _ := newActuator(t, ModeLive, inWindow, "SBU")
	win, _ := a.rates.ActiveWindow(inWindow)
	mustPlan(t, a, ChargePlan{Charging: true, GridKW: 5, Window: win}) // legitimately in bypass
	before := fake.selectCalls(prioEntity)

	submitSync(t, a, command{kind: cmdWatchdog}) // in-window UTI is expected
	if got := fake.selectCalls(prioEntity); got != before {
		t.Fatalf("watchdog must not fire in-window; blips %d→%d", before, got)
	}
}

// --- Close safes the hardware ---

func TestCloseSafesHardware(t *testing.T) {
	a, fake, _ := newActuator(t, ModeLive, inWindow, "SBU")
	win, _ := a.rates.ActiveWindow(inWindow)
	mustPlan(t, a, ChargePlan{Charging: true, GridKW: 5, Window: win}) // in bypass

	a.Close()
	if fake.lastSelect(prioEntity) != "SBU" {
		t.Fatal("Close must force SBU when in bypass")
	}
}

// --- startup reconciliation ---

func TestReconcileAdoptsOpenBypassInWindow(t *testing.T) {
	a, fake, _ := newActuator(t, ModeLive, inWindow, "UTI") // already in bypass, in window
	// Adoption must NOT blip (the enter was already spent before we started).
	if got := fake.selectCalls(prioEntity); got != 0 {
		t.Fatalf("adopt must not blip, got %d select calls", got)
	}
	if !a.ep.inBypass || !a.ep.entered {
		t.Fatalf("expected adopted open episode, got %+v", a.ep)
	}
	// A boundary exit for the adopted window produces exactly one (exit) blip.
	win, _ := a.rates.ActiveWindow(inWindow)
	submitSync(t, a, command{kind: cmdBoundary, windowID: win.ID})
	if got := fake.selectCalls(prioEntity); got != 1 {
		t.Fatalf("want 1 exit blip after adopt, got %d", got)
	}
}

func TestReconcileSafesUTIOutsideWindow(t *testing.T) {
	_, fake, _ := newActuator(t, ModeLive, outWindow, "UTI") // UTI but peak → unsafe
	if fake.lastSelect(prioEntity) != "SBU" {
		t.Fatal("startup must fail-safe UTI-outside-window to SBU")
	}
}

func TestReconcilePreservesBudgetAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	cfg := testCfg(t)
	cfg.StateDir = dir
	rates := testRates(t)
	win, _ := rates.ActiveWindow(inWindow)

	// First actuator: enter then exit within the window (budget spent), then close.
	fake1 := newFakeHA()
	fake1.setState(prioEntity, "SBU")
	a1, err := New(cfg, config.Battery{NominalVoltageV: 52.8}, rates, fake1, ModeLive)
	if err != nil {
		t.Fatal(err)
	}
	a1.now = func() time.Time { return inWindow }
	fastSettle(a1)
	if err := a1.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	mustPlan(t, a1, ChargePlan{Charging: true, GridKW: 5, Window: win})
	submitSync(t, a1, command{kind: cmdBoundary, windowID: win.ID})
	a1.Close()

	// Second actuator restarts mid-window (priority now SBU) — the persisted budget
	// must forbid a fresh enter in the same window.
	fake2 := newFakeHA()
	fake2.setState(prioEntity, "SBU")
	a2, err := New(cfg, config.Battery{NominalVoltageV: 52.8}, rates, fake2, ModeLive)
	if err != nil {
		t.Fatal(err)
	}
	a2.now = func() time.Time { return inWindow }
	fastSettle(a2)
	if err := a2.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(a2.Close)
	mustPlan(t, a2, ChargePlan{Charging: true, GridKW: 5, Window: win})
	if got := fake2.selectCalls(prioEntity); got != 0 {
		t.Fatalf("restart must not re-enter same spent window; got %d blips", got)
	}
}

// --- mode gating ---

func TestObserveModeWritesNothing(t *testing.T) {
	a, fake, clock := newActuator(t, ModeObserve, inWindow, "SBU")
	win, _ := a.rates.ActiveWindow(inWindow)
	mustPlan(t, a, ChargePlan{Charging: true, GridKW: 5, Window: win})
	submitSync(t, a, command{kind: cmdBoundary, windowID: win.ID})

	// Even a watchdog trigger must not write in observe mode.
	clock.set(outWindow)
	fake.setState(prioEntity, "UTI")
	submitSync(t, a, command{kind: cmdWatchdog})

	if got := fake.totalCalls(); got != 0 {
		t.Fatalf("observe mode must issue zero writes, got %d", got)
	}
}

func TestWatchdogOnlyModeOnlySafes(t *testing.T) {
	a, fake, clock := newActuator(t, ModeWatchdogOnly, inWindow, "SBU")
	win, _ := a.rates.ActiveWindow(inWindow)

	// Policy must not actuate.
	mustPlan(t, a, ChargePlan{Charging: true, GridKW: 5, Window: win})
	if got := fake.totalCalls(); got != 0 {
		t.Fatalf("watchdog-only must not run policy writes, got %d", got)
	}
	// But the safing path may write.
	clock.set(outWindow)
	fake.setState(prioEntity, "UTI")
	submitSync(t, a, command{kind: cmdWatchdog})
	if fake.lastSelect(prioEntity) != "SBU" {
		t.Fatal("watchdog-only must still fail-safe to SBU")
	}
}

// --- kW → amps ---

func TestKwToAmps(t *testing.T) {
	fake := newFakeHA()
	a, err := New(testCfg(t), config.Battery{NominalVoltageV: 52.8}, testRates(t), fake, ModeLive)
	if err != nil {
		t.Fatal(err)
	}

	// Fresh live voltage is used.
	fake.setState(voltEntity, "50.0")
	fake.fresh[voltEntity] = true
	if got := a.kwToAmps(5.28); !approx(got, 5280/(50.0*2)) { // 52.8
		t.Fatalf("live-voltage amps: got %.3f", got)
	}

	// Stale voltage falls back to nominal (52.8).
	fake.fresh[voltEntity] = false
	if got := a.kwToAmps(5.28); !approx(got, 5280/(52.8*2)) { // 50.0
		t.Fatalf("nominal-fallback amps: got %.3f", got)
	}

	// Clamp to the ceiling.
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

func TestEnterWriteFailureEscalatesToSafe(t *testing.T) {
	a, fake, _ := newActuator(t, ModeLive, inWindow, "SBU")
	win, _ := a.rates.ActiveWindow(inWindow)
	fake.ackErr = errors.New("boom") // every write now fails ack

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := a.SetChargePlan(ctx, ChargePlan{Charging: true, GridKW: 5, Window: win}); err == nil {
		t.Fatal("expected enter to error when writes fail")
	}
	// The priority ack failed, so no blip occurred and the enter budget is NOT
	// spent: the episode must not believe it is in bypass, and no fail-safe blip is
	// issued on the (unconfirmed) enter.
	if a.ep.inBypass {
		t.Fatal("failed enter must not leave inBypass set")
	}
	if a.ep.entered {
		t.Fatal("failed enter (ack rejected) must not spend the enter budget")
	}
	if got := fake.selectCalls(prioEntity); got != 1 {
		t.Fatalf("failed enter must attempt exactly one (failed) UTI write, no safe blip; got %d", got)
	}
}

// --- C1: enter-blip spent on ACK, not read-back (the make-or-break invariant) ---

// TestC1EnterBlipSpentOnAckNotReadBack proves that when the output_priority
// service ack lands but the state echo is DELAYED beyond read_back_timeout (the
// SRNE select entity's state_changed lag), the actuator (a) counts the enter as
// spent — entered=true, inBypass=true — after exactly ONE output_priority write,
// and (b) does NOT re-enter or fail-safe on the next charging tick in the same
// window. The pre-fix code marked entered only after a successful read-back, so a
// timed-out echo produced a spurious safe blip + a re-enter next tick (unbounded).
func TestC1EnterBlipSpentOnAckNotReadBack(t *testing.T) {
	a, fake, _ := newActuator(t, ModeLive, inWindow, "SBU")
	win, _ := a.rates.ActiveWindow(inWindow)

	// Ack succeeds but the select entity never echoes its new UTI state → the
	// advisory read-back times out (150ms) even though the write physically landed.
	fake.mu.Lock()
	fake.decoupleSelect = true
	fake.mu.Unlock()

	mustPlan(t, a, ChargePlan{Charging: true, GridKW: 5, Window: win}) // enter
	if got := fake.selectCalls(prioEntity); got != 1 {
		t.Fatalf("enter must be exactly ONE output_priority write despite read-back timeout, got %d", got)
	}
	if !a.ep.entered || !a.ep.inBypass {
		t.Fatalf("ack acceptance must spend the enter-blip: entered=%v inBypass=%v", a.ep.entered, a.ep.inBypass)
	}

	// Next charging tick in the SAME window: no re-enter, no spurious safe → still
	// exactly one output_priority write total.
	mustPlan(t, a, ChargePlan{Charging: true, GridKW: 5, Window: win})
	if got := fake.selectCalls(prioEntity); got != 1 {
		t.Fatalf("no double-spend: same-window tick must issue ZERO further output_priority writes, got %d total", got)
	}
	// The last write was the UTI enter (never reversed to SBU).
	if last := fake.lastSelect(prioEntity); last != "UTI" {
		t.Fatalf("must remain in UTI (no reversing safe), last select was %q", last)
	}
}

// --- H2: stale-feed watchdog blind spot (no ep.inBypass gate) ---

// TestWatchdogFailSafeStalePhysicalUTIEpNotInBypass covers a stale feed outside
// any window while the hardware is physically UTI but ep.inBypass is false (a
// failed/ambiguous enter). The watchdog must still force SBU. Pre-fix, the
// `&& a.ep.inBypass` gate left it stranded.
func TestWatchdogFailSafeStalePhysicalUTIEpNotInBypass(t *testing.T) {
	a, fake, clock := newActuator(t, ModeLive, inWindow, "SBU")
	if a.ep.inBypass {
		t.Fatal("precondition: ep.inBypass must be false (never entered)")
	}
	// Outside every window; feed goes stale (priority unknown) — hardware may be UTI.
	clock.set(outWindow)
	fake.mu.Lock()
	fake.fresh[prioEntity] = false
	fake.states[prioEntity] = "unknown"
	fake.mu.Unlock()

	submitSync(t, a, command{kind: cmdWatchdog})
	if fake.lastSelect(prioEntity) != "SBU" {
		t.Fatalf("stale feed outside window with ep.inBypass=false must force SBU; last=%q",
			fake.lastSelect(prioEntity))
	}
}

// --- H3: panic recovery in the command path forces safe ---

// TestPanicInTransitionRecoveredAndSafes injects a panic mid-enter (the HA client
// panics on the first service call). The handler must recover, force a best-effort
// safe to SBU, return an error, and keep the goroutine alive for later commands.
func TestPanicInTransitionRecoveredAndSafes(t *testing.T) {
	a, fake, _ := newActuator(t, ModeLive, inWindow, "SBU")
	win, _ := a.rates.ActiveWindow(inWindow)

	fake.mu.Lock()
	fake.panicNextCall = true // the enter's UTI write panics
	fake.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := a.SetChargePlan(ctx, ChargePlan{Charging: true, GridKW: 5, Window: win}); err == nil {
		t.Fatal("a recovered panic must surface as an error to the caller")
	}
	// Recovery forced a safe: final priority is SBU and we are not in bypass.
	if fake.lastSelect(prioEntity) != "SBU" {
		t.Fatalf("panic recovery must force SBU; last select=%q", fake.lastSelect(prioEntity))
	}
	if a.ep.inBypass {
		t.Fatal("panic recovery safe must clear inBypass")
	}
	// The goroutine survived: a subsequent command is still served.
	submitSync(t, a, command{kind: cmdWatchdog})
}

// --- M5: atomic persist ---

// TestPersistAtomicAndCorruptColdStart verifies (a) a corrupt persisted file
// cold-starts cleanly (no crash, no lost distinction) and (b) persist leaves no
// torn temp files and writes valid JSON via temp+rename.
func TestPersistAtomicAndCorruptColdStart(t *testing.T) {
	dir := t.TempDir()
	cfg := testCfg(t)
	cfg.StateDir = dir
	path := filepath.Join(dir, "actuator_episode.json")

	// (a) A corrupt existing file must not crash load, and must cold-start clean.
	if err := os.WriteFile(path, []byte("{ this is not valid json"), 0o644); err != nil {
		t.Fatal(err)
	}
	fake := newFakeHA()
	fake.setState(prioEntity, "SBU")
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
	if a.ep.windowID != "" || a.ep.entered || a.ep.inBypass {
		t.Fatalf("corrupt persisted file must cold-start clean, got %+v", a.ep)
	}

	// (b) Enter (persists), then assert no leftover temp files and a valid file.
	win, _ := a.rates.ActiveWindow(inWindow)
	mustPlan(t, a, ChargePlan{Charging: true, GridKW: 5, Window: win})

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".actuator-episode-") {
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
	if !ps.Entered || !ps.InBypass {
		t.Fatalf("persisted budget after enter must record entered+inBypass, got %+v", ps)
	}
}

// --- M6: aggregate-kW ceiling + low-voltage guard ---

// TestKwToAmpsAggregateCeilingAndLowVoltage checks the aggregate-kW clamp applies
// BEFORE the kW→A conversion (so >8kW never reaches the per-unit ceiling) and that
// an implausibly-low fresh voltage falls back to nominal instead of inflating amps.
func TestKwToAmpsAggregateCeilingAndLowVoltage(t *testing.T) {
	fake := newFakeHA()
	cfg := testCfg(t)
	cfg.MaxChargeKW = 8
	a, err := New(cfg, config.Battery{NominalVoltageV: 52.8}, testRates(t), fake, ModeLive)
	if err != nil {
		t.Fatal(err)
	}

	// A 20 kW ask clamps to 8 kW first → 8000/(52.8*2) ≈ 75.8 A, WELL under the
	// per-unit 100 A ceiling (so we can tell the aggregate clamp bit, not the amps
	// clamp: without it, 20 kW would saturate to 100 A).
	fake.setState(voltEntity, "52.8")
	fake.fresh[voltEntity] = true
	got := a.kwToAmps(20)
	want := 8 * 1000.0 / (52.8 * 2)
	if !approx(got, want) {
		t.Fatalf("aggregate ceiling: got %.3f want %.3f (per-unit clamp would give %.1f)",
			got, want, a.cfg.MaxChargeCurrentA)
	}

	// A glitched-low fresh voltage (5 V) must use nominal (52.8), not inflate amps.
	fake.setState(voltEntity, "5.0")
	fake.fresh[voltEntity] = true
	got = a.kwToAmps(5)
	want = 5 * 1000.0 / (52.8 * 2)
	if !approx(got, want) {
		t.Fatalf("low-voltage guard: got %.3f want %.3f (5 V would give ~500 A → clamp 100)", got, want)
	}
}

// --- L7: never enter bypass to charge zero ---

// TestNoEnterForZeroGridCharge asserts that a charge plan whose grid share rounds
// to ~0 A (PV surplus covers it) does NOT spend an enter-blip.
func TestNoEnterForZeroGridCharge(t *testing.T) {
	a, fake, _ := newActuator(t, ModeLive, inWindow, "SBU")
	win, _ := a.rates.ActiveWindow(inWindow)

	mustPlan(t, a, ChargePlan{Charging: true, GridKW: 0, Window: win})
	if got := fake.selectCalls(prioEntity); got != 0 {
		t.Fatalf("zero grid-charge must not enter bypass; got %d blips", got)
	}
	if a.ep.inBypass || a.ep.entered {
		t.Fatalf("zero grid-charge must not mark entered/inBypass, got %+v", a.ep)
	}
}

// --- C2: space consecutive inverter writes (the live-trigger-test finding) ---

// TestEnterWaitsForUTIAppliedBeforeAmps proves the enter GATES the amps write on
// the output_priority having actually APPLIED (UTI visible in the state feed), not
// merely on the service ack. The SRNE controller drops a rapid-fire second write;
// the fix waits for the (slow) UTI echo before issuing amps, so the amps write is
// not dropped. Exactly ONE output_priority write (one blip) must occur.
func TestEnterWaitsForUTIAppliedBeforeAmps(t *testing.T) {
	a, fake, _ := newActuator(t, ModeLive, inWindow, "SBU")
	win, _ := a.rates.ActiveWindow(inWindow)

	// The SRNE echoes UTI only after a delay (slow state feed).
	fake.mu.Lock()
	fake.selectEchoDelay = 60 * time.Millisecond
	fake.mu.Unlock()

	mustPlan(t, a, ChargePlan{Charging: true, GridKW: 5, Window: win})

	if got := fake.selectCalls(prioEntity); got != 1 {
		t.Fatalf("enter must be exactly one output_priority blip, got %d", got)
	}
	// The amps write must have been issued only AFTER output_priority read UTI.
	if seen := fake.firstAmpsWritePrio(ampsEntity); seen != "UTI" {
		t.Fatalf("amps write must be gated on UTI-applied; output_priority read %q at amps-write time", seen)
	}
	if v := fake.StateFloat(ampsEntity); v <= 0 {
		t.Fatalf("amps must be written nonzero after UTI applied, got %v", v)
	}
	if !a.ep.entered || !a.ep.inBypass {
		t.Fatalf("enter must remain entered/inBypass, got %+v", a.ep)
	}
}

// TestDroppedAmpsRetriedAndLands proves a dropped mains-charge-current write is
// retried within the same tick and lands, with NO extra output_priority blip.
func TestDroppedAmpsRetriedAndLands(t *testing.T) {
	a, fake, _ := newActuator(t, ModeLive, inWindow, "SBU")
	win, _ := a.rates.ActiveWindow(inWindow)

	fake.mu.Lock()
	fake.dropFirstAmps = true // SRNE drops the first amps write
	fake.mu.Unlock()

	mustPlan(t, a, ChargePlan{Charging: true, GridKW: 5, Window: win})

	if v := fake.StateFloat(ampsEntity); v <= 0 {
		t.Fatalf("dropped amps write must be retried and land nonzero, got %v", v)
	}
	if got := fake.ampsWriteCount(ampsEntity); got < 2 {
		t.Fatalf("dropped amps write must be retried at least once, got %d amps writes", got)
	}
	if got := fake.selectCalls(prioEntity); got != 1 {
		t.Fatalf("amps retry must not add output_priority blips; want 1, got %d", got)
	}
}

// TestSettleTimeoutStillSendsAmpsNoExtraBlip proves that when UTI never echoes
// (settle wait times out — the slow-feed worst case), the actuator still sends the
// amps write (advisory), does NOT reverse the blip, does NOT fail-safe, and stays
// entered. This is the C1-safety guarantee applied to the new settle gate.
func TestSettleTimeoutStillSendsAmpsNoExtraBlip(t *testing.T) {
	a, fake, _ := newActuator(t, ModeLive, inWindow, "SBU")
	win, _ := a.rates.ActiveWindow(inWindow)

	// UTI never echoes → the settle poll times out.
	fake.mu.Lock()
	fake.decoupleSelect = true
	fake.mu.Unlock()

	mustPlan(t, a, ChargePlan{Charging: true, GridKW: 5, Window: win})

	if got := fake.selectCalls(prioEntity); got != 1 {
		t.Fatalf("settle timeout must not add a blip; want 1 output_priority write, got %d", got)
	}
	if last := fake.lastSelect(prioEntity); last != "UTI" {
		t.Fatalf("settle timeout must not reverse to SBU; last select=%q", last)
	}
	if !a.ep.entered || !a.ep.inBypass {
		t.Fatalf("settle timeout must keep entered/inBypass; got %+v", a.ep)
	}
	if v := fake.StateFloat(ampsEntity); v <= 0 {
		t.Fatalf("settle timeout must still send amps (advisory), got %v", v)
	}
}

// TestExitSpacedDroppedSBUCaughtByWatchdog proves the spaced exit is still exactly
// one exit blip (2 per window total), and that a DROPPED SBU exit (echo never
// lands, hardware stays UTI outside the window) is re-safed by the watchdog.
func TestExitSpacedDroppedSBUCaughtByWatchdog(t *testing.T) {
	a, fake, clock := newActuator(t, ModeLive, inWindow, "SBU")
	win, _ := a.rates.ActiveWindow(inWindow)

	mustPlan(t, a, ChargePlan{Charging: true, GridKW: 5, Window: win}) // enter (blip 1)

	// Model the SRNE dropping the SBU exit write: select_option(SBU) is issued
	// (blip 2) but the state never echoes SBU (stays UTI).
	fake.mu.Lock()
	fake.decoupleSelect = true
	fake.mu.Unlock()
	submitSync(t, a, command{kind: cmdBoundary, windowID: win.ID}) // exit (blip 2)

	if got := fake.selectCalls(prioEntity); got != 2 {
		t.Fatalf("enter+exit must be exactly 2 blips per window, got %d", got)
	}
	if last := fake.lastSelect(prioEntity); last != "SBU" {
		t.Fatalf("exit must write SBU, got %q", last)
	}

	// Physically still UTI outside the window (dropped SBU) → watchdog re-safes.
	clock.set(outWindow)
	fake.mu.Lock()
	fake.decoupleSelect = false // allow the re-safe to take effect
	fake.states[prioEntity] = "UTI"
	fake.mu.Unlock()
	submitSync(t, a, command{kind: cmdWatchdog})
	if fake.lastSelect(prioEntity) != "SBU" {
		t.Fatalf("watchdog must re-safe a dropped SBU exit; last=%q", fake.lastSelect(prioEntity))
	}
}
