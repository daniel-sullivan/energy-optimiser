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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	mu         sync.Mutex
	states     map[string]string
	attrs      map[string]map[string]any
	generation map[string]uint64
	fresh      map[string]bool // per-entity liveness; absent ⇒ freshOK
	freshOK    bool            // default when entity absent from `fresh`
	connected  bool            // raw connection state (feedLive fallback when no liveness entities)
	ackErr     error           // if set, CallServiceAck returns it (no reflect)
	calls      []recordedCall

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
		states:     map[string]string{},
		attrs:      map[string]map[string]any{},
		generation: map[string]uint64{},
		fresh:      map[string]bool{},
		freshOK:    true,
		connected:  true,
		dropOnce:   map[string]int{},
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
	f.generation[entity]++
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

func (f *fakeHA) UpdateGeneration(entityID string) uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.generation[entityID]
}

func (f *fakeHA) Fresh(entityID string, _ time.Duration) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if v, ok := f.fresh[entityID]; ok {
		return v
	}
	return f.freshOK
}

func (f *fakeHA) Connected() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.connected
}

func (f *fakeHA) setState(entityID, state string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.states[entityID] = state
	f.generation[entityID]++
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
	switchEntity   = "switch.timed_charge"
	ampsEntity     = "number.mains_a"
	livenessEntity = "sensor.srne_battery_power" // fast-moving feed-liveness probe
	w1s          = "text.w1_start"
	w1e          = "text.w1_end"
	w2s          = "text.w2_start"
	w2e          = "text.w2_end"
	w3s          = "text.w3_start"
	w3e          = "text.w3_end"
)

func testCfg(t *testing.T) config.ActuatorHW {
	t.Helper()
	return config.ActuatorHW{
		TimedChargeSwitch:        switchEntity,
		MainsChargeCurrentNumber: ampsEntity,
		ChargeWindows: []config.ChargeWindowEntities{
			{Start: w1s, End: w1e},
			{Start: w2s, End: w2e},
			{Start: w3s, End: w3e},
		},
		NumUnits:          2,
		MaxChargeCurrentA: 50,
		ACChargeVoltageV:  103,
		StateDir:          t.TempDir(),
		WindowInset:       config.Duration{Duration: 5 * time.Minute},
		WatchdogInterval:  config.Duration{Duration: time.Hour}, // won't fire mid-test
		WriteTimeout:      config.Duration{Duration: time.Second},
		ReadBackTimeout:   config.Duration{Duration: 150 * time.Millisecond},
		StateStale:        config.Duration{Duration: 5 * time.Minute},
		LivenessEntities:  []string{livenessEntity},
	}
}

// seedSafeState pre-populates every hardware rail slot and the current entity
// with the fail-closed steady state. Slots 1 and 2 mirror inset off-peak windows;
// surplus slot 3 is explicitly disabled with equal non-midnight bounds.
func seedSafeState(f *fakeHA) {
	f.setState(ampsEntity, "0")
	f.setState(w1s, "01:05")
	f.setState(w1e, "04:55")
	f.setState(w2s, "11:05")
	f.setState(w2e, "12:55")
	f.setState(w3s, disabledWindow)
	f.setState(w3e, disabledWindow)
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
	a, err := New(testCfg(t), testRates(t), fake, mode)
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
	seedSafeState(fake)
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
	a, err := New(cfg, rates, fake, ModeLive)
	if err != nil {
		t.Fatal(err)
	}
	a.now = func() time.Time { return inWindow }
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

// TestReconcileMirrorsOffPeakWindows proves reconcile mirrors configured inset
// off-peak periods, explicitly disables every surplus slot, and retries a dropped
// write before reporting the rail safe.
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
	if fake.State(w3s) != disabledWindow || fake.State(w3e) != disabledWindow {
		t.Fatalf("surplus slot 3 must be disabled, got %q-%q", fake.State(w3s), fake.State(w3e))
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

// TestSurplusSlotDisabledBeforeEnable proves a charge start disables every surplus
// slot (during reconcile) before enabling timed charge.
func TestSurplusSlotDisabledBeforeEnable(t *testing.T) {
	fake := newFakeHA()
	fake.setState(ampsEntity, "0")
	fake.setState(switchEntity, switchOff)
	fake.setState(w1s, "01:05")
	fake.setState(w1e, "04:55")
	fake.setState(w2s, "11:05")
	fake.setState(w2e, "12:55")
	a, _ := startWith(t, fake, ModeLive, inWindow)
	before := fake.totalCalls()

	mustPlan(t, a, ChargePlan{Charging: true, GridKW: 5})

	enableIdx := fake.lastIndexOf(isSwitch("turn_on"))
	for _, entity := range []string{w3s, w3e} {
		idx := fake.lastIndexOf(func(c recordedCall) bool {
			return c.service == "set_value" && c.entity == entity && c.value == disabledWindow
		})
		if idx < 0 || idx >= enableIdx || idx >= before {
			assert.Failf(t, "assertion failed", "surplus rail %s must be disabled during reconcile before enable; index=%d enable=%d startup calls=%d", entity, idx, enableIdx, before)
		}
	}
}

func TestUnsafeRailBlocksTurnOn(t *testing.T) {
	fake := newFakeHA()
	seedSafeState(fake)
	fake.setState(switchEntity, switchOff)
	fake.setState(w1s, "09:00")
	fake.mu.Lock()
	fake.dropOnce[w1s] = 1000
	fake.mu.Unlock()
	a := rawActuator(t, fake, testRates(t), testCfg(t))
	a.now = func() time.Time { return inWindow }

	if err := a.handlePlan(ChargePlan{Charging: true, GridKW: 5}); err == nil {
		assert.Fail(t, "an unconfirmed unsafe rail repair must return an error")
	}
	if fake.countService("turn_on") != 0 || fake.State(switchEntity) != switchOff {
		assert.Fail(t, "an unsafe rail must block timed-charge enablement")
	}
}

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
	if !a.st.charging || a.ChargingObserved() != ChargeOn {
		t.Fatal("confirmed enable must report charging")
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
	if a.st.charging || a.ChargingObserved() == ChargeOn {
		t.Fatal("confirmed stop must clear the charging state")
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

// TestLiveFeedIdempotentNoOpConfirmsWithoutWrite is the outage regression: a value
// already at target on a live feed confirms as a no-op even for a quiescent entity
// that emits no further state_changed event. dropOnce would make any issued write
// unconfirmable, so a pass proves no write was issued — the pre-f8d59ba behaviour
// the per-entity freshness gate had broken (every idempotent write ran to a 45s
// timeout, wedging actuation).
func TestLiveFeedIdempotentNoOpConfirmsWithoutWrite(t *testing.T) {
	fake := newFakeHA()
	fake.setState(switchEntity, switchOff)
	fake.setState(ampsEntity, "10")
	fake.mu.Lock()
	fake.dropOnce[ampsEntity] = 1000
	fake.mu.Unlock()
	a := rawActuator(t, fake, testRates(t), testCfg(t))

	require.NoError(t, a.setCurrent(10), "live-feed idempotent target must confirm as a no-op")
	if got := fake.countSet(ampsEntity); got != 0 {
		assert.Failf(t, "assertion failed", "live-feed idempotent target must skip the write, got %d writes", got)
	}
}

// TestDroppedChangeNeverFalselyConfirms proves the dropped-write guard still holds
// where it matters: a real change (cache != target) whose writes the SRNE keeps
// dropping never confirms — it re-issues and finally errors rather than trusting
// an unlanded write.
func TestDroppedChangeNeverFalselyConfirms(t *testing.T) {
	fake := newFakeHA()
	fake.setState(switchEntity, switchOff)
	fake.setState(ampsEntity, "0")
	fake.mu.Lock()
	fake.dropOnce[ampsEntity] = 1000 // every write acked but never reflected
	fake.mu.Unlock()
	a := rawActuator(t, fake, testRates(t), testCfg(t))

	if err := a.setCurrent(10); err == nil {
		assert.Fail(t, "a change whose writes are all dropped must not confirm")
	}
	if got := fake.countSet(ampsEntity); got != maxWriteRetries+1 {
		assert.Failf(t, "assertion failed", "a dropped change must exhaust retries, got %d writes", got)
	}
}

// TestObserveChargingTrustsLiveFeed proves observeCharging trusts the last-known
// switch state while the feed is live — a quiescent ON switch reads ChargeOn, not
// the ChargeUnknown misread that made the watchdog stop every long charge — while
// a frozen feed (or disconnect) falls back to Unknown so the watchdog fails closed
// even though the cache still reads a concrete value.
func TestObserveChargingTrustsLiveFeed(t *testing.T) {
	fake := newFakeHA()
	fake.setState(switchEntity, switchOn)
	a := rawActuator(t, fake, testRates(t), testCfg(t))

	if got := a.ChargingObserved(); got != ChargeOn {
		assert.Failf(t, "assertion failed", "live-feed ON switch must read ChargeOn, got %v", got)
	}
	fake.mu.Lock()
	fake.fresh[livenessEntity] = false // feed frozen: cache still reads "on" but is no longer trustworthy
	fake.mu.Unlock()
	if got := a.ChargingObserved(); got != ChargeUnknown {
		assert.Failf(t, "assertion failed", "a frozen feed must read ChargeUnknown, got %v", got)
	}
}

// TestFeedLiveFallsBackToConnectedWithoutLivenessEntities proves a misconfig with
// no liveness sensors degrades to the raw connection state rather than wedging
// every gate false forever: observeCharging trusts a connected switch and goes
// Unknown only on disconnect.
func TestFeedLiveFallsBackToConnectedWithoutLivenessEntities(t *testing.T) {
	fake := newFakeHA()
	fake.setState(switchEntity, switchOn)
	cfg := testCfg(t)
	cfg.LivenessEntities = nil
	a := rawActuator(t, fake, testRates(t), cfg)

	if got := a.ChargingObserved(); got != ChargeOn {
		assert.Failf(t, "assertion failed", "no liveness entities + connected ON switch must read ChargeOn, got %v", got)
	}
	fake.mu.Lock()
	fake.connected = false
	fake.mu.Unlock()
	if got := a.ChargingObserved(); got != ChargeUnknown {
		assert.Failf(t, "assertion failed", "no liveness entities + disconnected must read ChargeUnknown, got %v", got)
	}
}

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
	if a.st.charging || a.ChargingObserved() == ChargeOn {
		t.Fatal("unconfirmed enable must not report charging (so the next tick retries)")
	}
	// Current was still set (safe — switch is off, nothing charges).
	if v := fake.StateFloat(ampsEntity); v <= 0 {
		t.Fatal("current must still be set even when the enable fails")
	}
}

func TestUnconfirmedDisableRetainsConfirmedState(t *testing.T) {
	a, fake, _ := newActuator(t, ModeLive, inWindow, switchOff)
	mustPlan(t, a, ChargePlan{Charging: true, GridKW: 5})
	fake.mu.Lock()
	fake.dropOnce[switchEntity] = 1000
	fake.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := a.SetChargePlan(ctx, ChargePlan{}); err == nil {
		assert.Fail(t, "an unconfirmed disable must surface an error")
	}
	if a.ChargingObserved() != ChargeOn {
		assert.Fail(t, "unconfirmed disable must retain confirmed state until timed charge is confirmed off")
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
	// Frozen feed: the switch cache still reads a concrete "on", but the feed has
	// stopped delivering so ownership can't be confirmed — the watchdog must fail
	// closed and disable rather than trust the stale reading.
	fake.mu.Lock()
	fake.states[switchEntity] = switchOn
	fake.fresh[livenessEntity] = false
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

func TestWatchdogLeavesActiveInWindowChargeUntouched(t *testing.T) {
	a, fake, _ := newActuator(t, ModeLive, inWindow, switchOff)
	mustPlan(t, a, ChargePlan{
		Charging:   true,
		GridKW:     5,
		CurrentSOC: 0.4,
		TargetSOC:  0.7,
		BlockEnd:   blockEnd(4, 55),
	})
	before := fake.totalCalls()
	commit := a.commit
	state := a.st

	submitSync(t, a, command{kind: cmdWatchdog})

	if got := fake.totalCalls(); got != before {
		assert.Failf(t, "assertion failed", "in-window watchdog must issue no switch, current, or window writes; writes %d→%d", before, got)
	}
	if a.commit != commit {
		assert.Failf(t, "assertion failed", "in-window watchdog must preserve charge commitment: got %+v want %+v", a.commit, commit)
	}
	if a.st != state || fake.State(switchEntity) != switchOn {
		assert.Failf(t, "assertion failed", "in-window watchdog must preserve active charge state: got %+v switch=%q want %+v/on", a.st, fake.State(switchEntity), state)
	}
}

func TestWatchdogSafesUnownedChargeInsideWindow(t *testing.T) {
	a, fake, _ := newActuator(t, ModeLive, inWindow, switchOff)
	fake.setState(switchEntity, switchOn)
	fake.setState(ampsEntity, "20")

	submitSync(t, a, command{kind: cmdWatchdog})

	if fake.State(switchEntity) != switchOff || fake.StateFloat(ampsEntity) != 0 || a.st != (chargeState{}) {
		assert.Failf(t, "assertion failed", "in-window watchdog must safe an unowned charge: state=%+v switch=%q current=%v", a.st, fake.State(switchEntity), fake.StateFloat(ampsEntity))
	}
}

func TestWatchdogSafesUnknownChargeInsideWindow(t *testing.T) {
	a, fake, _ := newActuator(t, ModeLive, inWindow, switchOff)
	fake.mu.Lock()
	fake.states[switchEntity] = "unknown"
	fake.mu.Unlock()

	submitSync(t, a, command{kind: cmdWatchdog})

	if fake.State(switchEntity) != switchOff || fake.StateFloat(ampsEntity) != 0 {
		assert.Failf(t, "assertion failed", "in-window watchdog must fail closed when ownership cannot be confirmed: switch=%q current=%v", fake.State(switchEntity), fake.StateFloat(ampsEntity))
	}
}

func TestWatchdogDoesNotRepairRailInsideWindow(t *testing.T) {
	a, fake, _ := newActuator(t, ModeLive, inWindow, switchOff)
	fake.setState(w1s, "09:00")
	before := fake.totalCalls()

	submitSync(t, a, command{kind: cmdWatchdog})

	if got := fake.totalCalls(); got != before {
		assert.Failf(t, "assertion failed", "in-window watchdog must not write while policy may own an active charge; writes %d→%d", before, got)
	}
	if got := fake.State(w1s); got != "09:00" {
		assert.Failf(t, "assertion failed", "in-window watchdog must leave rail repair to the policy path, got %q", got)
	}
}

// --- Close disables timed charge ---

func TestCloseDisablesTimedCharge(t *testing.T) {
	a, fake, _ := newActuator(t, ModeLive, inWindow, switchOff)
	mustPlan(t, a, ChargePlan{Charging: true, GridKW: 5}) // charging

	a.Close()
	if a.ChargingObserved() == ChargeOn {
		t.Fatal("Close must clear confirmed charging after the disable")
	}
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
	a, fake, _ := newActuator(t, ModeLive, inWindow, switchOn)
	if fake.State(switchEntity) != switchOff {
		t.Fatal("startup reconcile must disable a timed charge found enabled")
	}
	if a.ChargingObserved() == ChargeOn {
		t.Fatal("confirmed charge state must clear after startup disable confirms")
	}
	if got := fake.countService("turn_off"); got < 1 {
		t.Fatalf("reconcile must issue a disable when found on, got %d turn_off", got)
	}
}

func TestReconcileObserveModeDoesNotWrite(t *testing.T) {
	a, fake, _ := newActuator(t, ModeObserve, inWindow, switchOn)
	if got := fake.totalCalls(); got != 0 {
		t.Fatalf("observe-mode reconcile must not write even if found on, got %d", got)
	}
	if a.ChargingObserved() != ChargeOn {
		t.Fatal("observe mode must report the fresh observed-on state")
	}
}

func TestReconcileUnknownStartupSafesInLiveButRemainsUnknownInObserve(t *testing.T) {
	liveFake := newFakeHA()
	seedSafeState(liveFake)
	liveFake.setState(switchEntity, "unavailable")
	live, _ := startWith(t, liveFake, ModeLive, inWindow)
	if liveFake.countService("turn_off") == 0 || live.ChargingObserved() != ChargeOff {
		assert.Fail(t, "live startup must safe unknown state and require fresh observed off")
	}

	observeFake := newFakeHA()
	seedSafeState(observeFake)
	observeFake.setState(switchEntity, "unavailable")
	observe, _ := startWith(t, observeFake, ModeObserve, inWindow)
	if observeFake.totalCalls() != 0 || observe.ChargingObserved() != ChargeUnknown {
		assert.Fail(t, "observe startup cannot safe and must preserve unknown")
	}
}

func TestExternalOffCancelsCommitmentAndZerosCurrent(t *testing.T) {
	a, fake, _ := newActuator(t, ModeLive, inWindow, switchOff)
	plan := ChargePlan{Charging: true, GridKW: 5, CurrentSOC: 0.4, TargetSOC: 0.7, BlockEnd: blockEnd(4, 55)}
	mustPlan(t, a, plan)
	if !a.commit.active || fake.StateFloat(ampsEntity) == 0 {
		assert.Fail(t, "precondition: committed charge with nonzero current")
	}

	fake.setState(switchEntity, switchOff)
	onBefore := fake.countService("turn_on")
	mustPlan(t, a, plan)
	if a.commit.active {
		assert.Fail(t, "fresh external off must cancel the stale commitment")
	}
	if fake.State(switchEntity) != switchOff || fake.StateFloat(ampsEntity) != 0 {
		assert.Fail(t, "external off safety stop must keep the switch off and zero current")
	}
	if fake.countService("turn_on") != onBefore {
		assert.Fail(t, "external off must not auto-reenable from the stale commitment")
	}
}

func TestExternalOnWithOffPlanDisables(t *testing.T) {
	a, fake, _ := newActuator(t, ModeLive, inWindow, switchOff)
	fake.setState(switchEntity, switchOn)
	fake.setState(ampsEntity, "20")
	beforeOff := fake.countService("turn_off")
	mustPlan(t, a, ChargePlan{})
	if fake.countService("turn_off") <= beforeOff || a.ChargingObserved() != ChargeOff {
		assert.Fail(t, "fresh observed external on with an off plan must disable")
	}
	if fake.StateFloat(ampsEntity) != 0 {
		assert.Fail(t, "external-on stop must zero current")
	}
}

func TestStopObservedOffStillZerosNonzeroCurrent(t *testing.T) {
	a, fake, _ := newActuator(t, ModeLive, inWindow, switchOff)
	fake.setState(ampsEntity, "12")
	mustPlan(t, a, ChargePlan{})
	if fake.StateFloat(ampsEntity) != 0 || fake.countSet(ampsEntity) == 0 {
		assert.Fail(t, "an observed-off stop must still zero residual nonzero current")
	}
	if fake.countService("turn_off") != 0 {
		assert.Fail(t, "an already-observed-off stop need not rewrite the switch")
	}
}

// --- kW → amps ---

func TestKwToAmps(t *testing.T) {
	fake := newFakeHA()
	a, err := New(testCfg(t), testRates(t), fake, ModeLive)
	if err != nil {
		t.Fatal(err)
	}

	// Conversion uses the AC line voltage (per-unit = kW*1000/(V_ac*numUnits),
	// V_ac=103, numUnits=2), NOT the DC pack voltage.
	if got := a.kwToAmps(5.15); !approx(got, 5150/(103.0*2)) {
		t.Fatalf("ac-voltage amps: got %.3f", got)
	}

	// Clamp to the per-unit ceiling.
	if got := a.kwToAmps(1000); got != a.cfg.MaxChargeCurrentA {
		t.Fatalf("clamp: got %.3f want %.1f", got, a.cfg.MaxChargeCurrentA)
	}

	// Zero/negative → zero.
	if got := a.kwToAmps(0); got != 0 {
		t.Fatalf("zero kW must be 0 A, got %.3f", got)
	}

	// Fallback when ACChargeVoltageV is unset → 103 V.
	cfg := testCfg(t)
	cfg.ACChargeVoltageV = 0
	b, err := New(cfg, testRates(t), fake, ModeLive)
	if err != nil {
		t.Fatal(err)
	}
	if got := b.kwToAmps(5.15); !approx(got, 5150/(103.0*2)) {
		t.Fatalf("voltage fallback: got %.3f", got)
	}
}

func approx(a, b float64) bool {
	d := a - b
	if d < 0 {
		d = -d
	}
	return d < 1e-6
}

// TestKwToAmpsAggregateCeiling checks the aggregate-kW clamp (MaxChargeKW) applies
// BEFORE the kW→A conversion, so a high per-unit amp headroom can never command
// more than the pack can accept.
func TestKwToAmpsAggregateCeiling(t *testing.T) {
	fake := newFakeHA()
	cfg := testCfg(t)
	cfg.MaxChargeKW = 8
	a, err := New(cfg, testRates(t), fake, ModeLive)
	if err != nil {
		t.Fatal(err)
	}

	got := a.kwToAmps(20) // clamps to 8 kW first → 8000/(103*2) ≈ 38.8 A, under the 50 A per-unit ceiling
	want := 8 * 1000.0 / (103.0 * 2)
	if !approx(got, want) {
		t.Fatalf("aggregate ceiling: got %.3f want %.3f", got, want)
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

// TestShortWindowFailsClosed proves an unrepresentable off-peak interval returns
// an error and cannot lead to timed-charge enablement.
func TestShortWindowFailsClosed(t *testing.T) {
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
	seedSafeState(fake)
	fake.setState(switchEntity, switchOff)
	a := rawActuator(t, fake, parseRates(t, shortTOML), testCfg(t))
	a.now = func() time.Time { return time.Date(2026, 1, 15, 2, 4, 0, 0, tokyo) }
	if err := a.handlePlan(ChargePlan{Charging: true, GridKW: 5}); err == nil {
		assert.Fail(t, "an unrepresentable short window must fail closed")
	}
	if fake.countService("turn_on") != 0 || fake.State(switchEntity) != switchOff {
		assert.Fail(t, "an unsafe short window must prevent timed-charge enablement")
	}
}

// TestMidnightBoundFailsClosed proves a mirrored 00:00 bound returns an error and
// cannot lead to timed-charge enablement.
func TestMidnightBoundFailsClosed(t *testing.T) {
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
	seedSafeState(fake)
	fake.setState(switchEntity, switchOff)
	a := rawActuator(t, fake, parseRates(t, midnightTOML), testCfg(t))
	a.now = func() time.Time { return time.Date(2026, 1, 15, 1, 0, 0, 0, tokyo) }
	if err := a.handlePlan(ChargePlan{Charging: true, GridKW: 5}); err == nil {
		assert.Fail(t, "a midnight-bound window must fail closed")
	}
	if fake.countService("turn_on") != 0 || fake.State(switchEntity) != switchOff {
		assert.Fail(t, "a midnight-bound window must prevent timed-charge enablement")
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

func TestPanicInStartupReconcileRecoveredAndSafes(t *testing.T) {
	fake := newFakeHA()
	seedSafeState(fake)
	fake.setState(switchEntity, switchOn)
	fake.mu.Lock()
	fake.panicNextCall = true
	fake.mu.Unlock()
	a, err := New(testCfg(t), testRates(t), fake, ModeLive)
	require.NoError(t, err)
	a.now = func() time.Time { return inWindow }
	fastSettle(a)
	if err := a.Start(context.Background()); err == nil {
		assert.Fail(t, "a recovered startup reconcile panic must surface as an error")
	}
	t.Cleanup(a.Close)
	if fake.State(switchEntity) != switchOff {
		assert.Failf(t, "assertion failed", "startup panic recovery must best-effort disable timed charge; got %q", fake.State(switchEntity))
	}
}

func TestPanicInShutdownSafingIsContained(t *testing.T) {
	a, fake, _ := newActuator(t, ModeLive, inWindow, switchOff)
	mustPlan(t, a, ChargePlan{Charging: true, GridKW: 5})
	fake.mu.Lock()
	fake.panicNextCall = true
	fake.mu.Unlock()

	done := make(chan struct{})
	go func() {
		a.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		assert.Fail(t, "a panic during shutdown safing must be contained so Close returns")
	}
	if a.st != (chargeState{}) || a.commit != (chargeCommitment{}) {
		assert.Failf(t, "assertion failed", "shutdown panic recovery must clear in-memory charge intent: state=%+v commitment=%+v", a.st, a.commit)
	}
	if fake.State(switchEntity) != switchOff || fake.StateFloat(ampsEntity) != 0 {
		assert.Failf(t, "assertion failed", "shutdown panic recovery must retry the physical safe state; switch=%q current=%v", fake.State(switchEntity), fake.StateFloat(ampsEntity))
	}
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
	seedSafeState(fake)
	fake.setState(switchEntity, switchOff)
	a, err := New(cfg, testRates(t), fake, ModeLive)
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

// --- charge-block commitment (anti-flap) ---

// blockEnd is a helper: a plan block ending at HH:MM Tokyo on the fixture day.
func blockEnd(h, m int) time.Time { return time.Date(2026, 1, 15, h, m, 0, 0, tokyo) }

// TestCommitmentRidesOutSolverFlapping is the headline regression for the 07-24
// blip storm: once a block starts, the solver's per-tick Charging true/false
// hunting must NOT toggle the timed-charge contactor. One turn_on, zero turn_off
// while the block's SoC goal is unmet and its window/end have not passed.
func TestCommitmentRidesOutSolverFlapping(t *testing.T) {
	a, fake, _ := newActuator(t, ModeLive, inWindow, switchOff)

	// Start a block: charge to 60% by 04:55, currently 49%.
	mustPlan(t, a, ChargePlan{Charging: true, GridKW: 8, CurrentSOC: 0.49, TargetSOC: 0.60, BlockEnd: blockEnd(4, 55)})
	if !a.st.charging {
		t.Fatal("precondition: must be charging")
	}
	if !a.commit.active {
		t.Fatal("precondition: block must be committed")
	}

	// Replay solver hunting: Charging flaps off/on repeatedly while SoC stays
	// below target and time stays before the block end and inside the window.
	for range 8 {
		mustPlan(t, a, ChargePlan{Charging: false, CurrentSOC: 0.50, TargetSOC: 0.60, BlockEnd: blockEnd(4, 55)})
		mustPlan(t, a, ChargePlan{Charging: true, GridKW: 8, CurrentSOC: 0.51, TargetSOC: 0.60, BlockEnd: blockEnd(4, 55)})
	}

	if got := fake.countService("turn_off"); got != 0 {
		t.Errorf("commitment must suppress flapping: got %d turn_off, want 0", got)
	}
	if got := fake.countService("turn_on"); got != 1 {
		t.Errorf("commitment must keep one contiguous charge: got %d turn_on, want 1", got)
	}
	if !a.st.charging || a.ChargingObserved() != ChargeOn {
		t.Error("confirmed physical state must remain charging through solver flapping")
	}
}

// TestCommitmentExitsOnSocTarget: the clean, single exit — measured SoC reaching
// the block's goal stops the charge exactly once.
func TestCommitmentExitsOnSocTarget(t *testing.T) {
	a, fake, _ := newActuator(t, ModeLive, inWindow, switchOff)
	mustPlan(t, a, ChargePlan{Charging: true, GridKW: 8, CurrentSOC: 0.49, TargetSOC: 0.60, BlockEnd: blockEnd(4, 55)})

	// SoC reaches the goal — even though the plan still "wants" charge, the block is done.
	mustPlan(t, a, ChargePlan{Charging: true, GridKW: 8, CurrentSOC: 0.60, TargetSOC: 0.60, BlockEnd: blockEnd(4, 55)})

	if fake.countService("turn_off") != 1 {
		t.Errorf("SoC-target must stop once: got %d turn_off", fake.countService("turn_off"))
	}
	if a.st.charging || a.commit.active {
		t.Error("block must be cleared after SoC-target exit")
	}
	if fake.State(switchEntity) != switchOff || fake.StateFloat(ampsEntity) != 0 {
		t.Error("stop must leave timed charge off and current zero")
	}
}

// TestCommitmentExitsOnWindowEnd: leaving the off-peak window ends the block even
// if SoC is still below target (the tariff rail wins).
func TestCommitmentExitsOnWindowEnd(t *testing.T) {
	a, fake, clock := newActuator(t, ModeLive, inWindow, switchOff)
	mustPlan(t, a, ChargePlan{Charging: true, GridKW: 8, CurrentSOC: 0.49, TargetSOC: 0.90, BlockEnd: blockEnd(4, 55)})

	clock.set(outWindow) // 08:00 — peak
	mustPlan(t, a, ChargePlan{Charging: true, GridKW: 8, CurrentSOC: 0.55, TargetSOC: 0.90, BlockEnd: blockEnd(4, 55)})

	if fake.countService("turn_off") != 1 {
		t.Errorf("window end must stop the block: got %d turn_off", fake.countService("turn_off"))
	}
	if a.commit.active {
		t.Error("commitment must clear at window end")
	}
}

// TestCommitmentExitsOnBlockEndTime: past the planned block end, stop even if SoC
// is below target and still inside the window (time backstop for a slow rise).
func TestCommitmentExitsOnBlockEndTime(t *testing.T) {
	a, fake, clock := newActuator(t, ModeLive, inWindow, switchOff)
	mustPlan(t, a, ChargePlan{Charging: true, GridKW: 8, CurrentSOC: 0.49, TargetSOC: 0.90, BlockEnd: blockEnd(2, 30)})

	clock.set(time.Date(2026, 1, 15, 2, 35, 0, 0, tokyo)) // past block end, still off-peak
	mustPlan(t, a, ChargePlan{Charging: false, CurrentSOC: 0.55, TargetSOC: 0.90, BlockEnd: blockEnd(2, 30)})

	if fake.countService("turn_off") != 1 {
		t.Errorf("block-end time must stop the block: got %d turn_off", fake.countService("turn_off"))
	}
	if a.commit.active {
		t.Error("commitment must clear at block-end time")
	}
}

// TestCommitmentBlockEndExtendsNeverShrinks: a re-plan wanting to charge LATER
// pushes the time backstop out; SoC still below target must keep charging past the
// original planned end.
func TestCommitmentBlockEndExtendsNeverShrinks(t *testing.T) {
	a, fake, clock := newActuator(t, ModeLive, inWindow, switchOff)
	mustPlan(t, a, ChargePlan{Charging: true, GridKW: 8, CurrentSOC: 0.49, TargetSOC: 0.90, BlockEnd: blockEnd(3, 0)})

	// Fresh solve pushes the end out 03:00→04:00 (SoC still well below target).
	mustPlan(t, a, ChargePlan{Charging: true, GridKW: 8, CurrentSOC: 0.55, TargetSOC: 0.90, BlockEnd: blockEnd(4, 0)})
	if !a.commit.blockEnd.Equal(blockEnd(4, 0)) {
		t.Errorf("block end must extend to 04:00, got %v", a.commit.blockEnd)
	}

	// At 03:30 — past the OLD end, before the new one — it must still be charging.
	clock.set(time.Date(2026, 1, 15, 3, 30, 0, 0, tokyo))
	mustPlan(t, a, ChargePlan{Charging: true, GridKW: 8, CurrentSOC: 0.60, TargetSOC: 0.90, BlockEnd: blockEnd(4, 0)})
	if fake.countService("turn_off") != 0 {
		t.Errorf("extended end must not stop at the old end: got %d turn_off", fake.countService("turn_off"))
	}
	if !a.st.charging {
		t.Error("must still be charging past the original block end")
	}
}

// TestCommitmentTargetNotRatcheted: a higher plan target must NOT ratchet the block
// goal up (forecast noise must not silently over-charge). The block exits at the
// LATCH target; a genuinely higher goal re-latches a fresh, visible block.
func TestCommitmentTargetNotRatcheted(t *testing.T) {
	a, fake, _ := newActuator(t, ModeLive, inWindow, switchOff)
	mustPlan(t, a, ChargePlan{Charging: true, GridKW: 8, CurrentSOC: 0.49, TargetSOC: 0.55, BlockEnd: blockEnd(4, 55)})

	// A tick emits a higher target (0.75) with SoC just past the LATCH target (0.55).
	// The block must exit at 0.55 — not chase 0.75.
	mustPlan(t, a, ChargePlan{Charging: true, GridKW: 8, CurrentSOC: 0.56, TargetSOC: 0.75, BlockEnd: blockEnd(4, 55)})
	if fake.countService("turn_off") != 1 {
		t.Errorf("must exit at the latch target, not ratchet to a higher one: got %d turn_off", fake.countService("turn_off"))
	}
	if a.commit.active {
		t.Error("block must be cleared after latch-target exit")
	}

	// A sustained higher goal re-latches a new block (the visible, bounded re-plan).
	mustPlan(t, a, ChargePlan{Charging: true, GridKW: 8, CurrentSOC: 0.56, TargetSOC: 0.75, BlockEnd: blockEnd(4, 55)})
	if !a.commit.active || a.commit.targetSOC != 0.75 {
		t.Errorf("a sustained higher goal must re-latch at 0.75, got active=%v target=%v", a.commit.active, a.commit.targetSOC)
	}
	if fake.countService("turn_on") != 2 {
		t.Errorf("re-latch must be a visible new block: got %d turn_on, want 2", fake.countService("turn_on"))
	}
}

// TestCommitmentHoldsWhenSocGoesStale: if SoC becomes unknown mid-block the hub
// sends Charging=false + CurrentSOC=-1; the block must hold (no flapping) on the
// time backstop and exit once when the planned end passes.
func TestCommitmentHoldsWhenSocGoesStale(t *testing.T) {
	a, fake, clock := newActuator(t, ModeLive, inWindow, switchOff)
	mustPlan(t, a, ChargePlan{Charging: true, GridKW: 8, CurrentSOC: 0.49, TargetSOC: 0.90, BlockEnd: blockEnd(2, 30)})

	// SoC unknown for several ticks (still in window, before block end): hold.
	for range 4 {
		mustPlan(t, a, ChargePlan{Charging: false, CurrentSOC: -1, TargetSOC: 0.90, BlockEnd: blockEnd(2, 30)})
	}
	if fake.countService("turn_off") != 0 {
		t.Errorf("stale SoC must not stop the block early: got %d turn_off", fake.countService("turn_off"))
	}
	if !a.st.charging {
		t.Fatal("must still be charging with SoC unknown, before block end")
	}

	// Past the block end: the time backstop exits exactly once.
	clock.set(time.Date(2026, 1, 15, 2, 35, 0, 0, tokyo))
	mustPlan(t, a, ChargePlan{Charging: false, CurrentSOC: -1, TargetSOC: 0.90, BlockEnd: blockEnd(2, 30)})
	if fake.countService("turn_off") != 1 || a.commit.active {
		t.Errorf("block end must stop once when SoC is unknown: turn_off=%d active=%v", fake.countService("turn_off"), a.commit.active)
	}
}

// TestWatchdogClearsActiveCommitment: the out-of-window watchdog disables timed
// charge AND clears an active commitment, so the committed block can never re-start
// the charge the watchdog just halted (the primary stuck-on backstop).
func TestWatchdogClearsActiveCommitment(t *testing.T) {
	a, fake, clock := newActuator(t, ModeLive, inWindow, switchOff)
	mustPlan(t, a, ChargePlan{Charging: true, GridKW: 8, CurrentSOC: 0.49, TargetSOC: 0.90, BlockEnd: blockEnd(4, 55)})
	if !a.commit.active || !a.st.charging {
		t.Fatal("precondition: committed and charging")
	}

	// Clock jumps out of the off-peak window; the watchdog fires.
	clock.set(outWindow)
	submitSync(t, a, command{kind: cmdWatchdog})

	if a.commit.active {
		t.Error("watchdog must clear the commitment")
	}
	if a.st != (chargeState{}) || a.ChargingObserved() == ChargeOn || fake.State(switchEntity) != switchOff || fake.StateFloat(ampsEntity) != 0 {
		t.Errorf("watchdog must clear charge state, disable timed charge, and zero current out of window: state=%+v switch=%q current=%v", a.st, fake.State(switchEntity), fake.StateFloat(ampsEntity))
	}
	if fake.countService("turn_off") != 1 || fake.countSet(ampsEntity) != 2 {
		t.Errorf("outside-window watchdog must force both safe writes after the initial current set: turn_off=%d current_writes=%d", fake.countService("turn_off"), fake.countSet(ampsEntity))
	}
	// And a stale committed-looking plan must not re-start it.
	onBefore := fake.countService("turn_on")
	mustPlan(t, a, ChargePlan{Charging: false, CurrentSOC: 0.50, TargetSOC: 0.90, BlockEnd: blockEnd(4, 55)})
	if fake.countService("turn_on") != onBefore {
		t.Error("must not re-start after a watchdog stop")
	}
}

// TestSafeStopClearsCommitment: a fail-safe/watchdog stop must clear the block so
// the next committed tick cannot re-start the charge it just halted.
func TestSafeStopClearsCommitment(t *testing.T) {
	a, fake, _ := newActuator(t, ModeLive, inWindow, switchOff)
	mustPlan(t, a, ChargePlan{Charging: true, GridKW: 8, CurrentSOC: 0.49, TargetSOC: 0.90, BlockEnd: blockEnd(4, 55)})
	if !a.commit.active {
		t.Fatal("precondition: committed")
	}

	submitSync(t, a, command{kind: cmdSafe, reason: "test fail-safe"})
	if a.commit.active {
		t.Fatal("safe stop must clear the commitment")
	}
	onBefore := fake.countService("turn_on")

	// A committed-looking plan must NOT re-start (block was cleared).
	mustPlan(t, a, ChargePlan{Charging: false, CurrentSOC: 0.50, TargetSOC: 0.90, BlockEnd: blockEnd(4, 55)})
	if fake.countService("turn_on") != onBefore {
		t.Error("must not re-start a charge after a fail-safe stop")
	}
	if a.st.charging {
		t.Error("must remain stopped after fail-safe")
	}
}

// TestNoBlockContextFallsBackToPerTick: a charge with no block target (defensive /
// malformed plan) does not latch — a Charging=false stops immediately, so a bad
// plan can never hold the contactor on until window end.
func TestNoBlockContextFallsBackToPerTick(t *testing.T) {
	a, fake, _ := newActuator(t, ModeLive, inWindow, switchOff)
	mustPlan(t, a, ChargePlan{Charging: true, GridKW: 5}) // no TargetSOC/BlockEnd
	if a.commit.active {
		t.Fatal("no block context must not latch a commitment")
	}
	mustPlan(t, a, ChargePlan{Charging: false})
	if fake.countService("turn_off") != 1 || a.st.charging {
		t.Errorf("without a block, Charging=false must stop at once: turn_off=%d charging=%v", fake.countService("turn_off"), a.st.charging)
	}
}
