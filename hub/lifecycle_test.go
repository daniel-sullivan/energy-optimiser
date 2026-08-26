package hub

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"energy-optimiser/actuator"
	"energy-optimiser/alert"
	"energy-optimiser/config"
	"energy-optimiser/optimizer"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type lifecycleActuator struct {
	mu           sync.Mutex
	observed     actuator.ChargeObservation
	closed       int
	startEntered chan struct{}
	startRelease chan struct{}
}

func (a *lifecycleActuator) Start(context.Context) error {
	if a.startEntered != nil {
		close(a.startEntered)
		<-a.startRelease
	}
	return nil
}
func (a *lifecycleActuator) SetChargePlan(context.Context, actuator.ChargePlan) error {
	return nil
}
func (a *lifecycleActuator) ChargingObserved() actuator.ChargeObservation {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.observed
}
func (a *lifecycleActuator) Close() {
	a.mu.Lock()
	a.closed++
	a.mu.Unlock()
}

type lifecycleDelivery struct {
	at     time.Time
	status alert.ChargeStatus
}

type lifecycleNotifier struct {
	mu         sync.Mutex
	deliveries []lifecycleDelivery
}

func (n *lifecycleNotifier) Lifecycle(_ context.Context, at time.Time, status alert.ChargeStatus) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.deliveries = append(n.deliveries, lifecycleDelivery{at: at, status: status})
	return nil
}
func (*lifecycleNotifier) Forecast(context.Context, time.Time, *optimizer.Schedule) error { return nil }
func (*lifecycleNotifier) ResolveGridCharge(context.Context, time.Time) error             { return nil }

func lifecycleConfig(t *testing.T, poll time.Duration) *config.Config {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.toml")
	require.NoError(t, os.WriteFile(path, []byte(`
time_zone = "UTC"
[service]
poll_interval = "`+poll.String()+`"
slot_duration = "30m"
planning_horizon = "24h"
[rates]
peak_rate = 30
off_peak_rate = 10
feed_in_rate = 0
[battery]
capacity_kwh = 50
max_charge_kw = 8
max_discharge_kw = 8
soc_min = 0.1
soc_max = 0.9
efficiency = 0.9
`), 0o600))
	cfg, err := config.Parse(path)
	require.NoError(t, err)
	return cfg
}

func TestStartupHeartbeatContinuesWhileStartupBlocks(t *testing.T) {
	act := &lifecycleActuator{
		observed:     actuator.ChargeOn,
		startEntered: make(chan struct{}),
		startRelease: make(chan struct{}),
	}
	notifier := new(lifecycleNotifier)
	h := &Hub{cfg: lifecycleConfig(t, 10*time.Millisecond), actuator: act, notifier: notifier}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stop := h.startStartupLifecycle(ctx)
	t.Cleanup(stop)
	startDone := make(chan error, 1)
	go func() { startDone <- h.actuator.Start(ctx) }()
	<-act.startEntered

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		notifier.mu.Lock()
		count := len(notifier.deliveries)
		notifier.mu.Unlock()
		if count >= 2 {
			close(act.startRelease)
			require.NoError(t, <-startDone)
			return
		}
		time.Sleep(time.Millisecond)
	}
	close(act.startRelease)
	<-startDone
	assert.Fail(t, "startup heartbeat did not refresh while actuator reconciliation remained blocked")
}

func TestCloseBeforeStartDoesNotResolve(t *testing.T) {
	act := &lifecycleActuator{observed: actuator.ChargeOff}
	notifier := new(lifecycleNotifier)
	h := &Hub{cfg: lifecycleConfig(t, time.Minute), actuator: act, notifier: notifier}

	h.Close()
	notifier.mu.Lock()
	defer notifier.mu.Unlock()
	if len(notifier.deliveries) != 0 {
		assert.Failf(t, "assertion failed", "Close before Start must not deliver lifecycle state, got %d", len(notifier.deliveries))
	}
}

func TestCloseDeliversLifecycleOnlyOnce(t *testing.T) {
	act := &lifecycleActuator{observed: actuator.ChargeUnknown}
	notifier := new(lifecycleNotifier)
	h := &Hub{cfg: lifecycleConfig(t, time.Minute), actuator: act, notifier: notifier}
	h.started.Store(true)

	h.Close()
	h.Close()
	notifier.mu.Lock()
	defer notifier.mu.Unlock()
	if len(notifier.deliveries) != 1 || notifier.deliveries[0].status.State != alert.ChargeUnknown {
		assert.Failf(t, "assertion failed", "shutdown must heartbeat unknown exactly once, got %+v", notifier.deliveries)
	}
}

func TestChargingContextIsPassedOnlyForEpisodeCreation(t *testing.T) {
	act := &lifecycleActuator{observed: actuator.ChargeOn}
	notifier := new(lifecycleNotifier)
	ctx := &alert.ChargeContext{InitialSOC: 0.4, TargetSOC: 0.7}
	require.NoError(t, notifier.Lifecycle(context.Background(), time.Now(), alert.ChargeStatus{State: observedAlertState(act.ChargingObserved()), Context: ctx}))
	notifier.mu.Lock()
	defer notifier.mu.Unlock()
	if len(notifier.deliveries) != 1 || notifier.deliveries[0].status.Context != ctx {
		assert.Fail(t, "charging plan context must accompany the single lifecycle observation without a before/after race")
	}
}
