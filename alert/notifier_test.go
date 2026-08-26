package alert

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"energy-optimiser/config"
	"energy-optimiser/optimizer"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type gotAlert struct {
	Labels      map[string]string `json:"labels"`
	Annotations map[string]string `json:"annotations"`
	StartsAt    time.Time         `json:"startsAt"`
	EndsAt      time.Time         `json:"endsAt"`
}

// captureAM records every alert POSTed to /api/v2/alerts.
func captureAM(t *testing.T) (*httptest.Server, func() []gotAlert) {
	t.Helper()
	var mu sync.Mutex
	var all []gotAlert
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v2/alerts" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		var batch []gotAlert
		_ = json.NewDecoder(r.Body).Decode(&batch)
		mu.Lock()
		all = append(all, batch...)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	return srv, func() []gotAlert {
		mu.Lock()
		defer mu.Unlock()
		return append([]gotAlert(nil), all...)
	}
}

func testNotifier(amURL string) *Notifier {
	return &Notifier{
		am:        NewAlertManager(amURL),
		loc:       time.UTC,
		rates:     config.Rates{Currency: "¥"},
		site:      "home",
		currency:  "¥",
		capacity:  49.8,
		slotHours: 0.5,
		resolveIn: 15 * time.Minute,
		riskSOC:   0.15,
		expThresh: 300,
	}
}

func find(alerts []gotAlert, name string) *gotAlert {
	for i := range alerts {
		if alerts[i].Labels["alertname"] == name {
			return &alerts[i]
		}
	}
	return nil
}

func TestAlertManagerDisabledIsNoop(t *testing.T) {
	am := NewAlertManager("")
	if am.Enabled() {
		t.Fatal("empty URL should be disabled")
	}
	if err := am.Send(context.Background(), []Alert{{}}); err != nil {
		t.Fatalf("disabled Send should be a no-op, got %v", err)
	}
}

func TestConfirmedChargeLifecycleFreezesSummaryAcrossForecastChurn(t *testing.T) {
	srv, got := captureAM(t)
	defer srv.Close()
	n := testNotifier(srv.URL)
	now := time.Date(2026, 7, 21, 7, 0, 0, 0, time.UTC)

	assert.NoError(t, n.Lifecycle(context.Background(), now, ChargeStatus{
		State:   ChargeOn,
		Context: &ChargeContext{Start: now, InitialSOC: 0.4, TargetSOC: 0.7},
	}))
	require.NoError(t, n.Lifecycle(context.Background(), now.Add(5*time.Minute), ChargeStatus{State: ChargeOn}))
	require.NoError(t, n.Lifecycle(context.Background(), now.Add(10*time.Minute), ChargeStatus{State: ChargeOn}))

	alerts := got()
	require.Len(t, alerts, 3, "expected one firing post and two heartbeats")
	for i := range alerts {
		if alerts[i].Labels["alertname"] != "EnergyOptimiserGridCharge" {
			assert.Failf(t, "assertion failed", "post %d must be grid-charge alert: %+v", i, alerts[i])
		}
		if alerts[i].Labels["site"] != "home" || alerts[i].Labels["severity"] != "warning" {
			assert.Failf(t, "assertion failed", "post %d must retain fixed routable labels: %+v", i, alerts[i].Labels)
		}
		if !alerts[i].StartsAt.Equal(now) || alerts[i].Annotations["summary"] != alerts[0].Annotations["summary"] {
			assert.Failf(t, "assertion failed", "post %d changed the frozen episode identity or summary: %+v", i, alerts[i])
		}
	}
	if !strings.Contains(alerts[0].Annotations["summary"], "14.9 kWh") || !strings.Contains(alerts[0].Annotations["summary"], "40% → 70%") {
		assert.Failf(t, "assertion failed", "initial summary must preserve initial plan context: %q", alerts[0].Annotations["summary"])
	}
}

func TestConfirmedChargeStopPostsOneExplicitResolve(t *testing.T) {
	srv, got := captureAM(t)
	defer srv.Close()
	n := testNotifier(srv.URL)
	now := time.Date(2026, 7, 21, 7, 0, 0, 0, time.UTC)

	require.NoError(t, n.Lifecycle(context.Background(), now, ChargeStatus{State: ChargeOn}))
	stop := now.Add(5 * time.Minute)
	require.NoError(t, n.Lifecycle(context.Background(), stop, ChargeStatus{State: ChargeOff}))
	require.NoError(t, n.Lifecycle(context.Background(), stop.Add(5*time.Minute), ChargeStatus{State: ChargeOff}))

	alerts := got()
	require.Len(t, alerts, 2, "expected start and one explicit resolve")
	if !alerts[1].EndsAt.Equal(stop) {
		assert.Failf(t, "assertion failed", "resolved alert EndsAt=%v, want stop time %v", alerts[1].EndsAt, stop)
	}
	if !alerts[1].StartsAt.Equal(alerts[0].StartsAt) || alerts[1].Annotations["summary"] != alerts[0].Annotations["summary"] {
		assert.Failf(t, "assertion failed", "resolve must use same alert fingerprint and frozen summary: start=%+v resolve=%+v", alerts[0], alerts[1])
	}
}

func TestUnconfirmedChargeDoesNotAlertAndBasicStartIsValid(t *testing.T) {
	srv, got := captureAM(t)
	defer srv.Close()
	n := testNotifier(srv.URL)
	now := time.Date(2026, 7, 21, 7, 0, 0, 0, time.UTC)

	require.NoError(t, n.Lifecycle(context.Background(), now, ChargeStatus{State: ChargeOff}))
	if find(got(), "EnergyOptimiserGridCharge") != nil {
		assert.Fail(t, "a planned but unconfirmed charge must not alert")
	}
	require.NoError(t, n.Lifecycle(context.Background(), now.Add(time.Minute), ChargeStatus{State: ChargeOn}))
	alerts := got()
	if len(alerts) != 1 || alerts[0].Annotations["summary"] != "⚡ Grid charging observed." {
		assert.Failf(t, "assertion failed", "confirmed charge without block context must emit basic start summary: %+v", alerts)
	}
}

func TestResolveFailureRetriesTransactionally(t *testing.T) {
	var mu sync.Mutex
	fail := false
	var alerts []gotAlert
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if fail {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		var batch []gotAlert
		_ = json.NewDecoder(r.Body).Decode(&batch)
		alerts = append(alerts, batch...)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	n := testNotifier(srv.URL)
	now := time.Date(2026, 7, 21, 7, 0, 0, 0, time.UTC)
	require.NoError(t, n.Lifecycle(context.Background(), now, ChargeStatus{State: ChargeOn}))
	mu.Lock()
	fail = true
	mu.Unlock()
	if err := n.Lifecycle(context.Background(), now.Add(time.Minute), ChargeStatus{State: ChargeOff}); err == nil {
		assert.Fail(t, "failed resolve delivery must return an error")
	}
	mu.Lock()
	fail = false
	mu.Unlock()
	require.NoError(t, n.Lifecycle(context.Background(), now.Add(2*time.Minute), ChargeStatus{State: ChargeOff}))
	mu.Lock()
	defer mu.Unlock()
	if len(alerts) != 2 || !alerts[1].EndsAt.Equal(now.Add(2*time.Minute)) {
		assert.Failf(t, "assertion failed", "resolve must retry after failure, got %+v", alerts)
	}
}

func TestUnknownDoesNotResolveAndWallLeaseIsFuture(t *testing.T) {
	srv, got := captureAM(t)
	defer srv.Close()
	n := testNotifier(srv.URL)
	wallNow := time.Date(2026, 7, 21, 7, 4, 59, 0, time.UTC)
	require.NoError(t, n.Lifecycle(context.Background(), wallNow, ChargeStatus{State: ChargeOn}))
	require.NoError(t, n.Lifecycle(context.Background(), wallNow.Add(time.Minute), ChargeStatus{State: ChargeUnknown}))
	alerts := got()
	require.Len(t, alerts, 2, "unknown observation must heartbeat an existing episode")
	if !alerts[1].StartsAt.Equal(alerts[0].StartsAt) || !alerts[1].EndsAt.After(wallNow.Add(time.Minute)) {
		assert.Failf(t, "assertion failed", "unknown heartbeat must preserve the episode and extend its wall-clock lease: %+v", alerts)
	}
	if alerts[0].StartsAt.Truncate(5 * time.Minute).Equal(alerts[0].StartsAt) {
		assert.Failf(t, "assertion failed", "alert timestamps must use real wall time rather than slot-truncated decision time: %+v", alerts[0])
	}
}

func TestFailedStartRetriesWithFrozenContext(t *testing.T) {
	var mu sync.Mutex
	fail := true
	var alerts []gotAlert
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if fail {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		var batch []gotAlert
		_ = json.NewDecoder(r.Body).Decode(&batch)
		alerts = append(alerts, batch...)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	n := testNotifier(srv.URL)
	now := time.Date(2026, 7, 21, 7, 1, 0, 0, time.UTC)
	contextAtStart := &ChargeContext{InitialSOC: 0.4, TargetSOC: 0.7}
	if err := n.Lifecycle(context.Background(), now, ChargeStatus{State: ChargeOn, Context: contextAtStart}); err == nil {
		assert.Fail(t, "failed start delivery must return an error")
	}
	mu.Lock()
	fail = false
	mu.Unlock()
	assert.NoError(t, n.Lifecycle(context.Background(), now.Add(time.Minute), ChargeStatus{
		State:   ChargeOn,
		Context: &ChargeContext{InitialSOC: 0.5, TargetSOC: 0.9},
	}))
	mu.Lock()
	defer mu.Unlock()
	if len(alerts) != 1 || !strings.Contains(alerts[0].Annotations["summary"], "40% → 70%") {
		assert.Failf(t, "assertion failed", "retry must preserve the first start context, got %+v", alerts)
	}
	if !alerts[0].StartsAt.Equal(now) {
		assert.Failf(t, "assertion failed", "retry must preserve the first observed start time, got %v want %v", alerts[0].StartsAt, now)
	}
}

func TestFailedStartThenUnknownRetriesFrozenEpisode(t *testing.T) {
	var mu sync.Mutex
	fail := true
	var alerts []gotAlert
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if fail {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		var batch []gotAlert
		_ = json.NewDecoder(r.Body).Decode(&batch)
		alerts = append(alerts, batch...)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	n := testNotifier(srv.URL)
	now := time.Date(2026, 7, 21, 7, 1, 0, 0, time.UTC)
	if err := n.Lifecycle(context.Background(), now, ChargeStatus{
		State: ChargeOn, Context: &ChargeContext{InitialSOC: 0.4, TargetSOC: 0.7},
	}); err == nil {
		assert.Fail(t, "failed start delivery must return an error")
	}
	mu.Lock()
	fail = false
	mu.Unlock()
	require.NoError(t, n.Lifecycle(context.Background(), now.Add(time.Minute), ChargeStatus{State: ChargeUnknown}))
	mu.Lock()
	defer mu.Unlock()
	if len(alerts) != 1 || !alerts[0].StartsAt.Equal(now) || !strings.Contains(alerts[0].Annotations["summary"], "40% → 70%") {
		assert.Failf(t, "assertion failed", "unknown must retry the frozen pending episode, got %+v", alerts)
	}
}

func TestFailedStartThenOffResolvesFrozenEpisode(t *testing.T) {
	var mu sync.Mutex
	fail := true
	var alerts []gotAlert
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if fail {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		var batch []gotAlert
		_ = json.NewDecoder(r.Body).Decode(&batch)
		alerts = append(alerts, batch...)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	n := testNotifier(srv.URL)
	now := time.Date(2026, 7, 21, 7, 1, 0, 0, time.UTC)
	if err := n.Lifecycle(context.Background(), now, ChargeStatus{State: ChargeOn}); err == nil {
		assert.Fail(t, "failed start delivery must return an error")
	}
	mu.Lock()
	fail = false
	mu.Unlock()
	stopped := now.Add(time.Minute)
	require.NoError(t, n.Lifecycle(context.Background(), stopped, ChargeStatus{State: ChargeOff}))
	mu.Lock()
	defer mu.Unlock()
	if len(alerts) != 1 || !alerts[0].StartsAt.Equal(now) || !alerts[0].EndsAt.Equal(stopped) {
		assert.Failf(t, "assertion failed", "off must resolve the frozen pending episode, got %+v", alerts)
	}
}

func TestUnknownWithoutEpisodeDoesNothing(t *testing.T) {
	srv, got := captureAM(t)
	defer srv.Close()
	n := testNotifier(srv.URL)
	require.NoError(t, n.Lifecycle(context.Background(), time.Now(), ChargeStatus{State: ChargeUnknown}))
	if len(got()) != 0 {
		assert.Fail(t, "unknown without an existing or pending episode must not synthesize a start")
	}
}

func TestConcurrentStartAndResolveAreSerialized(t *testing.T) {
	var mu sync.Mutex
	var alerts []gotAlert
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	requests := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests++
		request := requests
		mu.Unlock()
		if request == 1 {
			close(firstStarted)
			<-releaseFirst
		}
		var batch []gotAlert
		_ = json.NewDecoder(r.Body).Decode(&batch)
		mu.Lock()
		alerts = append(alerts, batch...)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	n := testNotifier(srv.URL)
	now := time.Date(2026, 7, 21, 7, 0, 0, 0, time.UTC)
	startDone := make(chan error, 1)
	go func() { startDone <- n.Lifecycle(context.Background(), now, ChargeStatus{State: ChargeOn}) }()
	<-firstStarted
	resolveDone := make(chan error, 1)
	go func() {
		resolveDone <- n.Lifecycle(context.Background(), now.Add(time.Minute), ChargeStatus{State: ChargeOff})
	}()
	close(releaseFirst)
	require.NoError(t, <-startDone)
	require.NoError(t, <-resolveDone)
	mu.Lock()
	defer mu.Unlock()
	if len(alerts) != 2 || !alerts[1].EndsAt.Equal(now.Add(time.Minute)) {
		assert.Failf(t, "assertion failed", "serialized resolve must follow the firing delivery, got %+v", alerts)
	}
}

func TestLowSOCAlert(t *testing.T) {
	srv, got := captureAM(t)
	defer srv.Close()
	n := testNotifier(srv.URL)

	now := time.Date(2026, 7, 21, 20, 0, 0, 0, time.UTC)
	sched := &optimizer.Schedule{Slots: []optimizer.Slot{
		{Start: now.Add(2 * time.Hour), SOC: 0.30},
		{Start: now.Add(6 * time.Hour), SOC: 0.12},
	}}
	if err := n.Forecast(context.Background(), now, sched); err != nil {
		t.Fatal(err)
	}

	a := find(got(), "EnergyOptimiserLowSoC")
	if a == nil {
		t.Fatal("projected trough below threshold must fire the low-SoC alert")
	}
	if !strings.Contains(a.Annotations["summary"], "12%") {
		t.Fatalf("unexpected low-SoC summary: %q", a.Annotations["summary"])
	}
}
