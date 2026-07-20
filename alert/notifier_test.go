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
)

type gotAlert struct {
	Labels      map[string]string `json:"labels"`
	Annotations map[string]string `json:"annotations"`
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

// TestChargeAlertStableIdentity is the core regression: the alert identity (its
// label set) must NOT depend on live SoC, so Alertmanager dedupes it instead of
// re-firing every 1% tick. SoC lives only in the annotation.
func TestChargeAlertStableIdentity(t *testing.T) {
	srv, got := captureAM(t)
	defer srv.Close()
	n := testNotifier(srv.URL)

	now := time.Date(2026, 7, 21, 7, 0, 0, 0, time.UTC)
	sched := &optimizer.Schedule{Slots: []optimizer.Slot{
		{Start: now.Add(1 * time.Hour), SOC: 0.8},
		{Start: now.Add(2 * time.Hour), SOC: 0.8, GridCharge: true},
	}}

	n.Evaluate(context.Background(), now, sched, 0.30)
	n.Evaluate(context.Background(), now, sched, 0.55)

	alerts := got()
	if len(alerts) != 2 {
		t.Fatalf("expected 2 posts, got %d", len(alerts))
	}
	a0, a1 := find([]gotAlert{alerts[0]}, "EnergyOptimiserGridCharge"), find([]gotAlert{alerts[1]}, "EnergyOptimiserGridCharge")
	if a0 == nil || a1 == nil {
		t.Fatalf("both posts must carry the grid-charge alert: %+v", alerts)
	}
	// Identical labels (so AM dedupes) despite different SoC.
	if a0.Labels["alertname"] != a1.Labels["alertname"] || a0.Labels["site"] != "home" || a0.Labels["severity"] != "warning" {
		t.Fatalf("label identity must be stable + routable: %+v vs %+v", a0.Labels, a1.Labels)
	}
	// SoC differs only in the annotation.
	if !strings.Contains(a0.Annotations["summary"], "30%") || !strings.Contains(a1.Annotations["summary"], "55%") {
		t.Fatalf("SoC should vary in annotation: %q / %q", a0.Annotations["summary"], a1.Annotations["summary"])
	}
}

// TestActiveChargeSlotStillFires covers the review's HIGH-1: a grid-charge slot
// in progress (start in the past, end in the future) must still fire, not
// falsely resolve mid-charge.
func TestActiveChargeSlotStillFires(t *testing.T) {
	srv, got := captureAM(t)
	defer srv.Close()
	n := testNotifier(srv.URL)

	now := time.Date(2026, 7, 21, 2, 10, 0, 0, time.UTC)
	sched := &optimizer.Schedule{Slots: []optimizer.Slot{
		{Start: now.Add(-10 * time.Minute), SOC: 0.5, GridCharge: true}, // active (30-min slot)
	}}
	n.Evaluate(context.Background(), now, sched, 0.50)

	if find(got(), "EnergyOptimiserGridCharge") == nil {
		t.Fatal("active grid-charge slot must still fire the alert")
	}
}

func TestNoChargeNoAlert(t *testing.T) {
	srv, got := captureAM(t)
	defer srv.Close()
	n := testNotifier(srv.URL)

	now := time.Date(2026, 7, 21, 7, 0, 0, 0, time.UTC)
	sched := &optimizer.Schedule{Slots: []optimizer.Slot{
		{Start: now.Add(1 * time.Hour), SOC: 0.8},
	}}
	n.Evaluate(context.Background(), now, sched, 0.80)

	if find(got(), "EnergyOptimiserGridCharge") != nil {
		t.Fatal("no charge planned -> no grid-charge alert")
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
	n.Evaluate(context.Background(), now, sched, 0.30)

	a := find(got(), "EnergyOptimiserLowSoC")
	if a == nil {
		t.Fatal("projected trough below threshold must fire the low-SoC alert")
	}
	if !strings.Contains(a.Annotations["summary"], "12%") {
		t.Fatalf("unexpected low-SoC summary: %q", a.Annotations["summary"])
	}
}
