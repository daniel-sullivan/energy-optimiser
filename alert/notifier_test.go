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

// TestChargeAlertStableIdentity is the core regression: the alert identity (its
// label set) must NOT depend on live SoC, so Alertmanager dedupes it instead of
// re-firing every 1% tick. The summary reports energy added over the charge run.
func TestChargeAlertStableIdentity(t *testing.T) {
	srv, got := captureAM(t)
	defer srv.Close()
	n := testNotifier(srv.URL)

	now := time.Date(2026, 7, 21, 7, 0, 0, 0, time.UTC)
	sched := &optimizer.Schedule{Slots: []optimizer.Slot{
		{Start: now.Add(1 * time.Hour), SOC: 0.30},                       // "before" the run
		{Start: now.Add(2 * time.Hour), SOC: 0.50, GridCharge: true},     // run start
		{Start: now.Add(150 * time.Minute), SOC: 0.60, GridCharge: true}, // run end
	}}

	n.Evaluate(context.Background(), now, sched, 0.28)
	n.Evaluate(context.Background(), now, sched, 0.55) // live SoC differs; plan identical

	alerts := got()
	if len(alerts) != 2 {
		t.Fatalf("expected 2 posts, got %d", len(alerts))
	}
	a0, a1 := &alerts[0], &alerts[1]
	if a0.Labels["alertname"] != "EnergyOptimiserGridCharge" || a1.Labels["alertname"] != "EnergyOptimiserGridCharge" {
		t.Fatalf("both posts must carry the grid-charge alert: %+v", alerts)
	}
	// Identical labels (so AM dedupes) + identical summary despite different live SoC.
	if a0.Labels["site"] != "home" || a0.Labels["severity"] != "warning" {
		t.Fatalf("labels must be routable: %+v", a0.Labels)
	}
	if a0.Annotations["summary"] != a1.Annotations["summary"] {
		t.Fatalf("summary must not depend on live SoC: %q vs %q", a0.Annotations["summary"], a1.Annotations["summary"])
	}
	// Energy added over the run: (0.60 - 0.30) * 49.8 ≈ 14.9 kWh, and the SoC span.
	if !strings.Contains(a0.Annotations["summary"], "kWh") || !strings.Contains(a0.Annotations["summary"], "30% → 60%") {
		t.Fatalf("summary should report energy + SoC span: %q", a0.Annotations["summary"])
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
