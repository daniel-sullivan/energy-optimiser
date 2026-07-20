package serve

import (
	"encoding/json"
	"testing"
	"time"

	"energy-optimiser/config"
	"energy-optimiser/optimizer"
)

func TestTimeRemaining_Charging(t *testing.T) {
	cfg := config.Battery{CapacityKWh: 50, SOCMin: 0.10, SOCMax: 0.90}
	// SoC 50%, charging at 8 kW -> (0.90-0.50)*50/8 = 2.5h
	chargeH, dischargeH := TimeRemaining(cfg, 0.50, 8.0)
	if dischargeH != nil {
		t.Fatalf("dischargeH = %v, want nil while charging", *dischargeH)
	}
	if chargeH == nil {
		t.Fatal("chargeH = nil, want a value")
	}
	if got, want := *chargeH, 2.5; got != want {
		t.Errorf("chargeH = %v, want %v", got, want)
	}
}

func TestTimeRemaining_Discharging(t *testing.T) {
	cfg := config.Battery{CapacityKWh: 50, SOCMin: 0.10, SOCMax: 0.90}
	// SoC 50%, discharging at 5 kW -> (0.50-0.10)*50/5 = 4h
	chargeH, dischargeH := TimeRemaining(cfg, 0.50, -5.0)
	if chargeH != nil {
		t.Fatalf("chargeH = %v, want nil while discharging", *chargeH)
	}
	if dischargeH == nil {
		t.Fatal("dischargeH = nil, want a value")
	}
	if got, want := *dischargeH, 4.0; got != want {
		t.Errorf("dischargeH = %v, want %v", got, want)
	}
}

func TestTimeRemaining_NearZeroGuard(t *testing.T) {
	cfg := config.Battery{CapacityKWh: 50, SOCMin: 0.10, SOCMax: 0.90}
	for _, p := range []float64{0, 0.01, -0.01, 0.049, -0.049} {
		chargeH, dischargeH := TimeRemaining(cfg, 0.50, p)
		if chargeH != nil || dischargeH != nil {
			t.Errorf("power=%v: got chargeH=%v dischargeH=%v, want both nil (below epsilon)", p, chargeH, dischargeH)
		}
	}
}

func TestTimeRemaining_ClampsBelowZero(t *testing.T) {
	cfg := config.Battery{CapacityKWh: 50, SOCMin: 0.10, SOCMax: 0.90}
	// Already above SOCMax but still "charging" per live power sign -> clamp to 0, not negative.
	chargeH, _ := TimeRemaining(cfg, 0.95, 8.0)
	if chargeH == nil || *chargeH != 0 {
		t.Errorf("chargeH = %v, want 0", chargeH)
	}
	// Already at/below SOCMin but still "discharging" -> clamp to 0.
	_, dischargeH := TimeRemaining(cfg, 0.05, -5.0)
	if dischargeH == nil || *dischargeH != 0 {
		t.Errorf("dischargeH = %v, want 0", dischargeH)
	}
}

func TestFormatHours(t *testing.T) {
	tests := []struct {
		hours float64
		want  string
	}{
		{0, "0m"},
		{0.4, "24m"},
		{2.5, "2h 30m"},
		{30, "1d 6h 0m"},
		{-1, "0m"}, // negative clamps to 0
	}
	for _, tt := range tests {
		if got := FormatHours(tt.hours); got != tt.want {
			t.Errorf("FormatHours(%v) = %q, want %q", tt.hours, got, tt.want)
		}
	}
}

func TestRationaleFor_ChargeScheduled(t *testing.T) {
	loc := time.UTC
	now := time.Date(2026, 7, 20, 9, 0, 0, 0, loc)
	sched := &optimizer.Schedule{
		Slots: []optimizer.Slot{
			{Start: now, SOC: 0.45, GridCharge: false},
			{Start: time.Date(2026, 7, 20, 11, 0, 0, 0, loc), SOC: 0.40, GridCharge: true},
			{Start: time.Date(2026, 7, 20, 20, 0, 0, 0, loc), SOC: 0.22, GridCharge: false},
		},
	}
	got := RationaleFor(now, 0.45, sched)
	want := "SoC forecast 22% by 20:00 -> scheduling 11:00 charge"
	if got != want {
		t.Errorf("RationaleFor() = %q, want %q", got, want)
	}
}

func TestRationaleFor_NoChargeScheduled(t *testing.T) {
	loc := time.UTC
	now := time.Date(2026, 7, 20, 9, 0, 0, 0, loc)
	sched := &optimizer.Schedule{
		Slots: []optimizer.Slot{
			{Start: now, SOC: 0.60, GridCharge: false},
			{Start: time.Date(2026, 7, 20, 15, 0, 0, 0, loc), SOC: 0.80, GridCharge: false},
		},
	}
	got := RationaleFor(now, 0.60, sched)
	want := "No grid charge scheduled; solar covers the horizon"
	if got != want {
		t.Errorf("RationaleFor() = %q, want %q", got, want)
	}
}

func TestRationaleFor_NoSchedule(t *testing.T) {
	if got, want := RationaleFor(time.Now(), 0.5, nil), "No schedule available"; got != want {
		t.Errorf("RationaleFor(nil) = %q, want %q", got, want)
	}
	if got, want := RationaleFor(time.Now(), 0.5, &optimizer.Schedule{}), "No schedule available"; got != want {
		t.Errorf("RationaleFor(empty) = %q, want %q", got, want)
	}
}

func TestDiscoveryPayload_Shape(t *testing.T) {
	// decision_grid_charge is the binary_sensor entity; verify topic + payload shape.
	var def decisionSensorDef
	for _, s := range decisionSensors {
		if s.Key == "decision_grid_charge" {
			def = s
		}
	}
	if def.Key == "" {
		t.Fatal("decision_grid_charge not found in decisionSensors")
	}

	data, topic := discoveryPayload("homeassistant", "energy_optimiser", def)
	wantTopic := "homeassistant/binary_sensor/energy_optimiser/decision_grid_charge/config"
	if topic != wantTopic {
		t.Errorf("config topic = %q, want %q", topic, wantTopic)
	}

	var payload map[string]any
	if err := json.Unmarshal(data, &payload); err != nil {
		t.Fatalf("payload not valid JSON: %v", err)
	}

	if payload["unique_id"] != "energy_optimiser_decision_grid_charge" {
		t.Errorf("unique_id = %v, want energy_optimiser_decision_grid_charge", payload["unique_id"])
	}
	if payload["state_topic"] != "homeassistant/sensor/energy_optimiser/state" {
		t.Errorf("state_topic = %v, want homeassistant/sensor/energy_optimiser/state", payload["state_topic"])
	}
	if payload["availability_topic"] != "homeassistant/sensor/energy_optimiser/availability" {
		t.Errorf("availability_topic = %v, want homeassistant/sensor/energy_optimiser/availability", payload["availability_topic"])
	}
	if payload["payload_on"] != "ON" || payload["payload_off"] != "OFF" {
		t.Errorf("payload_on/off = %v/%v, want ON/OFF", payload["payload_on"], payload["payload_off"])
	}
	device, ok := payload["device"].(map[string]any)
	if !ok {
		t.Fatal("device field missing or wrong type")
	}
	ids, ok := device["identifiers"].([]any)
	if !ok || len(ids) != 1 || ids[0] != "energy_optimiser" {
		t.Errorf("device.identifiers = %v, want [energy_optimiser]", device["identifiers"])
	}

	// A plain sensor entity should carry unit_of_measurement/device_class and no payload_on/off.
	for _, s := range decisionSensors {
		if s.Key == "charge_time_remaining" {
			data, _ := discoveryPayload("homeassistant", "energy_optimiser", s)
			var p map[string]any
			_ = json.Unmarshal(data, &p)
			if p["unit_of_measurement"] != "h" {
				t.Errorf("charge_time_remaining unit = %v, want h", p["unit_of_measurement"])
			}
			if p["device_class"] != "duration" {
				t.Errorf("charge_time_remaining device_class = %v, want duration", p["device_class"])
			}
			if _, present := p["payload_on"]; present {
				t.Error("charge_time_remaining should not carry payload_on (it's a sensor, not binary_sensor)")
			}
		}
	}
}

func TestNewDecisionPublisher_DryRun(t *testing.T) {
	cfg := config.MQTT{Broker: "tcp://127.0.0.1:1", TopicPrefix: "homeassistant"}
	p, err := NewDecisionPublisher(cfg, true)
	if err != nil {
		t.Fatalf("NewDecisionPublisher dry-run: %v", err)
	}
	if p.deviceID != "energy_optimiser" {
		t.Errorf("deviceID = %q, want default energy_optimiser", p.deviceID)
	}
	// None of these should block or error against an unreachable broker in dry-run.
	if err := p.Connect(); err != nil {
		t.Errorf("Connect() in dry-run: %v", err)
	}
	p.PublishDiscovery()
	p.PublishState(DecisionState{Rationale: "test"})
	p.Close()
}
