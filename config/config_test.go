package config

import (
	"os"
	"strings"
	"testing"
	"time"

	_ "time/tzdata" // hermetic tz database for LoadLocation
)

const testTOML = `
[service]
poll_interval = "5m"
planning_horizon = "72h"
slot_duration = "30m"
web_port = 8080

[influxdb]
url = "http://localhost:8181"
token = "test-token"
database = "energy"

[homeassistant]
url = "ws://localhost:8123/api/websocket"
token = "ha-token"

[homeassistant.entities]
battery_soc = "sensor.battery_soc"
pv_power = "sensor.pv_power"
grid_power = "sensor.grid_power"
load_power = "sensor.load_power"

[solcast]
api_key = "solcast-key"
poll_times = ["06:00", "12:00"]

[[solcast.sites]]
name = "house"
id = "site-123"

[[solcast.sites]]
name = "shed"
id = "site-456"

[weather]
latitude = -33.87
longitude = 151.21

[battery]
capacity_kwh = 9.6
max_charge_kw = 5.0
max_discharge_kw = 5.0
soc_min = 0.20
soc_max = 1.0
efficiency = 0.95

[rates]
currency = "AUD"
peak_rate = 0.35
off_peak_rate = 0.12
feed_in_rate = 0.05

[[rates.off_peak_windows]]
start = "01:00"
end = "05:00"

[[rates.off_peak_windows]]
start = "11:00"
end = "13:00"

[optimizer]
soc_risk_weight = 2.0
confidence_threshold = 0.3

[mqtt]
broker = "tcp://localhost:1883"
topic_prefix = "homeassistant"
device_id = "srne_system"

[[circuit]]
name = "kitchen"
entity_id = "sensor.ct_kitchen_power"
category = "routine"

[[circuit]]
name = "ac"
entity_id = "sensor.ct_ac_power"
category = "weather"
`

func TestParse(t *testing.T) {
	f, err := os.CreateTemp("", "config-*.toml")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Remove(f.Name()) }()
	_, _ = f.WriteString(testTOML)
	_ = f.Close()

	cfg, err := Parse(f.Name())
	if err != nil {
		t.Fatal(err)
	}

	if cfg.Service.PollInterval.Duration != 5*time.Minute {
		t.Errorf("poll_interval = %v, want 5m", cfg.Service.PollInterval)
	}
	if cfg.Service.PlanningHorizon.Duration != 72*time.Hour {
		t.Errorf("planning_horizon = %v, want 72h", cfg.Service.PlanningHorizon)
	}
	if cfg.Battery.CapacityKWh != 9.6 {
		t.Errorf("capacity_kwh = %v, want 9.6", cfg.Battery.CapacityKWh)
	}
	if len(cfg.Rates.OffPeakWindows) != 2 {
		t.Fatalf("off_peak_windows = %d, want 2", len(cfg.Rates.OffPeakWindows))
	}
	if cfg.Rates.OffPeakWindows[0].Start.Hour != 1 {
		t.Errorf("first window start = %v, want 01:00", cfg.Rates.OffPeakWindows[0].Start)
	}
	if len(cfg.Solcast.PollTimes) != 2 {
		t.Fatalf("poll_times = %d, want 2", len(cfg.Solcast.PollTimes))
	}
	if cfg.Solcast.PollTimes[0].Hour != 6 {
		t.Errorf("first poll_time = %v, want 06:00", cfg.Solcast.PollTimes[0])
	}
	if len(cfg.Solcast.Sites) != 2 {
		t.Fatalf("solcast sites = %d, want 2", len(cfg.Solcast.Sites))
	}
	if cfg.Solcast.Sites[0].ID != "site-123" || cfg.Solcast.Sites[1].Name != "shed" {
		t.Errorf("solcast sites = %+v, want house/site-123 + shed/site-456", cfg.Solcast.Sites)
	}
	// Time zone defaults to Asia/Tokyo when unset.
	if cfg.Location() == nil || cfg.Location().String() != "Asia/Tokyo" {
		t.Errorf("Location() = %v, want Asia/Tokyo", cfg.Location())
	}
	if len(cfg.Circuits) != 2 {
		t.Fatalf("circuits = %d, want 2", len(cfg.Circuits))
	}
	if cfg.MQTT.TopicPrefix != "homeassistant" {
		t.Errorf("topic_prefix = %q, want homeassistant", cfg.MQTT.TopicPrefix)
	}
	// Defaults
	if cfg.InfluxDB.Measurements.Power != "W" {
		t.Errorf("power measurement = %q, want W", cfg.InfluxDB.Measurements.Power)
	}

	// Actuator (timed-charge) defaults.
	if cfg.ActuatorHW.ReadBackTimeout.Duration != 45*time.Second {
		t.Errorf("read_back_timeout default = %v, want 45s", cfg.ActuatorHW.ReadBackTimeout)
	}
	if cfg.ActuatorHW.WindowInset.Duration != 5*time.Minute {
		t.Errorf("window_inset default = %v, want 5m", cfg.ActuatorHW.WindowInset)
	}
	if cfg.ActuatorHW.TimedChargeSwitch != "switch.srne_solar_system_timed_charge_grid" {
		t.Errorf("timed_charge_switch default = %q", cfg.ActuatorHW.TimedChargeSwitch)
	}
	if len(cfg.ActuatorHW.ChargeWindows) != 3 {
		t.Errorf("charge_windows default = %d, want 3", len(cfg.ActuatorHW.ChargeWindows))
	}
}

func TestRateAt(t *testing.T) {
	cfg := Rates{
		PeakRate:    0.35,
		OffPeakRate: 0.12,
		OffPeakWindows: []TimeWindow{
			{Start: TimeOfDay{1, 0}, End: TimeOfDay{5, 0}},
			{Start: TimeOfDay{11, 0}, End: TimeOfDay{13, 0}},
		},
	}

	tests := []struct {
		hour, min int
		want      float64
	}{
		{0, 0, 0.35},  // peak
		{1, 0, 0.12},  // off-peak start
		{3, 0, 0.12},  // off-peak mid
		{4, 59, 0.12}, // off-peak end
		{5, 0, 0.35},  // peak again
		{11, 30, 0.12},
		{13, 0, 0.35},
		{18, 0, 0.35},
	}
	for _, tt := range tests {
		ts := time.Date(2024, 1, 15, tt.hour, tt.min, 0, 0, time.UTC)
		got := cfg.RateAt(ts)
		if got != tt.want {
			t.Errorf("RateAt(%02d:%02d) = %v, want %v", tt.hour, tt.min, got, tt.want)
		}
	}
}

func TestTimeWindowContains(t *testing.T) {
	w := TimeWindow{Start: TimeOfDay{23, 0}, End: TimeOfDay{3, 0}} // crosses midnight
	tests := []struct {
		tod  TimeOfDay
		want bool
	}{
		{TimeOfDay{23, 0}, true},
		{TimeOfDay{0, 0}, true},
		{TimeOfDay{2, 59}, true},
		{TimeOfDay{3, 0}, false},
		{TimeOfDay{12, 0}, false},
		{TimeOfDay{22, 59}, false},
	}
	for _, tt := range tests {
		got := w.Contains(tt.tod)
		if got != tt.want {
			t.Errorf("Contains(%s) = %v, want %v", tt.tod, got, tt.want)
		}
	}
}

func TestRateAtPerWindow(t *testing.T) {
	r := Rates{
		PeakRate:    32.78,
		OffPeakRate: 21.61, // fallback
		OffPeakWindows: []TimeWindow{
			{Start: TimeOfDay{1, 0}, End: TimeOfDay{5, 0}, Rate: 21.61},   // night
			{Start: TimeOfDay{11, 0}, End: TimeOfDay{13, 0}, Rate: 19.61}, // day (cheaper)
			{Start: TimeOfDay{22, 0}, End: TimeOfDay{23, 0}},              // no rate -> fallback
		},
	}
	tests := []struct {
		hour, min int
		want      float64
	}{
		{9, 0, 32.78},   // peak
		{2, 0, 21.61},   // night window
		{12, 0, 19.61},  // day window, distinct price
		{22, 30, 21.61}, // window without explicit rate -> OffPeakRate
	}
	for _, tt := range tests {
		ts := time.Date(2024, 1, 15, tt.hour, tt.min, 0, 0, time.UTC)
		if got := r.RateAt(ts); got != tt.want {
			t.Errorf("RateAt(%02d:%02d) = %v, want %v", tt.hour, tt.min, got, tt.want)
		}
	}
}

func TestMQTTTopics(t *testing.T) {
	m := MQTT{TopicPrefix: "homeassistant", DeviceID: "srne_system"}
	got := m.CommandTopic("switch", "charge_from_mains")
	want := "homeassistant/switch/srne_system/charge_from_mains/set"
	if got != want {
		t.Errorf("CommandTopic = %q, want %q", got, want)
	}

	got = m.StateTopic("sensor", "state")
	want = "homeassistant/sensor/srne_system/state/state"
	if got != want {
		t.Errorf("StateTopic = %q, want %q", got, want)
	}
}

// parseTOML writes toml to a temp file and parses it, returning any error.
func parseTOML(t *testing.T, toml string) error {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "cfg-*.toml")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(toml); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	_, err = Parse(f.Name())
	return err
}

// TestTelescopingRejectsHalfHourOffPeakEdge: a :30 off-peak edge under an active
// telescoping grid must fail fast at load — otherwise BuildGrid panics later
// (crash-loop) on the coarse slot that straddles the edge. A uniform grid (no
// telescoping) keeps accepting sub-hour edges, unchanged.
func TestTelescopingRejectsHalfHourOffPeakEdge(t *testing.T) {
	const rates = `
[battery]
capacity_kwh = 9.6

[rates]
peak_rate = 40.0
off_peak_rate = 5.0
feed_in_rate = 1.0

[[rates.off_peak_windows]]
start = "01:30"
end = "05:00"
rate = 5.0
`
	t.Run("telescoping_rejected", func(t *testing.T) {
		err := parseTOML(t, `
[service]
planning_horizon = "6h"
near_horizon = "2h"
slot_duration = "30m"
far_slot_duration = "60m"
`+rates)
		if err == nil {
			t.Fatal("expected load to reject a :30 off-peak edge under telescoping")
		}
		if !strings.Contains(err.Error(), "01:30-05:00") || !strings.Contains(err.Error(), "whole-hour") {
			t.Errorf("error = %q, want it to name window 01:30-05:00 and the whole-hour requirement", err)
		}
	})

	t.Run("uniform_accepted", func(t *testing.T) {
		// No far_slot_duration → uniform grid → sub-hour off-peak edges are fine.
		if err := parseTOML(t, `
[service]
planning_horizon = "6h"
slot_duration = "30m"
`+rates); err != nil {
			t.Errorf("uniform grid must accept a :30 off-peak edge, got %v", err)
		}
	})
}
