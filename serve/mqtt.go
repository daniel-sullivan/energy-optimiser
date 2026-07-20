package serve

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"energy-optimiser/config"
	"energy-optimiser/optimizer"
)

// decisionSensorDef defines one Home Assistant MQTT-discovery entity published
// on the decision device. Every entity reads from the same JSON state payload
// via value_template (mirroring srne-solar-controller's discovery pattern).
type decisionSensorDef struct {
	Key           string
	Name          string
	Component     string // "sensor" or "binary_sensor"
	Unit          string
	DeviceClass   string
	StateClass    string
	Icon          string
	ValueTemplate string
}

// decisionSensors are published on the "energy_optimiser" device — distinct
// from the srne_system device the actuator writes into.
var decisionSensors = []decisionSensorDef{
	{
		Key: "decision_grid_charge", Name: "Grid Charge Planned", Component: "binary_sensor",
		Icon:          "mdi:transmission-tower-import",
		ValueTemplate: "{{ 'ON' if value_json.grid_charge else 'OFF' }}",
	},
	{
		Key: "decision_battery_flow_kw", Name: "Planned Battery Flow", Component: "sensor",
		Unit: "kW", DeviceClass: "power", StateClass: "measurement", Icon: "mdi:battery-sync",
		ValueTemplate: "{{ value_json.battery_flow_kw }}",
	},
	{
		Key: "decision_grid_import_kw", Name: "Planned Grid Import", Component: "sensor",
		Unit: "kW", DeviceClass: "power", StateClass: "measurement", Icon: "mdi:transmission-tower",
		ValueTemplate: "{{ value_json.grid_import_kw }}",
	},
	{
		Key: "decision_soc_target", Name: "Planned SOC (End of Slot)", Component: "sensor",
		Unit: "%", DeviceClass: "battery", StateClass: "measurement", Icon: "mdi:battery-heart",
		ValueTemplate: "{{ value_json.soc_target_pct }}",
	},
	{
		Key: "decision_objective", Name: "Schedule Objective Cost", Component: "sensor",
		Unit: "¥", StateClass: "measurement", Icon: "mdi:currency-jpy",
		ValueTemplate: "{{ value_json.objective_value }}",
	},
	{
		Key: "decision_rationale", Name: "Decision Rationale", Component: "sensor",
		Icon:          "mdi:text-box-outline",
		ValueTemplate: "{{ value_json.rationale }}",
	},
	{
		Key: "charge_time_remaining", Name: "Charge Time Remaining", Component: "sensor",
		Unit: "h", DeviceClass: "duration", StateClass: "measurement", Icon: "mdi:battery-clock",
		ValueTemplate: "{{ value_json.charge_remaining_h | default('unknown') }}",
	},
	{
		Key: "discharge_time_remaining", Name: "Discharge Time Remaining", Component: "sensor",
		Unit: "h", DeviceClass: "duration", StateClass: "measurement", Icon: "mdi:battery-clock-outline",
		ValueTemplate: "{{ value_json.discharge_remaining_h | default('unknown') }}",
	},
	{
		Key: "charge_time_remaining_formatted", Name: "Charge Time Remaining (formatted)", Component: "sensor",
		Icon:          "mdi:battery-clock",
		ValueTemplate: "{{ value_json.charge_remaining_fmt | default('unknown') }}",
	},
	{
		Key: "discharge_time_remaining_formatted", Name: "Discharge Time Remaining (formatted)", Component: "sensor",
		Icon:          "mdi:battery-clock-outline",
		ValueTemplate: "{{ value_json.discharge_remaining_fmt | default('unknown') }}",
	},
}

// DecisionState is the JSON payload published to the decision device's state
// topic; every discovery entity's value_template reads a field from here.
type DecisionState struct {
	GridCharge     bool    `json:"grid_charge"`
	BatteryFlowKW  float64 `json:"battery_flow_kw"`
	GridImportKW   float64 `json:"grid_import_kw"`
	SOCTargetPct   float64 `json:"soc_target_pct"`
	ObjectiveValue float64 `json:"objective_value"`
	Rationale      string  `json:"rationale"`

	// Time-remaining fields are omitted (not nulled) when power is too close to
	// zero to extrapolate — the value_template's `default('unknown')` then
	// renders an "unknown" state in HA rather than 0 or a stale value.
	ChargeRemainingH      *float64 `json:"charge_remaining_h,omitempty"`
	DischargeRemainingH   *float64 `json:"discharge_remaining_h,omitempty"`
	ChargeRemainingFmt    string   `json:"charge_remaining_fmt,omitempty"`
	DischargeRemainingFmt string   `json:"discharge_remaining_fmt,omitempty"`
}

// DecisionPublisher publishes the optimizer's per-tick decision (and derived
// charge/discharge time-remaining estimates) to MQTT as Home Assistant
// discovery entities, under its own device id (config.MQTT.DecisionDeviceID)
// separate from the srne_system device the actuator commands.
type DecisionPublisher struct {
	client   mqtt.Client
	prefix   string
	deviceID string
	dryRun   bool
}

// NewDecisionPublisher builds the publisher. Mirrors actuator.New: the MQTT
// client is only constructed (not connected) when not in dry-run; hub.Run
// calls Connect + PublishDiscovery once Home Assistant is up.
func NewDecisionPublisher(cfg config.MQTT, dryRun bool) (*DecisionPublisher, error) {
	deviceID := cfg.DecisionDeviceID
	if deviceID == "" {
		deviceID = "energy_optimiser"
	}
	p := &DecisionPublisher{prefix: cfg.TopicPrefix, deviceID: deviceID, dryRun: dryRun}

	if dryRun {
		slog.Info("decision publisher: dry-run mode — MQTT disabled")
		return p, nil
	}

	availTopic := fmt.Sprintf("%s/sensor/%s/availability", cfg.TopicPrefix, deviceID)
	opts := mqtt.NewClientOptions().
		AddBroker(cfg.Broker).
		SetClientID("energy-optimiser-decisions").
		SetKeepAlive(30*time.Second).
		SetAutoReconnect(true).
		SetConnectRetry(true).
		SetConnectRetryInterval(5*time.Second).
		SetWill(availTopic, "offline", 1, true)
	if cfg.Username != "" {
		opts.SetUsername(cfg.Username)
		opts.SetPassword(cfg.Password)
	}

	opts.SetConnectionLostHandler(func(_ mqtt.Client, err error) {
		slog.Warn("decision publisher: mqtt connection lost", "error", err)
	})

	p.client = mqtt.NewClient(opts)
	return p, nil
}

// Connect opens the MQTT session. No-op in dry-run or if construction skipped
// the client.
func (p *DecisionPublisher) Connect() error {
	if p.dryRun || p.client == nil {
		return nil
	}
	tok := p.client.Connect()
	tok.Wait()
	if err := tok.Error(); err != nil {
		return fmt.Errorf("decision publisher mqtt connect: %w", err)
	}
	p.publishAvailability("online")
	return nil
}

// Close marks the device offline and disconnects.
func (p *DecisionPublisher) Close() {
	if p.client == nil {
		return
	}
	p.publishAvailability("offline")
	p.client.Disconnect(1000)
}

// PublishDiscovery publishes retained HA MQTT-discovery configs for every
// decision entity. No-op in dry-run.
func (p *DecisionPublisher) PublishDiscovery() {
	if p.dryRun || p.client == nil {
		return
	}
	for _, s := range decisionSensors {
		data, topic := discoveryPayload(p.prefix, p.deviceID, s)
		p.client.Publish(topic, 1, true, data)
	}
}

// discoveryPayload builds one entity's discovery config JSON and its config
// topic. Split out from PublishDiscovery so the shape is unit-testable
// without a broker connection.
func discoveryPayload(prefix, deviceID string, s decisionSensorDef) ([]byte, string) {
	stateTopic := fmt.Sprintf("%s/sensor/%s/state", prefix, deviceID)
	availTopic := fmt.Sprintf("%s/sensor/%s/availability", prefix, deviceID)
	configTopic := fmt.Sprintf("%s/%s/%s/%s/config", prefix, s.Component, deviceID, s.Key)

	device := map[string]any{
		"identifiers":  []string{deviceID},
		"name":         "Energy Optimiser",
		"manufacturer": "energy-optimiser",
	}

	payload := map[string]any{
		"name":               s.Name,
		"unique_id":          fmt.Sprintf("%s_%s", deviceID, s.Key),
		"state_topic":        stateTopic,
		"value_template":     s.ValueTemplate,
		"availability_topic": availTopic,
		"device":             device,
	}
	if s.Unit != "" {
		payload["unit_of_measurement"] = s.Unit
	}
	if s.DeviceClass != "" {
		payload["device_class"] = s.DeviceClass
	}
	if s.StateClass != "" {
		payload["state_class"] = s.StateClass
	}
	if s.Icon != "" {
		payload["icon"] = s.Icon
	}
	if s.Component == "binary_sensor" {
		payload["payload_on"] = "ON"
		payload["payload_off"] = "OFF"
	}

	data, _ := json.Marshal(payload)
	return data, configTopic
}

// PublishState publishes the current decision snapshot. In dry-run it only
// logs what would have been sent.
func (p *DecisionPublisher) PublishState(state DecisionState) {
	data, err := json.Marshal(state)
	if err != nil {
		slog.Error("decision publisher: state marshal failed", "error", err)
		return
	}
	if p.dryRun || p.client == nil {
		slog.Info("DRY-RUN: would publish decision state",
			"grid_charge", state.GridCharge, "rationale", state.Rationale)
		return
	}
	topic := fmt.Sprintf("%s/sensor/%s/state", p.prefix, p.deviceID)
	p.client.Publish(topic, 0, false, data)
}

func (p *DecisionPublisher) publishAvailability(status string) {
	if p.client == nil {
		return
	}
	topic := fmt.Sprintf("%s/sensor/%s/availability", p.prefix, p.deviceID)
	p.client.Publish(topic, 1, true, status)
}

// timeRemainingPowerEpsilon guards near-zero battery power: below this
// magnitude (kW) the extrapolated time-remaining blows up or flips sign
// noisily, so we report "unknown" instead (via omitted fields).
const timeRemainingPowerEpsilon = 0.05

// TimeRemaining estimates charge/discharge time remaining in hours from the
// live battery SoC (fraction 0-1) and signed battery power (kW,
// positive=charging, negative=discharging), using the optimiser's own
// battery config (capacity, SOC band). Returns nil for whichever direction
// doesn't apply (including when power is too close to zero to extrapolate).
func TimeRemaining(cfg config.Battery, socFrac, powerKW float64) (chargeH, dischargeH *float64) {
	switch {
	case powerKW > timeRemainingPowerEpsilon:
		h := (cfg.SOCMax - socFrac) * cfg.CapacityKWh / powerKW
		if h < 0 {
			h = 0
		}
		chargeH = &h
	case powerKW < -timeRemainingPowerEpsilon:
		h := (socFrac - cfg.SOCMin) * cfg.CapacityKWh / -powerKW
		if h < 0 {
			h = 0
		}
		dischargeH = &h
	}
	return chargeH, dischargeH
}

// FormatHours renders a fractional-hours duration as "Xd Yh Zm" (dropping
// leading zero components), mirroring camp's time-remaining presentation.
func FormatHours(h float64) string {
	if h < 0 {
		h = 0
	}
	totalMin := int(math.Round(h * 60))
	const minPerDay = 24 * 60
	d := totalMin / minPerDay
	rem := totalMin % minPerDay
	hh := rem / 60
	mm := rem % 60

	switch {
	case d > 0:
		return fmt.Sprintf("%dd %dh %dm", d, hh, mm)
	case hh > 0:
		return fmt.Sprintf("%dh %dm", hh, mm)
	default:
		return fmt.Sprintf("%dm", mm)
	}
}

// RationaleFor generates a human-readable explanation of the schedule's
// grid-charge decision: the forecast SoC trough (and when it occurs) and the
// next planned grid-charge slot, both restricted to slots at/after now.
// currentSOC (fraction 0-1) is used only for the no-schedule fallback.
func RationaleFor(now time.Time, currentSOC float64, sched *optimizer.Schedule) string {
	if sched == nil || len(sched.Slots) == 0 {
		return "No schedule available"
	}

	var trough, nextCharge *optimizer.Slot
	for i := range sched.Slots {
		s := &sched.Slots[i]
		if s.Start.Before(now) {
			continue
		}
		if trough == nil || s.SOC < trough.SOC {
			trough = s
		}
		if nextCharge == nil && s.GridCharge {
			nextCharge = s
		}
	}

	if trough == nil {
		return fmt.Sprintf("No schedule available (SoC %.0f%%)", currentSOC*100)
	}
	if nextCharge == nil {
		return "No grid charge scheduled; solar covers the horizon"
	}
	return fmt.Sprintf("SoC forecast %.0f%% by %s -> scheduling %s charge",
		trough.SOC*100, trough.Start.Format("15:04"), nextCharge.Start.Format("15:04"))
}
