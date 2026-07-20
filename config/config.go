package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
)

type Config struct {
	Service       Service       `toml:"service"`
	InfluxDB      InfluxDB      `toml:"influxdb"`
	HomeAssistant HomeAssistant `toml:"homeassistant"`
	Solcast       Solcast       `toml:"solcast"`
	Weather       Weather       `toml:"weather"`
	Battery       Battery       `toml:"battery"`
	Rates         Rates         `toml:"rates"`
	Optimizer     Optimizer     `toml:"optimizer"`
	MQTT          MQTT          `toml:"mqtt"`
	Circuits      []Circuit     `toml:"circuit"`
	Loads         []Load        `toml:"load"`

	// TimeZone pins all tariff-window / scheduling math. Defaults to Asia/Tokyo.
	TimeZone string `toml:"time_zone"`

	loc *time.Location
}

// Location returns the configured time zone (Asia/Tokyo by default).
func (c *Config) Location() *time.Location { return c.loc }

type Service struct {
	PollInterval    Duration `toml:"poll_interval"`
	PlanningHorizon Duration `toml:"planning_horizon"`
	SlotDuration    Duration `toml:"slot_duration"`
	WebPort         int      `toml:"web_port"`
}

type InfluxDB struct {
	URL          string       `toml:"url"`
	Token        string       `toml:"token"`
	Database     string       `toml:"database"`
	Measurements Measurements `toml:"measurements"`
}

// Measurements maps data types to InfluxDB table names.
// HA's default InfluxDB integration uses unit_of_measurement as the table name.
type Measurements struct {
	Power       string `toml:"power"`       // default "W"
	Temperature string `toml:"temperature"` // default "°C"
	Percentage  string `toml:"percentage"`  // default "%"
}

type HomeAssistant struct {
	URL      string     `toml:"url"`
	Token    string     `toml:"token"`
	Entities HAEntities `toml:"entities"`
}

type HAEntities struct {
	BatterySOC   string `toml:"battery_soc"`
	BatteryPower string `toml:"battery_power"` // signed: positive=charging, negative=discharging
	PVPower      string `toml:"pv_power"`
	GridPower    string `toml:"grid_power"`
	LoadPower    string `toml:"load_power"`
}

type Solcast struct {
	APIKey    string        `toml:"api_key"`
	Sites     []SolcastSite `toml:"sites"`
	PollTimes []TimeOfDay   `toml:"poll_times"`
}

// SolcastSite is one rooftop site; per-site forecasts are summed.
type SolcastSite struct {
	Name string `toml:"name"`
	ID   string `toml:"id"`
}

type Weather struct {
	Latitude  float64 `toml:"latitude"`
	Longitude float64 `toml:"longitude"`
}

type Battery struct {
	CapacityKWh     float64 `toml:"capacity_kwh"`
	MaxChargeKW     float64 `toml:"max_charge_kw"`
	MaxDischargeKW  float64 `toml:"max_discharge_kw"`
	SOCMin          float64 `toml:"soc_min"`
	SOCMax          float64 `toml:"soc_max"`
	Efficiency      float64 `toml:"efficiency"`
	NominalVoltageV float64 `toml:"nominal_voltage_v"` // for kW→A actuation (Phase 1)
}

type Rates struct {
	Currency       string       `toml:"currency"`
	PeakRate       float64      `toml:"peak_rate"`
	OffPeakRate    float64      `toml:"off_peak_rate"`
	FeedInRate     float64      `toml:"feed_in_rate"`
	OffPeakWindows []TimeWindow `toml:"off_peak_windows"`

	loc *time.Location
}

type TimeWindow struct {
	Start TimeOfDay `toml:"start"`
	End   TimeOfDay `toml:"end"`
	// Rate is this window's price (¥/kWh); falls back to Rates.OffPeakRate when 0.
	Rate float64 `toml:"rate"`
}

// Contains reports whether a time-of-day falls within this window.
func (w TimeWindow) Contains(t TimeOfDay) bool {
	start := w.Start.ToDuration()
	end := w.End.ToDuration()
	tod := t.ToDuration()
	if start < end {
		return tod >= start && tod < end
	}
	return tod >= start || tod < end
}

// localTOD converts an instant to a time-of-day in the configured zone.
func (r *Rates) localTOD(t time.Time) TimeOfDay {
	if r.loc != nil {
		t = t.In(r.loc)
	}
	return TimeOfDay{Hour: t.Hour(), Minute: t.Minute()}
}

// RateAt returns the electricity rate (¥/kWh) applicable at time t.
func (r *Rates) RateAt(t time.Time) float64 {
	tod := r.localTOD(t)
	for _, w := range r.OffPeakWindows {
		if w.Contains(tod) {
			if w.Rate > 0 {
				return w.Rate
			}
			return r.OffPeakRate
		}
	}
	return r.PeakRate
}

// IsOffPeak reports whether time t falls in an off-peak window.
func (r *Rates) IsOffPeak(t time.Time) bool {
	tod := r.localTOD(t)
	for _, w := range r.OffPeakWindows {
		if w.Contains(tod) {
			return true
		}
	}
	return false
}

type Optimizer struct {
	SOCRiskWeight       float64 `toml:"soc_risk_weight"`
	ConfidenceThreshold float64 `toml:"confidence_threshold"`
	// MinChargeKW: a grid-charge permit must yield at least this much charge,
	// killing the "enter bypass, charge nothing" degeneracy.
	MinChargeKW float64 `toml:"min_charge_kw"`
	// BlipCost: objective penalty (currency) per bypass entry, so marginal-gain
	// windows are skipped rather than incurring a load-transfer blip.
	BlipCost float64 `toml:"blip_cost"`
}

type MQTT struct {
	Broker      string `toml:"broker"`
	TopicPrefix string `toml:"topic_prefix"`
	DeviceID    string `toml:"device_id"` // actuation target — the SRNE controller's MQTT device (e.g. srne_system). Do not repurpose for decision publishing.
	Username    string `toml:"username"`
	Password    string `toml:"password"`

	// DecisionDeviceID is the HA-discovery device this binary's own decision
	// publisher (grid-charge plan, time-remaining) writes under. Deliberately
	// separate from DeviceID: the actuator targets the SRNE controller's
	// device, this is energy-optimiser's own device.
	DecisionDeviceID string `toml:"decision_device_id"`
}

// CommandTopic returns the MQTT topic for a command.
// e.g., CommandTopic("switch", "charge_from_mains") -> "homeassistant/switch/srne_system/charge_from_mains/set"
func (m MQTT) CommandTopic(component, key string) string {
	return fmt.Sprintf("%s/%s/%s/%s/set", m.TopicPrefix, component, m.DeviceID, key)
}

// StateTopic returns the MQTT topic for reading state.
func (m MQTT) StateTopic(component, key string) string {
	return fmt.Sprintf("%s/%s/%s/%s/state", m.TopicPrefix, component, m.DeviceID, key)
}

type Circuit struct {
	Name     string `toml:"name"`
	EntityID string `toml:"entity_id"`
	Category string `toml:"category"` // fixed, routine, weather, deferrable
}

type Load struct {
	Name          string    `toml:"name"`
	Entity        string    `toml:"entity"`
	Priority      int       `toml:"priority"`
	EnergyKWh     float64   `toml:"energy_kwh"`
	Deadline      TimeOfDay `toml:"deadline"`
	MinRunMinutes int       `toml:"min_run_minutes"`
	AvoidWindows  []string  `toml:"avoid_windows"`
}

// Duration wraps time.Duration for TOML unmarshaling from strings like "5m".
type Duration struct {
	time.Duration
}

func (d *Duration) UnmarshalText(text []byte) error {
	var err error
	d.Duration, err = time.ParseDuration(string(text))
	return err
}

func (d Duration) MarshalText() ([]byte, error) {
	return []byte(d.String()), nil
}

// TimeOfDay represents a time as HH:MM.
type TimeOfDay struct {
	Hour   int
	Minute int
}

func (t *TimeOfDay) UnmarshalText(text []byte) error {
	n, err := fmt.Sscanf(string(text), "%d:%d", &t.Hour, &t.Minute)
	if err != nil || n != 2 {
		return fmt.Errorf("invalid time of day %q", text)
	}
	if t.Hour < 0 || t.Hour > 23 || t.Minute < 0 || t.Minute > 59 {
		return fmt.Errorf("time of day out of range: %s", text)
	}
	return nil
}

func (t TimeOfDay) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

func (t TimeOfDay) String() string {
	return fmt.Sprintf("%02d:%02d", t.Hour, t.Minute)
}

func (t TimeOfDay) ToDuration() time.Duration {
	return time.Duration(t.Hour)*time.Hour + time.Duration(t.Minute)*time.Minute
}

// Parse reads and parses a TOML config file.
func Parse(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config: %w", err)
	}
	var cfg Config
	if err := toml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing config: %w", err)
	}
	if err := cfg.finalize(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (c *Config) finalize() error {
	if c.InfluxDB.Measurements.Power == "" {
		c.InfluxDB.Measurements.Power = "W"
	}
	if c.InfluxDB.Measurements.Temperature == "" {
		c.InfluxDB.Measurements.Temperature = "°C"
	}
	if c.InfluxDB.Measurements.Percentage == "" {
		c.InfluxDB.Measurements.Percentage = "%"
	}
	if c.MQTT.TopicPrefix == "" {
		c.MQTT.TopicPrefix = "homeassistant"
	}
	if c.MQTT.DeviceID == "" {
		c.MQTT.DeviceID = "srne_system"
	}
	if c.MQTT.DecisionDeviceID == "" {
		c.MQTT.DecisionDeviceID = "energy_optimiser"
	}
	if c.Optimizer.MinChargeKW == 0 {
		c.Optimizer.MinChargeKW = 1.0
	}
	if c.Optimizer.BlipCost == 0 {
		c.Optimizer.BlipCost = 5.0
	}

	if c.TimeZone == "" {
		c.TimeZone = "Asia/Tokyo"
	}
	loc, err := time.LoadLocation(c.TimeZone)
	if err != nil {
		return fmt.Errorf("loading time zone %q: %w", c.TimeZone, err)
	}
	c.loc = loc
	c.Rates.loc = loc

	// Resolve `env:VARNAME` secret indirection so tokens stay out of the config file.
	if c.HomeAssistant.Token, err = resolveSecret(c.HomeAssistant.Token); err != nil {
		return fmt.Errorf("homeassistant token: %w", err)
	}
	if c.InfluxDB.Token, err = resolveSecret(c.InfluxDB.Token); err != nil {
		return fmt.Errorf("influxdb token: %w", err)
	}
	if c.MQTT.Password, err = resolveSecret(c.MQTT.Password); err != nil {
		return fmt.Errorf("mqtt password: %w", err)
	}

	// Guard against an import→export money pump: feed-in must be cheaper than the
	// cheapest EFFECTIVE off-peak import (per-window rate, or OffPeakRate fallback).
	minOff := c.Rates.OffPeakRate
	first := true
	for _, w := range c.Rates.OffPeakWindows {
		r := w.Rate
		if r <= 0 {
			r = c.Rates.OffPeakRate
		}
		if first || r < minOff {
			minOff, first = r, false
		}
	}
	if c.Rates.FeedInRate >= minOff {
		return fmt.Errorf("feed_in_rate (%.2f) must be < cheapest off-peak rate (%.2f)", c.Rates.FeedInRate, minOff)
	}
	return nil
}

// resolveSecret expands an `env:VARNAME` value from the environment; plain
// values pass through unchanged.
func resolveSecret(v string) (string, error) {
	name, ok := strings.CutPrefix(v, "env:")
	if !ok {
		return v, nil
	}
	val := os.Getenv(name)
	if val == "" {
		return "", fmt.Errorf("environment variable %s is empty or unset", name)
	}
	return val, nil
}
