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
	LoadModel     LoadModel     `toml:"load_model"`
	PVModel       PVModel       `toml:"pv_model"`
	MQTT          MQTT          `toml:"mqtt"`
	ActuatorHW    ActuatorHW    `toml:"actuator"`
	Alertmanager  Alertmanager  `toml:"alertmanager"`
	Alerts        Alerts        `toml:"alerts"`
	Circuits      []Circuit     `toml:"circuit"`
	Loads         []Load        `toml:"load"`

	// TimeZone pins all tariff-window / scheduling math. Defaults to Asia/Tokyo.
	TimeZone string `toml:"time_zone"`

	// Observe runs the optimizer and publishes the dashboard + HA decision/
	// time-remaining entities WITHOUT actuating the inverter — a supervised
	// rollout step before enabling live grid-charge control. Retained for
	// back-compat; Mode (below) is the primary control and defaults to observe.
	Observe bool `toml:"observe"`

	// Mode selects the actuator behaviour: "observe" (compute + log only, no
	// inverter writes — the safe default), "watchdog" (only the fail-safe path
	// may write; never initiates charging), or "live" (full grid-charge control).
	// Empty resolves to observe, so live actuation is only ever reached by an
	// explicit mode = "live".
	Mode string `toml:"mode"`

	loc *time.Location
}

// Location returns the configured time zone (Asia/Tokyo by default).
func (c *Config) Location() *time.Location { return c.loc }

type Service struct {
	PollInterval    Duration `toml:"poll_interval"`
	PlanningHorizon Duration `toml:"planning_horizon"`
	SlotDuration    Duration `toml:"slot_duration"`
	// NearHorizon is the extent of the fine (SlotDuration) grid; beyond it the
	// grid telescopes to FarSlotDuration out to PlanningHorizon. Unset (0) ⇒ the
	// whole horizon uses SlotDuration (uniform grid, pre-telescoping behaviour).
	NearHorizon     Duration `toml:"near_horizon"`
	FarSlotDuration Duration `toml:"far_slot_duration"`
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
	// CacheDir persists the last fetched forecast to disk so a restart reuses
	// it instead of re-fetching — Solcast's free tier enforces a low daily
	// request cap that a handful of restarts can exhaust. Defaults to
	// PVModel.DataDir, the same persistent volume the PV model and actuator
	// state already use.
	CacheDir string `toml:"cache_dir"`
}

// SolcastSite is one rooftop site; per-site forecasts are summed. Tilt/Azimuth
// (degrees; azimuth 180=south) drive the Open-Meteo tilted-irradiance fetch used
// for the far-horizon PV model.
type SolcastSite struct {
	Name    string  `toml:"name"`
	ID      string  `toml:"id"`
	Tilt    float64 `toml:"tilt"`
	Azimuth float64 `toml:"azimuth"`
}

type Weather struct {
	Latitude  float64 `toml:"latitude"`
	Longitude float64 `toml:"longitude"`
}

type Battery struct {
	CapacityKWh    float64 `toml:"capacity_kwh"`
	MaxChargeKW    float64 `toml:"max_charge_kw"`
	MaxDischargeKW float64 `toml:"max_discharge_kw"`
	SOCMin         float64 `toml:"soc_min"`
	SOCMax         float64 `toml:"soc_max"`
	Efficiency     float64 `toml:"efficiency"`
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

// OffPeakOccurrence is a concrete off-peak window resolved to absolute instants,
// with a stable ID unique to this occurrence (used to key the actuator's
// per-window blip budget and to arm the window-end boundary timer).
type OffPeakOccurrence struct {
	Start time.Time
	End   time.Time
	Rate  float64
	ID    string
}

// ActiveWindow returns the off-peak occurrence containing t as an absolute
// [Start, End) interval, or ok=false when t is peak. Windows are matched against
// today's and yesterday's anchoring so a window wrapping midnight is handled.
func (r *Rates) ActiveWindow(t time.Time) (OffPeakOccurrence, bool) {
	if r.loc != nil {
		t = t.In(r.loc)
	}
	for _, w := range r.OffPeakWindows {
		for _, dayOffset := range [...]int{0, -1} {
			day := t.AddDate(0, 0, dayOffset)
			start := time.Date(day.Year(), day.Month(), day.Day(),
				w.Start.Hour, w.Start.Minute, 0, 0, t.Location())
			end := time.Date(day.Year(), day.Month(), day.Day(),
				w.End.Hour, w.End.Minute, 0, 0, t.Location())
			if !end.After(start) { // wraps past midnight
				end = end.AddDate(0, 0, 1)
			}
			if !t.Before(start) && t.Before(end) {
				rate := w.Rate
				if rate <= 0 {
					rate = r.OffPeakRate
				}
				return OffPeakOccurrence{
					Start: start,
					End:   end,
					Rate:  rate,
					ID:    start.Format(time.RFC3339),
				}, true
			}
		}
	}
	return OffPeakOccurrence{}, false
}

type Optimizer struct {
	SOCRiskWeight float64 `toml:"soc_risk_weight"`
	// ConfidenceThreshold: below this, a load-model bucket's prediction is
	// scaled up by LoadModel.ConservativeMargin rather than trusted as-is (see
	// loadmodel.CircuitModel.bucketConfidence).
	ConfidenceThreshold float64 `toml:"confidence_threshold"`
	// MinChargeKW: a grid-charge permit must yield at least this much charge,
	// killing the "enter bypass, charge nothing" degeneracy.
	MinChargeKW float64 `toml:"min_charge_kw"`
	// BlipCost: objective penalty (currency) per bypass entry, so marginal-gain
	// windows are skipped rather than incurring a load-transfer blip.
	BlipCost float64 `toml:"blip_cost"`
	// MaxGridImportKW is the hard electrical-connection limit on grid import
	// (kW) per slot — derived from the contracted service capacity, not a
	// tuning knob (the inverter's own MAX LINE setting is a separate,
	// independently-configured guard, and may be set more conservatively than
	// the service itself). Grid import is house load plus any grid-charge power
	// combined (the battery can't discharge to the house while a grid-charge
	// permit is active), so this bounds total draw during a bypass slot, not
	// just the charge rate. Unlike PVModel.MaxPVKW (0 = unbounded, opt-in
	// ceiling), 0/unset here resolves to a safe non-zero default in finalize —
	// leaving it unconfigured is exactly the failure mode this field exists to
	// prevent.
	//
	// Default: a 12 kVA single-phase-3-wire (単相3線) service ≈ 60 A/phase at
	// 100V; 10.0 kW ≈ 50 A/phase balanced across the two lines, leaving ~10
	// A/phase margin under the 60 A service. This assumes grid draw is roughly
	// balanced across L1/L2 (the two inverter units sit on the two phases) —
	// the solver only models a single aggregate grid-import kW, so a total-kW
	// cap maps to a per-phase current cap only under that balance assumption.
	// It is necessary but not sufficient: a badly imbalanced split (e.g. one
	// unit drawing far more than the other) could still push one phase over 60
	// A while the aggregate stays under 10 kW. A true per-phase guard would be
	// more precise; this total-kW cap plus ActuatorHW.MaxChargeCurrentA (a
	// per-unit amp clamp, independent of this aggregate) are the two guards
	// until one exists.
	MaxGridImportKW float64 `toml:"max_grid_import_kw"`
}

// LoadModel tunes the load model's recency/headroom estimation (see
// loadmodel.Model). It replaces a flat arithmetic mean over the whole
// training window — which dilutes a recent step-change in household load —
// with a recency-weighted LEVEL times a percentile-headroom SHAPE.
type LoadModel struct {
	// LookbackDays is how far back Train() reads samples from the time-series
	// store; also the window the per-bucket SHAPE/percentile is computed over.
	// Default 30.
	LookbackDays float64 `toml:"lookback_days"`
	// RecencyHalfLifeDays exponentially decays a training sample's weight by
	// age when computing the model's LEVEL (the current baseline power), so a
	// step change (new appliance, occupancy change, seasonal transition) is
	// tracked within days instead of being diluted across the full lookback.
	// Default 3.
	RecencyHalfLifeDays float64 `toml:"recency_half_life_days"`
	// Percentile (0-1) is used instead of the mean for each bucket's
	// hour-of-day/season SHAPE, so peaky buckets (e.g. a kettle + induction
	// hob at breakfast) bias predictions up rather than being averaged away.
	// Default 0.75 (p75).
	Percentile float64 `toml:"percentile"`
	// ConservativeMargin multiplies a bucket's prediction when its confidence
	// is below Optimizer.ConfidenceThreshold. Default 1.3 (+30%).
	ConservativeMargin float64 `toml:"conservative_margin"`
}

// PVModel configures the persistent PV-response learning model: the far-horizon
// asset that learns the transfer function from Open-Meteo tilted irradiance to
// measured PV, improving over years. State lives under DataDir; the decayed
// per-bin statistics survive the 60-day retention of the metrics store via a
// long half-life, so the model keeps maturing across seasons.
type PVModel struct {
	DataDir         string  `toml:"data_dir"`         // persistence directory (default /data)
	HalfLifeDays    float64 `toml:"half_life_days"`   // exponential-decay half-life for bin mass (default 900 ≈ 2.5yr)
	CalibrationDays int     `toml:"calibration_days"` // rolling window for kWp_ref + cold-start ingest (default 21)
	MinSamples      float64 `toml:"min_samples"`      // decayed-effective sample count for a bin to be trusted (default 12)
	MaxPVKW         float64 `toml:"max_pv_kw"`        // physical ceiling on modeled PV output (kW); 0 = unbounded. Bounds a kWp_ref runaway from a sustained PV-sensor fault.
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

// Alertmanager configures posting decision/risk alerts to an Alertmanager
// /api/v2/alerts endpoint, which routes them to Discord (and phone-push on
// critical) via the existing home alerting pipeline. Empty URL disables posting.
type Alertmanager struct {
	URL  string `toml:"url"`  // base URL, e.g. http://alertmanager:9093
	Site string `toml:"site"` // routing label (camp|home); default "home"
}

// Alerts tunes the decision/risk notifier thresholds. Zero values fall back to
// defaults (risk_soc 0.15, expensive_day_yen 300).
type Alerts struct {
	RiskSOCThreshold float64 `toml:"risk_soc_threshold"` // projected SoC (0-1) at/below this within 24h warns
	ExpensiveDayYen  float64 `toml:"expensive_day_yen"`  // projected peak-rate import cost above this warns
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

// ChargeWindowEntities are the "HH:MM" start/end text entity IDs for one
// inverter timed-charge window slot.
type ChargeWindowEntities struct {
	Start string `toml:"start"`
	End   string `toml:"end"`
}

// ActuatorHW configures the timed-charge grid-charge actuator: the HA entities it
// writes/reads and the kW→A conversion. The ONLY working grid-charge path on this
// ASP/SRNE inverter (confirmed live) is the timed-charge mechanism: enable
// TimedChargeSwitch while inside a programmed ChargeWindow, with
// MainsChargeCurrentNumber > 0, to draw grid charge; disable the switch to stop.
// Toggling it is NOT a power blip. Entity IDs are config-driven with sensible
// SRNE-add-on defaults.
type ActuatorHW struct {
	// TimedChargeSwitch enables/disables grid timed-charging — the sole grid-charge
	// gate. The actuator guarantees it is OFF whenever it is not actively
	// commanding a charge.
	TimedChargeSwitch string `toml:"timed_charge_switch"`
	// MainsChargeCurrentNumber is the per-unit grid-charge current (A) number
	// entity. It is adjustable live while timed charge is enabled.
	MainsChargeCurrentNumber string `toml:"mains_charge_current_number"`

	// ChargeWindows are the inverter's timed-charge window slots (start/end "HH:MM"
	// text entities). They are a STATIC HARDWARE RAIL: grid charging can only occur
	// inside a programmed window, so the actuator mirrors slot i to configured
	// off-peak window i (idempotently — only writing on a mismatch) and never
	// programs a peak interval. Slots without a corresponding off-peak window are
	// left untouched; the global timed-charge switch (off outside off-peak) governs
	// whether charging actually happens.
	ChargeWindows []ChargeWindowEntities `toml:"charge_windows"`

	// WindowInset shrinks each mirrored charge window by this much at BOTH ends
	// (slot = [offpeak.start + inset, offpeak.end − inset]) — a guard against
	// inverter/tariff clock skew so hardware charging can never leak into peak.
	// Default 5m. A window shorter than 2×inset is skipped (logged), as is any
	// mirrored bound that would land on 00:00 (which the inverter may misread as a
	// wrap / zero-length window).
	WindowInset Duration `toml:"window_inset"`

	// NumUnits is the count of parallel inverter units the per-unit current is
	// applied to (the pack current is NumUnits × per-unit amps).
	NumUnits int `toml:"num_units"`
	// MaxChargeCurrentA clamps the commanded per-unit current (A). Sized as the
	// per-unit share of the electrical service ceiling: at ACChargeVoltageV·NumUnits
	// it back-stops the optimizer's MaxGridImportKW cap (50 A/unit × 2 × 103 V ≈
	// 10 kW ≈ 50 A/phase), so a solver fault can never command beyond the cap.
	MaxChargeCurrentA float64 `toml:"max_charge_current_a"`
	// ACChargeVoltageV is the AC line voltage (per split-phase leg, ~103 V) used to
	// convert a grid-charge kW target to per-unit mains_charge_current amps. The
	// mains_charge_current register is an AC-side draw limit, so the DC pack voltage
	// must NOT be used here (doing so understated the divisor ~2× and doubled the
	// commanded amps). Defaults to 103 (measured 単相3線 leg voltage).
	ACChargeVoltageV float64 `toml:"ac_charge_voltage_v"`
	// MaxChargeKW is the AGGREGATE grid-charge ceiling (kW) applied to the whole
	// pack BEFORE the kW→A conversion, so a high per-unit amp headroom can never
	// command more than the pack can accept. Defaults to Battery.MaxChargeKW (or
	// 8 kW when that too is unset). Bounds the per-unit clamp as a second limit.
	MaxChargeKW float64 `toml:"max_charge_kw"`

	// StateDir persists the actuator's charge intent across restarts. This is
	// informational only: reconcile disables timed charge on every start, so a
	// lost/corrupt file never affects safety.
	StateDir string `toml:"state_dir"`

	// WatchdogInterval is the cadence of the independent never-charging-outside-
	// off-peak safety check. WriteTimeout bounds each inverter write+ack.
	WatchdogInterval Duration `toml:"watchdog_interval"`
	WriteTimeout     Duration `toml:"write_timeout"`

	// ReadBackTimeout bounds the state-cache poll that confirms an inverter write
	// echoed back before it is retried. It MUST comfortably exceed real device
	// state-propagation latency so a slow echo is not mistaken for a dropped write.
	// The SRNE timed-charge switch/number/text entities echo ~20-40s after the
	// service ack (measured live), so the default is 45s — tune up if your feed is
	// slower. Too short here causes every legitimate write to time out and churn
	// spurious retries.
	ReadBackTimeout Duration `toml:"read_back_timeout"`

	// StateStale marks the timed-charge switch reading stale (→ watchdog disables
	// it outside a window) past this age.
	StateStale Duration `toml:"state_stale"`
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
	if c.Optimizer.ConfidenceThreshold == 0 {
		c.Optimizer.ConfidenceThreshold = 0.3
	}
	if c.Optimizer.MaxGridImportKW == 0 {
		c.Optimizer.MaxGridImportKW = 10.0
	}

	if c.LoadModel.LookbackDays == 0 {
		c.LoadModel.LookbackDays = 30
	}
	if c.LoadModel.RecencyHalfLifeDays == 0 {
		c.LoadModel.RecencyHalfLifeDays = 3
	}
	if c.LoadModel.Percentile == 0 {
		c.LoadModel.Percentile = 0.75
	}
	if c.LoadModel.ConservativeMargin == 0 {
		c.LoadModel.ConservativeMargin = 1.3
	}

	if c.PVModel.DataDir == "" {
		c.PVModel.DataDir = "/data"
	}
	if c.PVModel.HalfLifeDays == 0 {
		c.PVModel.HalfLifeDays = 900
	}
	if c.PVModel.CalibrationDays == 0 {
		c.PVModel.CalibrationDays = 21
	}
	if c.PVModel.MinSamples == 0 {
		c.PVModel.MinSamples = 12
	}

	if c.Solcast.CacheDir == "" {
		c.Solcast.CacheDir = c.PVModel.DataDir
	}

	c.finalizeActuator()

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

	if err := c.validateTelescopingGrid(); err != nil {
		return err
	}
	return nil
}

// validateTelescopingGrid checks the telescoping-grid geometry when it is active
// (near_horizon < planning_horizon). The near region must be a whole number of
// fine slots, the far region a whole number of coarse slots, and each coarse slot
// must align to the hour so it carries a single tariff rate. A uniform grid
// (near_horizon unset, >= planning_horizon, or far_slot_duration unset) skips
// these checks — it is the pre-telescoping behaviour.
func (c *Config) validateTelescopingGrid() error {
	near := c.Service.NearHorizon.Duration
	planning := c.Service.PlanningHorizon.Duration
	slot := c.Service.SlotDuration.Duration
	far := c.Service.FarSlotDuration.Duration

	if near <= 0 || near >= planning || far <= 0 {
		return nil // uniform grid — no telescoping constraints
	}
	if slot <= 0 {
		return fmt.Errorf("slot_duration must be > 0 for a telescoping grid")
	}
	if near%slot != 0 {
		return fmt.Errorf("near_horizon (%s) must be a whole multiple of slot_duration (%s)", near, slot)
	}
	if (planning-near)%far != 0 {
		return fmt.Errorf("planning_horizon − near_horizon (%s) must be a whole multiple of far_slot_duration (%s)", planning-near, far)
	}
	if time.Hour%far != 0 {
		return fmt.Errorf("far_slot_duration (%s) must divide 60m so coarse slots align to the hour", far)
	}
	// A coarse slot carries a single tariff rate only if the off-peak windows it
	// may span never change rate mid-slot. Since coarse slots are hour-aligned
	// (checked above), any off-peak edge on a :MM ≠ :00 boundary would let a slot
	// straddle it — the case BuildGrid panics on. Reject it at load so the panic
	// is unreachable via config.
	for _, w := range c.Rates.OffPeakWindows {
		if w.Start.Minute != 0 || w.End.Minute != 0 {
			edge := w.Start.Minute
			if edge == 0 {
				edge = w.End.Minute
			}
			return fmt.Errorf("far_slot_duration requires whole-hour off_peak_windows; window %s-%s has a :%02d edge", w.Start, w.End, edge)
		}
	}
	return nil
}

// finalizeActuator fills the timed-charge actuator defaults. Entity IDs default
// to the standard SRNE-add-on aggregate entities (the same srne_system device the
// MQTT actuation targets); StateDir shares the PV model's persistence dir.
func (c *Config) finalizeActuator() {
	a := &c.ActuatorHW
	if a.TimedChargeSwitch == "" {
		a.TimedChargeSwitch = "switch.srne_solar_system_timed_charge_grid"
	}
	if a.MainsChargeCurrentNumber == "" {
		a.MainsChargeCurrentNumber = "number.srne_solar_system_mains_charge_current"
	}
	if len(a.ChargeWindows) == 0 {
		a.ChargeWindows = []ChargeWindowEntities{
			{Start: "text.srne_solar_system_charge_window_1_start", End: "text.srne_solar_system_charge_window_1_end"},
			{Start: "text.srne_solar_system_charge_window_2_start", End: "text.srne_solar_system_charge_window_2_end"},
			{Start: "text.srne_solar_system_charge_window_3_start", End: "text.srne_solar_system_charge_window_3_end"},
		}
	}
	if a.NumUnits == 0 {
		a.NumUnits = 2
	}
	if a.MaxChargeCurrentA == 0 {
		a.MaxChargeCurrentA = 50 // per-unit share of the 12 kVA service (2 × 50 A × 103 V ≈ 10 kW)
	}
	if a.ACChargeVoltageV == 0 {
		a.ACChargeVoltageV = 103 // measured 単相3線 leg voltage
	}
	if a.MaxChargeKW == 0 {
		a.MaxChargeKW = c.Battery.MaxChargeKW
	}
	if a.MaxChargeKW == 0 {
		a.MaxChargeKW = 8 // pack acceptance fallback when Battery.MaxChargeKW unset
	}
	if a.StateDir == "" {
		a.StateDir = c.PVModel.DataDir
	}
	if a.WatchdogInterval.Duration == 0 {
		a.WatchdogInterval.Duration = 30 * time.Second
	}
	if a.WindowInset.Duration == 0 {
		a.WindowInset.Duration = 5 * time.Minute
	}
	if a.WriteTimeout.Duration == 0 {
		a.WriteTimeout.Duration = 10 * time.Second
	}
	if a.ReadBackTimeout.Duration == 0 {
		// SRNE echo/apply lag is ~20-40s for these entities; 45s gives margin so a
		// legitimate write confirms on its first landing rather than churning retries.
		a.ReadBackTimeout.Duration = 45 * time.Second
	}
	if a.StateStale.Duration == 0 {
		a.StateStale.Duration = 5 * time.Minute
	}
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
