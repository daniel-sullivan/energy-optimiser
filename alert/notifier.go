package alert

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"energy-optimiser/config"
	"energy-optimiser/optimizer"
)

// Notifier turns optimiser decisions and forecast risks into Alertmanager
// alerts. It is stateless: each tick it posts the set of currently-firing
// alerts with fixed label identities, and Alertmanager owns dedup, grouping,
// routing to Discord (and phone-push on critical), and auto-resolve once an
// alert stops being re-sent.
type Notifier struct {
	am        *AlertManager
	loc       *time.Location
	rates     config.Rates
	site      string
	currency  string
	capacity  float64
	slotHours float64
	resolveIn time.Duration
	riskSOC   float64
	expThresh float64
}

// NewNotifier builds a Notifier from config. A missing Alertmanager URL disables
// posting; zeroed thresholds fall back to defaults (risk_soc 0.15, threshold 300).
func NewNotifier(cfg *config.Config) *Notifier {
	slot := cfg.Service.SlotDuration.Hours()
	if slot <= 0 {
		slot = 0.5
	}
	poll := cfg.Service.PollInterval.Duration
	if poll <= 0 {
		poll = 5 * time.Minute
	}
	site := cfg.Alertmanager.Site
	if site == "" {
		site = "home"
	}
	riskSOC := cfg.Alerts.RiskSOCThreshold
	if riskSOC <= 0 {
		riskSOC = 0.15
	}
	exp := cfg.Alerts.ExpensiveDayYen
	if exp <= 0 {
		exp = 300
	}
	return &Notifier{
		am:        NewAlertManager(cfg.Alertmanager.URL),
		loc:       cfg.Location(),
		rates:     cfg.Rates,
		site:      site,
		currency:  cfg.Rates.Currency,
		capacity:  cfg.Battery.CapacityKWh,
		slotHours: slot,
		resolveIn: 3 * poll,
		riskSOC:   riskSOC,
		expThresh: exp,
	}
}

// Evaluate posts the alerts currently firing for this schedule. Conditions that
// no longer hold are simply omitted; Alertmanager resolves them after EndsAt.
func (n *Notifier) Evaluate(ctx context.Context, now time.Time, sched *optimizer.Schedule, soc float64) {
	if sched == nil || len(sched.Slots) == 0 {
		return
	}
	var alerts []Alert
	if a := n.chargeAlert(now, sched, soc); a != nil {
		alerts = append(alerts, *a)
	}
	if a := n.lowSOCAlert(now, sched); a != nil {
		alerts = append(alerts, *a)
	}
	if a := n.expensiveAlert(now, sched); a != nil {
		alerts = append(alerts, *a)
	}
	if len(alerts) == 0 {
		return
	}
	if err := n.am.Send(ctx, alerts); err != nil {
		slog.Warn("alertmanager post failed", "error", err)
	}
}

func (n *Notifier) alert(name, severity, summary string, now time.Time) Alert {
	return Alert{
		Labels: map[string]string{
			"alertname": name,
			"site":      n.site,
			"severity":  severity,
			"source":    "energy_optimiser",
		},
		Annotations: map[string]string{"summary": summary},
		StartsAt:    now,
		EndsAt:      now.Add(n.resolveIn),
	}
}

// chargeAlert fires while a grid-charge is planned — including the slot in
// progress (slot end after now), so it does not falsely resolve mid-charge. The
// summary reports the energy the imminent charge run adds to the battery (the
// SoC gain across the contiguous run × capacity).
func (n *Notifier) chargeAlert(now time.Time, sched *optimizer.Schedule, soc float64) *Alert {
	slotDur := time.Duration(n.slotHours * float64(time.Hour))
	start := -1
	for i := range sched.Slots {
		s := &sched.Slots[i]
		if s.GridCharge && s.Start.Add(slotDur).After(now) {
			start = i
			break
		}
	}
	if start < 0 {
		return nil
	}
	// Extend across the contiguous grid-charge run (this imminent episode only).
	end := start
	for end < len(sched.Slots) && sched.Slots[end].GridCharge {
		end++
	}
	socBefore := soc
	if start > 0 {
		socBefore = sched.Slots[start-1].SOC
	}
	socEnd := sched.Slots[end-1].SOC
	addedKWh := (socEnd - socBefore) * n.capacity

	a := n.alert("EnergyOptimiserGridCharge", "warning", fmt.Sprintf(
		"⚡ Grid-charge scheduled for %s — plans to add ~%.1f kWh to the battery (%.0f%% → %.0f%%).",
		sched.Slots[start].Start.In(n.loc).Format("15:04 Mon"), addedKWh, socBefore*100, socEnd*100), now)
	return &a
}

// lowSOCAlert fires when the projected SoC trough over the next 24h is at/below
// the risk threshold.
func (n *Notifier) lowSOCAlert(now time.Time, sched *optimizer.Schedule) *Alert {
	horizon := now.Add(24 * time.Hour)
	minSOC := 2.0
	var at time.Time
	for i := range sched.Slots {
		s := &sched.Slots[i]
		if s.Start.Before(now) || s.Start.After(horizon) {
			continue
		}
		if s.SOC < minSOC {
			minSOC = s.SOC
			at = s.Start
		}
	}
	if minSOC > n.riskSOC || at.IsZero() {
		return nil
	}
	a := n.alert("EnergyOptimiserLowSoC", "warning", fmt.Sprintf(
		"🪫 Battery projected to reach %.0f%% by %s — even with the planned charge.",
		minSOC*100, at.In(n.loc).Format("15:04 Mon")), now)
	return &a
}

// expensiveAlert fires when projected peak-rate grid import over the next 24h
// exceeds the configured threshold (in the tariff currency).
func (n *Notifier) expensiveAlert(now time.Time, sched *optimizer.Schedule) *Alert {
	horizon := now.Add(24 * time.Hour)
	var cost float64
	for i := range sched.Slots {
		s := &sched.Slots[i]
		if s.Start.Before(now) || s.Start.After(horizon) {
			continue
		}
		if s.GridImportKW > 0 && !n.rates.IsOffPeak(s.Start) {
			cost += s.GridImportKW * n.slotHours * n.rates.RateAt(s.Start)
		}
	}
	if cost <= n.expThresh {
		return nil
	}
	a := n.alert("EnergyOptimiserExpensiveDay", "warning", fmt.Sprintf(
		"💸 Expensive day ahead: ~%s%.0f of peak-rate grid import projected in the next 24h.",
		n.currency, cost), now)
	return &a
}
