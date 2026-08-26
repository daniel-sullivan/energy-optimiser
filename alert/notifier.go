package alert

import (
	"context"
	"fmt"
	"sync"
	"time"

	"energy-optimiser/config"
	"energy-optimiser/optimizer"
)

// ChargeContext describes the plan that initiated a confirmed grid-charge
// episode. It is used only to freeze the initial alert summary.
type ChargeContext struct {
	Start      time.Time
	InitialSOC float64
	TargetSOC  float64
}

// ChargeState is the fresh observed timed-charge switch state.
type ChargeState int

const (
	ChargeUnknown ChargeState = iota
	ChargeOff
	ChargeOn
)

// ChargeStatus separates the observed physical state from the forecast schedule.
// Context is relevant only when an observed-on episode first starts.
type ChargeStatus struct {
	State   ChargeState
	Context *ChargeContext
}

// Notifier turns confirmed grid-charge state and forecast risks into Alertmanager
// alerts. Alertmanager owns deduplication, grouping, routing, and expiry.
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

	sendMu sync.Mutex

	chargeEpisode     *Alert
	pendingFiring     *Alert
	staleClearPending bool
	staleCleared      bool
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

// Lifecycle posts one observed grid-charge lifecycle transition or lease refresh.
// Deliveries are serialized, but the state lock is never held across HTTP.
func (n *Notifier) Lifecycle(ctx context.Context, now time.Time, status ChargeStatus) error {
	n.sendMu.Lock()
	defer n.sendMu.Unlock()

	snapshot := n.prepareLifecycle(now, status)
	if status.State == ChargeOn && n.chargeEpisode == nil && n.pendingFiring == nil && snapshot.candidate != nil {
		a := *snapshot.candidate
		n.pendingFiring = &a
	}
	if snapshot.candidate == nil {
		n.applyLifecycleSnapshot(snapshot)
		return nil
	}
	if err := n.am.Send(ctx, []Alert{*snapshot.candidate}); err != nil {
		return err
	}
	n.applyLifecycleSnapshot(snapshot)
	return nil
}

func (n *Notifier) applyLifecycleSnapshot(snapshot lifecycleSnapshot) {
	n.pendingFiring = snapshot.nextPendingFiring
	n.chargeEpisode = snapshot.nextEpisode
	n.staleClearPending = snapshot.nextStaleClearPending
	n.staleCleared = snapshot.nextStaleCleared
}

// ResolveGridCharge clears the fixed fingerprint that may have survived a prior
// process. Failure leaves the clear pending; a later fresh Off retries it.
func (n *Notifier) ResolveGridCharge(ctx context.Context, now time.Time) error {
	n.sendMu.Lock()
	defer n.sendMu.Unlock()

	if n.chargeEpisode == nil && n.pendingFiring == nil && !n.staleCleared {
		n.staleClearPending = true
	}
	snapshot := n.prepareLifecycle(now, ChargeStatus{State: ChargeOff})
	if snapshot.candidate == nil {
		n.applyLifecycleSnapshot(snapshot)
		return nil
	}
	if err := n.am.Send(ctx, []Alert{*snapshot.candidate}); err != nil {
		return err
	}
	n.applyLifecycleSnapshot(snapshot)
	return nil
}

type lifecycleSnapshot struct {
	candidate             *Alert
	nextPendingFiring     *Alert
	nextEpisode           *Alert
	nextStaleClearPending bool
	nextStaleCleared      bool
}

func (n *Notifier) prepareLifecycle(now time.Time, status ChargeStatus) lifecycleSnapshot {
	if status.State == ChargeOn {
		base := n.chargeEpisode
		if base == nil {
			base = n.pendingFiring
		}
		if base == nil {
			a := n.newChargeEpisode(now, status.Context)
			base = &a
		}
		a := *base
		a.EndsAt = now.Add(n.resolveIn)
		return lifecycleSnapshot{
			candidate:        &a,
			nextEpisode:      &a,
			nextStaleCleared: false,
		}
	}

	if status.State == ChargeUnknown {
		base := n.chargeEpisode
		if base == nil {
			base = n.pendingFiring
		}
		if base == nil {
			return lifecycleSnapshot{
				nextStaleClearPending: n.staleClearPending,
				nextStaleCleared:      n.staleCleared,
			}
		}
		a := *base
		a.EndsAt = now.Add(n.resolveIn)
		return lifecycleSnapshot{
			candidate:             &a,
			nextEpisode:           &a,
			nextStaleClearPending: n.staleClearPending,
			nextStaleCleared:      n.staleCleared,
		}
	}

	if n.chargeEpisode != nil {
		a := *n.chargeEpisode
		a.EndsAt = now
		return lifecycleSnapshot{
			candidate:        &a,
			nextStaleCleared: true,
		}
	}
	if n.pendingFiring != nil {
		a := *n.pendingFiring
		a.EndsAt = now
		return lifecycleSnapshot{
			candidate:        &a,
			nextStaleCleared: true,
		}
	}
	if !n.staleClearPending || n.staleCleared {
		return lifecycleSnapshot{
			nextStaleClearPending: n.staleClearPending,
			nextStaleCleared:      n.staleCleared,
		}
	}
	a := n.alert("EnergyOptimiserGridCharge", "warning", "⚡ Grid charging observed.", now)
	a.EndsAt = now
	return lifecycleSnapshot{
		candidate:        &a,
		nextStaleCleared: true,
	}
}

func (n *Notifier) newChargeEpisode(now time.Time, chargeContext *ChargeContext) Alert {
	summary := "⚡ Grid charging observed."
	if chargeContext != nil {
		start := chargeContext.Start
		if start.IsZero() {
			start = now
		}
		addedKWh := (chargeContext.TargetSOC - chargeContext.InitialSOC) * n.capacity
		summary = fmt.Sprintf(
			"⚡ Grid charging observed since %s — plan at start was ~%.1f kWh (%.0f%% → %.0f%%).",
			start.In(n.loc).Format("15:04 Mon"), addedKWh, chargeContext.InitialSOC*100, chargeContext.TargetSOC*100)
	}
	return n.alert("EnergyOptimiserGridCharge", "warning", summary, now)
}

// Forecast posts forecast-derived alerts using the slot-aligned decision time.
func (n *Notifier) Forecast(ctx context.Context, decisionNow time.Time, sched *optimizer.Schedule) error {
	if sched == nil || len(sched.Slots) == 0 {
		return nil
	}
	var alerts []Alert
	if a := n.lowSOCAlert(decisionNow, sched); a != nil {
		alerts = append(alerts, *a)
	}
	if a := n.expensiveAlert(decisionNow, sched); a != nil {
		alerts = append(alerts, *a)
	}
	n.sendMu.Lock()
	defer n.sendMu.Unlock()
	return n.am.Send(ctx, alerts)
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
