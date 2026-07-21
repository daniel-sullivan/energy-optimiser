package actuator

import "fmt"

// Mode selects how much of the actuator is permitted to write to the inverter.
// The zero value is ModeObserve — the safest — so an unset mode never actuates.
type Mode int

const (
	// ModeObserve computes and logs the intended action but performs no inverter
	// writes whatsoever. The default.
	ModeObserve Mode = iota
	// ModeWatchdogOnly permits only the fail-safe path to write (force battery-
	// priority + zero charge if the inverter is found stuck in bypass). It never
	// initiates charging.
	ModeWatchdogOnly
	// ModeLive grants full grid-charge control.
	ModeLive
)

func (m Mode) String() string {
	switch m {
	case ModeObserve:
		return "observe"
	case ModeWatchdogOnly:
		return "watchdog"
	case ModeLive:
		return "live"
	default:
		return fmt.Sprintf("Mode(%d)", int(m))
	}
}

// actuates reports whether policy (grid-charge) writes are permitted.
func (m Mode) actuates() bool { return m == ModeLive }

// mayWrite reports whether ANY inverter write is permitted (the fail-safe path
// in watchdog mode, or full control in live).
func (m Mode) mayWrite() bool { return m == ModeWatchdogOnly || m == ModeLive }

// ParseMode maps a config string to a Mode. An empty string resolves to
// ModeObserve so live actuation is only ever reached by an explicit "live".
func ParseMode(s string) (Mode, error) {
	switch s {
	case "", "observe":
		return ModeObserve, nil
	case "watchdog", "watchdog_only":
		return ModeWatchdogOnly, nil
	case "live":
		return ModeLive, nil
	default:
		return ModeObserve, fmt.Errorf("invalid actuator mode %q (want observe|watchdog|live)", s)
	}
}

// ResolveMode derives the effective mode from config. A --dry-run flag forces
// ModeObserve (read-only, overriding everything). Otherwise the explicit mode
// string wins; when it is empty the legacy observe bool is honoured but can only
// select observe (never live) — going live requires an explicit mode = "live".
func ResolveMode(modeStr string, observe, dryRun bool) (Mode, error) {
	if dryRun {
		return ModeObserve, nil
	}
	m, err := ParseMode(modeStr)
	if err != nil {
		return ModeObserve, err
	}
	// Legacy observe=true can only ever tighten to observe, never loosen.
	if observe && m == ModeLive {
		return ModeObserve, fmt.Errorf("config conflict: observe = true with mode = \"live\"; refusing to actuate")
	}
	return m, nil
}
