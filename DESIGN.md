# Energy Optimiser — Design

Architecture and formulation reference. See [README.md](README.md) for
installation and usage.

## System Overview

```
┌───────────────────────────────────────────────────────────────────┐
│                          Go Service (hub)                         │
│                                                                    │
│  ┌─────────────┐   ┌──────────────────┐   ┌────────────────────┐ │
│  │  Solcast     │   │  Load Model      │   │  Home Assistant    │ │
│  │  forecast    │   │  hour×dow×season │   │  WebSocket         │ │
│  │  (72h, cached│   │  buckets, trained│   │  live state:       │ │
│  │  multi-site) │   │  from the time-  │   │  SOC, PV, grid,    │ │
│  │              │   │  series store    │   │  load, batt power  │ │
│  └──────┬───────┘   └────────┬─────────┘   └──────────┬──────────┘ │
│         │                    │                         │           │
│         └──────────┬─────────┴─────────────────────────┘           │
│                     ▼                                              │
│  ┌─────────────────────────────────────────────────────────────┐  │
│  │              MILP Optimizer (go-milp, pure Go)               │  │
│  │                                                                │
│  │  T = 144 slots (72h × 30min), re-solved every poll_interval   │
│  │  (default 5 min). Variables, objective, and constraints       │
│  │  below.                                                        │
│  └───────────────────────────┬────────────────────────────────┘  │
│                               │ current slot's decision            │
│         ┌─────────────────────┼─────────────────────┐              │
│         ▼                     ▼                     ▼              │
│  ┌─────────────┐   ┌────────────────────┐   ┌─────────────────┐   │
│  │  Actuator    │   │  Decision publisher │   │  Dashboard      │   │
│  │  MQTT:       │   │  MQTT discovery:    │   │  HTMX + SSE +   │   │
│  │  charge_from │   │  plan, rationale,   │   │  hub pub/sub    │   │
│  │  _mains → the│   │  time-remaining     │   │                 │   │
│  │  inverter    │   │  (own device)       │   │                 │   │
│  │  controller  │   │                     │   │                 │   │
│  └─────────────┘   └────────────────────┘   └─────────────────┘   │
└──────────┬──────────────────────────┬───────────────────────────────┘
           │                          │
   Time-series store            Home Assistant
   (VictoriaMetrics /            (WebSocket +
   InfluxDB line proto)           MQTT)
```

The hub's tick loop (`hub.Hub.tick`): refresh solar forecast if a poll time
has passed → read live SOC from HA → predict load for every slot → build and
solve the MILP → actuate the current slot's grid-charge decision → record the
decision → publish the MQTT decision/discovery state → broadcast to SSE
subscribers.

## Data Spine

- **Time-series store** (`influx/`) — queries a VictoriaMetrics instance (or
  anything speaking the same wire protocol: InfluxDB line protocol for
  writes, the `/api/v1/export` endpoint for reads) fed by Home Assistant's
  native history integration. A HA sensor with `unit_of_measurement="W"` and
  `entity_id="sensor.load_power"` becomes the series
  `W_value{entity_id="load_power"}`; the client resolves data-type →
  measurement-name mappings from config (`power`, `temperature`,
  `percentage`) so it works whether HA is configured against InfluxDB 1.x/3.x
  or a VictoriaMetrics endpoint speaking the same protocol.
- **Home Assistant WebSocket** (`ha/`) — one persistent connection for live
  state (battery SOC/power, PV, grid, load) and, in future, service calls for
  deferrable loads. History is never read this way — that's the time-series
  store's job.
- **Solcast** (`forecast/`) — per-site rooftop PV forecasts, summed across
  sites into one system-level profile, polled at configured times (free
  hobbyist tier: a handful of calls/day) and cached for the full horizon.
- **Open-Meteo** (`forecast/`) — a free weather client (temperature, cloud
  cover) is wired up but not yet consumed by the load model; it's a
  reserved input for a future weather-driven load category (see below).

## Load Model

`loadmodel/` trains one primary "whole-house" model (plus optional
per-circuit models, currently unused for prediction) from historical power
samples, bucketed by:

- **Fixed** categories: `hour × season`
- **Routine** categories: `hour × day-of-week × season`

Each bucket predicts a **LEVEL × SHAPE** product rather than a flat mean over
the whole lookback window (which dilutes a real step-change in household
baseline — e.g. a doubled load over a few days gets averaged against 30 days
of the old baseline): LEVEL is an exponentially recency-decayed mean across
all samples (`recency_half_life_days`, default 3), tracking the current
baseline within days; SHAPE is each bucket's high-percentile
(`percentile`, default p75) relative to the wide-window overall mean, so
peaky hour-of-day patterns bias predictions up instead of averaging spikes
away. Prediction looks up the bucket for the target slot's time, falling back
to a day-of-week-agnostic bucket and then the recency-tracked LEVEL (or a
hardcoded default with no data at all) if a bucket has fewer than 3 samples.
A per-bucket **confidence** score (sample-count coverage × a distribution-
shift signal — how far the recency LEVEL has drifted from the wide-window
mean) gates `optimizer.confidence_threshold`: a low-confidence bucket's
prediction is scaled up by `load_model.conservative_margin` rather than
trusted as-is. The same shift-aware score feeds `Model.Confidence()` (the
dashboard/rationale score); a `weather` circuit category and per-circuit
deferrable-load splitting exist as
extension points but aren't populated by the current training loop.

## MILP Formulation

Solved with [go-milp](https://github.com/daniel-sullivan/go-milp) — a pure
Go, zero-cgo MILP solver (bounded-variable revised simplex + branch-and-
bound). One solve per tick, T = `planning_horizon / slot_duration` slots
(144 by default: 72h at 30-minute resolution).

### Variables (per slot t)

| Variable | Type | Meaning |
|---|---|---|
| `gridCharge[t]` | binary (off-peak) / fixed 0 (peak) | Permit to draw grid power for charging this slot |
| `charge[t]` | continuous, `[0, maxChargeKW]` | Battery charge power |
| `discharge[t]` | continuous, `[0, maxDischargeKW]` | Battery discharge power |
| `import[t]` | continuous, `≥ 0` | Grid import power (soft-capped at the electrical service limit via `over[t]`) |
| `export[t]` | continuous, `≥ 0` | Grid export power |
| `over[t]` | continuous, `≥ 0` | Grid import above `max_grid_import_kw` (soft-cap slack; allocated only when a cap is set) |
| `start[t]` | continuous, `[0, 1]` | 1 if this slot begins a new bypass entry (grid-charge turning on) |
| `soc[t]` | continuous, `[socMin, socMax]` | Battery state of charge at the end of slot t (soc[0] fixed to the current, clamped reading) |
| `penLow/penMed/penHigh[t]` | continuous, `≥ 0` | SOC-risk penalty slack variables |

Round-trip efficiency is split into symmetric charge/discharge factors
(`ηc = ηd = √efficiency`) so charging gains less and discharging loses more,
multiplying back to the configured round-trip efficiency.

### Objective (minimize)

```
Σ import[t]·rate[t]            grid energy cost
− Σ export[t]·feedInRate        export revenue (0 = curtailment)
+ Σ start[t]·blipCost           penalty per bypass entry
+ Σ (penLow[t]·wLow + penMed[t]·wMed + penHigh[t]·wHigh)·socRiskWeight·peakRate
+ Σ over[t]·overImportWeight·peakRate   grid import above the service cap (soft)
```

The SOC penalties are scaled into currency via the peak rate so battery
protection is directly comparable to grid-import cost.

### Constraints (9 kinds, per slot, plus one cross-slot budget)

1. **Energy balance** — `import + discharge − charge − export = load − solar`
2. **SOC tracking** — `soc[t+1] − soc[t] − (ηc·charge − discharge/ηd)·Δh/capacity = 0`
3. **Charge limit, solar-gated** — `charge − gridCharge·(maxCharge − surplus) ≤ surplus`: with the permit off, charging is capped at solar surplus; with it on, up to the full charge rate.
4. **Minimum-charge link** — `charge − minChargeKW·gridCharge ≥ 0`: a granted permit must produce at least `minChargeKW` of real charging, eliminating the degenerate "enter bypass, charge nothing" solution.
5. **No discharge while permitted** — `discharge + maxDischargeKW·gridCharge ≤ maxDischargeKW`: the battery can't discharge to the house while in grid-charge bypass.
6. **Start link** — `start[t] ≥ gridCharge[t] − gridCharge[t−1]`: `start[t]` is forced to 1 exactly when grid-charge transitions off→on.
7–9. **SOC-risk penalties** (piecewise-linear, one constraint per threshold) — `penX[t] + soc[t+1] ≥ threshold`, at 50%/30%/20% of capacity with increasing weight, so the solver trades off a little grid cost against staying above each floor.

**Per-window grid-charge budget** — the horizon is segmented into maximal
contiguous off-peak runs; within each run, `Σ start[t] ≤ 1`. Combined with
the blip-cost objective term, this caps grid-charge entries to at most one
per off-peak window and discourages marginal-gain entries entirely — the
schedule reads as "charge or don't" per window rather than toggling the
inverter's grid-charge state repeatedly. (The optimiser's `start[t]`/`blipCost`
term predates the timed-charge actuator; it is retained as an anti-chatter /
marginal-window penalty even though enabling timed charge is no longer a power
"blip".)

Peak-tariff slots have `gridCharge` fixed to a constant 0 (a continuous
variable pinned to `[0,0]`, avoiding an unnecessary binary), so grid-charging
can only happen inside configured off-peak windows.

**Grid-import cap** — `max_grid_import_kw` (the electrical service limit, default
10 kW) bounds the *planned* draw in any slot. It is enforced as a **soft**
constraint: `import[t]` itself is unbounded, but a slack `over[t] ≥ import[t] −
cap` is priced in the objective at `overImportWeight·peakRate` — enormous relative
to any tariff or SOC term — so the optimiser drives it to zero whenever any
within-cap solution exists (a deep off-peak deficit is spread across slots rather
than pulled in one spike). The softness matters: if real load alone exceeds the
cap with a depleted battery, a *hard* bound would make the whole MILP infeasible,
and `hub.tick` would early-return — serving a stale plan, never re-commanding the
actuator, and firing no alert. The soft cap keeps the model feasible (paying the
penalty, `over[t] > 0`) so a plan is still produced and the actuation/alert path
still runs. This is the software half of a two-layer defence: the solver never
*plans* beyond the cap, and the actuator's per-unit `max_charge_current_a` clamp
back-stops it in hardware even if a plan (or the kW→A conversion) were wrong.
Note the cap is a planning bound only — it never physically limits the house's
grid draw, which is set by real load.

### Multi-day planning

The 72h rolling horizon lets a poor forecast for tomorrow pull today's
solution toward charging higher, and a run of sunny days ahead lets tonight
discharge deeper — re-solved every `poll_interval` with fresh actuals, so the
plan stays current without any explicit re-planning logic.

## Actuation

`actuator/` executes the current slot's grid-charge decision through the SRNE
**timed-charge** mechanism — the only path that grid-charges this ASP/SRNE
inverter (confirmed live). It calls Home Assistant service calls (not the
inverter's protocol) against the aggregate SRNE-controller entities: a
timed-charge enable switch, a per-unit mains-charge-current number, and the
"HH:MM" charge-window text entities. The charge windows are a **static rail**:
the actuator mirrors slot _i_ to configured off-peak window _i_, inset by
`window_inset` (default 5 min) at both ends as a clock-skew guard so charging
never leaks into peak, idempotently (only writing on a mismatch, so no churn),
establishing it at startup and re-asserting it on the watchdog tick, and never
touches the windows to start or stop a charge. A window shorter than 2×inset, or
one whose inset bound would land on `00:00` (a wrap/zero-length misread), is
skipped with a warning. Charging is driven by the
**global enable switch** plus the per-unit current: to charge, ensure the rail is
in place (usually a no-op), set the current, then enable the switch **last**; to
stop, disable the switch **first**, then zero the current. Consecutive writes are
spaced and confirmed by read-back (the SRNE drops rapid-fire writes); re-issuing
is harmless (idempotent, not a power event). The per-unit current is
`gridKW·1000 / (ac_charge_voltage_v · numUnits)`, numUnits = 2 parallel inverters.
`mains_charge_current` is an **AC-side** draw limit, so the divisor is the AC line
voltage (`ac_charge_voltage_v`, ~103 V per split-phase leg) — **not** the DC pack
voltage; using ~52.8 V there understates the divisor ~2× and doubles the commanded
amps (confirmed live: 30 A/unit drew ~6 kW over ~56 A mains ⇒ ~103 V/leg). The
result is clamped to `max_charge_current_a` per unit (default 50 A ⇒ 2 × 50 × 103 V
≈ 10 kW ≈ 50 A/phase), the per-unit share of the 12 kVA service that back-stops the
optimiser's grid-import cap in hardware.

Actuation is mode-gated — `observe` (log only), `watchdog` (fail-safe writes
only), `live` (full control) — resolved once at startup with a config-conflict
guard.

**Safety model.** The hazard is timed charge left *enabled* when it shouldn't be
(unwanted/expensive grid charge, e.g. a crashed daemon leaving it on). Disabling
is always the safe direction. The invariant: **timed charge is off whenever the
actuator is not actively commanding a charge.** Three layers enforce it:

1. **The charge windows are a static hardware rail** — grid charging can only
   occur inside a programmed window; the actuator mirrors the configured off-peak
   periods into the slots and an internal guard refuses to write a peak interval,
   so no window is ever set to peak (and none is ever zeroed).
2. **An out-of-window watchdog** — on its own ticker, it re-asserts the window
   rail idempotently (self-healing a mid-life scramble) and, if timed charge reads
   enabled (or the feed is stale) outside every off-peak window, disables it.
3. **An out-of-band HA dead-man automation** (deployed separately) — if the
   daemon's MQTT availability goes `offline` (its LWT fires on crash/disconnect),
   HA disables the timed-charge switch and zeroes the current, so a dead daemon
   can never leave grid charging running.

Startup reconcile brings timed charge *off* (no fresh optimiser decision exists
yet); the first tick re-enables within one poll interval if the solve wants grid
charge — cheap, since toggling timed charge is not a power event.

`serve.DecisionPublisher` is a separate concern: it publishes the *plan*
(not a command) — grid-charge/battery-flow/import/SOC-target for the current
slot, the objective value, a human-readable rationale, and derived
charge/discharge time-remaining — as its own HA MQTT-discovery device
(`energy_optimiser` by default), independent from and never overwriting the
inverter controller's device.

Time-remaining is a simple linear extrapolation from live SOC and live
battery power against the optimiser's own battery config (capacity, SOC
band): `(socMax − soc) · capacity / power` for charging,
`(soc − socMin) · capacity / |power|` for discharging. Below a small power
epsilon the estimate is omitted (not zeroed) so Home Assistant renders
"unknown" rather than a misleading number.

## Dashboard

Stack: HTMX + server-sent events + a small in-process pub/sub hub — no
client-side build step, no framework. `hub.Hub` holds a set of SSE
subscriber channels; every tick (`broadcast()`) wakes all of them for a full
re-render, and a 3-second heartbeat independently re-renders the fast-moving
tiles (live SOC/PV/grid/load and the derived gauges) between ticks so the
page stays live even when the plan hasn't changed.

`serve.DashboardView` is the single view model shared by the full-page
render and every SSE partial, built fresh from live state on each render so
the on-screen numbers and the MQTT sensor states can never disagree:

- **Decision** — the current action in plain words (`GRID CHARGE` /
  `DISCHARGE` / `ON SOLAR` / `HOLD` / `STANDBY`), the rationale, and what's
  next.
- **Tiles** — SOC, solar, load, grid, battery power, live-refreshed.
- **Charge/discharge gauges** — donut time-remaining glances, sharing the
  same `TimeRemaining`/`FormatHours` logic as the MQTT entities.
- **Ribbon** — the 72-hour SOC/tariff timeline: an SVG area+line for planned
  SOC, merged charge/discharge rails, merged off-peak shading, a "now"
  marker, and per-slot hover tooltips.
- **Forecast** — a solar sparkline over the horizon with peak and total kWh.
- **Events** — the next several planned state changes (start charge, start
  discharge, hold), as a short human-readable list.

## Configuration Surface

Key `config.toml` sections: `service` (poll interval, planning horizon, slot
duration, web port), `influxdb` (time-series store URL/token/measurement
names), `homeassistant` (WebSocket URL/token, entity IDs), `solcast`
(API key, one or more `[[solcast.sites]]`), `weather` (lat/long, reserved),
`battery` (capacity, charge/discharge limits, SOC band, round-trip
efficiency), `rates` (currency, peak/off-peak/feed-in rates, one or more
`[[rates.off_peak_windows]]` with optional per-window override rates),
`optimizer` (SOC-risk weight, confidence threshold, min-charge-kW, blip
cost), `mqtt` (broker, topic prefix, the inverter controller's device ID,
and a separate `decision_device_id` for this app's own entities), and
repeated `[[circuit]]` blocks for load-model categories (`fixed` / `routine`
/ `weather` / `deferrable`). `time_zone` pins all tariff-window and
scheduling math (`config.toml`'s example defaults to `Asia/Tokyo`; any IANA
zone works). Secrets support an `env:VARNAME` indirection resolved at load
time, so real values never need to live in the checked-in config file.

Config is validated at load: the feed-in rate must be strictly cheaper than
the cheapest effective off-peak rate, or the optimiser would have a
profitable import→export money-pump and the config is rejected outright.

## Deferrable Loads (future work)

`config.Load` and the `deferrable` circuit category exist as extension
points — priority, energy requirement, deadline, minimum run time, avoid
windows — but the optimizer doesn't yet schedule anything through them.
Wiring them in would mean adding deferrable-load decision variables and
(for thermal loads) piecewise-linear comfort penalties to the MILP, the same
pattern the SOC-risk penalties already use.
