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

Each bucket stores a running mean; prediction looks up the bucket for the
target slot's time, falling back to a day-of-week-agnostic bucket and then a
conservative default if a bucket has fewer than 3 samples. A **confidence**
score (fraction of hour×season — or hour×dow×season — buckets with enough
data) gates how much the dashboard and rationale trust the model; a `weather`
circuit category and per-circuit deferrable-load splitting exist as
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
| `import[t]` | continuous, `≥ 0` | Grid import power |
| `export[t]` | continuous, `≥ 0` | Grid export power |
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

**Per-window bypass budget** — the horizon is segmented into maximal
contiguous off-peak runs; within each run, `Σ start[t] ≤ 1`. Combined with
the blip-cost objective term, this caps grid-charge entries to at most one
per off-peak window and discourages marginal-gain entries entirely — the
schedule reads as "charge or don't" per window rather than chattering the
inverter's bypass relay.

Peak-tariff slots have `gridCharge` fixed to a constant 0 (a continuous
variable pinned to `[0,0]`, avoiding an unnecessary binary), so grid-charging
can only happen inside configured off-peak windows.

### Multi-day planning

The 72h rolling horizon lets a poor forecast for tomorrow pull today's
solution toward charging higher, and a run of sunny days ahead lets tonight
discharge deeper — re-solved every `poll_interval` with fresh actuals, so the
plan stays current without any explicit re-planning logic.

## Actuation

`actuator/` executes only the current slot's grid-charge decision, via an
MQTT publish to `{topic_prefix}/switch/{device_id}/charge_from_mains/set`
(ON/OFF). `device_id` targets an existing MQTT-discovery device published by
your inverter's own controller — energy-optimiser never talks to the
inverter directly (no Modbus, no vendor protocol), it only flips the switch
that controller already exposes. The reference target is an SRNE-class
hybrid inverter driven by an MQTT controller, but any device exposing the
same switch topic works.

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
