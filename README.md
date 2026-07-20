# Energy Optimiser

[![CI](https://github.com/daniel-sullivan/energy-optimiser/actions/workflows/ci.yml/badge.svg)](https://github.com/daniel-sullivan/energy-optimiser/actions/workflows/ci.yml)

Forecast-driven off-peak battery charge optimiser for home solar + battery systems.

Every few minutes it re-solves a mixed-integer linear program over a 72-hour,
30-minute-slot horizon: given a solar production forecast and a learned load
model, it decides exactly which off-peak windows (if any) need a grid-charge
top-up to keep the battery above a safe floor, and how much. It doesn't charge
"whenever off-peak is cheap" — it charges only the forecast deficit, in the
fewest, cheapest slots that cover it. The plan is re-solved from live state on
every tick, so it self-corrects as forecasts and actuals diverge.

The decision — grid-charge on/off, planned battery flow, the rationale behind
it, and derived charge/discharge time-remaining — is actuated over MQTT and
published back as Home Assistant entities, alongside a live web dashboard.

## How it works

1. **Forecast** — [Solcast](https://www.solcast.com) per-site solar forecasts
   (summed across sites), polled a couple of times a day and cached for the
   horizon.
2. **Load model** — a per-slot demand prediction trained from historical
   power readings in a VictoriaMetrics (or any InfluxDB-line-protocol
   compatible) time-series store fed by Home Assistant. Cold-start safe: low
   data coverage falls back to conservative defaults and a confidence score
   gates how aggressively the optimiser trusts its own predictions.
3. **Optimise** — a MILP (variables: grid-charge permit, charge/discharge
   flow, grid import/export, SOC trajectory, bypass-entry and SOC-risk
   penalties) picks the cost-minimising schedule subject to the battery's
   physical limits, the tariff structure, and a per-window budget that caps
   grid-charge entries to avoid excessive inverter mode-switching. See
   [DESIGN.md](DESIGN.md) for the full formulation.
4. **Actuate** — the current slot's grid-charge decision is published as an
   MQTT command to your inverter's existing "charge from mains" control.
5. **Report** — the schedule, rationale, and time-remaining estimates are
   published as Home Assistant MQTT-discovery entities and rendered live on
   an HTMX + SSE dashboard.

### The solver: pure Go, no cgo

The MILP is solved with
[go-milp](https://github.com/daniel-sullivan/go-milp), a pure-Go
mixed-integer linear programming solver — a native bounded-variable revised
simplex plus branch-and-bound, zero cgo and zero third-party runtime
dependencies (`CGO_ENABLED=0 go build` just works). No GLPK, no solver
binary to install, no process-global solver state to lock around.

## Home Assistant Add-on

### Prerequisites

- A solar/battery system with an existing MQTT-based controller exposing (at
  minimum) a "charge from mains" switch — energy-optimiser doesn't speak to
  your inverter directly, it commands whatever already listens on that
  topic. Reference target: an SRNE-class hybrid inverter driven by an
  MQTT controller such as
  [srne-solar-controller](https://github.com/daniel-sullivan/srne-solar-controller);
  any MQTT switch with the same topic shape works.
- Home Assistant sensors (or equivalent MQTT entities) for battery SOC, PV
  power, grid power, and load power.
- A time-series datastore fed by Home Assistant's history (VictoriaMetrics,
  or anything else that speaks InfluxDB line protocol / the `/api/v1/export`
  query API) — this is what trains the load model and drives `backtest`.
- A free-tier [Solcast](https://www.solcast.com) account and rooftop site(s).
- The [Mosquitto broker](https://github.com/home-assistant/addons/tree/master/mosquitto)
  add-on (or an external MQTT broker).

### Installation

Add this repository URL to your Home Assistant add-on store:

```
https://github.com/daniel-sullivan/energy-optimiser
```

Install the add-on, then fill in the add-on options: Home Assistant
WebSocket URL + token, entity IDs, time-series datastore URL, Solcast API
key + site ID(s), battery capacity/limits, tariff rates and off-peak
windows, and the MQTT device ID your inverter controller listens on. The
add-on renders these into a `config.toml` at startup.

Start the add-on. The dashboard appears in the HA sidebar (Ingress), and the
decision/time-remaining entities appear under the MQTT integration.

### MQTT Entities

Published under the `energy_optimiser` device (separate from the inverter
controller's own device — this app never overwrites its entities, it only
commands the "charge from mains" switch):

| Entity | Description |
|---|---|
| Grid Charge Planned | Whether the current slot is scheduled to grid-charge |
| Planned Battery Flow | Signed kW (charge positive, discharge negative) for the current slot |
| Planned Grid Import | kW drawn from the grid in the current slot |
| Planned SOC (End of Slot) | Forecast SOC at the end of the current slot |
| Schedule Objective Cost | The solved schedule's total objective value |
| Decision Rationale | Human-readable explanation ("SoC forecast 34% by 04:30 → scheduling 02:00 charge") |
| Charge Time Remaining | Hours to full at the current charge rate (raw + formatted) |
| Discharge Time Remaining | Hours to the SOC floor at the current discharge rate (raw + formatted) |

### Dashboard

The add-on includes a live dashboard (HTMX + server-sent events, no client
build step) accessible via the HA sidebar: a 72-hour SOC/tariff ribbon,
current-state tiles, charge/discharge time-remaining gauges, a solar
forecast sparkline, and an upcoming-events list — all pushed from the
server's tick loop and a lightweight pub/sub hub, so nothing polls.

## Standalone Usage

The optimiser can also run outside Home Assistant, e.g. for development or
on your own infrastructure.

### Prerequisites

- [mise](https://mise.jdx.dev/) (manages the Go toolchain)

```sh
mise install
```

### Quickstart

```sh
cp config.toml config.local.toml
```

Fill in `config.local.toml`: Home Assistant URL/token, time-series datastore
URL, Solcast API key + site ID, weather coordinates, battery capacity and
limits, your tariff rates and off-peak windows, and the MQTT broker + device
ID your inverter controller listens on. Secrets can be inlined or referenced
as `env:VARNAME` and resolved from a gitignored `.env.local` (loaded
automatically via mise's `[env] _.file`) — see the comments in `config.toml`.

Three commands, all reading `-c`/`--config` (default `config.toml`):

```sh
# Start the daemon: tick loop, MQTT actuation + decision publishing, dashboard
mise run serve
# or, read-only (no commands sent, nothing written):
mise exec -- go run . serve --dry-run -c config.local.toml

# Replay historical data through the optimizer with perfect foresight, to see
# when and why it would have grid-charged — a tuning aid for the SOC-risk
# weight and bypass-entry cost.
mise exec -- go run . backtest -c config.local.toml --days 7

# Confirm the time-series datastore returns samples for every configured
# entity (the load model degrades silently to defaults on an empty query).
mise exec -- go run . verify -c config.local.toml
```

The dashboard is available at `http://localhost:8080` (or whatever
`service.web_port` is set to).

## Architecture

| Package | Role |
|---|---|
| `cmd/` | Cobra CLI: `serve`, `backtest`, `verify` |
| `config/` | TOML configuration structs, secret indirection, tariff-window logic |
| `forecast/` | Solcast solar forecast client (multi-site, cached); Open-Meteo weather client |
| `loadmodel/` | Per-slot load prediction from hour × day-of-week × season buckets, trained from the time-series store, with a data-coverage confidence score |
| `optimizer/` | go-milp problem builder, solver, typed schedule extraction |
| `influx/` | Time-series client: queries via `/api/v1/export`, writes via InfluxDB line protocol |
| `ha/` | Home Assistant WebSocket client: state subscriptions, service calls |
| `actuator/` | Executes the current slot's decision: MQTT commands to the inverter controller |
| `hub/` | The tick loop: forecast → predict load → solve → actuate → record → publish → broadcast |
| `serve/` | HTTP server, MQTT decision/discovery publisher, HTMX + SSE dashboard |
| `ha-addon/` | Home Assistant add-on packaging (Dockerfile, options schema, Ingress) |

See [DESIGN.md](DESIGN.md) for the MILP formulation and dashboard internals.

## Building

```sh
mise run check   # build + test (race detector + coverage) + lint
```

## License

See [LICENSE](LICENSE) for details.
