# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

Go service for forecast-driven off-peak battery charge optimisation in home
solar + battery systems. Uses a pure-Go MILP solver ([go-milp](https://github.com/daniel-sullivan/go-milp))
to schedule grid-charging against solar forecasts, tariff windows, and a
learned load model, then actuates an MQTT-based inverter controller and
publishes the decision back to Home Assistant.

See `DESIGN.md` for full architecture, the MILP formulation, and the
dashboard internals.

## Environment Setup

- **mise** manages the Go toolchain and dev tools
- Run `mise install` to set up the environment
- Prefix commands with `mise exec --` or use `mise shell` to activate

## Commands

- **Build**: `mise exec -- go build ./...`
- **Run tests**: `mise exec -- go test ./...`
- **Run a single test**: `mise exec -- go test ./... -run TestName`
- **Lint**: `mise run lint`
- **Full check**: `mise run check` (build + test + lint) — run this before considering any change done
- **Run the daemon**: `mise run serve` (or `--dry-run` for read-only)
- **Backtest**: `mise exec -- go run . backtest -c config.local.toml --days 7` — replays historical data through the optimizer with perfect foresight; a tuning aid for `soc_risk_weight` and `blip_cost`
- **Verify the data spine**: `mise exec -- go run . verify -c config.local.toml` — confirms the time-series store returns samples for every configured entity

## Architecture

- `cmd/` — Cobra CLI: `serve`, `backtest`, `verify`, config loading
- `config/` — TOML configuration structs (battery, rates, loads, services), secret indirection (`env:VARNAME`), tariff-window logic
- `forecast/` — Solcast solar forecast client (multi-site, cached) + Open-Meteo weather client (fetched, not yet wired into the load model)
- `loadmodel/` — hour×dow×season bucket load profiles (recency-weighted level × percentile-headroom shape), trained from the time-series store, cold-start safe with a data-coverage + distribution-shift confidence score
- `optimizer/` — go-milp problem builder, solver, typed schedule extraction
- `influx/` — time-series client (VictoriaMetrics / any InfluxDB-line-protocol-compatible store): `/api/v1/export` queries, line-protocol writes
- `ha/` — Home Assistant WebSocket client (state subscriptions, service calls)
- `actuator/` — executes the current slot's decision: an MQTT command to the inverter controller's "charge from mains" switch
- `hub/` — the tick loop: refresh forecast → predict load → solve → actuate → record → publish decision → broadcast to SSE subscribers
- `serve/` — MQTT decision/discovery publisher, HTMX + SSE dashboard, HTTP server
- `ha-addon/` — Home Assistant add-on packaging (Dockerfile, options schema, Ingress)

### Key Design Decisions

- **MILP via go-milp** — pure Go, zero cgo, zero third-party runtime dependencies. 72h rolling horizon, 30-min slots, re-solved every `poll_interval` (default 5 min). Binary grid-charge permit per off-peak slot (fixed to 0 in peak slots), continuous battery flow, piecewise-linear SOC-risk penalties, a per-off-peak-window bypass-entry budget (`Σstart ≤ 1`) plus a blip-cost objective term to avoid inverter mode-chatter.
- **Cold start** — the system is useful from day one with conservative defaults. Load-model confidence (bucket data coverage × a recency-vs-history distribution-shift signal) gates `optimizer.confidence_threshold`, scaling up a low-confidence bucket's prediction rather than trusting it as-is.
- **Time-series store as the data layer** — HA pushes sensor history in; the service reads it for load-model training and `backtest`, and writes optimizer decisions back for auditing. Package is named `influx/` for historical reasons (migrated InfluxDB → VictoriaMetrics); both speak the same wire protocol.
- **HA WebSocket for live state** — SOC/PV/grid/load/battery-power subscriptions and service calls through one persistent connection; history is never read this way.
- **Actuator never speaks inverter protocol** — it only flips an existing MQTT "charge from mains" switch published by a separate inverter controller. The decision publisher writes to its own, distinct MQTT-discovery device so it never collides with or overwrites the controller's entities.
- **Deferrable load hooks** — `config.Load` and the `deferrable` circuit category are extension points; the optimizer doesn't yet schedule anything through them.

## CI

- GitHub Actions: build, test (race detector + coverage), vet, lint
- golangci-lint v2 via golangci-lint-action

## External Services

- **Solcast** — solar forecast (free hobbyist tier, limited calls/day)
- **Open-Meteo** — weather forecast (free, no key; not yet consumed by the load model)
- **Time-series store** — VictoriaMetrics or any InfluxDB-line-protocol-compatible datastore
- **Home Assistant** — state + control via WebSocket API
- **Inverter controller** — an MQTT-based controller (e.g. an SRNE-class hybrid inverter driven by a controller like [srne-solar-controller](https://github.com/daniel-sullivan/srne-solar-controller)) exposing a "charge from mains" switch
