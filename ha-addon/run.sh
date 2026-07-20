#!/usr/bin/with-contenv bashio
# shellcheck shell=bash
#
# Secret handling: bashio already reads add-on options out of the (root-only,
# HA-supervisor-managed) /data/options.json, so there's no additional exposure
# in copying HA token / Solcast API key / MQTT credentials straight into the
# rendered config.toml — same trust boundary. We take that simpler path
# (matching srne-solar-controller's run.sh) rather than the binary's
# `env:VARNAME` indirection, and chmod the file 600 as defense in depth.

CONFIG="/data/config.toml"

TIME_ZONE=$(bashio::config 'time_zone')
OBSERVE=$(bashio::config 'observe')
INGRESS_PORT=$(bashio::addon.ingress_port)

POLL_INTERVAL=$(bashio::config 'service.poll_interval')
PLANNING_HORIZON=$(bashio::config 'service.planning_horizon')
SLOT_DURATION=$(bashio::config 'service.slot_duration')

cat > "${CONFIG}" <<EOF
time_zone = "${TIME_ZONE}"
observe = ${OBSERVE}

[service]
poll_interval = "${POLL_INTERVAL}s"
planning_horizon = "${PLANNING_HORIZON}s"
slot_duration = "${SLOT_DURATION}s"
web_port = ${INGRESS_PORT}
EOF

# --- Home Assistant ---
HA_URL=$(bashio::config 'homeassistant.url')
HA_TOKEN=$(bashio::config 'homeassistant.token')
BATTERY_SOC=$(bashio::config 'entities.battery_soc')
BATTERY_POWER=$(bashio::config 'entities.battery_power')
PV_POWER=$(bashio::config 'entities.pv_power')
GRID_POWER=$(bashio::config 'entities.grid_power')
LOAD_POWER=$(bashio::config 'entities.load_power')

cat >> "${CONFIG}" <<EOF

[homeassistant]
url = "${HA_URL}"
token = "${HA_TOKEN}"

[homeassistant.entities]
battery_soc = "${BATTERY_SOC}"
pv_power = "${PV_POWER}"
grid_power = "${GRID_POWER}"
load_power = "${LOAD_POWER}"
EOF
if [ -n "${BATTERY_POWER}" ] && [ "${BATTERY_POWER}" != "null" ]; then
    echo "battery_power = \"${BATTERY_POWER}\"" >> "${CONFIG}"
fi

# --- InfluxDB / VictoriaMetrics (history datastore) ---
INFLUX_URL=$(bashio::config 'influxdb.url')
INFLUX_TOKEN=$(bashio::config 'influxdb.token')
INFLUX_DATABASE=$(bashio::config 'influxdb.database')

cat >> "${CONFIG}" <<EOF

[influxdb]
url = "${INFLUX_URL}"
database = "${INFLUX_DATABASE}"
EOF
if [ -n "${INFLUX_TOKEN}" ] && [ "${INFLUX_TOKEN}" != "null" ]; then
    echo "token = \"${INFLUX_TOKEN}\"" >> "${CONFIG}"
fi

# --- Solcast solar forecast ---
SOLCAST_KEY=$(bashio::config 'solcast.api_key')

cat >> "${CONFIG}" <<EOF

[solcast]
api_key = "${SOLCAST_KEY}"
poll_times = ["06:00", "12:00"]
EOF
for site in $(bashio::config 'solcast.sites|keys'); do
    SITE_NAME=$(bashio::config "solcast.sites[${site}].name")
    SITE_ID=$(bashio::config "solcast.sites[${site}].id")
    cat >> "${CONFIG}" <<EOF

[[solcast.sites]]
name = "${SITE_NAME}"
id = "${SITE_ID}"
EOF
done

# --- Weather (latitude/longitude for the weather client) ---
LATITUDE=$(bashio::config 'weather.latitude')
LONGITUDE=$(bashio::config 'weather.longitude')

cat >> "${CONFIG}" <<EOF

[weather]
latitude = ${LATITUDE}
longitude = ${LONGITUDE}
EOF

# --- Battery model ---
CAPACITY_KWH=$(bashio::config 'battery.capacity_kwh')
MAX_CHARGE_KW=$(bashio::config 'battery.max_charge_kw')
MAX_DISCHARGE_KW=$(bashio::config 'battery.max_discharge_kw')
SOC_MIN=$(bashio::config 'battery.soc_min')
SOC_MAX=$(bashio::config 'battery.soc_max')
EFFICIENCY=$(bashio::config 'battery.efficiency')
NOMINAL_VOLTAGE_V=$(bashio::config 'battery.nominal_voltage_v')

cat >> "${CONFIG}" <<EOF

[battery]
capacity_kwh = ${CAPACITY_KWH}
max_charge_kw = ${MAX_CHARGE_KW}
max_discharge_kw = ${MAX_DISCHARGE_KW}
soc_min = ${SOC_MIN}
soc_max = ${SOC_MAX}
efficiency = ${EFFICIENCY}
EOF
if [ -n "${NOMINAL_VOLTAGE_V}" ] && [ "${NOMINAL_VOLTAGE_V}" != "null" ]; then
    echo "nominal_voltage_v = ${NOMINAL_VOLTAGE_V}" >> "${CONFIG}"
fi

# --- Electricity rates / tariff windows ---
CURRENCY=$(bashio::config 'rates.currency')
PEAK_RATE=$(bashio::config 'rates.peak_rate')
OFF_PEAK_RATE=$(bashio::config 'rates.off_peak_rate')
FEED_IN_RATE=$(bashio::config 'rates.feed_in_rate')

cat >> "${CONFIG}" <<EOF

[rates]
currency = "${CURRENCY}"
peak_rate = ${PEAK_RATE}
off_peak_rate = ${OFF_PEAK_RATE}
feed_in_rate = ${FEED_IN_RATE}
EOF
for window in $(bashio::config 'rates.off_peak_windows|keys'); do
    WSTART=$(bashio::config "rates.off_peak_windows[${window}].start")
    WEND=$(bashio::config "rates.off_peak_windows[${window}].end")
    WRATE=$(bashio::config "rates.off_peak_windows[${window}].rate")
    cat >> "${CONFIG}" <<EOF

[[rates.off_peak_windows]]
start = "${WSTART}"
end = "${WEND}"
EOF
    if [ -n "${WRATE}" ] && [ "${WRATE}" != "null" ]; then
        echo "rate = ${WRATE}" >> "${CONFIG}"
    fi
done

# --- Optimizer knobs ---
SOC_RISK_WEIGHT=$(bashio::config 'optimizer.soc_risk_weight')
CONFIDENCE_THRESHOLD=$(bashio::config 'optimizer.confidence_threshold')
MIN_CHARGE_KW=$(bashio::config 'optimizer.min_charge_kw')
BLIP_COST=$(bashio::config 'optimizer.blip_cost')

cat >> "${CONFIG}" <<EOF

[optimizer]
soc_risk_weight = ${SOC_RISK_WEIGHT}
confidence_threshold = ${CONFIDENCE_THRESHOLD}
min_charge_kw = ${MIN_CHARGE_KW}
blip_cost = ${BLIP_COST}
EOF

# --- MQTT (actuation commands + the optimiser's decision/time-remaining sensors) ---
MQTT_BROKER=""
MQTT_USER=""
MQTT_PASS=""
MQTT_PREFIX=$(bashio::config 'mqtt.topic_prefix')
MQTT_DEVICE_ID=$(bashio::config 'mqtt.device_id')

# Manual config takes precedence over auto-discovery
if bashio::config.has_value 'mqtt.broker'; then
    MQTT_BROKER=$(bashio::config 'mqtt.broker')
    bashio::config.has_value 'mqtt.username' && MQTT_USER=$(bashio::config 'mqtt.username')
    bashio::config.has_value 'mqtt.password' && MQTT_PASS=$(bashio::config 'mqtt.password')
    bashio::log.info "Using manual MQTT broker: ${MQTT_BROKER}"
elif bashio::services.available "mqtt"; then
    MQTT_HOST=$(bashio::services mqtt "host")
    MQTT_PORT=$(bashio::services mqtt "port")
    MQTT_USER=$(bashio::services mqtt "username")
    MQTT_PASS=$(bashio::services mqtt "password")
    MQTT_BROKER="tcp://${MQTT_HOST}:${MQTT_PORT}"
    bashio::log.info "Auto-discovered MQTT broker: ${MQTT_BROKER}"
fi

if [ -n "${MQTT_BROKER}" ]; then
    cat >> "${CONFIG}" <<EOF

[mqtt]
broker = "${MQTT_BROKER}"
topic_prefix = "${MQTT_PREFIX}"
device_id = "${MQTT_DEVICE_ID}"
username = "${MQTT_USER}"
password = "${MQTT_PASS}"
EOF
else
    bashio::log.error "No MQTT broker available. Install the Mosquitto broker add-on or set mqtt.broker in the add-on configuration."
    exit 1
fi

# --- Alertmanager routing + alert thresholds ---
AM_URL=""
bashio::config.has_value 'alertmanager.url' && AM_URL=$(bashio::config 'alertmanager.url')
AM_SITE=$(bashio::config 'alertmanager.site')
RISK_SOC=$(bashio::config 'alerts.risk_soc_threshold')
EXPENSIVE_YEN=$(bashio::config 'alerts.expensive_day_yen')

if [ -n "${AM_URL}" ] && [ "${AM_URL}" != "null" ]; then
    cat >> "${CONFIG}" <<EOF

[alertmanager]
url = "${AM_URL}"
site = "${AM_SITE}"
EOF
fi

cat >> "${CONFIG}" <<EOF

[alerts]
risk_soc_threshold = ${RISK_SOC}
expensive_day_yen = ${EXPENSIVE_YEN}
EOF

chmod 600 "${CONFIG}"

bashio::log.info "Starting Energy Optimiser..."

exec /usr/bin/energy-optimiser serve --config "${CONFIG}"
