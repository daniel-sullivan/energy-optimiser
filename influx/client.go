package influx

// Client queries VictoriaMetrics over HTTP. (Formerly InfluxDB 3; the home datastore
// was migrated InfluxDB -> VictoriaMetrics in 2026-07. Package name kept for now to
// limit churn — rename to `tsdb`/`metrics` in the hygiene phase.)
//
// HA's InfluxDB integration ingests into VM via line protocol, so a HA sensor with
// unit-of-measurement "W" and entity_id "sensor.load_power" becomes
// the VM series  W_value{entity_id="load_power", domain="sensor"}.
// Numeric value = "<unit>_value"; string attributes = "<unit>_<field>_str".

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"energy-optimiser/config"
)

// Client queries VictoriaMetrics (/api/v1/export) and writes via InfluxDB line protocol.
type Client struct {
	url          string
	measurements config.Measurements
	http         *http.Client
}

// Sample is a timestamped numeric value.
type Sample struct {
	Time  time.Time
	Value float64
}

// Point represents a data point to write (InfluxDB line protocol; VM-compatible).
type Point struct {
	Time   time.Time
	Tags   map[string]string
	Fields map[string]any
}

func New(cfg config.InfluxDB) (*Client, error) {
	transport := &http.Transport{
		MaxIdleConns:        5,
		IdleConnTimeout:     30 * time.Second,
		MaxIdleConnsPerHost: 2,
	}
	return &Client{
		url:          strings.TrimRight(cfg.URL, "/"),
		measurements: cfg.Measurements,
		http:         &http.Client{Timeout: 60 * time.Second, Transport: transport},
	}, nil
}

func (c *Client) Close() error { return nil }

// QueryPower returns power readings (W) for a HA entity within a time range.
func (c *Client) QueryPower(ctx context.Context, entityID string, from, to time.Time) ([]Sample, error) {
	return c.querySeries(ctx, c.measurements.Power, entityID, from, to)
}

// QueryTemperature returns temperature readings for a HA entity.
func (c *Client) QueryTemperature(ctx context.Context, entityID string, from, to time.Time) ([]Sample, error) {
	return c.querySeries(ctx, c.measurements.Temperature, entityID, from, to)
}

// QueryPercentage returns percentage readings (e.g., SOC, humidity).
func (c *Client) QueryPercentage(ctx context.Context, entityID string, from, to time.Time) ([]Sample, error) {
	return c.querySeries(ctx, c.measurements.Percentage, entityID, from, to)
}

// querySeries pulls raw samples for "<unit>_value{entity_id=<short>}" from VM's export API.
// The __name__ label form is used because unit-derived metric names ("%_value", "°C_value")
// are not valid bare PromQL identifiers.
func (c *Client) querySeries(ctx context.Context, unit, entityID string, from, to time.Time) ([]Sample, error) {
	metric := unit + "_value"
	short := shortEntityID(entityID)
	// domain="sensor" is defense-in-depth: entity_id alone is the HA short id
	// (domain stripped), so a future switch/number entity that happens to
	// share a sensor's short id would otherwise silently match too.
	match := fmt.Sprintf(`{__name__=%q,entity_id=%q,domain="sensor"}`, metric, short)

	q := url.Values{}
	q.Set("match[]", match)
	q.Set("start", strconv.FormatInt(from.Unix(), 10))
	q.Set("end", strconv.FormatInt(to.Unix(), 10))
	endpoint := c.url + "/api/v1/export?" + q.Encode()

	body, err := c.get(ctx, endpoint)
	if err != nil {
		return nil, fmt.Errorf("vm export %s/%s: %w", metric, short, err)
	}

	// /api/v1/export returns JSONL: one object per series.
	var samples []Sample
	sc := bufio.NewScanner(bytes.NewReader(body))
	sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var series struct {
			Values     []float64 `json:"values"`
			Timestamps []int64   `json:"timestamps"` // milliseconds
		}
		if err := json.Unmarshal(line, &series); err != nil {
			return nil, fmt.Errorf("vm export decode: %w", err)
		}
		for i := range series.Values {
			if i >= len(series.Timestamps) {
				break
			}
			samples = append(samples, Sample{
				Time:  time.UnixMilli(series.Timestamps[i]),
				Value: series.Values[i],
			})
		}
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("vm export read: %w", err)
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i].Time.Before(samples[j].Time) })
	return samples, nil
}

// WritePoints writes data points via InfluxDB line protocol (VM /write endpoint).
func (c *Client) WritePoints(ctx context.Context, measurement string, points []Point) error {
	var buf bytes.Buffer
	for _, p := range points {
		buf.WriteString(measurement)
		for k, v := range p.Tags {
			fmt.Fprintf(&buf, ",%s=%s", k, lineEscapeTag(v))
		}
		buf.WriteByte(' ')
		first := true
		for k, v := range p.Fields {
			if !first {
				buf.WriteByte(',')
			}
			switch val := v.(type) {
			case float64:
				fmt.Fprintf(&buf, "%s=%f", k, val)
			case int64:
				fmt.Fprintf(&buf, "%s=%di", k, val)
			case string:
				fmt.Fprintf(&buf, "%s=%q", k, val)
			case bool:
				fmt.Fprintf(&buf, "%s=%t", k, val)
			default:
				fmt.Fprintf(&buf, "%s=%v", k, val)
			}
			first = false
		}
		fmt.Fprintf(&buf, " %d\n", p.Time.UnixNano())
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url+"/write", &buf)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "text/plain")

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("vm write: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("vm write: HTTP %d: %s", resp.StatusCode, body)
	}
	return nil
}

// get performs a GET with a small transient-error retry.
func (c *Client) get(ctx context.Context, endpoint string) ([]byte, error) {
	var lastErr error
	for attempt := range 3 {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(time.Duration(attempt) * 500 * time.Millisecond):
			}
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, err
		}
		resp, err := c.http.Do(req)
		if err != nil {
			if isTransient(err) {
				lastErr = err
				continue
			}
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if readErr != nil {
			lastErr = readErr
			continue
		}
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, bytes.TrimSpace(body))
		}
		return body, nil
	}
	return nil, fmt.Errorf("%w (after 3 attempts)", lastErr)
}

// shortEntityID strips the HA domain prefix (VM stores entity_id without the domain).
func shortEntityID(entityID string) string {
	if i := strings.IndexByte(entityID, '.'); i >= 0 {
		return entityID[i+1:]
	}
	return entityID
}

// lineEscapeTag escapes commas/spaces/equals in InfluxDB line-protocol tag values.
func lineEscapeTag(v string) string {
	r := strings.NewReplacer(",", `\,`, " ", `\ `, "=", `\=`)
	return r.Replace(v)
}

// isTransient returns true for connection errors worth retrying.
func isTransient(err error) bool {
	if errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.ECONNREFUSED) {
		return true
	}
	var netErr *net.OpError
	if errors.As(err, &netErr) {
		return true
	}
	return strings.Contains(err.Error(), "connection reset")
}
