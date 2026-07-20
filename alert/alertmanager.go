package alert

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// Alert is one Alertmanager alert in the /api/v2/alerts schema. Identity is the
// label set (Alertmanager dedupes on it), so labels stay fixed per alert type
// and all dynamic detail (times, %, cost) goes in annotations — otherwise a
// changing value would spawn a new alert every tick.
type Alert struct {
	Labels      map[string]string `json:"labels"`
	Annotations map[string]string `json:"annotations,omitempty"`
	StartsAt    time.Time         `json:"startsAt"`
	EndsAt      time.Time         `json:"endsAt"`
}

// AlertManager posts alerts to an Alertmanager /api/v2/alerts endpoint. An empty
// base URL yields a disabled client whose Send is a no-op, so callers need not
// branch on config.
type AlertManager struct {
	base string
	http *http.Client
}

func NewAlertManager(baseURL string) *AlertManager {
	return &AlertManager{
		base: baseURL,
		http: &http.Client{Timeout: 10 * time.Second},
	}
}

// Enabled reports whether a base URL is configured.
func (a *AlertManager) Enabled() bool { return a != nil && a.base != "" }

// Send posts the given firing alerts. Re-sending the same label set refreshes an
// alert; omitting it lets Alertmanager auto-resolve it after EndsAt.
func (a *AlertManager) Send(ctx context.Context, alerts []Alert) error {
	if !a.Enabled() || len(alerts) == 0 {
		return nil
	}
	body, err := json.Marshal(alerts)
	if err != nil {
		return err
	}
	url := a.base + "/api/v2/alerts"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := a.http.Do(req)
	if err != nil {
		return fmt.Errorf("alertmanager post: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("alertmanager: HTTP %d", resp.StatusCode)
	}
	return nil
}
