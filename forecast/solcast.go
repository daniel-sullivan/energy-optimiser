package forecast

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"

	"energy-optimiser/config"
)

// SolcastClient fetches and caches 72h PV forecasts from the Solcast API.
// Forecasts from all configured sites are summed into a single system profile.
type SolcastClient struct {
	apiKey  string
	siteIDs []string
	http    *http.Client

	mu    sync.RWMutex
	cache *SolarForecast
}

// SolarForecast is a cached solar forecast.
type SolarForecast struct {
	FetchedAt time.Time
	Points    []SolarPoint
}

// SolarPoint is a single forecast period.
type SolarPoint struct {
	Time       time.Time // start of period
	EstimateKW float64   // P50 estimate in kW
}

func NewSolcast(cfg config.Solcast) *SolcastClient {
	ids := make([]string, 0, len(cfg.Sites))
	for _, s := range cfg.Sites {
		if s.ID != "" {
			ids = append(ids, s.ID)
		}
	}
	return &SolcastClient{
		apiKey:  cfg.APIKey,
		siteIDs: ids,
		http:    &http.Client{Timeout: 30 * time.Second},
	}
}

// Fetch retrieves the latest forecast for every site, sums them per period,
// and caches the combined result.
func (c *SolcastClient) Fetch(ctx context.Context) (*SolarForecast, error) {
	if len(c.siteIDs) == 0 {
		return nil, fmt.Errorf("solcast: no sites configured")
	}

	// Sum per-site estimates keyed by period-start (unix seconds avoids
	// time.Time map-key location/monotonic pitfalls).
	sumKW := map[int64]float64{}
	times := map[int64]time.Time{}
	for _, id := range c.siteIDs {
		points, err := c.fetchSite(ctx, id)
		if err != nil {
			return nil, fmt.Errorf("solcast site %s: %w", id, err)
		}
		for _, p := range points {
			key := p.Time.Unix()
			sumKW[key] += p.EstimateKW
			times[key] = p.Time
		}
	}

	points := make([]SolarPoint, 0, len(sumKW))
	for key, kw := range sumKW {
		points = append(points, SolarPoint{Time: times[key], EstimateKW: kw})
	}
	sort.Slice(points, func(i, j int) bool { return points[i].Time.Before(points[j].Time) })

	forecast := &SolarForecast{FetchedAt: time.Now(), Points: points}
	c.mu.Lock()
	c.cache = forecast
	c.mu.Unlock()
	return forecast, nil
}

// fetchSite retrieves one site's forecast periods.
func (c *SolcastClient) fetchSite(ctx context.Context, siteID string) ([]SolarPoint, error) {
	url := fmt.Sprintf(
		"https://api.solcast.com.au/rooftop_sites/%s/forecasts?format=json",
		siteID,
	)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.apiKey)

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("solcast fetch: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("solcast: HTTP %d", resp.StatusCode)
	}

	var result solcastResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("solcast decode: %w", err)
	}

	points := make([]SolarPoint, 0, len(result.Forecasts))
	for _, f := range result.Forecasts {
		end, err := time.Parse(time.RFC3339, f.PeriodEnd)
		if err != nil {
			continue
		}
		dur := parseISODuration(f.Period)
		points = append(points, SolarPoint{
			Time:       end.Add(-dur), // convert period_end to period_start
			EstimateKW: f.PVEstimate,
		})
	}
	return points, nil
}

// Cached returns the most recent cached forecast, or nil.
func (c *SolcastClient) Cached() *SolarForecast {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cache
}

type solcastResponse struct {
	Forecasts []struct {
		PVEstimate   float64 `json:"pv_estimate"`
		PVEstimate10 float64 `json:"pv_estimate10"`
		PVEstimate90 float64 `json:"pv_estimate90"`
		PeriodEnd    string  `json:"period_end"`
		Period       string  `json:"period"`
	} `json:"forecasts"`
}

// parseISODuration handles simple ISO 8601 durations like "PT30M", "PT1H".
func parseISODuration(iso string) time.Duration {
	var h, m int
	if _, err := fmt.Sscanf(iso, "PT%dH%dM", &h, &m); err == nil {
		return time.Duration(h)*time.Hour + time.Duration(m)*time.Minute
	}
	if _, err := fmt.Sscanf(iso, "PT%dM", &m); err == nil {
		return time.Duration(m) * time.Minute
	}
	if _, err := fmt.Sscanf(iso, "PT%dH", &h); err == nil {
		return time.Duration(h) * time.Hour
	}
	return 30 * time.Minute
}
