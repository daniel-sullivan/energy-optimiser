package forecast

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"energy-optimiser/config"
)

// solcastBaseURL is the production Solcast API host.
const solcastBaseURL = "https://api.solcast.com.au"

// SolcastCacheFileName is the persisted-forecast cache file name written under
// a Solcast client's CacheDir. Exported so tests (including other packages')
// can pre-seed or inspect the cache file without duplicating the literal.
const SolcastCacheFileName = "solcast_cache.json"

// SolcastClient fetches and caches 72h PV forecasts from the Solcast API.
// Forecasts from all configured sites are summed into a single system profile.
type SolcastClient struct {
	apiKey   string
	siteIDs  []string
	http     *http.Client
	cacheDir string

	// BaseURL overrides the Solcast API host; empty (the NewSolcast default)
	// uses the production host. Exported so integration tests in other
	// packages can point a real SolcastClient at a local fake server.
	BaseURL string

	mu    sync.RWMutex
	cache *SolarForecast

	// persistWarned rate-limits the "cache unwritable, degrading to in-memory
	// only" warning to once per process, even though every successful Fetch
	// retries the write.
	persistWarned atomic.Bool
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

// NewSolcast constructs a client and, if cfg.CacheDir holds a previously
// persisted forecast, loads it immediately so Cached() returns it without
// waiting for the first Fetch — the key behaviour that lets a restart reuse
// the last forecast instead of spending a Solcast API call.
func NewSolcast(cfg config.Solcast) *SolcastClient {
	ids := make([]string, 0, len(cfg.Sites))
	for _, s := range cfg.Sites {
		if s.ID != "" {
			ids = append(ids, s.ID)
		}
	}
	c := &SolcastClient{
		apiKey:   cfg.APIKey,
		siteIDs:  ids,
		http:     &http.Client{Timeout: 30 * time.Second},
		cacheDir: cfg.CacheDir,
		BaseURL:  solcastBaseURL,
	}
	c.loadCache()
	return c
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
	c.persistCache(forecast)
	return forecast, nil
}

// fetchSite retrieves one site's forecast periods.
func (c *SolcastClient) fetchSite(ctx context.Context, siteID string) ([]SolarPoint, error) {
	url := fmt.Sprintf(
		"%s/rooftop_sites/%s/forecasts?format=json",
		c.BaseURL, siteID,
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

func (c *SolcastClient) cachePath() string {
	return filepath.Join(c.cacheDir, SolcastCacheFileName)
}

// loadCache restores the last persisted forecast from CacheDir so a restart
// can reuse it instead of spending a Solcast API call — the free tier's daily
// request cap is easily exhausted by a handful of restarts. A missing or
// corrupt cache file is a silent clean start: never fatal, the client just
// behaves as if it had never fetched.
func (c *SolcastClient) loadCache() {
	if c.cacheDir == "" {
		return
	}
	data, err := os.ReadFile(c.cachePath())
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			slog.Warn("solcast: cache read failed, starting without a cached forecast", "error", err)
		}
		return
	}
	var sf SolarForecast
	if err := json.Unmarshal(data, &sf); err != nil {
		slog.Warn("solcast: cache decode failed, starting without a cached forecast", "error", err)
		return
	}
	c.mu.Lock()
	c.cache = &sf
	c.mu.Unlock()
}

// persistCache writes the just-fetched forecast to CacheDir (atomic temp+
// rename, mirroring pvmodel's persistence pattern) so it survives a restart.
// The cache holds forecast data only — no API key — so it's safe to leave on
// the persistent volume. An unwritable directory degrades to in-memory-only
// operation; that failure is logged once, not on every fetch, to avoid log
// spam from a persistently read-only volume.
func (c *SolcastClient) persistCache(forecast *SolarForecast) {
	if c.cacheDir == "" {
		return
	}
	data, err := json.MarshalIndent(forecast, "", "  ")
	if err != nil {
		slog.Warn("solcast: cache encode failed", "error", err)
		return
	}
	if err := atomicWriteCache(c.cachePath(), data); err != nil {
		if !c.persistWarned.Swap(true) {
			slog.Warn("solcast: cache persist failed — continuing in-memory only", "error", err)
		}
		return
	}
}

// atomicWriteCache writes data to a temp file in dir and renames it over
// path, so a crash never leaves a half-written cache file.
func atomicWriteCache(path string, data []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("solcast: preparing cache dir: %w", err)
	}
	tmp, err := os.CreateTemp(dir, ".solcast-cache-*.tmp")
	if err != nil {
		return fmt.Errorf("solcast: temp file: %w", err)
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return fmt.Errorf("solcast: write temp: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return fmt.Errorf("solcast: sync temp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("solcast: close temp: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("solcast: rename cache: %w", err)
	}
	return nil
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
