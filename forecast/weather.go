package forecast

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"sync"
	"time"

	"energy-optimiser/config"
)

// openMeteoBaseURL is the production Open-Meteo host.
const openMeteoBaseURL = "https://api.open-meteo.com"

// WeatherClient fetches global tilted irradiance (GTI) forecasts from
// Open-Meteo for the far-horizon PV model. Sites (with their tilt/azimuth)
// are supplied at Fetch time, not construction.
type WeatherClient struct {
	lat, lon float64
	http     *http.Client
	baseURL  string // overridden in tests; defaults to openMeteoBaseURL

	mu    sync.RWMutex
	cache *WeatherForecast
}

// WeatherForecast is a cached, combined GTI forecast spanning past+future days.
type WeatherForecast struct {
	FetchedAt time.Time
	Points    []GTIPoint // hourly, chronological, period-START timestamps in UTC
}

// GTIPoint is one hour's global tilted irradiance, per site.
type GTIPoint struct {
	Time time.Time          // start of the hour, UTC
	GTI  map[string]float64 // site name -> plane-of-array irradiance (W/m²) for that hour
}

func NewWeather(cfg config.Weather) *WeatherClient {
	return &WeatherClient{
		lat:     cfg.Latitude,
		lon:     cfg.Longitude,
		http:    &http.Client{Timeout: 30 * time.Second},
		baseURL: openMeteoBaseURL,
	}
}

// orientation identifies a distinct tilt/azimuth combination. Sites sharing
// an orientation are served from a single Open-Meteo request.
type orientation struct {
	tilt, azimuth float64 // azimuth in the config's compass convention (180=south)
}

// gtiSample is one hourly GTI value for a single orientation.
type gtiSample struct {
	Time  time.Time
	Value float64
}

// Fetch retrieves hourly GTI (past 21 days + next 7 days) for each configured
// site's orientation, merges the per-orientation series into a per-site GTI
// map keyed by hour, and caches the combined result. Sites that share a
// tilt/azimuth are fetched once and reused.
func (c *WeatherClient) Fetch(ctx context.Context, sites []config.SolcastSite) (*WeatherForecast, error) {
	if len(sites) == 0 {
		return nil, fmt.Errorf("weather: no sites configured")
	}

	// Group site names by orientation so identical tilt/azimuth pairs only
	// trigger one HTTP call.
	groups := map[orientation][]string{}
	for _, s := range sites {
		key := orientation{tilt: s.Tilt, azimuth: s.Azimuth}
		groups[key] = append(groups[key], s.Name)
	}

	times := map[int64]time.Time{}
	gti := map[int64]map[string]float64{} // period-start unix -> site name -> W/m²

	for key, names := range groups {
		samples, err := c.fetchOrientation(ctx, key)
		if err != nil {
			return nil, fmt.Errorf("weather orientation tilt=%.1f azimuth=%.1f: %w", key.tilt, key.azimuth, err)
		}
		for _, sm := range samples {
			ts := sm.Time.Unix()
			times[ts] = sm.Time
			if gti[ts] == nil {
				gti[ts] = make(map[string]float64, len(names))
			}
			for _, name := range names {
				gti[ts][name] = sm.Value
			}
		}
	}

	points := make([]GTIPoint, 0, len(gti))
	for ts, m := range gti {
		points = append(points, GTIPoint{Time: times[ts], GTI: m})
	}
	sort.Slice(points, func(i, j int) bool { return points[i].Time.Before(points[j].Time) })

	forecast := &WeatherForecast{FetchedAt: time.Now(), Points: points}
	c.mu.Lock()
	c.cache = forecast
	c.mu.Unlock()
	return forecast, nil
}

// fetchOrientation retrieves the hourly GTI series for one tilt/azimuth pair,
// spanning the past 21 days and next 7 days.
func (c *WeatherClient) fetchOrientation(ctx context.Context, o orientation) ([]gtiSample, error) {
	// Open-Meteo's azimuth convention is 0=south, -90=east, +90=west, which
	// differs from the config's compass convention (180=south). Convert.
	omAzimuth := o.azimuth - 180

	q := url.Values{}
	q.Set("latitude", fmt.Sprintf("%.4f", c.lat))
	q.Set("longitude", fmt.Sprintf("%.4f", c.lon))
	q.Set("hourly", "global_tilted_irradiance")
	q.Set("tilt", fmt.Sprintf("%.1f", o.tilt))
	q.Set("azimuth", fmt.Sprintf("%.1f", omAzimuth))
	q.Set("past_days", "21")
	q.Set("forecast_days", "7")
	// timezone=UTC + timeformat=unixtime avoids the local-string/UTC
	// ambiguity of timezone=auto: timestamps decode as unambiguous epoch
	// seconds rather than requiring a timezone-aware string parse.
	q.Set("timezone", "UTC")
	q.Set("timeformat", "unixtime")

	reqURL := c.baseURL + "/v1/forecast?" + q.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("weather fetch: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("weather: HTTP %d", resp.StatusCode)
	}

	var result openMeteoResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("weather decode: %w", err)
	}

	h := result.Hourly
	n := min(len(h.Time), len(h.GTI))
	samples := make([]gtiSample, 0, n)
	for i := range n {
		// Open-Meteo hourly radiation values are the mean of the PRECEDING
		// hour (the value stamped 10:00 covers 09:00-10:00). Shift back one
		// hour so Time is the START of the hour the irradiance applies to.
		t := time.Unix(h.Time[i], 0).UTC().Add(-time.Hour)
		samples = append(samples, gtiSample{Time: t, Value: h.GTI[i]})
	}
	return samples, nil
}

// Cached returns the most recent cached forecast, or nil.
func (c *WeatherClient) Cached() *WeatherForecast {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cache
}

type openMeteoResponse struct {
	Hourly struct {
		Time []int64   `json:"time"`
		GTI  []float64 `json:"global_tilted_irradiance"`
	} `json:"hourly"`
}
