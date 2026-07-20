package forecast

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"energy-optimiser/config"
)

// WeatherClient fetches 72h weather forecasts from Open-Meteo.
type WeatherClient struct {
	lat, lon float64
	http     *http.Client
}

// WeatherForecast contains hourly weather data.
type WeatherForecast struct {
	FetchedAt time.Time
	Points    []WeatherPoint
}

// WeatherPoint is a single hourly observation.
type WeatherPoint struct {
	Time        time.Time
	Temperature float64 // °C
	Humidity    float64 // %
	CloudCover  float64 // %
}

func NewWeather(cfg config.Weather) *WeatherClient {
	return &WeatherClient{
		lat:  cfg.Latitude,
		lon:  cfg.Longitude,
		http: &http.Client{Timeout: 30 * time.Second},
	}
}

// Fetch retrieves the 72h weather forecast.
func (c *WeatherClient) Fetch(ctx context.Context) (*WeatherForecast, error) {
	url := fmt.Sprintf(
		"https://api.open-meteo.com/v1/forecast?latitude=%.4f&longitude=%.4f"+
			"&hourly=temperature_2m,relative_humidity_2m,cloud_cover"+
			"&forecast_days=3&timezone=auto",
		c.lat, c.lon,
	)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
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
	n := min(len(h.Time), len(h.Temperature), len(h.Humidity), len(h.CloudCover))
	forecast := &WeatherForecast{
		FetchedAt: time.Now(),
		Points:    make([]WeatherPoint, 0, n),
	}
	for i := range n {
		t, err := time.Parse("2006-01-02T15:04", h.Time[i])
		if err != nil {
			continue
		}
		forecast.Points = append(forecast.Points, WeatherPoint{
			Time:        t,
			Temperature: h.Temperature[i],
			Humidity:    h.Humidity[i],
			CloudCover:  h.CloudCover[i],
		})
	}
	return forecast, nil
}

// TemperatureAt returns the interpolated temperature at a given time.
func (f *WeatherForecast) TemperatureAt(t time.Time) float64 {
	if f == nil || len(f.Points) == 0 {
		return 25 // default
	}
	// Find bracketing points
	for i := 1; i < len(f.Points); i++ {
		if f.Points[i].Time.After(t) {
			p0, p1 := f.Points[i-1], f.Points[i]
			frac := t.Sub(p0.Time).Seconds() / p1.Time.Sub(p0.Time).Seconds()
			return p0.Temperature + frac*(p1.Temperature-p0.Temperature)
		}
	}
	return f.Points[len(f.Points)-1].Temperature
}

type openMeteoResponse struct {
	Hourly struct {
		Time        []string  `json:"time"`
		Temperature []float64 `json:"temperature_2m"`
		Humidity    []float64 `json:"relative_humidity_2m"`
		CloudCover  []float64 `json:"cloud_cover"`
	} `json:"hourly"`
}
