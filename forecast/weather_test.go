package forecast

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"energy-optimiser/config"
)

// TestWeatherFetch spins a canned Open-Meteo server and exercises Fetch
// against three sites: two distinct orientations plus one duplicate of the
// first (to prove orientation-sharing sites are deduped into one request).
func TestWeatherFetch(t *testing.T) {
	// Canned hourly GTI series stamped at Open-Meteo's PRECEDING-hour
	// convention, spanning several days apart to stand in for the
	// past-21/future-7 window.
	stamps := []time.Time{
		time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC),  // "past" end of window
		time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC), // "now"
		time.Date(2024, 1, 22, 10, 0, 0, 0, time.UTC), // "future" end of window
	}

	var mu sync.Mutex
	var requests []url.Values

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		mu.Lock()
		requests = append(requests, q)
		mu.Unlock()

		tilt, err := strconv.ParseFloat(q.Get("tilt"), 64)
		if err != nil {
			http.Error(w, "bad tilt", http.StatusBadRequest)
			return
		}
		// Value is derived from tilt so the test can verify each site's GTI
		// map entry came from the right orientation's response.
		value := tilt * 10

		resp := openMeteoResponse{}
		for _, s := range stamps {
			resp.Hourly.Time = append(resp.Hourly.Time, s.Unix())
			resp.Hourly.GTI = append(resp.Hourly.GTI, value)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewWeather(config.Weather{Latitude: -27.4698, Longitude: 153.0251})
	client.baseURL = server.URL

	sites := []config.SolcastSite{
		{Name: "roof-a", ID: "a", Tilt: 30, Azimuth: 160}, // compass 160 -> om -20
		{Name: "roof-b", ID: "b", Tilt: 20, Azimuth: 112}, // compass 112 -> om -68
		{Name: "roof-c", ID: "c", Tilt: 30, Azimuth: 160}, // shares roof-a's orientation
	}

	forecast, err := client.Fetch(context.Background(), sites)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}

	// Dedup: roof-a and roof-c share an orientation, so only 2 distinct
	// requests (one per orientation) should have been made despite 3 sites.
	mu.Lock()
	gotRequests := requests
	mu.Unlock()
	if len(gotRequests) != 2 {
		t.Fatalf("expected 2 deduped HTTP requests (2 distinct orientations), got %d", len(gotRequests))
	}

	// (d) compass -> Open-Meteo azimuth conversion, plus C2a request params
	// (timezone=UTC, timeformat=unixtime) and the past/future window.
	wantAzimuthByTilt := map[string]string{"30.0": "-20.0", "20.0": "-68.0"}
	for _, q := range gotRequests {
		wantAz, ok := wantAzimuthByTilt[q.Get("tilt")]
		if !ok {
			t.Fatalf("unexpected tilt in request: %q", q.Get("tilt"))
		}
		if got := q.Get("azimuth"); got != wantAz {
			t.Errorf("tilt=%s: azimuth = %q, want %q (compass->Open-Meteo conversion)", q.Get("tilt"), got, wantAz)
		}
		if got := q.Get("timezone"); got != "UTC" {
			t.Errorf("timezone = %q, want UTC", got)
		}
		if got := q.Get("timeformat"); got != "unixtime" {
			t.Errorf("timeformat = %q, want unixtime", got)
		}
		if got := q.Get("past_days"); got != "21" {
			t.Errorf("past_days = %q, want 21", got)
		}
		if got := q.Get("forecast_days"); got != "7" {
			t.Errorf("forecast_days = %q, want 7", got)
		}
	}

	// (c) past+future span present.
	if len(forecast.Points) != len(stamps) {
		t.Fatalf("got %d points, want %d", len(forecast.Points), len(stamps))
	}
	if !sort.SliceIsSorted(forecast.Points, func(i, j int) bool {
		return forecast.Points[i].Time.Before(forecast.Points[j].Time)
	}) {
		t.Errorf("points are not chronologically sorted")
	}
	span := forecast.Points[len(forecast.Points)-1].Time.Sub(forecast.Points[0].Time)
	if span < 20*24*time.Hour {
		t.Errorf("span between earliest and latest point = %v, want >= 20 days (past+future coverage)", span)
	}

	// (a) C2b period-start shift: the server stamped 2024-01-15T10:00:00Z
	// (mean of the PRECEDING hour), so the point must land at 09:00 UTC.
	wantMid := time.Date(2024, 1, 15, 9, 0, 0, 0, time.UTC)
	var midPoint *GTIPoint
	for i := range forecast.Points {
		if forecast.Points[i].Time.Equal(wantMid) {
			midPoint = &forecast.Points[i]
			break
		}
	}
	if midPoint == nil {
		t.Fatalf("no point at shifted period-start time %v; times: %v", wantMid, pointTimes(forecast.Points))
	}
	if midPoint.Time.Location() != time.UTC {
		t.Errorf("point time location = %v, want UTC", midPoint.Time.Location())
	}

	// (b) per-site GTI map has an entry per configured site, and each entry
	// came from the correct orientation's response (value = tilt*10).
	for name, wantVal := range map[string]float64{"roof-a": 300, "roof-b": 200, "roof-c": 300} {
		got, ok := midPoint.GTI[name]
		if !ok {
			t.Errorf("GTI map missing site %q", name)
			continue
		}
		if got != wantVal {
			t.Errorf("GTI[%q] = %v, want %v", name, got, wantVal)
		}
	}

	// Cached() should return the same forecast just fetched.
	if cached := client.Cached(); cached != forecast {
		t.Errorf("Cached() did not return the fetched forecast")
	}
}

func TestWeatherFetchNoSites(t *testing.T) {
	client := NewWeather(config.Weather{Latitude: 1, Longitude: 2})
	if _, err := client.Fetch(context.Background(), nil); err == nil {
		t.Fatalf("expected error for no sites configured")
	}
}

func pointTimes(points []GTIPoint) []time.Time {
	times := make([]time.Time, len(points))
	for i, p := range points {
		times[i] = p.Time
	}
	return times
}
