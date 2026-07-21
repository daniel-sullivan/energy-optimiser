package hub

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"energy-optimiser/config"
	"energy-optimiser/forecast"
)

// solcastFakeServer counts hits and serves a minimal, valid forecast so a
// triggered Fetch succeeds cleanly.
func solcastFakeServer(hits *int32) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(hits, 1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"forecasts":[
			{"pv_estimate":1.0,"period_end":"2026-07-20T11:00:00Z","period":"PT30M"}
		]}`))
	}))
}

// writeSolcastCache seeds a cache file under dir exactly as forecast.NewSolcast
// would read it back at construction time — i.e. it simulates a prior
// process's persisted forecast, so constructing a new SolcastClient against
// dir is equivalent to a process restart.
func writeSolcastCache(t *testing.T, dir string, f forecast.SolarForecast) {
	t.Helper()
	data, err := json.MarshalIndent(&f, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, forecast.SolcastCacheFileName), data, 0o644); err != nil {
		t.Fatal(err)
	}
}

func testSolcastConfig(cacheDir string) config.Solcast {
	return config.Solcast{
		APIKey:    "test-key",
		Sites:     []config.SolcastSite{{Name: "roof", ID: "site-1"}},
		PollTimes: []config.TimeOfDay{{Hour: 6, Minute: 0}},
		CacheDir:  cacheDir,
	}
}

// TestMaybeRefreshSolarRestartWithinWindowSkipsFetch is the restart scenario
// the cache exists to fix: a persisted forecast fetched after today's only
// poll_time is still "fresh" by maybeRefreshSolar's own gate, so a client
// constructed against that cache dir (standing in for a fresh process after a
// restart) must make ZERO Solcast API calls.
func TestMaybeRefreshSolarRestartWithinWindowSkipsFetch(t *testing.T) {
	var hits int32
	server := solcastFakeServer(&hits)
	defer server.Close()

	dir := t.TempDir()
	now := time.Date(2026, 7, 20, 10, 0, 0, 0, time.UTC)
	// Fetched after today's 06:00 poll_time already passed — fresh.
	writeSolcastCache(t, dir, forecast.SolarForecast{FetchedAt: now.Add(-1 * time.Hour)})

	cfg := testSolcastConfig(dir)
	sc := forecast.NewSolcast(cfg) // "restart": loads the persisted cache from disk
	sc.BaseURL = server.URL

	h := &Hub{cfg: &config.Config{Solcast: cfg}, solcast: sc}
	h.maybeRefreshSolar(context.Background(), now)

	if got := atomic.LoadInt32(&hits); got != 0 {
		t.Fatalf("HTTP calls = %d, want 0 (restart within the freshness window must not fetch)", got)
	}
	cached := sc.Cached()
	if cached == nil || !cached.FetchedAt.Equal(now.Add(-1*time.Hour)) {
		t.Fatalf("cache was mutated despite no fetch: %+v", cached)
	}
}

// TestMaybeRefreshSolarStaleCacheStillFetches is the control case: a
// persisted cache fetched BEFORE the last poll_time still triggers exactly
// one fetch, proving the freshness skip doesn't just always skip.
func TestMaybeRefreshSolarStaleCacheStillFetches(t *testing.T) {
	var hits int32
	server := solcastFakeServer(&hits)
	defer server.Close()

	dir := t.TempDir()
	now := time.Date(2026, 7, 20, 10, 0, 0, 0, time.UTC)
	// Fetched yesterday, before today's 06:00 poll_time — stale.
	writeSolcastCache(t, dir, forecast.SolarForecast{FetchedAt: now.Add(-24 * time.Hour)})

	cfg := testSolcastConfig(dir)
	sc := forecast.NewSolcast(cfg)
	sc.BaseURL = server.URL

	h := &Hub{cfg: &config.Config{Solcast: cfg}, solcast: sc}
	h.maybeRefreshSolar(context.Background(), now)

	if got := atomic.LoadInt32(&hits); got != 1 {
		t.Fatalf("HTTP calls = %d, want 1 (stale cache must trigger exactly one fetch)", got)
	}
	cached := sc.Cached()
	if cached == nil {
		t.Fatal("cache is nil after a fetch")
	}
	if time.Since(cached.FetchedAt) > time.Minute {
		t.Fatalf("cache not refreshed: FetchedAt = %v, want close to wall-clock now", cached.FetchedAt)
	}
}

// TestMaybeRefreshSolarNoCacheFetchesImmediately covers the pre-existing
// cold-start path (no persisted cache at all, e.g. first-ever run): it must
// still fetch immediately rather than waiting for a poll_time.
func TestMaybeRefreshSolarNoCacheFetchesImmediately(t *testing.T) {
	var hits int32
	server := solcastFakeServer(&hits)
	defer server.Close()

	dir := t.TempDir() // no cache file written
	cfg := testSolcastConfig(dir)
	sc := forecast.NewSolcast(cfg)
	sc.BaseURL = server.URL

	h := &Hub{cfg: &config.Config{Solcast: cfg}, solcast: sc}
	now := time.Date(2026, 7, 20, 3, 0, 0, 0, time.UTC) // well before the 06:00 poll_time
	h.maybeRefreshSolar(context.Background(), now)

	if got := atomic.LoadInt32(&hits); got != 1 {
		t.Fatalf("HTTP calls = %d, want 1 (a nil cache must always trigger an immediate fetch)", got)
	}
}
