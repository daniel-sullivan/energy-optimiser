package forecast

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"energy-optimiser/config"
)

// solcastFakeServer returns a canned single-period forecast for any site,
// counting requests so tests can assert on call counts.
func solcastFakeServer(t *testing.T, hits *int) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*hits++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"forecasts":[
			{"pv_estimate":1.5,"pv_estimate10":1.0,"pv_estimate90":2.0,"period_end":"2026-07-20T01:00:00Z","period":"PT30M"}
		]}`))
	}))
}

func testSolcastCfg(cacheDir string) config.Solcast {
	return config.Solcast{
		APIKey:   "test-key",
		Sites:    []config.SolcastSite{{Name: "roof", ID: "site-1"}},
		CacheDir: cacheDir,
	}
}

// TestSolcastFetchPersistsCache proves a successful Fetch writes the combined
// forecast to CacheDir as SolcastCacheFileName.
func TestSolcastFetchPersistsCache(t *testing.T) {
	var hits int
	server := solcastFakeServer(t, &hits)
	defer server.Close()

	dir := t.TempDir()
	client := NewSolcast(testSolcastCfg(dir))
	client.BaseURL = server.URL

	forecast, err := client.Fetch(context.Background())
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if hits != 1 {
		t.Fatalf("hits = %d, want 1", hits)
	}

	path := filepath.Join(dir, SolcastCacheFileName)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("cache file not written: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("cache file empty")
	}
	if len(forecast.Points) != 1 || forecast.Points[0].EstimateKW != 1.5 {
		t.Fatalf("unexpected forecast: %+v", forecast)
	}
}

// TestSolcastCacheRoundTrip proves a fresh client constructed against the same
// CacheDir loads the persisted forecast without any HTTP call, and Cached()
// returns the same points a prior client fetched — the restart scenario.
func TestSolcastCacheRoundTrip(t *testing.T) {
	var hits int
	server := solcastFakeServer(t, &hits)
	defer server.Close()

	dir := t.TempDir()
	cfg := testSolcastCfg(dir)

	client1 := NewSolcast(cfg)
	client1.BaseURL = server.URL
	fetched, err := client1.Fetch(context.Background())
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if hits != 1 {
		t.Fatalf("hits after first fetch = %d, want 1", hits)
	}

	// A brand-new client (simulating a process restart) against the same
	// CacheDir must load the persisted forecast at construction time, with NO
	// HTTP call.
	client2 := NewSolcast(cfg)
	client2.BaseURL = server.URL
	if hits != 1 {
		t.Fatalf("hits after constructing a fresh client = %d, want 1 (construction must not fetch)", hits)
	}

	got := client2.Cached()
	if got == nil {
		t.Fatal("Cached() = nil after loading a persisted cache")
	}
	if !got.FetchedAt.Equal(fetched.FetchedAt) {
		t.Errorf("FetchedAt = %v, want %v", got.FetchedAt, fetched.FetchedAt)
	}
	if len(got.Points) != len(fetched.Points) {
		t.Fatalf("Points len = %d, want %d", len(got.Points), len(fetched.Points))
	}
	for i := range got.Points {
		if !got.Points[i].Time.Equal(fetched.Points[i].Time) || got.Points[i].EstimateKW != fetched.Points[i].EstimateKW {
			t.Errorf("point %d = %+v, want %+v", i, got.Points[i], fetched.Points[i])
		}
	}
}

// TestSolcastLoadCacheMissing proves a missing cache file is a silent clean
// start: no panic, Cached() returns nil.
func TestSolcastLoadCacheMissing(t *testing.T) {
	client := NewSolcast(testSolcastCfg(t.TempDir()))
	if got := client.Cached(); got != nil {
		t.Fatalf("Cached() = %+v, want nil for a missing cache file", got)
	}
}

// TestSolcastLoadCacheCorrupt proves a corrupt cache file is a silent clean
// start: no panic, Cached() returns nil.
func TestSolcastLoadCacheCorrupt(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, SolcastCacheFileName), []byte("{not valid json"), 0o644); err != nil {
		t.Fatal(err)
	}
	client := NewSolcast(testSolcastCfg(dir))
	if got := client.Cached(); got != nil {
		t.Fatalf("Cached() = %+v, want nil for a corrupt cache file", got)
	}
}

// TestSolcastLoadCacheNoCacheDir proves an empty CacheDir (cache disabled)
// never attempts a read and behaves exactly as before this feature existed.
func TestSolcastLoadCacheNoCacheDir(t *testing.T) {
	client := NewSolcast(testSolcastCfg(""))
	if got := client.Cached(); got != nil {
		t.Fatalf("Cached() = %+v, want nil with no CacheDir configured", got)
	}
}

// TestSolcastPersistUnwritableDirDegradesInMemory proves that when CacheDir
// cannot be written to, Fetch still succeeds and Cached() still returns the
// freshly fetched forecast — persistence failure degrades to in-memory-only,
// it never breaks the fetch path.
func TestSolcastPersistUnwritableDirDegradesInMemory(t *testing.T) {
	var hits int
	server := solcastFakeServer(t, &hits)
	defer server.Close()

	// A file (not a directory) as CacheDir makes MkdirAll fail every time.
	parent := t.TempDir()
	blocker := filepath.Join(parent, "blocker")
	if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	unwritable := filepath.Join(blocker, "cache")

	client := NewSolcast(testSolcastCfg(unwritable))
	client.BaseURL = server.URL

	forecast, err := client.Fetch(context.Background())
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if got := client.Cached(); got != forecast {
		t.Error("Cached() did not return the fetched forecast despite an unwritable cache dir")
	}
}
