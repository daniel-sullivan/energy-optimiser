package influx

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"energy-optimiser/config"
)

// TestQueryPowerFiltersDomain verifies the VM export query pins domain="sensor"
// in the match[] label filter — defense-in-depth so a future non-sensor
// entity (switch/number) that happens to share a sensor's short entity_id
// can never be silently picked up by the load model's training query.
func TestQueryPowerFiltersDomain(t *testing.T) {
	var gotMatch string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMatch = r.URL.Query().Get("match[]")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c, err := New(config.InfluxDB{
		URL:          srv.URL,
		Measurements: config.Measurements{Power: "W"},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	to := time.Now()
	if _, err := c.QueryPower(context.Background(), "sensor.load_power", to.Add(-time.Hour), to); err != nil {
		t.Fatalf("QueryPower: %v", err)
	}

	want := `{__name__="W_value",entity_id="load_power",domain="sensor"}`
	if gotMatch != want {
		t.Errorf("match[] = %q, want %q", gotMatch, want)
	}
}
