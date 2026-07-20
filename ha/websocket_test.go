package ha

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"energy-optimiser/config"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

// fakeHA is a minimal Home Assistant websocket server for tests. It performs
// the auth handshake, answers get_states / subscribe_events, and (on the first
// connection only) closes the socket right after the client subscribes — which
// forces the client's supervisor to reconnect. The second connection reports a
// different SoC so the test can prove the reconnect re-fetched fresh state.
func fakeHA(t *testing.T) *httptest.Server {
	t.Helper()
	var conns atomic.Int64
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			return
		}
		n := conns.Add(1)
		ctx := r.Context()

		_ = wsjson.Write(ctx, c, map[string]any{"type": "auth_required"})
		var auth map[string]any
		if err := wsjson.Read(ctx, c, &auth); err != nil {
			return
		}
		_ = wsjson.Write(ctx, c, map[string]any{"type": "auth_ok", "ha_version": "test"})

		soc := "50"
		if n >= 2 {
			soc = "20" // fresh value after reconnect
		}
		for {
			var req map[string]any
			if err := wsjson.Read(ctx, c, &req); err != nil {
				return
			}
			switch req["type"] {
			case "get_states":
				_ = wsjson.Write(ctx, c, map[string]any{
					"id": req["id"], "type": "result", "success": true,
					"result": []map[string]any{{
						"entity_id": "sensor.soc", "state": soc, "attributes": map[string]any{},
					}},
				})
			case "subscribe_events":
				_ = wsjson.Write(ctx, c, map[string]any{"id": req["id"], "type": "result", "success": true})
				if n == 1 {
					_ = c.Close(websocket.StatusNormalClosure, "drop") // force a reconnect
					return
				}
			}
		}
	}))
}

func wsURL(s *httptest.Server) string { return "ws" + strings.TrimPrefix(s.URL, "http") }

// TestClientReconnects proves a dropped connection is transparently recovered:
// the client re-authenticates, re-fetches states, and the cache reflects the
// post-reconnect value instead of freezing at the pre-drop one (the production
// failure this fix addresses).
func TestClientReconnects(t *testing.T) {
	srv := fakeHA(t)
	defer srv.Close()

	c := New(config.HomeAssistant{URL: wsURL(srv), Token: "x"})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := c.Connect(ctx); err != nil {
		t.Fatalf("connect: %v", err)
	}
	if got := c.StateFloat("sensor.soc"); got != 50 {
		t.Fatalf("initial soc = %v, want 50", got)
	}
	if err := c.SubscribeEvents(ctx); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	// The server drops connection #1; the supervisor must reconnect and pick up
	// the fresh value (20). Without reconnect the cache would freeze at 50.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if c.StateFloat("sensor.soc") == 20 && c.Connected() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("after reconnect soc = %v (connected=%v), want 20", c.StateFloat("sensor.soc"), c.Connected())
}

// TestNewestUpdateTracksFreshness confirms the staleness signal advances as
// state arrives, so a frozen feed can be detected downstream.
func TestNewestUpdateTracksFreshness(t *testing.T) {
	srv := fakeHA(t)
	defer srv.Close()

	c := New(config.HomeAssistant{URL: wsURL(srv), Token: "x"})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	before := time.Now()
	if err := c.Connect(ctx); err != nil {
		t.Fatalf("connect: %v", err)
	}
	if nu := c.NewestUpdate(); nu.Before(before) {
		t.Fatalf("NewestUpdate = %v, want >= %v", nu, before)
	}
}
