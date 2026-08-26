package ha

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"energy-optimiser/config"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
				if n == 1 && req["id"].(float64) >= 3 {
					_ = c.Close(websocket.StatusNormalClosure, "drop")
					return
				}
			case "subscribe_events":
				_ = wsjson.Write(ctx, c, map[string]any{"id": req["id"], "type": "result", "success": true})
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
	if err := c.SubscribeEvents(ctx); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer func() { _ = c.Close() }()
	if got := c.StateFloat("sensor.soc"); got != 50 {
		t.Fatalf("initial soc = %v, want 50", got)
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
func TestUpdateGenerationAdvancesForFetchedState(t *testing.T) {
	srv := fakeHA(t)
	defer srv.Close()

	c := New(config.HomeAssistant{URL: wsURL(srv), Token: "x"})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, c.Connect(ctx), "connect")
	require.NoError(t, c.SubscribeEvents(ctx), "subscribe")
	defer func() { _ = c.Close() }()
	if generation := c.UpdateGeneration("sensor.soc"); generation != 1 {
		assert.Failf(t, "assertion failed", "initial fetched state generation = %d, want 1", generation)
	}
}

func TestSubscriptionSurvivesRunContextCancellationUntilClose(t *testing.T) {
	serviceCalled := make(chan struct{}, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			return
		}
		defer func() { _ = conn.Close(websocket.StatusNormalClosure, "test done") }()
		ctx := r.Context()
		_ = wsjson.Write(ctx, conn, map[string]any{"type": "auth_required"})
		var auth map[string]any
		if wsjson.Read(ctx, conn, &auth) != nil {
			return
		}
		_ = wsjson.Write(ctx, conn, map[string]any{"type": "auth_ok"})
		for {
			var req map[string]any
			if wsjson.Read(ctx, conn, &req) != nil {
				return
			}
			switch req["type"] {
			case "get_states":
				_ = wsjson.Write(ctx, conn, map[string]any{"id": req["id"], "type": "result", "success": true, "result": []any{}})
			case "subscribe_events":
				_ = wsjson.Write(ctx, conn, map[string]any{"id": req["id"], "type": "result", "success": true})
			case "call_service":
				serviceCalled <- struct{}{}
				_ = wsjson.Write(ctx, conn, map[string]any{"id": req["id"], "type": "result", "success": true})
				_ = wsjson.Write(ctx, conn, map[string]any{
					"type": "event",
					"event": map[string]any{"data": map[string]any{
						"entity_id": "switch.timed_charge",
						"new_state": map[string]any{"state": "off", "attributes": map[string]any{}},
					}},
				})
			}
		}
	}))
	defer srv.Close()

	c := New(config.HomeAssistant{URL: wsURL(srv), Token: "x"})
	runCtx, cancelRun := context.WithCancel(context.Background())
	require.NoError(t, c.Connect(runCtx), "connect")
	require.NoError(t, c.SubscribeEvents(runCtx), "subscribe")
	cancelRun()

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), time.Second)
	defer cancelShutdown()
	require.NoError(t, c.CallServiceAck(shutdownCtx, "switch", "turn_off", map[string]any{"entity_id": "switch.timed_charge"}), "shutdown disable acknowledgement after Run cancellation")
	select {
	case <-serviceCalled:
	case <-time.After(time.Second):
		assert.Fail(t, "shutdown disable was not delivered after Run cancellation")
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if c.Fresh("switch.timed_charge", time.Second) && c.State("switch.timed_charge") == "off" {
			_ = c.Close()
			return
		}
		time.Sleep(time.Millisecond)
	}
	_ = c.Close()
	assert.Fail(t, "shutdown disable state update was not consumed after Run cancellation")
}

func TestInitialSubscribeCancellationClosesCandidate(t *testing.T) {
	subscribed := make(chan struct{})
	clientGone := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			return
		}
		ctx := r.Context()
		_ = wsjson.Write(ctx, conn, map[string]any{"type": "auth_required"})
		var auth map[string]any
		if wsjson.Read(ctx, conn, &auth) != nil {
			return
		}
		_ = wsjson.Write(ctx, conn, map[string]any{"type": "auth_ok"})
		for {
			var req map[string]any
			if wsjson.Read(ctx, conn, &req) != nil {
				close(clientGone)
				return
			}
			switch req["type"] {
			case "get_states":
				_ = wsjson.Write(ctx, conn, map[string]any{"id": req["id"], "type": "result", "success": true, "result": []any{}})
			case "subscribe_events":
				close(subscribed)
			}
		}
	}))
	defer srv.Close()

	c := New(config.HomeAssistant{URL: wsURL(srv), Token: "x"})
	require.NoError(t, c.Connect(context.Background()), "connect")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.SubscribeEvents(ctx) }()
	<-subscribed
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		assert.Failf(t, "assertion failed", "subscribe error = %v, want context canceled", err)
	}
	select {
	case <-clientGone:
	case <-time.After(time.Second):
		assert.Fail(t, "cancelled initial subscribe did not close its private candidate")
	}
	if c.Connected() {
		assert.Fail(t, "cancelled initial subscribe must not activate the candidate")
	}
}

func TestConnectRejectsInvalidGetStatesResponses(t *testing.T) {
	for _, tc := range []struct {
		name     string
		response func(id any) map[string]any
	}{
		{name: "wrong id", response: func(id any) map[string]any {
			return map[string]any{"id": id.(float64) + 1, "type": "result", "success": true, "result": []any{}}
		}},
		{name: "unsuccessful", response: func(id any) map[string]any {
			return map[string]any{"id": id, "type": "result", "success": false, "error": map[string]any{"message": "nope"}}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
				if err != nil {
					return
				}
				ctx := r.Context()
				_ = wsjson.Write(ctx, conn, map[string]any{"type": "auth_required"})
				var auth, req map[string]any
				if wsjson.Read(ctx, conn, &auth) != nil || wsjson.Write(ctx, conn, map[string]any{"type": "auth_ok"}) != nil || wsjson.Read(ctx, conn, &req) != nil {
					return
				}
				_ = wsjson.Write(ctx, conn, tc.response(req["id"]))
			}))
			defer srv.Close()
			c := New(config.HomeAssistant{URL: wsURL(srv), Token: "x"})
			if err := c.Connect(context.Background()); err == nil {
				assert.Fail(t, "invalid get_states response must fail connection setup")
			}
			if c.Connected() {
				assert.Fail(t, "failed get_states must not publish a connection")
			}
		})
	}
}

func TestCallServiceRejectedUntilInitialSubscribeCompletes(t *testing.T) {
	subscribed := make(chan struct{})
	releaseSubscribe := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			return
		}
		ctx := r.Context()
		_ = wsjson.Write(ctx, conn, map[string]any{"type": "auth_required"})
		var auth map[string]any
		if wsjson.Read(ctx, conn, &auth) != nil {
			return
		}
		_ = wsjson.Write(ctx, conn, map[string]any{"type": "auth_ok"})
		for {
			var req map[string]any
			if wsjson.Read(ctx, conn, &req) != nil {
				return
			}
			switch req["type"] {
			case "get_states":
				_ = wsjson.Write(ctx, conn, map[string]any{"id": req["id"], "type": "result", "success": true, "result": []any{}})
			case "subscribe_events":
				close(subscribed)
				<-releaseSubscribe
				_ = wsjson.Write(ctx, conn, map[string]any{"id": req["id"], "type": "result", "success": true})
			case "call_service":
				_ = wsjson.Write(ctx, conn, map[string]any{"id": req["id"], "type": "result", "success": true})
			}
		}
	}))
	defer srv.Close()

	c := New(config.HomeAssistant{URL: wsURL(srv), Token: "x"})
	require.NoError(t, c.Connect(context.Background()), "connect")
	subscribeDone := make(chan error, 1)
	go func() { subscribeDone <- c.SubscribeEvents(context.Background()) }()
	<-subscribed
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := c.CallServiceAck(ctx, "number", "set_value", map[string]any{"entity_id": "number.test", "value": 1}); err == nil || !strings.Contains(err.Error(), "not connected") {
		assert.Failf(t, "assertion failed", "service call during private setup error = %v, want not connected", err)
	}
	close(releaseSubscribe)
	require.NoError(t, <-subscribeDone, "subscribe")
	defer func() { _ = c.Close() }()
	require.NoError(t, c.CallServiceAck(ctx, "number", "set_value", map[string]any{"entity_id": "number.test", "value": 1}), "service call after activation")
}

func TestSubscribedSnapshotBuffersEventsBeforeGetStatesResult(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			return
		}
		ctx := r.Context()
		_ = wsjson.Write(ctx, conn, map[string]any{"type": "auth_required"})
		var auth map[string]any
		if wsjson.Read(ctx, conn, &auth) != nil {
			return
		}
		_ = wsjson.Write(ctx, conn, map[string]any{"type": "auth_ok"})
		getStates := 0
		for {
			var req map[string]any
			if wsjson.Read(ctx, conn, &req) != nil {
				return
			}
			switch req["type"] {
			case "get_states":
				getStates++
				if getStates == 2 {
					_ = wsjson.Write(ctx, conn, map[string]any{
						"type": "event",
						"event": map[string]any{"data": map[string]any{
							"entity_id": "sensor.soc",
							"new_state": map[string]any{"state": "60", "attributes": map[string]any{}},
						}},
					})
				}
				_ = wsjson.Write(ctx, conn, map[string]any{
					"id": req["id"], "type": "result", "success": true,
					"result": []map[string]any{{"entity_id": "sensor.soc", "state": "50", "attributes": map[string]any{}}},
				})
			case "subscribe_events":
				_ = wsjson.Write(ctx, conn, map[string]any{"id": req["id"], "type": "result", "success": true})
			}
		}
	}))
	defer srv.Close()

	c := New(config.HomeAssistant{URL: wsURL(srv), Token: "x"})
	require.NoError(t, c.Connect(context.Background()))
	require.NoError(t, c.SubscribeEvents(context.Background()), "subscribe must tolerate an event before the post-subscribe snapshot result")
	defer func() { _ = c.Close() }()
	if got := c.State("sensor.soc"); got != "60" {
		assert.Failf(t, "assertion failed", "buffered event must overlay the snapshot before activation: got %q want 60", got)
	}
}

func TestSubscribedSnapshotRemovesEntityRemovedBeforeResult(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			return
		}
		ctx := r.Context()
		_ = wsjson.Write(ctx, conn, map[string]any{"type": "auth_required"})
		var auth map[string]any
		if wsjson.Read(ctx, conn, &auth) != nil {
			return
		}
		_ = wsjson.Write(ctx, conn, map[string]any{"type": "auth_ok"})
		getStates := 0
		for {
			var req map[string]any
			if wsjson.Read(ctx, conn, &req) != nil {
				return
			}
			switch req["type"] {
			case "get_states":
				getStates++
				if getStates == 2 {
					_ = wsjson.Write(ctx, conn, map[string]any{
						"type": "event",
						"event": map[string]any{"data": map[string]any{
							"entity_id": "sensor.removed",
							"new_state": nil,
						}},
					})
				}
				_ = wsjson.Write(ctx, conn, map[string]any{
					"id": req["id"], "type": "result", "success": true,
					"result": []map[string]any{{"entity_id": "sensor.removed", "state": "99", "attributes": map[string]any{}}},
				})
			case "subscribe_events":
				_ = wsjson.Write(ctx, conn, map[string]any{"id": req["id"], "type": "result", "success": true})
			}
		}
	}))
	defer srv.Close()

	c := New(config.HomeAssistant{URL: wsURL(srv), Token: "x"})
	require.NoError(t, c.Connect(context.Background()))
	require.NoError(t, c.SubscribeEvents(context.Background()))
	defer func() { _ = c.Close() }()
	if c.State("sensor.removed") != "" || c.Fresh("sensor.removed", time.Second) {
		assert.Failf(t, "assertion failed", "removal event must override the snapshot: state=%q fresh=%v", c.State("sensor.removed"), c.Fresh("sensor.removed", time.Second))
	}
}

func TestSubscribeTimesOutWithoutAcknowledgement(t *testing.T) {
	previousTimeout := subscribeTimeout
	subscribeTimeout = 20 * time.Millisecond
	t.Cleanup(func() { subscribeTimeout = previousTimeout })

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			return
		}
		defer func() { _ = conn.Close(websocket.StatusNormalClosure, "test done") }()
		ctx := r.Context()
		_ = wsjson.Write(ctx, conn, map[string]any{"type": "auth_required"})
		var auth map[string]any
		if wsjson.Read(ctx, conn, &auth) != nil {
			return
		}
		_ = wsjson.Write(ctx, conn, map[string]any{"type": "auth_ok"})
		for {
			var req map[string]any
			if wsjson.Read(ctx, conn, &req) != nil {
				return
			}
			switch req["type"] {
			case "get_states":
				_ = wsjson.Write(ctx, conn, map[string]any{"id": req["id"], "type": "result", "success": true, "result": []any{}})
			case "subscribe_events":
				<-ctx.Done()
				return
			}
		}
	}))
	defer srv.Close()

	c := New(config.HomeAssistant{URL: wsURL(srv), Token: "x"})
	require.NoError(t, c.Connect(context.Background()))
	started := time.Now()
	err := c.SubscribeEvents(context.Background())
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(started) > time.Second {
		assert.Failf(t, "assertion failed", "subscribe acknowledgement timeout error=%v elapsed=%s", err, time.Since(started))
	}
}

func TestReconnectSnapshotRemovesMissingEntities(t *testing.T) {
	var conns atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			return
		}
		n := conns.Add(1)
		ctx := r.Context()
		_ = wsjson.Write(ctx, conn, map[string]any{"type": "auth_required"})
		var auth map[string]any
		if wsjson.Read(ctx, conn, &auth) != nil {
			return
		}
		_ = wsjson.Write(ctx, conn, map[string]any{"type": "auth_ok"})
		getStates := 0
		for {
			var req map[string]any
			if wsjson.Read(ctx, conn, &req) != nil {
				return
			}
			switch req["type"] {
			case "get_states":
				getStates++
				result := []map[string]any{{"entity_id": "sensor.keep", "state": "1", "attributes": map[string]any{}}}
				if n == 1 {
					result = append(result, map[string]any{"entity_id": "sensor.removed", "state": "99", "attributes": map[string]any{}})
				}
				_ = wsjson.Write(ctx, conn, map[string]any{"id": req["id"], "type": "result", "success": true, "result": result})
				if n == 1 && getStates == 2 {
					_ = conn.Close(websocket.StatusNormalClosure, "reconnect")
					return
				}
			case "subscribe_events":
				_ = wsjson.Write(ctx, conn, map[string]any{"id": req["id"], "type": "result", "success": true})
			}
		}
	}))
	defer srv.Close()

	c := New(config.HomeAssistant{URL: wsURL(srv), Token: "x"})
	require.NoError(t, c.Connect(context.Background()))
	require.NoError(t, c.SubscribeEvents(context.Background()))
	defer func() { _ = c.Close() }()
	if c.State("sensor.removed") != "99" {
		assert.Fail(t, "precondition: initial snapshot must contain removed entity")
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if conns.Load() >= 2 && c.Connected() && c.State("sensor.removed") == "" {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	assert.Failf(t, "assertion failed", "reconnect retained removed entity state %q", c.State("sensor.removed"))
}

func TestConcurrentCallServiceAck(t *testing.T) {
	const calls = 16
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			return
		}
		ctx := r.Context()
		_ = wsjson.Write(ctx, conn, map[string]any{"type": "auth_required"})
		var auth map[string]any
		if wsjson.Read(ctx, conn, &auth) != nil {
			return
		}
		_ = wsjson.Write(ctx, conn, map[string]any{"type": "auth_ok"})
		for {
			var req map[string]any
			if wsjson.Read(ctx, conn, &req) != nil {
				return
			}
			switch req["type"] {
			case "get_states":
				_ = wsjson.Write(ctx, conn, map[string]any{"id": req["id"], "type": "result", "success": true, "result": []any{}})
			case "subscribe_events", "call_service":
				_ = wsjson.Write(ctx, conn, map[string]any{"id": req["id"], "type": "result", "success": true})
			}
		}
	}))
	defer srv.Close()

	c := New(config.HomeAssistant{URL: wsURL(srv), Token: "x"})
	require.NoError(t, c.Connect(context.Background()), "connect")
	require.NoError(t, c.SubscribeEvents(context.Background()), "subscribe")
	defer func() { _ = c.Close() }()

	var wg sync.WaitGroup
	errs := make(chan error, calls)
	for i := range calls {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			errs <- c.CallServiceAck(ctx, "number", "set_value", map[string]any{"entity_id": "number.test", "value": i})
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			assert.Failf(t, "assertion failed", "concurrent acknowledged call failed: %v", err)
		}
	}
}

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
	if err := c.SubscribeEvents(ctx); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer func() { _ = c.Close() }()
	if nu := c.NewestUpdate(); nu.Before(before) {
		t.Fatalf("NewestUpdate = %v, want >= %v", nu, before)
	}
}
