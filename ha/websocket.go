package ha

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"energy-optimiser/config"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

// Client is a Home Assistant WebSocket client for state subscriptions and
// service calls. A supervised read loop keeps the state cache current and
// transparently reconnects (re-auth, re-fetch, re-subscribe) whenever the
// connection drops, so a network blip or an HA restart can never silently
// freeze the cache at stale values.
type Client struct {
	url   string
	token string

	// connMu guards conn and serializes all writes. coder/websocket permits one
	// concurrent reader plus one writer but not concurrent writers; every write
	// path (CallService, subscribe, get_states) goes through connMu.
	connMu sync.Mutex
	conn   *websocket.Conn

	mu         sync.RWMutex
	states     map[string]EntityState
	lastUpdate map[string]time.Time

	connected atomic.Bool
	msgID     atomic.Int64
}

// EntityState holds the latest known state of an HA entity.
type EntityState struct {
	EntityID   string
	State      string
	Attributes map[string]any
}

func New(cfg config.HomeAssistant) *Client {
	return &Client{
		url:        cfg.URL,
		token:      cfg.Token,
		states:     make(map[string]EntityState),
		lastUpdate: make(map[string]time.Time),
	}
}

// Connect establishes the initial connection, authenticates, and loads current
// states. It fails fast (bad token / unreachable HA) so startup surfaces a
// misconfiguration; later drops are handled by the supervisor started in
// SubscribeEvents.
func (c *Client) Connect(ctx context.Context) error {
	if err := c.dialAuth(ctx); err != nil {
		return err
	}
	if err := c.fetchStates(ctx); err != nil {
		slog.Warn("ha: failed to fetch initial states", "error", err)
	}
	c.connected.Store(true)
	return nil
}

// dialAuth dials the socket, performs the auth handshake on the local (not-yet-
// published) connection, then publishes it under connMu and closes any prior
// connection. The handshake reads/writes the local conn so it never races the
// supervisor's read loop.
func (c *Client) dialAuth(ctx context.Context) error {
	conn, _, err := websocket.Dial(ctx, c.url, nil)
	if err != nil {
		return fmt.Errorf("ha dial: %w", err)
	}
	conn.SetReadLimit(1 << 20) // 1 MB

	var msg map[string]any
	if err := wsjson.Read(ctx, conn, &msg); err != nil {
		_ = conn.Close(websocket.StatusInternalError, "auth read")
		return fmt.Errorf("ha read auth_required: %w", err)
	}
	if msg["type"] != "auth_required" {
		_ = conn.Close(websocket.StatusInternalError, "auth proto")
		return fmt.Errorf("ha: expected auth_required, got %v", msg["type"])
	}
	if err := wsjson.Write(ctx, conn, map[string]string{
		"type":         "auth",
		"access_token": c.token,
	}); err != nil {
		_ = conn.Close(websocket.StatusInternalError, "auth write")
		return fmt.Errorf("ha send auth: %w", err)
	}
	if err := wsjson.Read(ctx, conn, &msg); err != nil {
		_ = conn.Close(websocket.StatusInternalError, "auth result")
		return fmt.Errorf("ha read auth result: %w", err)
	}
	if msg["type"] != "auth_ok" {
		_ = conn.Close(websocket.StatusInternalError, "auth failed")
		return fmt.Errorf("ha auth failed: %v", msg["type"])
	}

	c.connMu.Lock()
	old := c.conn
	c.conn = conn
	c.connMu.Unlock()
	if old != nil {
		_ = old.Close(websocket.StatusNormalClosure, "reconnect")
	}
	slog.Info("ha: authenticated", "version", msg["ha_version"])
	return nil
}

// fetchStates requests all current entity states. It holds connMu across the
// correlated write+read so no other writer can interleave, and it runs only
// when the read loop is not reading this connection (startup + reconnect), so
// the synchronous read cannot swallow an event frame.
func (c *Client) fetchStates(ctx context.Context) error {
	// Bound the correlated read: connMu is held for the duration, so a half-open
	// HA that authenticates but never answers get_states must not wedge writers
	// (CallService/notify) behind this lock indefinitely — time out and let the
	// supervisor retry with backoff.
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	c.connMu.Lock()
	defer c.connMu.Unlock()
	conn := c.conn
	if conn == nil {
		return errors.New("ha: not connected")
	}

	id := c.nextID()
	if err := wsjson.Write(ctx, conn, map[string]any{
		"id":   id,
		"type": "get_states",
	}); err != nil {
		return err
	}

	var resp struct {
		Type   string `json:"type"`
		Result []struct {
			EntityID   string         `json:"entity_id"`
			State      string         `json:"state"`
			Attributes map[string]any `json:"attributes"`
		} `json:"result"`
	}
	if err := wsjson.Read(ctx, conn, &resp); err != nil {
		return err
	}

	now := time.Now()
	c.mu.Lock()
	for _, s := range resp.Result {
		c.states[s.EntityID] = EntityState{
			EntityID:   s.EntityID,
			State:      s.State,
			Attributes: s.Attributes,
		}
		c.lastUpdate[s.EntityID] = now
	}
	c.mu.Unlock()
	slog.Info("ha: loaded states", "count", len(resp.Result))
	return nil
}

// SubscribeEvents subscribes to state_changed and starts the supervisor that
// owns the read loop and reconnects on drop. Must be called after Connect.
func (c *Client) SubscribeEvents(ctx context.Context) error {
	if err := c.sendSubscribe(ctx); err != nil {
		return fmt.Errorf("ha subscribe: %w", err)
	}
	go c.supervise(ctx)
	return nil
}

func (c *Client) sendSubscribe(ctx context.Context) error {
	return c.writeJSON(ctx, map[string]any{
		"id":         c.nextID(),
		"type":       "subscribe_events",
		"event_type": "state_changed",
	})
}

// supervise runs the read loop and, whenever it returns before ctx is done,
// reconnects with capped exponential backoff (re-auth, re-fetch, re-subscribe).
func (c *Client) supervise(ctx context.Context) {
	for {
		err := c.readLoop(ctx, c.currentConn())
		if ctx.Err() != nil {
			return
		}
		c.connected.Store(false)
		slog.Warn("ha: connection lost, reconnecting", "error", err)

		backoff := time.Second
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			if err := c.reconnect(ctx); err != nil {
				slog.Warn("ha: reconnect failed", "error", err, "retry_in", backoff)
				backoff = min(backoff*2, 30*time.Second)
				continue
			}
			c.connected.Store(true)
			slog.Info("ha: reconnected")
			break
		}
	}
}

// reconnect re-establishes a working session: dial+auth, then re-fetch states
// (to catch up on everything missed while disconnected), then re-subscribe.
// Fetch precedes subscribe so the synchronous get_states read never races the
// event stream.
func (c *Client) reconnect(ctx context.Context) error {
	if err := c.dialAuth(ctx); err != nil {
		return err
	}
	if err := c.fetchStates(ctx); err != nil {
		return fmt.Errorf("refetch states: %w", err)
	}
	if err := c.sendSubscribe(ctx); err != nil {
		return fmt.Errorf("resubscribe: %w", err)
	}
	return nil
}

// readLoop reads state_changed events from conn until an error, returning it so
// the supervisor can reconnect. It is the sole reader of the connection.
func (c *Client) readLoop(ctx context.Context, conn *websocket.Conn) error {
	if conn == nil {
		return errors.New("ha: no connection")
	}
	for {
		var raw json.RawMessage
		if err := wsjson.Read(ctx, conn, &raw); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}

		var env struct {
			Type  string `json:"type"`
			Event *struct {
				Data struct {
					EntityID string `json:"entity_id"`
					NewState *struct {
						State      string         `json:"state"`
						Attributes map[string]any `json:"attributes"`
					} `json:"new_state"`
				} `json:"data"`
			} `json:"event"`
		}
		if err := json.Unmarshal(raw, &env); err != nil || env.Event == nil {
			continue
		}
		if ns := env.Event.Data.NewState; ns != nil {
			eid := env.Event.Data.EntityID
			now := time.Now()
			c.mu.Lock()
			c.states[eid] = EntityState{
				EntityID:   eid,
				State:      ns.State,
				Attributes: ns.Attributes,
			}
			c.lastUpdate[eid] = now
			c.mu.Unlock()
		}
	}
}

func (c *Client) currentConn() *websocket.Conn {
	c.connMu.Lock()
	defer c.connMu.Unlock()
	return c.conn
}

// writeJSON serializes a write against connMu so writers never overlap.
func (c *Client) writeJSON(ctx context.Context, v any) error {
	c.connMu.Lock()
	defer c.connMu.Unlock()
	if c.conn == nil {
		return errors.New("ha: not connected")
	}
	return wsjson.Write(ctx, c.conn, v)
}

// State returns the current state string for an entity.
func (c *Client) State(entityID string) string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.states[entityID].State
}

// StateFloat returns the state parsed as float64, or 0 if unavailable.
func (c *Client) StateFloat(entityID string) float64 {
	s := c.State(entityID)
	v, _ := strconv.ParseFloat(s, 64)
	return v
}

// Connected reports whether the live-state feed is currently up.
func (c *Client) Connected() bool { return c.connected.Load() }

// NewestUpdate is the most recent time any entity refreshed. A frozen feed is
// detected by the freshest entity going stale (power values refresh every few
// seconds), which catches a dead connection even before Connected() flips.
func (c *Client) NewestUpdate() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var newest time.Time
	for _, t := range c.lastUpdate {
		if t.After(newest) {
			newest = t
		}
	}
	return newest
}

// CallService invokes a Home Assistant service (fire-and-forget over the shared
// connection; a drop surfaces as a write error and the supervisor reconnects).
func (c *Client) CallService(ctx context.Context, domain, service string, data map[string]any) error {
	return c.writeJSON(ctx, map[string]any{
		"id":           c.nextID(),
		"type":         "call_service",
		"domain":       domain,
		"service":      service,
		"service_data": data,
	})
}

// Close closes the connection. Shutdown requires cancelling the context passed
// to SubscribeEvents first: Close alone only drops the socket, which the
// supervisor would treat as a lost connection and reconnect.
func (c *Client) Close() error {
	c.connMu.Lock()
	defer c.connMu.Unlock()
	if c.conn != nil {
		return c.conn.Close(websocket.StatusNormalClosure, "shutdown")
	}
	return nil
}

func (c *Client) nextID() int64 {
	return c.msgID.Add(1)
}
