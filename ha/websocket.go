package ha

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"

	"energy-optimiser/config"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

// Client is a Home Assistant WebSocket client for state subscriptions and service calls.
type Client struct {
	url   string
	token string
	conn  *websocket.Conn

	mu     sync.RWMutex
	states map[string]EntityState

	msgID atomic.Int64
}

// EntityState holds the latest known state of an HA entity.
type EntityState struct {
	EntityID   string
	State      string
	Attributes map[string]any
}

func New(cfg config.HomeAssistant) *Client {
	return &Client{
		url:    cfg.URL,
		token:  cfg.Token,
		states: make(map[string]EntityState),
	}
}

// Connect establishes the WebSocket connection and authenticates.
func (c *Client) Connect(ctx context.Context) error {
	conn, _, err := websocket.Dial(ctx, c.url, nil)
	if err != nil {
		return fmt.Errorf("ha dial: %w", err)
	}
	c.conn = conn
	conn.SetReadLimit(1 << 20) // 1 MB

	// Read auth_required
	var msg map[string]any
	if err := wsjson.Read(ctx, conn, &msg); err != nil {
		return fmt.Errorf("ha read auth_required: %w", err)
	}
	if msg["type"] != "auth_required" {
		return fmt.Errorf("ha: expected auth_required, got %v", msg["type"])
	}

	// Send auth
	if err := wsjson.Write(ctx, conn, map[string]string{
		"type":         "auth",
		"access_token": c.token,
	}); err != nil {
		return fmt.Errorf("ha send auth: %w", err)
	}

	// Read result
	if err := wsjson.Read(ctx, conn, &msg); err != nil {
		return fmt.Errorf("ha read auth result: %w", err)
	}
	if msg["type"] != "auth_ok" {
		return fmt.Errorf("ha auth failed: %v", msg["type"])
	}

	slog.Info("ha: authenticated", "version", msg["ha_version"])

	// Fetch all current states so we have data before the first tick
	if err := c.fetchStates(ctx); err != nil {
		slog.Warn("ha: failed to fetch initial states", "error", err)
	}

	return nil
}

// fetchStates requests all current entity states from HA.
func (c *Client) fetchStates(ctx context.Context) error {
	id := c.nextID()
	if err := wsjson.Write(ctx, c.conn, map[string]any{
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
	if err := wsjson.Read(ctx, c.conn, &resp); err != nil {
		return err
	}

	c.mu.Lock()
	for _, s := range resp.Result {
		c.states[s.EntityID] = EntityState{
			EntityID:   s.EntityID,
			State:      s.State,
			Attributes: s.Attributes,
		}
	}
	c.mu.Unlock()
	slog.Info("ha: loaded initial states", "count", len(resp.Result))
	return nil
}

func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close(websocket.StatusNormalClosure, "shutdown")
	}
	return nil
}

// SubscribeEvents starts listening for state_changed events.
// Must be called after Connect. Spawns a background goroutine for reads.
func (c *Client) SubscribeEvents(ctx context.Context) error {
	id := c.nextID()
	if err := wsjson.Write(ctx, c.conn, map[string]any{
		"id":         id,
		"type":       "subscribe_events",
		"event_type": "state_changed",
	}); err != nil {
		return fmt.Errorf("ha subscribe: %w", err)
	}

	go c.readLoop(ctx)
	return nil
}

func (c *Client) readLoop(ctx context.Context) {
	for {
		var raw json.RawMessage
		if err := wsjson.Read(ctx, c.conn, &raw); err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Error("ha read error", "error", err)
			return
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
			c.mu.Lock()
			c.states[eid] = EntityState{
				EntityID:   eid,
				State:      ns.State,
				Attributes: ns.Attributes,
			}
			c.mu.Unlock()
		}
	}
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

// CallService invokes a Home Assistant service.
func (c *Client) CallService(ctx context.Context, domain, service string, data map[string]any) error {
	id := c.nextID()
	return wsjson.Write(ctx, c.conn, map[string]any{
		"id":           id,
		"type":         "call_service",
		"domain":       domain,
		"service":      service,
		"service_data": data,
	})
}

func (c *Client) nextID() int64 {
	return c.msgID.Add(1)
}
