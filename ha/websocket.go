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
var subscribeTimeout = 15 * time.Second

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
	generation map[string]uint64

	connected atomic.Bool
	closed    atomic.Bool
	msgID     atomic.Int64

	setupMu     sync.Mutex
	subscribeMu sync.Mutex
	setup       *stagedConnection

	superviseMu     sync.Mutex
	superviseCancel context.CancelFunc
	superviseWG     sync.WaitGroup

	// pending correlates command IDs to callers awaiting a `result` frame, so a
	// service call can be confirmed (or fail on a drop). The read loop routes
	// results here; a lost connection fails all outstanding calls.
	pendingMu sync.Mutex
	pending   map[int64]chan callResult
}

// callResult is a Home Assistant `result` frame outcome for a correlated write.
type callResult struct {
	success bool
	errMsg  string
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
		generation: make(map[string]uint64),
		pending:    make(map[int64]chan callResult),
	}
}

// Connect establishes the initial connection, authenticates, and loads current
// states. It fails fast (bad token / unreachable HA) so startup surfaces a
// misconfiguration; later drops are handled by the supervisor started in
// SubscribeEvents.
func (c *Client) Connect(ctx context.Context) error {
	if c.closed.Load() {
		return errors.New("ha: closed")
	}
	conn, err := c.dialAuth(ctx)
	if err != nil {
		return err
	}
	states, err := c.fetchStates(ctx, conn)
	if err != nil {
		_ = conn.Close(websocket.StatusInternalError, "initial state fetch failed")
		return fmt.Errorf("ha fetch initial states: %w", err)
	}

	c.setupMu.Lock()
	defer c.setupMu.Unlock()
	if c.closed.Load() {
		_ = conn.Close(websocket.StatusNormalClosure, "shutdown")
		return errors.New("ha: closed")
	}
	if c.setup != nil || c.currentConn() != nil {
		_ = conn.Close(websocket.StatusPolicyViolation, "already connected")
		return errors.New("ha: already connected")
	}
	c.setup = &stagedConnection{conn: conn, states: states}
	return nil
}

type fetchedState struct {
	state   EntityState
	at      time.Time
	removed bool
}

type stagedConnection struct {
	conn   *websocket.Conn
	states map[string]fetchedState
}

func (c *Client) dialAuth(ctx context.Context) (*websocket.Conn, error) {
	conn, _, err := websocket.Dial(ctx, c.url, nil)
	if err != nil {
		return nil, fmt.Errorf("ha dial: %w", err)
	}
	conn.SetReadLimit(1 << 20)
	fail := func(err error) (*websocket.Conn, error) {
		_ = conn.Close(websocket.StatusInternalError, "setup failed")
		return nil, err
	}
	var msg map[string]any
	if err := wsjson.Read(ctx, conn, &msg); err != nil {
		return fail(fmt.Errorf("ha read auth_required: %w", err))
	}
	if msg["type"] != "auth_required" {
		return fail(fmt.Errorf("ha: expected auth_required, got %v", msg["type"]))
	}
	if err := wsjson.Write(ctx, conn, map[string]string{"type": "auth", "access_token": c.token}); err != nil {
		return fail(fmt.Errorf("ha send auth: %w", err))
	}
	if err := wsjson.Read(ctx, conn, &msg); err != nil {
		return fail(fmt.Errorf("ha read auth result: %w", err))
	}
	if msg["type"] != "auth_ok" {
		return fail(fmt.Errorf("ha auth failed: %v", msg["type"]))
	}
	slog.Info("ha: authenticated", "version", msg["ha_version"])
	return conn, nil
}

func (c *Client) fetchStates(ctx context.Context, conn *websocket.Conn) (map[string]fetchedState, error) {
	return c.fetchStatesWithEvents(ctx, conn, false)
}

func (c *Client) fetchSubscribedStates(ctx context.Context, conn *websocket.Conn) (map[string]fetchedState, error) {
	return c.fetchStatesWithEvents(ctx, conn, true)
}

func (c *Client) fetchStatesWithEvents(ctx context.Context, conn *websocket.Conn, subscribed bool) (map[string]fetchedState, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	id := c.nextID()
	if err := wsjson.Write(ctx, conn, map[string]any{"id": id, "type": "get_states"}); err != nil {
		return nil, err
	}
	pending := make(map[string]fetchedState)
	for {
		var raw json.RawMessage
		if err := wsjson.Read(ctx, conn, &raw); err != nil {
			return nil, err
		}
		var resp struct {
			ID      int64           `json:"id"`
			Type    string          `json:"type"`
			Success *bool           `json:"success"`
			Result  json.RawMessage `json:"result"`
			Error   *struct {
				Message string `json:"message"`
			} `json:"error"`
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
		if err := json.Unmarshal(raw, &resp); err != nil {
			return nil, fmt.Errorf("ha get_states response: %w", err)
		}
		if subscribed && resp.Type == "event" && resp.Event != nil {
			entityID := resp.Event.Data.EntityID
			if state := resp.Event.Data.NewState; state != nil {
				pending[entityID] = fetchedState{
					state: EntityState{EntityID: entityID, State: state.State, Attributes: state.Attributes},
					at:    time.Now(),
				}
			} else {
				pending[entityID] = fetchedState{removed: true}
			}
			continue
		}
		if resp.ID != id || resp.Type != "result" || resp.Success == nil || !*resp.Success {
			message := "unsuccessful response"
			if resp.Error != nil && resp.Error.Message != "" {
				message = resp.Error.Message
			}
			return nil, fmt.Errorf("ha get_states: expected successful result id %d, got id=%d type=%q: %s", id, resp.ID, resp.Type, message)
		}
		var result []struct {
			EntityID   string         `json:"entity_id"`
			State      string         `json:"state"`
			Attributes map[string]any `json:"attributes"`
		}
		if err := json.Unmarshal(resp.Result, &result); err != nil {
			return nil, fmt.Errorf("ha get_states result: %w", err)
		}
		now := time.Now()
		states := make(map[string]fetchedState, len(result)+len(pending))
		for _, s := range result {
			states[s.EntityID] = fetchedState{state: EntityState{EntityID: s.EntityID, State: s.State, Attributes: s.Attributes}, at: now}
		}
		for entityID, state := range pending {
			states[entityID] = state
		}
		return states, nil
	}
}

func (c *Client) activate(conn *websocket.Conn, states map[string]fetchedState) error {
	c.connMu.Lock()
	if c.closed.Load() {
		c.connMu.Unlock()
		return errors.New("ha: closed")
	}
	old := c.conn
	c.conn = conn
	c.mu.Lock()
	clear(c.states)
	clear(c.lastUpdate)
	for entityID, fetched := range states {
		if fetched.removed {
			continue
		}
		c.states[entityID] = fetched.state
		c.lastUpdate[entityID] = fetched.at
		c.generation[entityID]++
	}
	c.mu.Unlock()
	c.connected.Store(true)
	c.connMu.Unlock()
	if old != nil && old != conn {
		_ = old.Close(websocket.StatusNormalClosure, "reconnect")
	}
	slog.Info("ha: loaded states", "count", len(states))
	return nil
}

// SubscribeEvents subscribes to state_changed and starts the supervisor that
// owns the read loop and reconnects on drop. Must be called after Connect. The
// supervisor lives until Close so actuator shutdown can keep using the subscribed
// connection after the service Run context has been cancelled.
func (c *Client) SubscribeEvents(ctx context.Context) error {
	c.subscribeMu.Lock()
	defer c.subscribeMu.Unlock()

	c.superviseMu.Lock()
	if c.superviseCancel != nil {
		c.superviseMu.Unlock()
		return nil
	}
	c.superviseMu.Unlock()

	c.setupMu.Lock()
	staged := c.setup
	c.setupMu.Unlock()
	if staged == nil {
		return errors.New("ha: not connected")
	}
	if err := c.subscribe(ctx, staged.conn); err != nil {
		c.discardSetup(staged)
		_ = staged.conn.Close(websocket.StatusInternalError, "initial subscribe failed")
		return fmt.Errorf("ha subscribe: %w", err)
	}
	states, err := c.fetchSubscribedStates(ctx, staged.conn)
	if err != nil {
		c.discardSetup(staged)
		_ = staged.conn.Close(websocket.StatusInternalError, "post-subscribe state fetch failed")
		return fmt.Errorf("ha fetch subscribed states: %w", err)
	}
	c.setupMu.Lock()
	if c.setup != staged {
		c.setupMu.Unlock()
		_ = staged.conn.Close(websocket.StatusNormalClosure, "shutdown")
		return errors.New("ha: closed")
	}
	c.setup = nil
	c.setupMu.Unlock()
	if err := c.activate(staged.conn, states); err != nil {
		_ = staged.conn.Close(websocket.StatusNormalClosure, "shutdown")
		return err
	}

	c.superviseMu.Lock()
	if c.closed.Load() {
		c.superviseMu.Unlock()
		c.disconnectIfCurrent(staged.conn, errors.New("ha: closed"))
		return errors.New("ha: closed")
	}
	supervisorCtx, cancel := context.WithCancel(context.Background())
	c.superviseCancel = cancel
	c.superviseWG.Add(1)
	go func() {
		defer c.superviseWG.Done()
		c.supervise(supervisorCtx)
	}()
	c.superviseMu.Unlock()
	return nil
}

func (c *Client) discardSetup(staged *stagedConnection) {
	c.setupMu.Lock()
	if c.setup == staged {
		c.setup = nil
	}
	c.setupMu.Unlock()
}

func (c *Client) subscribe(ctx context.Context, conn *websocket.Conn) error {
	ctx, cancel := context.WithTimeout(ctx, subscribeTimeout)
	defer cancel()
	id := c.nextID()
	if err := wsjson.Write(ctx, conn, map[string]any{
		"id":         id,
		"type":       "subscribe_events",
		"event_type": "state_changed",
	}); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return err
	}
	var resp struct {
		ID      int64  `json:"id"`
		Type    string `json:"type"`
		Success *bool  `json:"success"`
		Error   *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := wsjson.Read(ctx, conn, &resp); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return err
	}
	if resp.ID != id || resp.Type != "result" || resp.Success == nil || !*resp.Success {
		message := "unsuccessful response"
		if resp.Error != nil && resp.Error.Message != "" {
			message = resp.Error.Message
		}
		return fmt.Errorf("expected successful subscribe result id %d, got id=%d type=%q: %s", id, resp.ID, resp.Type, message)
	}
	return nil
}

// supervise runs the read loop and, whenever it returns before ctx is done,
// reconnects with capped exponential backoff (re-auth, re-fetch, re-subscribe).
func (c *Client) supervise(ctx context.Context) {
	for {
		conn := c.currentConn()
		err := c.readLoop(ctx, conn)
		c.disconnectIfCurrent(conn, errors.New("ha: connection lost"))
		if ctx.Err() != nil {
			return
		}
		slog.Warn("ha: connection lost, reconnecting", "error", err)

		backoff := time.Second
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			if err := c.reconnect(ctx); err != nil {
				if ctx.Err() != nil {
					return
				}
				slog.Warn("ha: reconnect failed", "error", err, "retry_in", backoff)
				backoff = min(backoff*2, 30*time.Second)
				continue
			}
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
	conn, err := c.dialAuth(ctx)
	if err != nil {
		return err
	}
	if _, err := c.fetchStates(ctx, conn); err != nil {
		_ = conn.Close(websocket.StatusInternalError, "refetch states")
		return fmt.Errorf("refetch states: %w", err)
	}
	if err := c.subscribe(ctx, conn); err != nil {
		_ = conn.Close(websocket.StatusInternalError, "resubscribe")
		return fmt.Errorf("resubscribe: %w", err)
	}
	states, err := c.fetchSubscribedStates(ctx, conn)
	if err != nil {
		_ = conn.Close(websocket.StatusInternalError, "post-subscribe refetch states")
		return fmt.Errorf("post-subscribe refetch states: %w", err)
	}
	if err := ctx.Err(); err != nil {
		_ = conn.Close(websocket.StatusNormalClosure, "reconnect cancelled")
		return err
	}
	if err := c.activate(conn, states); err != nil {
		_ = conn.Close(websocket.StatusNormalClosure, "shutdown")
		return err
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
			Type    string `json:"type"`
			ID      int64  `json:"id"`
			Success *bool  `json:"success"`
			Error   *struct {
				Message string `json:"message"`
			} `json:"error"`
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
		if err := json.Unmarshal(raw, &env); err != nil {
			continue
		}
		if env.Type == "result" {
			c.deliverResult(env.ID, env.Success, env.Error)
			continue
		}
		if env.Event == nil {
			continue
		}
		eid := env.Event.Data.EntityID
		c.mu.Lock()
		if ns := env.Event.Data.NewState; ns != nil {
			c.states[eid] = EntityState{
				EntityID:   eid,
				State:      ns.State,
				Attributes: ns.Attributes,
			}
			c.lastUpdate[eid] = time.Now()
		} else {
			delete(c.states, eid)
			delete(c.lastUpdate, eid)
		}
		c.generation[eid]++
		c.mu.Unlock()
	}
}

func (c *Client) currentConn() *websocket.Conn {
	c.connMu.Lock()
	defer c.connMu.Unlock()
	return c.conn
}

func (c *Client) disconnectIfCurrent(conn *websocket.Conn, err error) bool {
	c.connMu.Lock()
	if c.conn != conn {
		c.connMu.Unlock()
		return false
	}
	c.conn = nil
	c.connected.Store(false)
	c.mu.Lock()
	clear(c.lastUpdate)
	c.mu.Unlock()
	c.connMu.Unlock()
	c.failPending(err)
	if conn != nil {
		_ = conn.Close(websocket.StatusInternalError, "connection lost")
	}
	return true
}

func (c *Client) writeJSONAllowConnecting(ctx context.Context, v any) error {
	c.connMu.Lock()
	if c.conn == nil {
		c.connMu.Unlock()
		return errors.New("ha: not connected")
	}
	conn := c.conn
	err := wsjson.Write(ctx, conn, v)
	c.connMu.Unlock()
	if err != nil {
		c.disconnectIfCurrent(conn, err)
	}
	return err
}

// writeJSON serializes a write against connMu so writers never overlap.
func (c *Client) writeJSON(ctx context.Context, v any) error {
	if !c.connected.Load() {
		return errors.New("ha: not connected")
	}
	return c.writeJSONAllowConnecting(ctx, v)
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

// Attributes returns the latest known attribute map for an entity (nil if
// unseen). The returned map must not be mutated by the caller.
func (c *Client) Attributes(entityID string) map[string]any {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.states[entityID].Attributes
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

// CallServiceAck invokes a service and waits for Home Assistant's correlated
// `result` frame, returning an error if the call failed, the context expired, or
// the connection dropped before acknowledgement. This lets a caller (the
// actuator) confirm an inverter write was accepted rather than fire-and-forget.
func (c *Client) CallServiceAck(ctx context.Context, domain, service string, data map[string]any) error {
	id := c.nextID()
	ch := make(chan callResult, 1)

	c.pendingMu.Lock()
	c.pending[id] = ch
	c.pendingMu.Unlock()
	defer func() {
		c.pendingMu.Lock()
		delete(c.pending, id)
		c.pendingMu.Unlock()
	}()

	if err := c.writeJSON(ctx, map[string]any{
		"id":           id,
		"type":         "call_service",
		"domain":       domain,
		"service":      service,
		"service_data": data,
	}); err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case r := <-ch:
		if !r.success {
			return fmt.Errorf("ha call_service %s.%s failed: %s", domain, service, r.errMsg)
		}
		return nil
	}
}

// deliverResult routes a `result` frame to the caller waiting on its ID (if any).
func (c *Client) deliverResult(id int64, success *bool, e *struct {
	Message string `json:"message"`
}) {
	c.pendingMu.Lock()
	ch, ok := c.pending[id]
	c.pendingMu.Unlock()
	if !ok {
		return // subscribe/get_states acks and other uncorrelated results
	}
	res := callResult{success: success != nil && *success}
	if e != nil {
		res.errMsg = e.Message
	}
	select {
	case ch <- res:
	default:
	}
}

// failPending fails every outstanding correlated call — invoked on a connection
// drop so an in-flight inverter write returns promptly instead of blocking to its
// context deadline.
func (c *Client) failPending(err error) {
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()
	for id, ch := range c.pending {
		select {
		case ch <- callResult{success: false, errMsg: err.Error()}:
		default:
		}
		delete(c.pending, id)
	}
}

// UpdateGeneration returns the number of state snapshots observed for an entity.
// It is monotonic within this client process and changes even when Home Assistant's
// timestamps are equal or ambiguous.
func (c *Client) UpdateGeneration(entityID string) uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.generation[entityID]
}

// LastUpdate returns the last time the given entity's state refreshed, or the
// zero time if it has never been seen.
func (c *Client) LastUpdate(entityID string) time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.lastUpdate[entityID]
}

// Fresh reports whether the entity has a known, recently-updated state — the
// feed is up, the entity has been seen, and it refreshed within the window.
func (c *Client) Fresh(entityID string, within time.Duration) bool {
	if !c.connected.Load() {
		return false
	}
	t := c.LastUpdate(entityID)
	return !t.IsZero() && time.Since(t) <= within
}

// Close stops the supervisor before closing the connection. Call it after the
// actuator's fail-safe so the subscribed connection remains available until its
// final disable is confirmed.
func (c *Client) Close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}
	c.superviseMu.Lock()
	cancel := c.superviseCancel
	c.superviseCancel = nil
	c.superviseMu.Unlock()
	if cancel != nil {
		cancel()
	}

	c.setupMu.Lock()
	staged := c.setup
	c.setup = nil
	c.setupMu.Unlock()
	if staged != nil {
		_ = staged.conn.Close(websocket.StatusNormalClosure, "shutdown")
	}

	c.superviseWG.Wait()
	c.connMu.Lock()
	conn := c.conn
	c.conn = nil
	c.connected.Store(false)
	c.mu.Lock()
	clear(c.lastUpdate)
	c.mu.Unlock()
	c.connMu.Unlock()
	c.failPending(errors.New("ha: closed"))
	if conn != nil {
		return conn.Close(websocket.StatusNormalClosure, "shutdown")
	}
	return nil
}

func (c *Client) nextID() int64 {
	return c.msgID.Add(1)
}
