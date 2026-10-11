package chat

import (
	"context"
	"log/slog"
	"sync"

	"github.com/vladSlipushkin/go-grpc-chat/internal/auth"
)

// Hub is the registry of rooms and the factory of sessions. It is safe for
// concurrent use.
//
// The registry is a map under a mutex; each room runs in its own goroutine
// that orders the room's messages (ADR-0008). The hub alone decides when a
// room stops: a room is removed from the map and stopped under the hub's
// lock once no session holds it, so a join can never reach a room that is
// stopping.
type Hub struct {
	logger *slog.Logger
	rooms  sync.WaitGroup

	mu     sync.Mutex
	byID   map[string]*room
	closed bool
}

// NewHub returns an empty hub that logs room failures to logger.
func NewHub(logger *slog.Logger) *Hub {
	return &Hub{logger: logger, byID: make(map[string]*room)}
}

// Connect opens a session for the user authenticated in ctx; the sender of
// every message the session sends is that user. end is called with the
// cause when the hub drops the session (ErrSlowClient, ErrRoomFailed,
// ErrHubClosed): the caller derives it with context.WithCancelCause and
// stops serving the session when that context ends.
func (h *Hub) Connect(ctx context.Context, end context.CancelCauseFunc) (*Session, error) {
	claims, ok := auth.FromContext(ctx)
	if !ok {
		return nil, ErrNoUser
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil, ErrHubClosed
	}
	return &Session{
		hub:   h,
		user:  claims,
		end:   end,
		queue: make(chan Event, QueueLen),
		rooms: make(map[string]*room),
	}, nil
}

// Close stops every room, ending the sessions still in one with
// ErrHubClosed, and waits for the rooms' goroutines to exit. Later calls to
// Connect and Join fail with ErrHubClosed. The server calls Close after its
// last handler has returned, when no session is left (ADR-0008).
func (h *Hub) Close() {
	h.mu.Lock()
	h.closed = true
	for _, r := range h.byID {
		h.stopLocked(r)
	}
	h.mu.Unlock()
	h.rooms.Wait()
}

// acquire returns the room with id, starting it if needed, and counts the
// caller as holding it until the matching release.
func (h *Hub) acquire(id string) (*room, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil, ErrHubClosed
	}
	r, ok := h.byID[id]
	if !ok {
		r = newRoom(id, h)
		h.byID[id] = r
		h.rooms.Go(r.run)
	}
	r.holders++
	return r, nil
}

// release drops one hold on r and stops r when it was the last.
func (h *Hub) release(r *room) {
	h.mu.Lock()
	defer h.mu.Unlock()
	r.holders--
	if r.holders == 0 {
		h.stopLocked(r)
	}
}

// retire removes a room whose goroutine has failed.
func (h *Hub) retire(r *room) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.stopLocked(r)
}

// stopLocked removes r from the registry and tells its goroutine to exit.
// A new join for the same id starts a new room. h.mu must be held.
func (h *Hub) stopLocked(r *room) {
	if r.stopped {
		return
	}
	r.stopped = true
	if h.byID[r.id] == r {
		delete(h.byID, r.id)
	}
	close(r.stop)
}
