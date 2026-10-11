package chat

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"

	"github.com/google/uuid"
)

// room is one chat room. Its goroutine, run, owns the member set and the
// sequence counter and applies commands one at a time, which is what gives
// every member the same order of messages without locks (ADR-0003).
type room struct {
	id  string
	hub *Hub

	inbox chan func()
	// stop is closed by the hub, under its lock, to make run return.
	stop chan struct{}
	// done is closed when run has returned; err then says why.
	done chan struct{}
	err  error

	// Guarded by hub.mu.
	holders int
	stopped bool

	// Owned by run.
	members map[*Session]struct{}
	seq     int64
}

func newRoom(id string, h *Hub) *room {
	return &room{
		id:      id,
		hub:     h,
		inbox:   make(chan func()),
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
		members: make(map[*Session]struct{}),
	}
}

func (r *room) run() {
	defer close(r.done)
	defer func() {
		if v := recover(); v != nil {
			r.hub.logger.Error("room failed",
				slog.String("room_id", r.id),
				slog.Any("panic", v),
				slog.String("stack", string(debug.Stack())),
			)
			r.err = ErrRoomFailed
			// Out of the registry first, so new joins start a fresh room
			// instead of waiting on this one.
			r.hub.retire(r)
			r.endAll(ErrRoomFailed)
		}
	}()

	for {
		select {
		case cmd := <-r.inbox:
			cmd()
		case <-r.stop:
			// A room stopped because its last holder left has no members
			// and nobody waiting on it; one that still has members is
			// being stopped by Hub.Close.
			r.err = ErrHubClosed
			r.endAll(ErrHubClosed)
			return
		}
	}
}

// do runs cmd on the room's goroutine and waits until it has run. It
// returns nil if and only if cmd ran: ctx's error if ctx ends before the
// room accepts cmd, or the room's error if the room is gone first.
//
// Once the room has accepted cmd, do waits for it regardless of ctx:
// commands never block, so the wait is short, and giving up halfway would
// leave the caller not knowing whether, say, its join was applied.
func (r *room) do(ctx context.Context, cmd func()) error {
	// Checked first so that a context that has already ended always wins;
	// the select below picks at random among ready cases.
	if err := ctx.Err(); err != nil {
		return err
	}
	ran := make(chan struct{})
	select {
	case r.inbox <- func() { cmd(); close(ran) }:
	case <-r.done:
		return r.err
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-ran:
		return nil
	case <-r.done:
		// The room may have run cmd and then stopped before this select;
		// cmd's effects stand, so report it as run.
		select {
		case <-ran:
			return nil
		default:
			return r.err
		}
	}
}

// gone reports whether the room's goroutine has exited.
func (r *room) gone() bool {
	select {
	case <-r.done:
		return true
	default:
		return false
	}
}

// post assigns the next seq to a message from s and delivers it to every
// member. It runs on the room's goroutine.
func (r *room) post(s *Session, text string) (*Message, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("generate message id: %w", err)
	}
	r.seq++
	m := &Message{
		ID:             id.String(),
		RoomID:         r.id,
		SenderID:       s.user.UserID,
		SenderUsername: s.user.Username,
		Text:           text,
		SentAt:         time.Now(),
		Seq:            r.seq,
	}
	for member := range r.members {
		member.Enqueue(m)
	}
	return m, nil
}

func (r *room) endAll(cause error) {
	for s := range r.members {
		s.end(cause)
	}
}
