package chat

import (
	"context"

	"github.com/vladSlipushkin/go-grpc-chat/internal/auth"
)

// Session is one connection's membership in the hub: the rooms it is in and
// its outbound queue.
//
// Join, Leave, Send and Close must be called from one goroutine, the
// stream's reader; they return once the room has applied them. Enqueue and
// Events are safe for concurrent use.
type Session struct {
	hub   *Hub
	user  auth.Claims
	end   context.CancelCauseFunc
	queue chan Event

	// Owned by the reader goroutine.
	rooms  map[string]*room
	closed bool
}

// Join adds the session to the room with roomID, creating the room if it
// does not exist. Joining a room the session is already in does nothing,
// even at the limit of MaxRooms; a join past the limit fails with
// ErrRoomLimit.
func (s *Session) Join(ctx context.Context, roomID string) error {
	if s.closed {
		return ErrSessionClosed
	}
	if _, ok := s.rooms[roomID]; ok {
		return nil
	}
	if len(s.rooms) >= MaxRooms {
		return ErrRoomLimit
	}
	r, err := s.hub.acquire(roomID)
	if err != nil {
		return err
	}
	if err := r.do(ctx, func() { r.members[s] = struct{}{} }); err != nil {
		s.hub.release(r)
		return err
	}
	s.rooms[roomID] = r
	return nil
}

// Leave removes the session from the room with roomID; leaving a room the
// session is not in does nothing. Once Leave returns, the room delivers
// nothing more to the session.
func (s *Session) Leave(ctx context.Context, roomID string) error {
	if s.closed {
		return ErrSessionClosed
	}
	return s.leave(ctx, roomID)
}

func (s *Session) leave(ctx context.Context, roomID string) error {
	r, ok := s.rooms[roomID]
	if !ok {
		return nil
	}
	// A room that is gone has no members left to leave.
	if err := r.do(ctx, func() { delete(r.members, s) }); err != nil && !r.gone() {
		return err
	}
	delete(s.rooms, roomID)
	s.hub.release(r)
	return nil
}

// Send posts text to the room with roomID and returns the message's id.
func (s *Session) Send(ctx context.Context, roomID, text string) (string, error) {
	if s.closed {
		return "", ErrSessionClosed
	}
	r, ok := s.rooms[roomID]
	if !ok {
		return "", ErrNotMember
	}
	var (
		m       *Message
		postErr error
	)
	if err := r.do(ctx, func() { m, postErr = r.post(s, text) }); err != nil {
		return "", err
	}
	if postErr != nil {
		return "", postErr
	}
	return m.ID, nil
}

// Enqueue adds e to the outbound queue without waiting. If the queue is
// full, the session ends with ErrSlowClient (ADR-0003): nothing in the hub
// ever waits for a client.
func (s *Session) Enqueue(e Event) {
	select {
	case s.queue <- e:
	default:
		s.end(ErrSlowClient)
	}
}

// Events returns the outbound queue, read by the stream's writer.
func (s *Session) Events() <-chan Event {
	return s.queue
}

// Close leaves every room. The stream handler calls it when it returns, for
// whatever reason the session ended; later calls do nothing.
func (s *Session) Close() {
	if s.closed {
		return
	}
	s.closed = true
	// Leaving cannot be cancelled: the room applies it without blocking.
	ctx := context.Background()
	for id := range s.rooms {
		_ = s.leave(ctx, id)
	}
}
