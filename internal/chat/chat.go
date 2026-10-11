// Package chat routes messages between connected users: rooms, their
// members, message order and outbound queues. It knows nothing about gRPC;
// grpcapi translates its events and errors into the chat.v1 protocol.
// Design decisions are in ADR-0003 and ADR-0008.
package chat

import (
	"errors"
	"time"
)

const (
	// MaxRooms is how many rooms one session can be in at once.
	MaxRooms = 16
	// QueueLen is the capacity of a session's outbound queue. A producer that
	// finds it full ends the session with ErrSlowClient (ADR-0003).
	QueueLen = 64
)

var (
	// ErrNoUser reports a session opened without an authenticated user in
	// the context.
	ErrNoUser = errors.New("chat: no authenticated user")
	// ErrHubClosed reports a call made after the hub was closed; it is also
	// the cause a session ends with when the hub closes under it.
	ErrHubClosed = errors.New("chat: hub closed")
	// ErrSessionClosed reports a call on a session after Close.
	ErrSessionClosed = errors.New("chat: session closed")
	// ErrRoomLimit reports a join that would put a session in more than
	// MaxRooms rooms.
	ErrRoomLimit = errors.New("chat: room limit reached")
	// ErrNotMember reports a message sent to a room the session is not in.
	ErrNotMember = errors.New("chat: not a member of the room")
	// ErrSlowClient is the cause a session ends with when its outbound queue
	// was full.
	ErrSlowClient = errors.New("chat: outbound queue full")
	// ErrRoomFailed is the cause the members of a room end with when the
	// room's goroutine panicked.
	ErrRoomFailed = errors.New("chat: room failed")
)

// Event is an item of a session's outbound queue: *Message, Ack or
// Rejection.
type Event interface {
	event()
}

// Message is a chat message as delivered to every member of its room. One
// value is shared by all recipients and must not be modified.
type Message struct {
	ID             string
	RoomID         string
	SenderID       string
	SenderUsername string
	Text           string
	SentAt         time.Time
	// Seq is the message's position in its room: 1, 2, 3 and so on without
	// gaps while the room exists.
	Seq int64
}

// Ack confirms that the request with RequestID was applied. MessageID is set
// for a sent message.
type Ack struct {
	RequestID string
	MessageID string
}

// Rejection reports that the request with RequestID failed with Err; the
// session stays open.
type Rejection struct {
	RequestID string
	Err       error
}

func (*Message) event()  {}
func (Ack) event()       {}
func (Rejection) event() {}
