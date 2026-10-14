package client

import (
	"time"

	chatv1 "github.com/vladSlipushkin/go-grpc-chat/gen/chat/v1"
)

// Event is something that happened in a chat session: a Message, Missed,
// Disconnected or Connected.
type Event interface {
	event()
}

// Message is a chat message in a room the session has joined, its own
// included.
type Message struct {
	ID       string
	Room     string
	SenderID string
	Sender   string
	Text     string
	SentAt   time.Time
	// Seq is the message's position in its room (ADR-0003).
	Seq int64
}

// Missed reports a gap in a room's seq: Count messages were sent there
// that the session did not receive. It comes just before the message
// after the gap.
type Missed struct {
	Room  string
	Count int64
}

// Disconnected reports a stream that ended. RetryIn is the wait before the
// next attempt; it is zero when the token expired and Relogin is called
// instead.
type Disconnected struct {
	Err     error
	RetryIn time.Duration
}

// Connected reports a stream that is open, with Rooms joined on it. After
// a reconnect, rooms the server refused to join again are missing.
type Connected struct {
	Rooms []string
}

func (Message) event()      {}
func (Missed) event()       {}
func (Disconnected) event() {}
func (Connected) event()    {}

func toMessage(m *chatv1.Message) Message {
	return Message{
		ID:       m.GetId(),
		Room:     m.GetRoomId(),
		SenderID: m.GetSenderId(),
		Sender:   m.GetSenderUsername(),
		Text:     m.GetText(),
		SentAt:   m.GetSentAt().AsTime(),
		Seq:      m.GetSeq(),
	}
}

// seqTracker finds gaps in the seq of each room on one stream. A new
// stream starts a new tracker: before messages are stored, a room's seq
// restarts when it empties or the server restarts, so values from before a
// reconnect cannot be compared with values after it (ADR-0003).
type seqTracker map[string]int64

// observe records seq for room and returns how many messages were missed
// just before it. A seq at or below the last one seen means the room
// started counting again, not a gap.
func (t seqTracker) observe(room string, seq int64) int64 {
	last, ok := t[room]
	t[room] = seq
	if !ok || seq <= last {
		return 0
	}
	return seq - last - 1
}

// forget drops room after the session left it.
func (t seqTracker) forget(room string) {
	delete(t, room)
}
