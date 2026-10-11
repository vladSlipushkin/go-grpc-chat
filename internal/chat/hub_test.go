package chat_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/go-cmp/cmp"
	"go.uber.org/goleak"

	"github.com/vladSlipushkin/go-grpc-chat/internal/auth"
	"github.com/vladSlipushkin/go-grpc-chat/internal/chat"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// client is a connected session together with the context the hub ends
// when it drops the session, as the stream handler will hold them.
type client struct {
	*chat.Session
	done  <-chan struct{}
	cause func() error
}

// connect opens a session for a user named name whose id is "id-" + name.
func connect(t *testing.T, h *chat.Hub, name string) client {
	t.Helper()
	return connectWith(t, h, t.Context(), name)
}

func connectWith(t *testing.T, h *chat.Hub, parent context.Context, name string) client {
	t.Helper()
	ctx, cancel := context.WithCancelCause(auth.NewContext(parent, auth.Claims{UserID: "id-" + name, Username: name}))
	t.Cleanup(func() { cancel(nil) })
	s, err := h.Connect(ctx, cancel)
	if err != nil {
		t.Fatalf("Connect(%q) error = %v", name, err)
	}
	return client{Session: s, done: ctx.Done(), cause: func() error { return context.Cause(ctx) }}
}

func newHub(t *testing.T) *chat.Hub {
	t.Helper()
	return chat.NewHub(slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func join(t *testing.T, c client, room string) {
	t.Helper()
	if err := c.Join(t.Context(), room); err != nil {
		t.Fatalf("Join(%q) error = %v", room, err)
	}
}

func send(t *testing.T, c client, room, text string) string {
	t.Helper()
	id, err := c.Send(t.Context(), room, text)
	if err != nil {
		t.Fatalf("Send(%q, %q) error = %v", room, text, err)
	}
	return id
}

// drain returns the events queued for c so far. Join, Leave and Send return
// only after the room has applied them, so their events are already queued.
func drain(c client) []chat.Event {
	var events []chat.Event
	for {
		select {
		case e := <-c.Events():
			events = append(events, e)
		default:
			return events
		}
	}
}

func TestMessagesReachEveryMemberInOrder(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		h := newHub(t)
		defer h.Close()
		alice, bob := connect(t, h, "alice"), connect(t, h, "bob")
		join(t, alice, "general")
		join(t, bob, "general")
		start := time.Now()

		id1 := send(t, alice, "general", "one")
		time.Sleep(time.Second)
		id2 := send(t, bob, "general", "two")

		want := []chat.Event{
			&chat.Message{ID: id1, RoomID: "general", SenderID: "id-alice", SenderUsername: "alice", Text: "one", SentAt: start, Seq: 1},
			&chat.Message{ID: id2, RoomID: "general", SenderID: "id-bob", SenderUsername: "bob", Text: "two", SentAt: start.Add(time.Second), Seq: 2},
		}
		for _, c := range []client{alice, bob} {
			if diff := cmp.Diff(want, drain(c)); diff != "" {
				t.Errorf("events mismatch (-want +got):\n%s", diff)
			}
		}
		if id1 == "" || id1 == id2 {
			t.Errorf("message ids %q and %q, want distinct non-empty ids", id1, id2)
		}
	})
}

func TestConcurrentSendersShareOneOrder(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		h := newHub(t)
		defer h.Close()
		const senders, perSender = 4, 10
		clients := make([]client, senders)
		for i := range clients {
			clients[i] = connect(t, h, fmt.Sprintf("user%d", i))
			join(t, clients[i], "general")
		}

		var wg sync.WaitGroup
		for _, c := range clients {
			wg.Go(func() {
				for i := range perSender {
					if _, err := c.Send(t.Context(), "general", fmt.Sprint(i)); err != nil {
						t.Errorf("Send() error = %v", err)
					}
				}
			})
		}
		wg.Wait()

		first := drain(clients[0])
		if len(first) != senders*perSender {
			t.Fatalf("got %d events, want %d", len(first), senders*perSender)
		}
		for i, e := range first {
			if m := e.(*chat.Message); m.Seq != int64(i+1) {
				t.Fatalf("event %d has seq %d, want %d", i, m.Seq, i+1)
			}
		}
		for _, c := range clients[1:] {
			if diff := cmp.Diff(first, drain(c)); diff != "" {
				t.Errorf("members disagree on order (-first +got):\n%s", diff)
			}
		}
	})
}

func TestRoomLimit(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		h := newHub(t)
		defer h.Close()
		alice := connect(t, h, "alice")
		for i := range chat.MaxRooms {
			join(t, alice, fmt.Sprintf("room-%d", i))
		}

		if err := alice.Join(t.Context(), "one-too-many"); !errors.Is(err, chat.ErrRoomLimit) {
			t.Errorf("Join() past the limit = %v, want ErrRoomLimit", err)
		}
		// Membership is checked before the limit (ADR-0003).
		if err := alice.Join(t.Context(), "room-0"); err != nil {
			t.Errorf("Join() of a room already joined at the limit = %v, want nil", err)
		}
	})
}

func TestJoinTwiceDeliversOnce(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		h := newHub(t)
		defer h.Close()
		alice := connect(t, h, "alice")
		join(t, alice, "general")
		join(t, alice, "general")

		send(t, alice, "general", "hello")

		if got := drain(alice); len(got) != 1 {
			t.Errorf("got %d events after joining twice, want 1", len(got))
		}
	})
}

func TestLeave(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		h := newHub(t)
		defer h.Close()
		alice, bob := connect(t, h, "alice"), connect(t, h, "bob")
		join(t, alice, "general")
		join(t, bob, "general")

		if err := alice.Leave(t.Context(), "elsewhere"); err != nil {
			t.Errorf("Leave() of a room not joined = %v, want nil", err)
		}
		send(t, bob, "general", "before")
		if err := alice.Leave(t.Context(), "general"); err != nil {
			t.Fatalf("Leave() error = %v", err)
		}
		send(t, bob, "general", "after")

		got := drain(alice)
		if len(got) != 1 || got[0].(*chat.Message).Text != "before" {
			t.Errorf("events after leaving = %v, want only the message sent before", got)
		}
		if _, err := alice.Send(t.Context(), "general", "hi"); !errors.Is(err, chat.ErrNotMember) {
			t.Errorf("Send() after leaving = %v, want ErrNotMember", err)
		}
	})
}

func TestSendToRoomNotJoined(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		h := newHub(t)
		defer h.Close()
		alice, bob := connect(t, h, "alice"), connect(t, h, "bob")
		join(t, bob, "general")

		if _, err := alice.Send(t.Context(), "general", "hi"); !errors.Is(err, chat.ErrNotMember) {
			t.Errorf("Send() = %v, want ErrNotMember", err)
		}
		if got := drain(bob); len(got) != 0 {
			t.Errorf("member got %v from a non-member", got)
		}
	})
}

func TestCloseLeavesEveryRoom(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		h := newHub(t)
		defer h.Close()
		alice, bob := connect(t, h, "alice"), connect(t, h, "bob")
		for _, room := range []string{"one", "two"} {
			join(t, alice, room)
			join(t, bob, room)
		}

		alice.Close()
		alice.Close()
		send(t, bob, "one", "hi")
		send(t, bob, "two", "hi")

		if got := drain(alice); len(got) != 0 {
			t.Errorf("closed session got %v", got)
		}
		if err := alice.Join(t.Context(), "three"); !errors.Is(err, chat.ErrSessionClosed) {
			t.Errorf("Join() after Close = %v, want ErrSessionClosed", err)
		}
		if _, err := alice.Send(t.Context(), "one", "hi"); !errors.Is(err, chat.ErrSessionClosed) {
			t.Errorf("Send() after Close = %v, want ErrSessionClosed", err)
		}
	})
}

func TestEmptyRoomStopsAndStartsOver(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		h := newHub(t)
		defer h.Close()
		alice, bob := connect(t, h, "alice"), connect(t, h, "bob")
		join(t, alice, "general")
		join(t, alice, "general")
		send(t, alice, "general", "one")
		send(t, alice, "general", "two")

		if err := alice.Leave(t.Context(), "general"); err != nil {
			t.Fatalf("Leave() error = %v", err)
		}
		if n := chat.RoomCount(h); n != 0 {
			t.Errorf("RoomCount() after the last member left = %d, want 0", n)
		}

		join(t, bob, "general")
		send(t, bob, "general", "again")
		if got := drain(bob); len(got) != 1 || got[0].(*chat.Message).Seq != 1 {
			t.Errorf("events in the recreated room = %v, want one message with seq 1", got)
		}
	})
}

func TestConcurrentJoinAndLeaveKeepJoins(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		h := newHub(t)
		defer h.Close()
		const members, rounds = 8, 50
		clients := make([]client, members)
		for i := range clients {
			clients[i] = connect(t, h, fmt.Sprintf("user%d", i))
		}

		// Every round can empty the room and stop it while other members
		// are joining; a join must never land in a room that is stopping.
		var wg sync.WaitGroup
		for _, c := range clients {
			wg.Go(func() {
				for range rounds {
					if err := c.Join(t.Context(), "general"); err != nil {
						t.Errorf("Join() error = %v", err)
					}
					if err := c.Leave(t.Context(), "general"); err != nil {
						t.Errorf("Leave() error = %v", err)
					}
				}
				if err := c.Join(t.Context(), "general"); err != nil {
					t.Errorf("Join() error = %v", err)
				}
			})
		}
		wg.Wait()

		send(t, clients[0], "general", "everyone here?")
		for i, c := range clients {
			if got := drain(c); len(got) != 1 {
				t.Errorf("member %d got %d events, want 1", i, len(got))
			}
		}
		if n := chat.RoomCount(h); n != 1 {
			t.Errorf("RoomCount() = %d, want 1", n)
		}
	})
}

func TestSlowClientIsDropped(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		h := newHub(t)
		defer h.Close()
		slow, fast := connect(t, h, "slow"), connect(t, h, "fast")
		join(t, slow, "general")
		join(t, fast, "general")

		// slow never reads; fast keeps up. The room must not wait for slow.
		for i := range chat.QueueLen + 1 {
			send(t, fast, "general", fmt.Sprint(i))
			if got := drain(fast); len(got) != 1 {
				t.Fatalf("fast member got %d events for message %d, want 1", len(got), i)
			}
		}

		select {
		case <-slow.done:
		default:
			t.Fatal("slow session was not ended")
		}
		if err := slow.cause(); !errors.Is(err, chat.ErrSlowClient) {
			t.Errorf("slow session cause = %v, want ErrSlowClient", err)
		}
		if err := fast.cause(); err != nil {
			t.Errorf("fast session cause = %v, want nil", err)
		}
	})
}

func TestEnqueueOnFullQueueDropsSession(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		h := newHub(t)
		defer h.Close()
		alice := connect(t, h, "alice")

		for i := range chat.QueueLen {
			alice.Enqueue(chat.Ack{RequestID: fmt.Sprint(i)})
		}
		if err := alice.cause(); err != nil {
			t.Fatalf("cause with a full queue = %v, want nil", err)
		}
		alice.Enqueue(chat.Ack{RequestID: "overflow"})
		if err := alice.cause(); !errors.Is(err, chat.ErrSlowClient) {
			t.Errorf("cause after overflow = %v, want ErrSlowClient", err)
		}
	})
}

// A session ended by the hub and by its token expiring at about the same
// time reports whichever came first, and the later one changes nothing.
func TestFirstEndCauseWins(t *testing.T) {
	t.Parallel()

	errExpired := errors.New("token expired")
	tests := []struct {
		name         string
		overflowAt   time.Duration
		wantCause    error
		wantNotCause error
	}{
		{name: "dropped before expiry", overflowAt: 30 * time.Minute, wantCause: chat.ErrSlowClient, wantNotCause: errExpired},
		{name: "expired before drop", overflowAt: 90 * time.Minute, wantCause: errExpired, wantNotCause: chat.ErrSlowClient},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				h := newHub(t)
				defer h.Close()
				parent, cancel := context.WithDeadlineCause(t.Context(), time.Now().Add(time.Hour), errExpired)
				defer cancel()
				alice := connectWith(t, h, parent, "alice")

				time.Sleep(tt.overflowAt)
				for range chat.QueueLen + 1 {
					alice.Enqueue(chat.Ack{})
				}
				time.Sleep(time.Hour)

				if err := alice.cause(); !errors.Is(err, tt.wantCause) || errors.Is(err, tt.wantNotCause) {
					t.Errorf("cause = %v, want %v", err, tt.wantCause)
				}
			})
		})
	}
}
