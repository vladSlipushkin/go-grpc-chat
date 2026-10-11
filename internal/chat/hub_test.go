package chat_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
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
	return connectWith(t.Context(), t, h, name)
}

func connectWith(parent context.Context, t *testing.T, h *chat.Hub, name string) client {
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
	return chat.NewHub(slog.New(slog.DiscardHandler))
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

// onlyMessage returns the text and seq of the single message in events.
func onlyMessage(t *testing.T, events []chat.Event) (string, int64) {
	t.Helper()
	if len(events) != 1 {
		t.Fatalf("got %d events %v, want one message", len(events), events)
	}
	m, ok := events[0].(*chat.Message)
	if !ok {
		t.Fatalf("event = %T, want *chat.Message", events[0])
	}
	return m.Text, m.Seq
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
			m, ok := e.(*chat.Message)
			if !ok {
				t.Fatalf("event %d = %T, want *chat.Message", i, e)
			}
			if m.Seq != int64(i+1) {
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

		if text, _ := onlyMessage(t, drain(alice)); text != "before" {
			t.Errorf("message after leaving = %q, want only the one sent before", text)
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
		if _, seq := onlyMessage(t, drain(bob)); seq != 1 {
			t.Errorf("seq in the recreated room = %d, want 1", seq)
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
		name       string
		overflowAt time.Duration
		wantCause  error
	}{
		{name: "dropped before expiry", overflowAt: 30 * time.Minute, wantCause: chat.ErrSlowClient},
		{name: "expired before drop", overflowAt: 90 * time.Minute, wantCause: errExpired},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				h := newHub(t)
				defer h.Close()
				parent, cancel := context.WithDeadlineCause(t.Context(), time.Now().Add(time.Hour), errExpired)
				defer cancel()
				alice := connectWith(parent, t, h, "alice")

				time.Sleep(tt.overflowAt)
				for range chat.QueueLen + 1 {
					alice.Enqueue(chat.Ack{})
				}
				time.Sleep(time.Hour)

				if err := alice.cause(); !errors.Is(err, tt.wantCause) {
					t.Errorf("cause = %v, want %v", err, tt.wantCause)
				}
			})
		})
	}
}

func TestRoomPanicEndsItsMembers(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		var logs bytes.Buffer
		h := chat.NewHub(slog.New(slog.NewJSONHandler(&logs, nil)))
		defer h.Close()
		alice, bob := connect(t, h, "alice"), connect(t, h, "bob")
		join(t, alice, "general")
		join(t, bob, "general")
		join(t, alice, "other")

		if err := chat.Crash(t.Context(), alice.Session, "general"); !errors.Is(err, chat.ErrRoomFailed) {
			t.Fatalf("call into the crashing room = %v, want ErrRoomFailed", err)
		}

		for _, c := range []client{alice, bob} {
			if err := c.cause(); !errors.Is(err, chat.ErrRoomFailed) {
				t.Errorf("member cause = %v, want ErrRoomFailed", err)
			}
		}
		if !strings.Contains(logs.String(), `"msg":"room failed"`) || !strings.Contains(logs.String(), `"room_id":"general"`) {
			t.Errorf("log = %s, want a room failed record for general", logs.String())
		}
		if n := chat.RoomCount(h); n != 1 {
			t.Errorf("RoomCount() after the crash = %d, want 1 (other)", n)
		}

		// The members' handlers close their sessions; the room is already
		// gone, so leaving it must not fail or hang.
		alice.Close()
		bob.Close()
		if n := chat.RoomCount(h); n != 0 {
			t.Errorf("RoomCount() after members closed = %d, want 0", n)
		}

		carol := connect(t, h, "carol")
		join(t, carol, "general")
		send(t, carol, "general", "fresh start")
		if _, seq := onlyMessage(t, drain(carol)); seq != 1 {
			t.Errorf("seq in the recreated room = %d, want 1", seq)
		}
	})
}

func TestHubCloseEndsRemainingSessions(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		h := newHub(t)
		alice, idle := connect(t, h, "alice"), connect(t, h, "idle")
		join(t, alice, "general")

		h.Close()

		if err := alice.cause(); !errors.Is(err, chat.ErrHubClosed) {
			t.Errorf("member cause = %v, want ErrHubClosed", err)
		}
		if _, err := alice.Send(t.Context(), "general", "hi"); !errors.Is(err, chat.ErrHubClosed) {
			t.Errorf("Send() after hub Close = %v, want ErrHubClosed", err)
		}
		if err := idle.Join(t.Context(), "general"); !errors.Is(err, chat.ErrHubClosed) {
			t.Errorf("Join() after hub Close = %v, want ErrHubClosed", err)
		}
		alice.Close()
		if _, err := h.Connect(auth.NewContext(t.Context(), auth.Claims{UserID: "id-late", Username: "late"}), func(error) {}); !errors.Is(err, chat.ErrHubClosed) {
			t.Errorf("Connect() after hub Close = %v, want ErrHubClosed", err)
		}
	})
}

func TestConnectNeedsAuthenticatedUser(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		h := newHub(t)
		defer h.Close()
		if _, err := h.Connect(t.Context(), func(error) {}); !errors.Is(err, chat.ErrNoUser) {
			t.Errorf("Connect() without a user = %v, want ErrNoUser", err)
		}
	})
}

func TestEndedContextChangesNothing(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		h := newHub(t)
		defer h.Close()
		alice, bob := connect(t, h, "alice"), connect(t, h, "bob")
		join(t, bob, "general")
		ended, cancel := context.WithCancel(t.Context())
		cancel()

		if err := alice.Join(ended, "general"); !errors.Is(err, context.Canceled) {
			t.Errorf("Join() with an ended context = %v, want context.Canceled", err)
		}
		if err := bob.Leave(ended, "general"); !errors.Is(err, context.Canceled) {
			t.Errorf("Leave() with an ended context = %v, want context.Canceled", err)
		}

		send(t, bob, "general", "still here")
		if got := drain(alice); len(got) != 0 {
			t.Errorf("session whose join was cancelled got %v", got)
		}
		if text, _ := onlyMessage(t, drain(bob)); text != "still here" {
			t.Errorf("member whose leave was cancelled got %q", text)
		}
		if n := chat.RoomCount(h); n != 1 {
			t.Errorf("RoomCount() = %d, want 1", n)
		}
	})
}

func TestConnectRejectsNilEnd(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		h := newHub(t)
		defer h.Close()
		defer func() {
			if recover() == nil {
				t.Error("Connect() with a nil end did not panic")
			}
		}()
		_, _ = h.Connect(auth.NewContext(t.Context(), auth.Claims{UserID: "id-alice", Username: "alice"}), nil)
	})
}
