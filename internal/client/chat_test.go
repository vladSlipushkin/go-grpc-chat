package client_test

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"go.uber.org/goleak"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	chatv1 "github.com/vladSlipushkin/go-grpc-chat/gen/chat/v1"
	"github.com/vladSlipushkin/go-grpc-chat/internal/certtest"
	"github.com/vladSlipushkin/go-grpc-chat/internal/chattest"
	"github.com/vladSlipushkin/go-grpc-chat/internal/client"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

const password = "correct horse"

func TestChatJoinSendLeave(t *testing.T) {
	t.Parallel()

	srv := chattest.Start(t, chattest.Options{})
	alice, bob := connect(t, srv, "alice", options{}), connect(t, srv, "bob", options{})
	alice.join(t, "general")
	bob.join(t, "general")

	alice.say(t, "hello")
	want := client.Message{Room: "general", Sender: "alice", Text: "hello", Seq: 1}
	for _, u := range []*user{alice, bob} {
		if diff := cmp.Diff(want, u.message(t), ignoreIdentity); diff != "" {
			t.Errorf("%s's message mismatch (-want +got):\n%s", u.name, diff)
		}
	}

	if err := alice.chat.Leave(ctxFor(t), "general"); err != nil {
		t.Fatalf("Leave() error = %v", err)
	}
	if rooms := alice.chat.Rooms(); len(rooms) != 0 {
		t.Errorf("Rooms() after leaving = %v, want none", rooms)
	}
	if err := alice.chat.Send(ctxFor(t), "general", "still here?"); !errors.Is(err, client.ErrNotMember) {
		t.Errorf("Send() after leaving = %v, want ErrNotMember", err)
	}
}

func TestChatRejoinsAfterServerRestart(t *testing.T) {
	t.Parallel()

	srv := chattest.Start(t, chattest.Options{})
	alice, bob := connect(t, srv, "alice", options{}), connect(t, srv, "bob", options{})
	alice.join(t, "general")
	alice.join(t, "random")
	bob.join(t, "general")
	bob.say(t, "before")
	alice.message(t)
	bob.message(t)

	srv.Restart()

	for _, u := range []*user{alice, bob} {
		d := waitFor[client.Disconnected](t, u)
		if code := status.Code(d.Err); code != codes.Unavailable {
			t.Errorf("%s disconnected with %v, want UNAVAILABLE", u.name, d.Err)
		}
	}
	if diff := cmp.Diff([]string{"general", "random"}, waitFor[client.Connected](t, alice).Rooms); diff != "" {
		t.Errorf("rooms alice rejoined mismatch (-want +got):\n%s", diff)
	}
	waitFor[client.Connected](t, bob)

	// The new server's room starts from seq 1 again, which is not a gap.
	bob.say(t, "after")
	want := client.Message{Room: "general", Sender: "bob", Text: "after", Seq: 1}
	if diff := cmp.Diff(want, alice.message(t), ignoreIdentity); diff != "" {
		t.Errorf("alice's message after the restart mismatch (-want +got):\n%s", diff)
	}
}

func TestChatRejoinsAfterSlowClient(t *testing.T) {
	t.Parallel()

	srv := chattest.Start(t, chattest.Options{})
	// Alice reads her events only when the test does. A fixed 64 KiB window
	// keeps the server's writer from buffering without bound, so the
	// server's queue for her fills up (as in the server's own test).
	alice := connect(t, srv, "alice", options{
		unread: true,
		dial: []grpc.DialOption{
			grpc.WithInitialWindowSize(64 << 10),
			grpc.WithInitialConnWindowSize(64 << 10),
		},
	})
	bob := connect(t, srv, "bob", options{})
	waitFor[client.Connected](t, alice)
	alice.join(t, "general")
	bob.join(t, "general")

	// One stream may not post this fast (ADR-0008): each sender posts a
	// handful of messages, well within the server's burst. Their events are
	// relayed into buffers, so they keep up with the room.
	const sent, perSender = 200, 5
	for start := 0; start < sent; start += perSender {
		sender := connect(t, srv, fmt.Sprintf("sender_%d", start), options{})
		sender.join(t, "general")
		for i := start; i < start+perSender; i++ {
			sender.say(t, fmt.Sprintf("%03d%s", i, strings.Repeat("x", 4000)))
		}
	}

	var got int64
	for {
		e := alice.next(t)
		if d, ok := e.(client.Disconnected); ok {
			if reason := client.Reason(d.Err); reason != client.ReasonSlowClient {
				t.Fatalf("alice disconnected with %v, want SLOW_CLIENT", d.Err)
			}
			break
		}
		m, ok := e.(client.Message)
		if !ok {
			t.Fatalf("alice got %#v before being dropped, want only messages", e)
		}
		got++
		if m.Seq != got {
			t.Fatalf("message %d has seq %d: a gap before the drop", got, m.Seq)
		}
	}
	if got >= sent {
		t.Errorf("alice received all %d messages, want her dropped before the end", sent)
	}
	if diff := cmp.Diff([]string{"general"}, waitFor[client.Connected](t, alice).Rooms); diff != "" {
		t.Errorf("rooms alice rejoined mismatch (-want +got):\n%s", diff)
	}

	bob.say(t, "welcome back")
	if m := alice.message(t); m.Text != "welcome back" {
		t.Errorf("alice's message after reconnecting = %q, want %q", m.Text, "welcome back")
	}
}

func TestChatLogsInAgainAfterTokenExpiry(t *testing.T) {
	t.Parallel()

	// A two-second token: the expiry races nothing but in-process work
	// (ADR-0002).
	srv := chattest.Start(t, chattest.Options{TokenTTL: 2 * time.Second})
	var logins atomic.Int32
	alice := connect(t, srv, "alice", options{relogin: func(ctx context.Context, conn *client.Conn) (string, error) {
		logins.Add(1)
		tok, err := conn.Login(ctx, "alice", password)
		return tok.Value, err
	}})
	alice.join(t, "general")

	d := waitFor[client.Disconnected](t, alice)
	if reason := client.Reason(d.Err); reason != client.ReasonTokenExpired || d.RetryIn != 0 {
		t.Fatalf("Disconnected = %+v, want TOKEN_EXPIRED without a retry delay", d)
	}
	if diff := cmp.Diff([]string{"general"}, waitFor[client.Connected](t, alice).Rooms); diff != "" {
		t.Errorf("rooms alice rejoined mismatch (-want +got):\n%s", diff)
	}
	if n := logins.Load(); n != 1 {
		t.Errorf("Relogin called %d times, want 1", n)
	}
	alice.say(t, "back again")
}

func TestChatCloseWaitsForReplies(t *testing.T) {
	t.Parallel()

	srv := chattest.Start(t, chattest.Options{})
	alice, bob := connect(t, srv, "alice", options{}), connect(t, srv, "bob", options{})
	alice.join(t, "general")
	bob.join(t, "general")

	// Within the server's burst (ADR-0008), so every reply is an Ack.
	const sends = 10
	ctx := ctxFor(t)
	results := make(chan error, sends)
	for i := range sends {
		go func() { results <- alice.chat.Send(ctx, "general", fmt.Sprint(i)) }()
	}
	// One of her messages is back, so at least that request is in flight
	// or answered when Close starts.
	alice.message(t)
	if err := alice.chat.Close(ctxFor(t)); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	acked := 0
	for range sends {
		switch err := <-results; {
		case err == nil:
			acked++
		case errors.Is(err, client.ErrClosed):
			// Refused before it was sent.
		default:
			t.Errorf("Send() = %v, want nil or ErrClosed: Close must not cut a request short", err)
		}
	}

	// Bob's marker follows everything alice had sent: the room orders
	// messages, and alice's were acknowledged before Close returned.
	bob.say(t, "end")
	delivered := 0
	for m := bob.message(t); m.Text != "end"; m = bob.message(t) {
		delivered++
	}
	if delivered != acked {
		t.Errorf("bob got %d of alice's messages, want the %d she had acknowledged", delivered, acked)
	}
}

func TestChatCloseBeforeRun(t *testing.T) {
	t.Parallel()

	t.Run("run starts", func(t *testing.T) {
		t.Parallel()

		// As when Run is started on a goroutine that has not run yet.
		chat := startFake(t, silentServer{}).NewChat("token", client.ChatOptions{})
		closed := make(chan error, 1)
		go func() { closed <- chat.Close(ctxFor(t)) }()
		go func() {
			for range chat.Events() {
			}
		}()
		if err := chat.Run(ctxFor(t)); err != nil {
			t.Errorf("Run() = %v, want nil", err)
		}
		if err := <-closed; err != nil {
			t.Errorf("Close() = %v, want nil once Run has started", err)
		}
	})
	t.Run("run never starts", func(t *testing.T) {
		t.Parallel()

		chat := startFake(t, silentServer{}).NewChat("token", client.ChatOptions{})
		ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		defer cancel()
		err := chat.Close(ctx)
		if !errors.Is(err, client.ErrNotRunning) || !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Close() without Run = %v, want ErrNotRunning and DeadlineExceeded", err)
		}
		go func() {
			for range chat.Events() {
			}
		}()
		if err := chat.Run(ctxFor(t)); err != nil {
			t.Errorf("Run() after Close = %v, want nil at once", err)
		}
	})
}

func TestChatCloseGivesUpAtDeadline(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		srv      chatv1.ChatServiceServer
		inFlight bool // a Send is waiting for its reply when Close starts
	}{
		// gRPC lets a server go on after the client half-closes.
		{name: "server ignores half-close", srv: stubbornServer{}},
		{name: "request never answered", srv: silentServer{}, inFlight: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			onStream := make(chan struct{}, 1)
			conn := startFake(t, tt.srv, recordSends(func(*chatv1.ConnectRequest) { onStream <- struct{}{} }))
			u := start(t, "alice", conn.NewChat("token", client.ChatOptions{}), true)
			waitFor[client.Connected](t, u)
			sent := make(chan error, 1)
			if tt.inFlight {
				go func() { sent <- u.chat.Send(ctxFor(t), "general", "hello?") }()
				// Close refuses requests that have not reached the stream.
				<-onStream
			}

			ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
			defer cancel()
			if err := u.chat.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("Close() = %v, want DeadlineExceeded", err)
			}
			if tt.inFlight {
				// The stream was cancelled under it.
				if err := <-sent; !errors.Is(err, client.ErrConnectionLost) {
					t.Errorf("Send() cut short by Close = %v, want ErrConnectionLost", err)
				}
			}
		})
	}
}

// stubbornServer acknowledges nothing and keeps the stream open after the
// client half-closes it, until the client cancels.
type stubbornServer struct {
	chatv1.UnimplementedChatServiceServer
}

func (stubbornServer) Connect(stream chatv1.ChatService_ConnectServer) error {
	for {
		if _, err := stream.Recv(); errors.Is(err, io.EOF) {
			<-stream.Context().Done()
			return stream.Context().Err()
		} else if err != nil {
			return err
		}
	}
}

func TestChatSendingTooFastKeepsStreamOpen(t *testing.T) {
	t.Parallel()

	srv := chattest.Start(t, chattest.Options{})
	alice := connect(t, srv, "alice", options{})
	alice.join(t, "general")

	// Far more than the server's burst, sent as fast as replies come back.
	var err error
	for i := 0; i < 100 && err == nil; i++ {
		err = alice.chat.Send(ctxFor(t), "general", fmt.Sprint(i))
	}
	if !errors.Is(err, client.ErrRateLimited) {
		t.Fatalf("Send() past the server's burst = %v, want ErrRateLimited", err)
	}
	alice.join(t, "random")
	// Everything relayed so far is in the buffer: no reconnect happened.
	for {
		select {
		case e := <-alice.events:
			if d, ok := e.(client.Disconnected); ok {
				t.Fatalf("stream ended after RATE_LIMITED: %v", d.Err)
			}
		default:
			return
		}
	}
}

func TestChatSeqRestartIsNotAGap(t *testing.T) {
	t.Parallel()

	srv := chattest.Start(t, chattest.Options{})
	alice, bob := connect(t, srv, "alice", options{}), connect(t, srv, "bob", options{})
	alice.join(t, "general")
	bob.join(t, "general")
	bob.say(t, "one")
	bob.say(t, "two")
	if m := alice.messageAfter(t, "one"); m.Seq != 2 {
		t.Fatalf("second message has seq %d, want 2", m.Seq)
	}

	// The room empties, and its next run counts from 1 again.
	for _, u := range []*user{alice, bob} {
		if err := u.chat.Leave(ctxFor(t), "general"); err != nil {
			t.Fatalf("Leave(%s) error = %v", u.name, err)
		}
	}
	alice.join(t, "general")
	bob.join(t, "general")
	bob.say(t, "three")

	e := alice.next(t)
	if m, ok := e.(client.Message); !ok || m.Text != "three" || m.Seq != 1 {
		t.Errorf("alice's next event = %#v, want message three with seq 1 and no Missed", e)
	}
}

func TestChatEndsOnRejectedToken(t *testing.T) {
	t.Parallel()

	srv := chattest.Start(t, chattest.Options{})
	conn := dial(t, srv, nil)
	chat := conn.NewChat("forged", client.ChatOptions{})
	go func() {
		for range chat.Events() {
		}
	}()
	if err := chat.Run(ctxFor(t)); status.Code(err) != codes.Unauthenticated {
		t.Errorf("Run() with a forged token = %v, want UNAUTHENTICATED", err)
	}
	if err := chat.Join(ctxFor(t), "general"); !errors.Is(err, client.ErrClosed) {
		t.Errorf("Join() after Run returned = %v, want ErrClosed", err)
	}
}

func TestChatIgnoresUnknownEvents(t *testing.T) {
	t.Parallel()

	conn := startFake(t, futureServer{})
	u := start(t, "alice", conn.NewChat("token", client.ChatOptions{}), true)
	u.join(t, "general")
	// The unknown event produces nothing; the message comes next.
	if m := u.message(t); m.Text != "from the future" {
		t.Errorf("message after the unknown event = %q, want %q", m.Text, "from the future")
	}
}

// futureServer is a newer server: after acknowledging a join it sends an
// event this client does not know, then a message.
type futureServer struct {
	chatv1.UnimplementedChatServiceServer
}

func (futureServer) Connect(stream chatv1.ChatService_ConnectServer) error {
	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		for _, resp := range []*chatv1.ConnectResponse{
			{Event: &chatv1.ConnectResponse_Ack{Ack: &chatv1.Ack{RequestId: req.GetRequestId()}}},
			{}, // an event added after this client was built
			{Event: &chatv1.ConnectResponse_Message{Message: &chatv1.Message{RoomId: "general", Text: "from the future"}}},
		} {
			if err := stream.Send(resp); err != nil {
				return err
			}
		}
	}
}

func TestChatResendsJoinAndLeaveButNotSend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		request func(context.Context, *client.Chat) error
		wantErr error
		want    []string // what the server received, stream by stream
	}{
		{
			name:    "join",
			request: func(ctx context.Context, c *client.Chat) error { return c.Join(ctx, "general") },
			want:    []string{"1: join general", "2: join general"},
		},
		{
			name:    "leave",
			request: func(ctx context.Context, c *client.Chat) error { return c.Leave(ctx, "general") },
			want:    []string{"1: leave general", "2: leave general"},
		},
		{
			name:    "send",
			request: func(ctx context.Context, c *client.Chat) error { return c.Send(ctx, "general", "hi") },
			wantErr: client.ErrConnectionLost,
			want:    []string{"1: send hi"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			srv := &breakingServer{}
			conn := startFake(t, srv)
			chat := conn.NewChat("token", client.ChatOptions{MinBackoff: time.Millisecond, MaxBackoff: 10 * time.Millisecond})
			u := start(t, "alice", chat, true)

			if err := tt.request(ctxFor(t), u.chat); !errors.Is(err, tt.wantErr) {
				t.Fatalf("request = %v, want %v", err, tt.wantErr)
			}
			if diff := cmp.Diff(tt.want, srv.received()); diff != "" {
				t.Errorf("requests the server received mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// TestChatResendLeavesSentRequestAlone checks the contract of SendMsg: a
// message may not be modified once it has been sent, since stats handlers
// and interceptors may still hold it.
func TestChatResendLeavesSentRequestAlone(t *testing.T) {
	t.Parallel()

	var (
		mu   sync.Mutex
		sent []sentRequest
	)
	conn := startFake(t, &breakingServer{}, recordSends(func(req *chatv1.ConnectRequest) {
		mu.Lock()
		defer mu.Unlock()
		sent = append(sent, sentRequest{req: req, id: req.GetRequestId()})
	}))
	chat := conn.NewChat("token", client.ChatOptions{MinBackoff: time.Millisecond, MaxBackoff: 10 * time.Millisecond})
	u := start(t, "alice", chat, true)
	u.join(t, "general")

	mu.Lock()
	defer mu.Unlock()
	if len(sent) != 2 {
		t.Fatalf("sent %d requests, want the join and its resend", len(sent))
	}
	for i, s := range sent {
		if got := s.req.GetRequestId(); got != s.id {
			t.Errorf("request %d has request_id %q, want %q as sent", i, got, s.id)
		}
	}
	if sent[0].req == sent[1].req {
		t.Error("the resend reused the message already sent")
	}
}

type sentRequest struct {
	req *chatv1.ConnectRequest
	id  string // request_id at the time it was sent
}

// recordSends calls record with every request the client sends, before
// gRPC takes it.
func recordSends(record func(*chatv1.ConnectRequest)) grpc.DialOption {
	return grpc.WithStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		cs, err := streamer(ctx, desc, cc, method, opts...)
		return recordingStream{ClientStream: cs, record: record}, err
	})
}

// recordingStream reports every request sent on a stream.
type recordingStream struct {
	grpc.ClientStream
	record func(*chatv1.ConnectRequest)
}

func (s recordingStream) SendMsg(m any) error {
	if req, ok := m.(*chatv1.ConnectRequest); ok {
		s.record(req)
	}
	return s.ClientStream.SendMsg(m)
}

func TestChatBackoffGrowsUntilAStreamLasts(t *testing.T) {
	t.Parallel()

	// A short stream must end well within maxBackoff, even on a slow
	// machine under -race.
	const minBackoff, maxBackoff = 10 * time.Millisecond, time.Second
	// Streams end at once except the fourth, which outlives the longest
	// wait and so shows the server is back to normal.
	srv := &flappingServer{last: func(n int) time.Duration {
		if n == 4 {
			return 2 * maxBackoff
		}
		return 0
	}}
	conn := startFake(t, srv)
	chat := conn.NewChat("token", client.ChatOptions{MinBackoff: minBackoff, MaxBackoff: maxBackoff})
	u := start(t, "alice", chat, true)

	// The ceiling of each wait; the wait itself is between half of it and
	// all of it.
	ceilings := []time.Duration{
		minBackoff, 2 * minBackoff, 4 * minBackoff,
		// The fourth stream lasted: the count starts again.
		minBackoff, 2 * minBackoff,
	}
	for i, ceiling := range ceilings {
		d := waitFor[client.Disconnected](t, u)
		if d.RetryIn < ceiling/2 || d.RetryIn > ceiling {
			t.Errorf("wait after stream %d = %v, want within [%v, %v]", i+1, d.RetryIn, ceiling/2, ceiling)
		}
	}
}

// flappingServer answers on each stream at once, as a server acknowledging
// a rejoin would, and ends stream n with UNAVAILABLE after last(n), counted
// from 1.
type flappingServer struct {
	chatv1.UnimplementedChatServiceServer
	last func(n int) time.Duration

	streams atomic.Int32
}

func (s *flappingServer) Connect(stream chatv1.ChatService_ConnectServer) error {
	n := int(s.streams.Add(1))
	// An event this client does not know: a reply all the same.
	if err := stream.Send(&chatv1.ConnectResponse{}); err != nil {
		return err
	}
	select {
	case <-time.After(s.last(n)):
		return status.Error(codes.Unavailable, "flapping")
	case <-stream.Context().Done():
		return stream.Context().Err()
	}
}

func TestChatReportsRoomsNotRejoined(t *testing.T) {
	t.Parallel()

	conn := startFake(t, &refusingServer{refuse: "random"})
	u := start(t, "alice", conn.NewChat("token", client.ChatOptions{MinBackoff: time.Millisecond, MaxBackoff: 10 * time.Millisecond}), true)
	u.join(t, "general")
	u.join(t, "random")
	// The first stream breaks on a send; the rejoin on the next is half
	// refused.
	if err := u.chat.Send(ctxFor(t), "general", "hi"); !errors.Is(err, client.ErrConnectionLost) {
		t.Fatalf("Send() = %v, want ErrConnectionLost", err)
	}
	waitFor[client.Connected](t, u) // the first stream
	want := client.Connected{Rooms: []string{"general"}, Dropped: []string{"random"}}
	if diff := cmp.Diff(want, waitFor[client.Connected](t, u)); diff != "" {
		t.Errorf("Connected after the rejoin mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff([]string{"general"}, u.chat.Rooms()); diff != "" {
		t.Errorf("Rooms() mismatch (-want +got):\n%s", diff)
	}
}

// refusingServer acknowledges joins on the first stream and ends it on the
// first message without replying. On later streams it refuses to join
// refuse, as a server at its room limit would, and acknowledges the rest.
type refusingServer struct {
	chatv1.UnimplementedChatServiceServer
	refuse string

	streams atomic.Int32
}

func (s *refusingServer) Connect(stream chatv1.ChatService_ConnectServer) error {
	first := s.streams.Add(1) == 1
	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if first && req.GetSendMessage() != nil {
			return status.Error(codes.Unavailable, "going down")
		}
		resp := &chatv1.ConnectResponse{Event: &chatv1.ConnectResponse_Ack{Ack: &chatv1.Ack{RequestId: req.GetRequestId()}}}
		if !first && req.GetJoinRoom().GetRoomId() == s.refuse {
			resp = &chatv1.ConnectResponse{Event: &chatv1.ConnectResponse_Error{Error: &chatv1.Error{
				RequestId: req.GetRequestId(),
				Status:    roomLimitStatus,
			}}}
		}
		if err := stream.Send(resp); err != nil {
			return err
		}
	}
}

var roomLimitStatus = mustRoomLimit()

func mustRoomLimit() *spb.Status {
	st, err := status.New(codes.ResourceExhausted, "room limit reached").WithDetails(
		&errdetails.ErrorInfo{Reason: client.ReasonRoomLimit, Domain: "chat.v1"},
	)
	if err != nil {
		panic(err)
	}
	return st.Proto()
}

// breakingServer ends the first stream on its first request without
// replying, as a server that goes down at that moment would, and
// acknowledges everything on later streams.
type breakingServer struct {
	chatv1.UnimplementedChatServiceServer

	mu      sync.Mutex
	streams int
	log     []string
}

func (s *breakingServer) Connect(stream chatv1.ChatService_ConnectServer) error {
	s.mu.Lock()
	s.streams++
	n := s.streams
	s.mu.Unlock()
	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		s.record(n, req)
		if n == 1 {
			return status.Error(codes.Unavailable, "going down")
		}
		if err := stream.Send(&chatv1.ConnectResponse{Event: &chatv1.ConnectResponse_Ack{
			Ack: &chatv1.Ack{RequestId: req.GetRequestId()},
		}}); err != nil {
			return err
		}
	}
}

func (s *breakingServer) record(stream int, req *chatv1.ConnectRequest) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var what string
	switch a := req.GetAction().(type) {
	case *chatv1.ConnectRequest_JoinRoom:
		what = "join " + a.JoinRoom.GetRoomId()
	case *chatv1.ConnectRequest_LeaveRoom:
		what = "leave " + a.LeaveRoom.GetRoomId()
	case *chatv1.ConnectRequest_SendMessage:
		what = "send " + a.SendMessage.GetText()
	}
	s.log = append(s.log, fmt.Sprintf("%d: %s", stream, what))
}

func (s *breakingServer) received() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.log)
}

func TestChatRequestWithoutReply(t *testing.T) {
	t.Parallel()

	conn := startFake(t, silentServer{})
	u := start(t, "alice", conn.NewChat("token", client.ChatOptions{}), true)
	waitFor[client.Connected](t, u)

	// Nothing will answer: the timeout only ends the wait.
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	if err := u.chat.Send(ctx, "general", "hello?"); !errors.Is(err, client.ErrNoReply) {
		t.Errorf("Send() without a reply = %v, want ErrNoReply", err)
	}
}

// silentServer reads requests and never answers them.
type silentServer struct {
	chatv1.UnimplementedChatServiceServer
}

func (silentServer) Connect(stream chatv1.ChatService_ConnectServer) error {
	for {
		if _, err := stream.Recv(); errors.Is(err, io.EOF) {
			return nil
		} else if err != nil {
			return err
		}
	}
}

// startFake serves impl, a chat service written for the test, and returns
// a connection to it.
func startFake(t *testing.T, impl chatv1.ChatServiceServer, opts ...grpc.DialOption) *client.Conn {
	t.Helper()

	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	chatv1.RegisterChatServiceServer(gs, impl)
	served := make(chan error, 1)
	go func() { served <- gs.Serve(lis) }()
	t.Cleanup(func() {
		gs.Stop()
		if err := <-served; err != nil {
			t.Errorf("Serve() = %v", err)
		}
	})

	conn, err := client.Dial("passthrough:///fake", client.Options{
		Insecure: true,
		DialOptions: append([]grpc.DialOption{grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		})}, opts...),
	})
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Errorf("close connection: %v", err)
		}
	})
	return conn
}

func TestDialRefusesPlaintextToRemoteAddress(t *testing.T) {
	t.Parallel()

	tests := []struct {
		addr     string
		insecure bool
		wantErr  bool
	}{
		{addr: "localhost:50051"},
		{addr: "LOCALHOST:50051"},
		{addr: "127.0.0.1:50051"},
		{addr: "127.1.2.3:50051"},
		{addr: "[::1]:50051"},
		{addr: "chat.example.com:50051", wantErr: true},
		{addr: "10.0.0.7:50051", wantErr: true},
		{addr: ":50051", wantErr: true},
		{addr: "localhost", wantErr: true},
		{addr: "chat.example.com:50051", insecure: true},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s insecure=%v", tt.addr, tt.insecure), func(t *testing.T) {
			t.Parallel()

			conn, err := client.Dial(tt.addr, client.Options{Insecure: tt.insecure})
			if err == nil {
				defer conn.Close()
			}
			if got := errors.Is(err, client.ErrPlaintext); got != tt.wantErr {
				t.Errorf("Dial(%q, Insecure: %v) = %v, want ErrPlaintext: %v", tt.addr, tt.insecure, err, tt.wantErr)
			}
		})
	}
}

func TestChatOverTLS(t *testing.T) {
	t.Parallel()

	// Issued through an intermediate, as by a real CA.
	srv := chattest.Start(t, chattest.Options{TLS: certtest.Issued(t, chattest.Host)})
	alice, bob := connect(t, srv, "alice", options{}), connect(t, srv, "bob", options{})
	alice.join(t, "general")
	bob.join(t, "general")
	alice.say(t, "over TLS")
	if m := bob.message(t); m.Text != "over TLS" {
		t.Errorf("bob's message = %q, want %q", m.Text, "over TLS")
	}
}

func TestDialTLSChecksTheServer(t *testing.T) {
	t.Parallel()

	files := certtest.SelfSigned(t, chattest.Host)
	srv := chattest.Start(t, chattest.Options{TLS: files})
	trusted := srv.ClientTLS(t)
	otherName := trusted.Clone()
	otherName.ServerName = "chat.example.com"

	tests := []struct {
		name string
		opts client.Options
	}{
		// The system roots do not include a self-signed certificate.
		{name: "untrusted certificate", opts: client.Options{TLS: &tls.Config{MinVersion: tls.VersionTLS13}}},
		{name: "certificate for another name", opts: client.Options{TLS: otherName}},
		{name: "plaintext", opts: client.Options{Insecure: true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.opts.DialOptions = srv.DialOptions()
			conn, err := client.Dial(chattest.Target, tt.opts)
			if err != nil {
				t.Fatalf("Dial() error = %v", err)
			}
			defer conn.Close()
			if _, err := conn.Login(ctxFor(t), "alice", password); status.Code(err) != codes.Unavailable {
				t.Errorf("Login() = %v, want UNAVAILABLE: the connection must fail", err)
			}
		})
	}
}

func TestTokenNeedsTLSWhenTLSWasAskedFor(t *testing.T) {
	t.Parallel()

	srv := chattest.Start(t, chattest.Options{}) // plaintext
	srv.Register(t, "alice", password)
	conn, err := client.Dial(chattest.Target, client.Options{
		TLS: &tls.Config{MinVersion: tls.VersionTLS13},
		// A later transport option replaces TLS, as a misconfigured caller
		// would do.
		DialOptions: append(srv.DialOptions(), grpc.WithTransportCredentials(insecure.NewCredentials())),
	})
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	defer conn.Close()
	// Login carries no token.
	tok, err := conn.Login(ctxFor(t), "alice", password)
	if err != nil {
		t.Fatalf("Login() error = %v", err)
	}

	chat := conn.NewChat(tok.Value, client.ChatOptions{})
	go func() {
		for range chat.Events() {
		}
	}()
	if err := chat.Run(ctxFor(t)); status.Code(err) != codes.Unauthenticated {
		t.Errorf("Run() = %v, want UNAUTHENTICATED: a valid token must not go over plaintext", err)
	}
}

func TestRegisterAndLoginErrors(t *testing.T) {
	t.Parallel()

	srv := chattest.Start(t, chattest.Options{})
	conn := dial(t, srv, nil)
	if err := conn.Register(ctxFor(t), "alice", password); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if err := conn.Register(ctxFor(t), "alice", password); !errors.Is(err, client.ErrUsernameTaken) {
		t.Errorf("Register() of a taken name = %v, want ErrUsernameTaken", err)
	}
	if _, err := conn.Login(ctxFor(t), "alice", "wrong password"); !errors.Is(err, client.ErrInvalidCredentials) {
		t.Errorf("Login() with a wrong password = %v, want ErrInvalidCredentials", err)
	}

	err := conn.Register(ctxFor(t), "bob", "short")
	var invalid *client.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("Register() with a short password = %v, want InvalidError", err)
	}
	if len(invalid.Fields) != 1 || !strings.HasPrefix(invalid.Fields[0], "password: ") {
		t.Errorf("InvalidError.Fields = %q, want one violation of password", invalid.Fields)
	}
}

// ignoreIdentity leaves out what the server generates.
var ignoreIdentity = cmpopts.IgnoreFields(client.Message{}, "ID", "SenderID", "SentAt")

type options struct {
	// unread leaves events in the chat's own channel, so the session stalls
	// whenever the test does not read.
	unread  bool
	dial    []grpc.DialOption
	relogin func(ctx context.Context, conn *client.Conn) (string, error)
}

// user is one running chat session.
type user struct {
	name   string
	chat   *client.Chat
	events <-chan client.Event
}

// connect registers name, logs in and runs a chat session until the test
// ends. Unless opts.unread is set, events are relayed into a buffer, so a
// test that does not read them does not stall the session.
func connect(t *testing.T, srv *chattest.Server, name string, opts options) *user {
	t.Helper()

	srv.Register(t, name, password)
	conn := dial(t, srv, opts.dial)
	tok, err := conn.Login(ctxFor(t), name, password)
	if err != nil {
		t.Fatalf("Login(%s) error = %v", name, err)
	}
	chatOpts := client.ChatOptions{MinBackoff: 10 * time.Millisecond, MaxBackoff: 100 * time.Millisecond}
	if opts.relogin != nil {
		chatOpts.Relogin = func(ctx context.Context) (string, error) { return opts.relogin(ctx, conn) }
	}
	return start(t, name, conn.NewChat(tok.Value, chatOpts), !opts.unread)
}

// dial connects to srv, with TLS if srv serves it.
func dial(t *testing.T, srv *chattest.Server, extra []grpc.DialOption) *client.Conn {
	t.Helper()
	conn, err := client.Dial(chattest.Target, client.Options{
		TLS:         srv.ClientTLS(t),
		Insecure:    true,
		DialOptions: append(srv.DialOptions(), extra...),
	})
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Errorf("close connection: %v", err)
		}
	})
	return conn
}

// start runs chat until the test ends, then closes it the graceful way.
func start(t *testing.T, name string, chat *client.Chat, relay bool) *user {
	t.Helper()

	u := &user{name: name, chat: chat, events: chat.Events()}
	if relay {
		buf := make(chan client.Event, 1000)
		go func() {
			defer close(buf)
			for e := range chat.Events() {
				buf <- e
			}
		}()
		u.events = buf
	}

	// Not t.Context(): it is cancelled before cleanups run, and Close is
	// what should end Run.
	ran := make(chan error, 1)
	go func() { ran <- chat.Run(context.Background()) }()
	// Registered after dial's cleanup, so it runs before the connection
	// closes.
	t.Cleanup(func() {
		var drained sync.WaitGroup
		drained.Go(func() {
			for range u.events {
			}
		})
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := chat.Close(ctx); err != nil {
			t.Errorf("Close(%s) error = %v", name, err)
		}
		if err := <-ran; err != nil {
			t.Errorf("Run(%s) = %v, want nil after Close", name, err)
		}
		drained.Wait()
	})
	return u
}

func (u *user) join(t *testing.T, room string) {
	t.Helper()
	if err := u.chat.Join(ctxFor(t), room); err != nil {
		t.Fatalf("Join(%s, %s) error = %v", u.name, room, err)
	}
}

// say sends text to general, the room every test talks in.
func (u *user) say(t *testing.T, text string) {
	t.Helper()
	if err := u.chat.Send(ctxFor(t), "general", text); err != nil {
		t.Fatalf("Send(%s, %q) error = %v", u.name, text, err)
	}
}

// next returns the next event.
func (u *user) next(t *testing.T) client.Event {
	t.Helper()
	select {
	case e, ok := <-u.events:
		if !ok {
			t.Fatalf("%s's events closed", u.name)
		}
		return e
	case <-time.After(5 * time.Second):
		t.Fatalf("no event for %s within 5s", u.name)
		return nil
	}
}

// message returns the next Message.
func (u *user) message(t *testing.T) client.Message {
	t.Helper()
	return waitFor[client.Message](t, u)
}

// messageAfter skips messages up to and including the one with text and
// returns the next.
func (u *user) messageAfter(t *testing.T, text string) client.Message {
	t.Helper()
	for {
		if u.message(t).Text == text {
			return u.message(t)
		}
	}
}

// waitFor returns the next event of type E. Connection events may be
// skipped on the way, since a reconnect can take more than one attempt;
// anything else is unexpected.
func waitFor[E client.Event](t *testing.T, u *user) E {
	t.Helper()
	for {
		e := u.next(t)
		if want, ok := e.(E); ok {
			return want
		}
		switch e.(type) {
		case client.Connected, client.Disconnected:
		default:
			t.Fatalf("%s got %#v while waiting for a %T", u.name, e, *new(E))
		}
	}
}

// ctxFor bounds a call that must not hang the test.
func ctxFor(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}
