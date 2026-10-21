package grpcapi_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/testing/protocmp"

	chatv1 "github.com/vladSlipushkin/go-grpc-chat/gen/chat/v1"
	"github.com/vladSlipushkin/go-grpc-chat/internal/auth"
	"github.com/vladSlipushkin/go-grpc-chat/internal/chat"
	"github.com/vladSlipushkin/go-grpc-chat/internal/grpcapi"
	"github.com/vladSlipushkin/go-grpc-chat/internal/server"
)

func TestChatJoinSendLeave(t *testing.T) {
	t.Parallel()

	env := startChat(t, time.Second)
	alice, bob := env.connect(t, "alice", time.Hour), env.connect(t, "bob", time.Hour)
	wantAck(t, alice, joinRoom("a1", "general"))
	wantAck(t, bob, joinRoom("b1", "general"))

	send(t, alice, sendMessage("a2", "general", "hello"))
	msgs, reply := nextReply(t, alice)
	// The sender's own message is queued before its Ack (ADR-0008).
	if len(msgs) != 1 {
		t.Fatalf("alice got %d messages before her Ack, want 1", len(msgs))
	}
	sent := msgs[0]
	wantMsg := &chatv1.Message{RoomId: "general", SenderId: "id-alice", SenderUsername: "alice", Text: "hello", Seq: 1}
	if diff := cmp.Diff(wantMsg, sent, protocmp.Transform(), protocmp.IgnoreFields(&chatv1.Message{}, "id", "sent_at")); diff != "" {
		t.Errorf("alice's message mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(&chatv1.Ack{RequestId: "a2", MessageId: sent.GetId()}, reply.GetAck(), protocmp.Transform()); diff != "" {
		t.Errorf("Ack for the send mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(sent, recv(t, bob).GetMessage(), protocmp.Transform()); diff != "" {
		t.Errorf("bob's copy of the message mismatch (-want +got):\n%s", diff)
	}

	wantAck(t, alice, leaveRoom("a3", "general"))
	wantAck(t, bob, sendMessage("b2", "general", "anyone?"))
	// Bob's Ack means the room has delivered "anyone?" to every member, so
	// had alice still been one, it would be queued ahead of her next Ack.
	send(t, alice, joinRoom("a4", "other"))
	if msgs, reply := nextReply(t, alice); len(msgs) != 0 || reply.GetAck().GetRequestId() != "a4" {
		t.Errorf("after leaving, alice got messages %v and reply %v, want only the Ack for a4", msgs, reply)
	}
}

func TestChatRequestErrorsKeepStreamOpen(t *testing.T) {
	t.Parallel()

	env := startChat(t, time.Second)
	alice := env.connect(t, "alice", time.Hour)

	tests := []struct {
		name string
		req  *chatv1.ConnectRequest
		want reply
	}{
		{
			name: "invalid room id",
			req:  joinRoom("r1", "Not A Room"),
			want: reply{requestID: "r1", code: codes.InvalidArgument, fields: []string{"join_room.room_id"}},
		},
		{
			name: "missing request id",
			req:  joinRoom("", "general"),
			want: reply{requestID: "", code: codes.InvalidArgument, fields: []string{"request_id"}},
		},
		{
			name: "no action",
			req:  &chatv1.ConnectRequest{RequestId: "r2"},
			want: reply{requestID: "r2", code: codes.InvalidArgument, fields: []string{"action"}},
		},
		{
			name: "blank text",
			req:  sendMessage("r3", "general", " \u200b "),
			want: reply{requestID: "r3", code: codes.InvalidArgument, fields: []string{"send_message.text"}},
		},
		{
			name: "send to a room not joined",
			req:  sendMessage("r4", "elsewhere", "hi"),
			want: reply{requestID: "r4", code: codes.FailedPrecondition},
		},
	}
	// Sequential: the cases share one stream, which is the point.
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			send(t, alice, tt.req)
			_, got := nextReply(t, alice)
			if diff := cmp.Diff(tt.want, summarize(t, got), cmp.AllowUnexported(reply{})); diff != "" {
				t.Errorf("reply mismatch (-want +got):\n%s", diff)
			}
		})
	}

	wantAck(t, alice, joinRoom("r5", "general"))
}

func TestChatRoomLimit(t *testing.T) {
	t.Parallel()

	env := startChat(t, time.Second)
	alice := env.connect(t, "alice", time.Hour)
	for i := range chat.MaxRooms {
		wantAck(t, alice, joinRoom(fmt.Sprintf("j%d", i), fmt.Sprintf("room-%d", i)))
	}

	send(t, alice, joinRoom("over", "one-too-many"))
	_, got := nextReply(t, alice)
	want := reply{requestID: "over", code: codes.ResourceExhausted, reason: "ROOM_LIMIT_EXCEEDED"}
	if diff := cmp.Diff(want, summarize(t, got), cmp.AllowUnexported(reply{})); diff != "" {
		t.Errorf("reply to a join past the limit mismatch (-want +got):\n%s", diff)
	}
	// Membership is checked before the limit (ADR-0003).
	wantAck(t, alice, joinRoom("again", "room-0"))
}

func TestChatHalfCloseEndsStreamWithOK(t *testing.T) {
	t.Parallel()

	env := startChat(t, time.Second)
	alice := env.connect(t, "alice", time.Hour)
	wantAck(t, alice, joinRoom("a1", "general"))

	if err := alice.CloseSend(); err != nil {
		t.Fatalf("CloseSend() error = %v", err)
	}
	if _, err := alice.Recv(); !errors.Is(err, io.EOF) {
		t.Errorf("Recv() after half-close = %v, want io.EOF (status OK)", err)
	}
}

func TestChatTokenExpiryEndsSilentClient(t *testing.T) {
	t.Parallel()

	env := startChat(t, time.Second)
	alice := env.connect(t, "alice", 2*time.Second)
	wantAck(t, alice, joinRoom("a1", "general"))

	// The client sends nothing more, so the reader stays blocked in Recv:
	// the stream ends only because the handler returns without it.
	_, err := alice.Recv()
	assertStatus(t, err, codes.Unauthenticated, "TOKEN_EXPIRED")
	if !strings.Contains(env.logs.String(), `"cause":"access token expired"`) {
		t.Errorf("log = %s, want the session's end cause", env.logs.String())
	}
}

func TestChatSlowClientIsDroppedWithoutGap(t *testing.T) {
	t.Parallel()

	env := startChat(t, time.Second)
	// A fixed 64 KiB window turns off grpc-go's window growth, so the
	// server's writer blocks after a bounded number of unread messages.
	alice := env.connect(t, "alice", time.Hour,
		grpc.WithInitialWindowSize(64<<10),
		grpc.WithInitialConnWindowSize(64<<10),
	)
	wantAck(t, alice, joinRoom("a1", "general"))

	// Alice stops reading. 200 messages of 4 KiB are far more than her
	// queue, her window and the server's write buffer hold together.
	const sent = 200
	flood(t, env, sent)
	// The room keeps serving the others after dropping alice.
	bob := env.connect(t, "bob", time.Hour)
	wantAck(t, bob, joinRoom("b1", "general"))
	wantAck(t, bob, sendMessage("b-last", "general", "still here"))

	var got int64
	for {
		resp, err := alice.Recv()
		if err != nil {
			assertStatus(t, err, codes.ResourceExhausted, "SLOW_CLIENT")
			break
		}
		got++
		if seq := resp.GetMessage().GetSeq(); seq != got {
			t.Fatalf("message %d has seq %d, want %d: a gap before the final status", got, seq, got)
		}
	}
	t.Logf("alice received %d messages before SLOW_CLIENT", got)
	if got == 0 || got >= sent {
		t.Errorf("alice received %d of %d messages, want some but not all", got, sent)
	}
}

func TestChatShutdownEndsStreamsWithUnavailable(t *testing.T) {
	t.Parallel()

	// The shutdown timeout is far above the test's wait: the stream must
	// end because the handler watches shutdown, not because Stop cut it.
	env := startChat(t, time.Minute)
	alice := env.connect(t, "alice", time.Hour)
	wantAck(t, alice, joinRoom("a1", "general"))

	stopped := make(chan error, 1)
	go func() { stopped <- env.stop() }()

	_, err := alice.Recv()
	assertStatus(t, err, codes.Unavailable, "")
	select {
	case err := <-stopped:
		if err != nil {
			t.Errorf("Serve() = %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve() did not return after the chat stream ended")
	}
}

func TestChatSessionLogNamesUserAndPeer(t *testing.T) {
	t.Parallel()

	env := startChat(t, time.Minute)
	alice := env.connect(t, "alice", time.Hour)
	wantAck(t, alice, joinRoom("a1", "general"))
	// Serve returns after the handler, so its records are written by then.
	if err := env.stop(); err != nil {
		t.Fatalf("Serve() = %v", err)
	}

	type record struct {
		Msg    string `json:"msg"`
		UserID string `json:"user_id"`
		Peer   string `json:"peer"`
	}
	var got []record
	dec := json.NewDecoder(strings.NewReader(env.logs.String()))
	for dec.More() {
		var r record
		if err := dec.Decode(&r); err != nil {
			t.Fatalf("decode log record: %v", err)
		}
		if r.Msg == "chat session ended" {
			got = append(got, r)
		}
	}
	want := []record{{Msg: "chat session ended", UserID: "id-alice", Peer: "bufconn"}}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("session end records mismatch (-want +got):\n%s", diff)
	}
}

func TestChatRejectsOversizedRequest(t *testing.T) {
	t.Parallel()

	env := startChat(t, time.Second)
	alice := env.connect(t, "alice", time.Hour)

	// Over the 16 KiB receive limit: gRPC fails the stream before the
	// handler sees the request (ADR-0003).
	_ = alice.Send(sendMessage("big", "general", strings.Repeat("x", 17<<10)))
	_, err := alice.Recv()
	assertStatus(t, err, codes.ResourceExhausted, "")
}

func TestChatGoroutinePanicEndsStreamWithInternal(t *testing.T) {
	t.Parallel()

	join := joinRoom("a1", "general")
	tests := []struct {
		name   string
		stream func(ctx context.Context) *fakeStream
	}{
		{
			name: "reader",
			stream: func(ctx context.Context) *fakeStream {
				return &fakeStream{ctx: ctx, recv: func() (*chatv1.ConnectRequest, error) { panic("recv") }}
			},
		},
		{
			name: "writer",
			stream: func(ctx context.Context) *fakeStream {
				// One request, whose Ack makes the writer call Send.
				reqs := make(chan *chatv1.ConnectRequest, 1)
				reqs <- join
				return &fakeStream{
					ctx: ctx,
					recv: func() (*chatv1.ConnectRequest, error) {
						select {
						case req := <-reqs:
							return req, nil
						case <-ctx.Done():
							return nil, ctx.Err()
						}
					},
					send: func(*chatv1.ConnectResponse) error { panic("send") },
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var logs lockedBuffer
			logger := slog.New(slog.NewJSONHandler(&logs, nil))
			hub := chat.NewHub(logger)
			defer hub.Close()
			srv := grpcapi.NewChatServer(hub, nil, logger)

			// Cancelled when the test ends, as gRPC cancels a stream once
			// its handler returns; that releases the fake's Recv.
			ctx, cancel := context.WithCancel(auth.NewContext(t.Context(), auth.Claims{UserID: "id-alice", Username: "alice"}))
			defer cancel()

			err := srv.Connect(tt.stream(ctx))
			assertStatus(t, err, codes.Internal, "")
			want := fmt.Sprintf(`"goroutine":%q`, tt.name)
			if !strings.Contains(logs.String(), "chat stream goroutine panicked") || !strings.Contains(logs.String(), want) {
				t.Errorf("log = %s, want a panic record for the %s", logs.String(), tt.name)
			}
		})
	}
}

// TestWriterSendsNothingAfterSessionEnds covers what a real client cannot
// arrange: the writer is inside Send when the session is dropped, and
// events are still queued when Send returns.
func TestWriterSendsNothingAfterSessionEnds(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		logger := slog.New(slog.DiscardHandler)
		hub := chat.NewHub(logger)
		defer hub.Close()
		srv := grpcapi.NewChatServer(hub, nil, logger)

		ctx, cancel := context.WithCancel(auth.NewContext(t.Context(), auth.Claims{UserID: "id-alice", Username: "alice"}))
		defer cancel()
		reqs := make(chan *chatv1.ConnectRequest, 1)
		reqs <- joinRoom("a1", "general")
		release := make(chan struct{})
		var (
			mu   sync.Mutex
			sent []*chatv1.ConnectResponse
		)
		stream := &fakeStream{
			ctx: ctx,
			recv: func() (*chatv1.ConnectRequest, error) {
				select {
				case req := <-reqs:
					return req, nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			},
			send: func(resp *chatv1.ConnectResponse) error {
				mu.Lock()
				sent = append(sent, resp)
				first := len(sent) == 1
				mu.Unlock()
				if first {
					<-release // a client that stopped reading
				}
				return nil
			},
		}
		done := make(chan error, 1)
		go func() { done <- srv.Connect(stream) }()
		synctest.Wait() // alice has joined; the writer is stuck sending her Ack

		bobCtx, bobEnd := context.WithCancelCause(auth.NewContext(t.Context(), auth.Claims{UserID: "id-bob", Username: "bob"}))
		defer bobEnd(nil)
		bob, err := hub.Connect(bobCtx, bobEnd)
		if err != nil {
			t.Fatalf("Connect(bob) error = %v", err)
		}
		defer bob.Close()
		if err := bob.Join(bobCtx, "general"); err != nil {
			t.Fatalf("Join(bob) error = %v", err)
		}
		// One message more than alice's queue holds.
		for i := range chat.QueueLen + 1 {
			if _, err := bob.Send(bobCtx, "general", fmt.Sprint(i)); err != nil {
				t.Fatalf("Send(bob) error = %v", err)
			}
			<-bob.Events()
		}

		assertStatus(t, <-done, codes.ResourceExhausted, "SLOW_CLIENT")
		close(release)
		synctest.Wait() // the writer has either exited or sent what it could

		mu.Lock()
		defer mu.Unlock()
		if len(sent) != 1 {
			t.Errorf("writer sent %d events, want only the Ack it was sending when the session ended", len(sent))
		}
	})
}

// flood posts n messages of 4 KiB to general. One stream may not post that
// fast (ADR-0008), so each sender joins for one burst. It leaves after its
// burst: it stops reading then, and staying would get it dropped as slow.
func flood(t *testing.T, env chatEnv, n int) {
	t.Helper()
	for start := 0; start < n; start += grpcapi.MessageBurst {
		sender := env.connect(t, fmt.Sprintf("sender-%d", start), time.Hour)
		wantAck(t, sender, joinRoom("join", "general"))
		for i := start; i < min(start+grpcapi.MessageBurst, n); i++ {
			text := fmt.Sprintf("%03d%s", i, strings.Repeat("x", 4000))
			wantAck(t, sender, sendMessage(fmt.Sprintf("m-%d", i), "general", text))
		}
		wantAck(t, sender, leaveRoom("leave", "general"))
	}
}

// TestChatRateLimitsMessages runs the handler in a synctest bubble, so the
// limiter's clock is fake and its refill exact.
func TestChatRateLimitsMessages(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		logger := slog.New(slog.DiscardHandler)
		hub := chat.NewHub(logger)
		defer hub.Close()
		srv := grpcapi.NewChatServer(hub, nil, logger)

		ctx, cancel := context.WithCancel(auth.NewContext(t.Context(), auth.Claims{UserID: "id-alice", Username: "alice"}))
		defer cancel()
		stream := connectPipe(ctx, srv)

		if got := stream.request(t, joinRoom("j", "general")); got.GetAck() == nil {
			t.Fatalf("reply to join = %v, want an Ack", got)
		}
		// Messages that are not posted cost nothing: an invalid one is
		// rejected before the limiter, one to a room not joined gives its
		// token back.
		rejected := []struct {
			req  *chatv1.ConnectRequest
			want reply
		}{
			{
				req:  sendMessage("blank", "general", " "),
				want: reply{requestID: "blank", code: codes.InvalidArgument, fields: []string{"send_message.text"}},
			},
			{
				req:  sendMessage("elsewhere", "random", "hi"),
				want: reply{requestID: "elsewhere", code: codes.FailedPrecondition},
			},
		}
		for _, r := range rejected {
			got := stream.request(t, r.req)
			if diff := cmp.Diff(r.want, summarize(t, got), cmp.AllowUnexported(reply{})); diff != "" {
				t.Errorf("reply to %v mismatch (-want +got):\n%s", r.req, diff)
			}
		}
		for i := range grpcapi.MessageBurst {
			if got := stream.request(t, sendMessage(fmt.Sprint(i), "general", "hi")); got.GetAck() == nil {
				t.Fatalf("reply to message %d of the burst = %v, want an Ack", i, got)
			}
		}

		got := stream.request(t, sendMessage("over", "general", "hi"))
		want := reply{requestID: "over", code: codes.ResourceExhausted, reason: "RATE_LIMITED"}
		if diff := cmp.Diff(want, summarize(t, got), cmp.AllowUnexported(reply{})); diff != "" {
			t.Errorf("reply past the burst mismatch (-want +got):\n%s", diff)
		}
		if wait := retryDelay(t, got); wait != grpcapi.MessageInterval {
			t.Errorf("RetryInfo delay = %v, want %v", wait, grpcapi.MessageInterval)
		}

		// The rejected message took no token: one interval frees exactly one.
		time.Sleep(grpcapi.MessageInterval)
		if got := stream.request(t, sendMessage("later", "general", "hi")); got.GetAck() == nil {
			t.Errorf("reply after waiting = %v, want an Ack: the stream stays open", got)
		}
		got = stream.request(t, sendMessage("again", "general", "hi"))
		want = reply{requestID: "again", code: codes.ResourceExhausted, reason: "RATE_LIMITED"}
		if diff := cmp.Diff(want, summarize(t, got), cmp.AllowUnexported(reply{})); diff != "" {
			t.Errorf("reply to a second message without waiting mismatch (-want +got):\n%s", diff)
		}

		cancel()
		assertStatus(t, <-stream.done, codes.Canceled, "")
	})
}

// TestChatRateLimitIsPerUser runs in a synctest bubble, where no time
// passes unless the test sleeps, so no token refills between requests.
func TestChatRateLimitIsPerUser(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		logger := slog.New(slog.DiscardHandler)
		hub := chat.NewHub(logger)
		defer hub.Close()
		srv := grpcapi.NewChatServer(hub, nil, logger)

		alice := auth.Claims{UserID: "id-alice", Username: "alice"}
		open := func(claims auth.Claims) (*pipeStream, context.CancelFunc) {
			ctx, cancel := context.WithCancel(auth.NewContext(t.Context(), claims))
			s := connectPipe(ctx, srv)
			if got := s.request(t, joinRoom("j", "general")); got.GetAck() == nil {
				t.Fatalf("reply to join = %v, want an Ack", got)
			}
			return s, cancel
		}
		post := func(s *pipeStream, id string) *chatv1.ConnectResponse {
			return s.request(t, sendMessage(id, "general", "hi"))
		}
		wantLimited := func(s *pipeStream, id string) {
			t.Helper()
			want := reply{requestID: id, code: codes.ResourceExhausted, reason: "RATE_LIMITED"}
			if diff := cmp.Diff(want, summarize(t, post(s, id)), cmp.AllowUnexported(reply{})); diff != "" {
				t.Errorf("reply to %s mismatch (-want +got):\n%s", id, diff)
			}
		}

		first, closeFirst := open(alice)
		second, closeSecond := open(alice)
		for i := range grpcapi.MessageBurst {
			if got := post(first, fmt.Sprint(i)); got.GetAck() == nil {
				t.Fatalf("reply to message %d of the burst = %v, want an Ack", i, got)
			}
		}
		// The second stream draws on the allowance the first has spent.
		wantLimited(second, "second stream")

		// Another user has an allowance of their own.
		bob, closeBob := open(auth.Claims{UserID: "id-bob", Username: "bob"})
		if got := post(bob, "bob"); got.GetAck() == nil {
			t.Errorf("bob's reply = %v, want an Ack", got)
		}

		// The allowance outlives the stream that spent it while another of
		// the user's streams is open.
		closeFirst()
		assertStatus(t, <-first.done, codes.Canceled, "")
		third, closeThird := open(alice)
		wantLimited(third, "third stream")

		// With the last stream gone, the next one starts with a full burst.
		closeSecond()
		closeThird()
		assertStatus(t, <-second.done, codes.Canceled, "")
		assertStatus(t, <-third.done, codes.Canceled, "")
		fresh, closeFresh := open(alice)
		if got := post(fresh, "fresh"); got.GetAck() == nil {
			t.Errorf("reply on a stream opened after the others closed = %v, want an Ack", got)
		}

		closeFresh()
		closeBob()
		assertStatus(t, <-fresh.done, codes.Canceled, "")
		assertStatus(t, <-bob.done, codes.Canceled, "")
		if n := grpcapi.LimitedUsers(srv); n != 0 {
			t.Errorf("limiters kept after every stream closed: %d, want 0", n)
		}
	})
}

// pipeStream is a fake stream driven one request at a time from inside a
// synctest bubble.
type pipeStream struct {
	*fakeStream
	in   chan *chatv1.ConnectRequest
	out  chan *chatv1.ConnectResponse
	done chan error // what Connect returned
}

// connectPipe runs srv.Connect on a pipeStream until ctx ends.
func connectPipe(ctx context.Context, srv *grpcapi.ChatServer) *pipeStream {
	s := &pipeStream{
		in:   make(chan *chatv1.ConnectRequest),
		out:  make(chan *chatv1.ConnectResponse, 256),
		done: make(chan error, 1),
	}
	s.fakeStream = &fakeStream{
		ctx: ctx,
		recv: func() (*chatv1.ConnectRequest, error) {
			select {
			case req := <-s.in:
				return req, nil
			case <-ctx.Done():
				// RecvMsg reports a cancelled stream as a status.
				return nil, status.FromContextError(ctx.Err()).Err()
			}
		},
		send: func(resp *chatv1.ConnectResponse) error {
			s.out <- resp
			return nil
		},
	}
	go func() { s.done <- srv.Connect(s) }() //nolint:contextcheck // the stream carries ctx
	return s
}

// request sends req, waits until the bubble is idle and returns the reply,
// skipping messages.
func (s *pipeStream) request(t *testing.T, req *chatv1.ConnectRequest) *chatv1.ConnectResponse {
	t.Helper()
	select {
	case s.in <- req:
	case err := <-s.done:
		t.Fatalf("Connect() = %v before %v", err, req)
	}
	synctest.Wait()
	for {
		select {
		case resp := <-s.out:
			if resp.GetMessage() == nil {
				return resp
			}
		default:
			t.Fatalf("no reply to %v", req)
			return nil
		}
	}
}

func retryDelay(t *testing.T, resp *chatv1.ConnectResponse) time.Duration {
	t.Helper()
	for _, d := range status.FromProto(resp.GetError().GetStatus()).Details() {
		if info, ok := d.(*errdetails.RetryInfo); ok {
			return info.GetRetryDelay().AsDuration()
		}
	}
	t.Fatalf("reply %v has no RetryInfo", resp)
	return 0
}

// fakeStream is a chat stream without gRPC behind it, for faults a real
// client cannot cause.
type fakeStream struct {
	grpc.ServerStream
	ctx  context.Context
	recv func() (*chatv1.ConnectRequest, error)
	send func(*chatv1.ConnectResponse) error
}

func (s *fakeStream) Context() context.Context                { return s.ctx }
func (s *fakeStream) Recv() (*chatv1.ConnectRequest, error)   { return s.recv() }
func (s *fakeStream) Send(resp *chatv1.ConnectResponse) error { return s.send(resp) }

type chatStream = grpc.BidiStreamingClient[chatv1.ConnectRequest, chatv1.ConnectResponse]

type chatEnv struct {
	lis    *bufconn.Listener
	tokens *fakeTokens
	logs   *lockedBuffer
	stop   func() error
}

// startChat runs the server with the chat service wired as cmd/server does
// it, including the hub closing after Serve, over an in-memory listener.
func startChat(t *testing.T, shutdownTimeout time.Duration) chatEnv {
	t.Helper()

	logs := &lockedBuffer{}
	logger := slog.New(slog.NewJSONHandler(logs, nil))
	tokens := &fakeTokens{claims: make(map[string]auth.Claims)}
	srv := server.New(logger, tokens, server.Options{ShutdownTimeout: shutdownTimeout})

	// Not t.Context(): it is cancelled before cleanups run.
	ctx, cancel := context.WithCancel(context.Background())
	hub := chat.NewHub(logger)
	chatv1.RegisterChatServiceServer(srv, grpcapi.NewChatServer(hub, ctx.Done(), logger))

	lis := bufconn.Listen(1 << 20)
	served := make(chan error, 1)
	go func() {
		err := srv.Serve(ctx, lis)
		hub.Close()
		served <- err
	}()
	stop := sync.OnceValue(func() error {
		cancel()
		return <-served
	})
	t.Cleanup(func() {
		if err := stop(); err != nil {
			t.Errorf("Serve() = %v", err)
		}
	})
	return chatEnv{lis: lis, tokens: tokens, logs: logs, stop: stop}
}

// connect opens a chat stream on a connection of its own for a user named
// name whose token expires after ttl. The stream's deadline of 30 seconds
// only fails a test that hangs.
func (e chatEnv) connect(t *testing.T, name string, ttl time.Duration, opts ...grpc.DialOption) chatStream {
	t.Helper()

	opts = append([]grpc.DialOption{
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return e.lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}, opts...)
	conn, err := grpc.NewClient("passthrough:///bufnet", opts...)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	// Runs before the server's cleanup, which was registered earlier.
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Errorf("close client: %v", err)
		}
	})

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+e.tokens.issue(name, ttl))
	stream, err := chatv1.NewChatServiceClient(conn).Connect(ctx)
	if err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	return stream
}

// fakeTokens accepts the tokens it issued: each is the user's name.
type fakeTokens struct {
	mu     sync.Mutex
	claims map[string]auth.Claims
}

func (f *fakeTokens) issue(name string, ttl time.Duration) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.claims[name] = auth.Claims{UserID: "id-" + name, Username: name, ExpiresAt: time.Now().Add(ttl)}
	return name
}

func (f *fakeTokens) Verify(token string) (auth.Claims, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.claims[token]
	if !ok {
		return auth.Claims{}, auth.ErrInvalidToken
	}
	return c, nil
}

func joinRoom(id, room string) *chatv1.ConnectRequest {
	return &chatv1.ConnectRequest{RequestId: id, Action: &chatv1.ConnectRequest_JoinRoom{JoinRoom: &chatv1.JoinRoom{RoomId: room}}}
}

func leaveRoom(id, room string) *chatv1.ConnectRequest {
	return &chatv1.ConnectRequest{RequestId: id, Action: &chatv1.ConnectRequest_LeaveRoom{LeaveRoom: &chatv1.LeaveRoom{RoomId: room}}}
}

func sendMessage(id, room, text string) *chatv1.ConnectRequest {
	return &chatv1.ConnectRequest{RequestId: id, Action: &chatv1.ConnectRequest_SendMessage{SendMessage: &chatv1.SendMessage{RoomId: room, Text: text}}}
}

func send(t *testing.T, s chatStream, req *chatv1.ConnectRequest) {
	t.Helper()
	if err := s.Send(req); err != nil {
		t.Fatalf("Send(%v) error = %v", req, err)
	}
}

func recv(t *testing.T, s chatStream) *chatv1.ConnectResponse {
	t.Helper()
	resp, err := s.Recv()
	if err != nil {
		t.Fatalf("Recv() error = %v", err)
	}
	return resp
}

// nextReply reads up to the next Ack or Error and returns it with the
// messages that arrived before it.
func nextReply(t *testing.T, s chatStream) ([]*chatv1.Message, *chatv1.ConnectResponse) {
	t.Helper()
	var msgs []*chatv1.Message
	for {
		resp := recv(t, s)
		if m := resp.GetMessage(); m != nil {
			msgs = append(msgs, m)
			continue
		}
		return msgs, resp
	}
}

// wantAck sends req and fails unless the reply is its Ack.
func wantAck(t *testing.T, s chatStream, req *chatv1.ConnectRequest) {
	t.Helper()
	send(t, s, req)
	if _, resp := nextReply(t, s); resp.GetAck().GetRequestId() != req.GetRequestId() {
		t.Fatalf("reply to %v = %v, want its Ack", req, resp)
	}
}

// reply is what a test checks in an Error: who it answers, its code and
// either the fields that failed validation or the ErrorInfo reason.
type reply struct {
	requestID string
	code      codes.Code
	fields    []string
	reason    string
}

func summarize(t *testing.T, resp *chatv1.ConnectResponse) reply {
	t.Helper()
	e := resp.GetError()
	if e == nil {
		t.Fatalf("reply = %v, want an Error", resp)
	}
	st := status.FromProto(e.GetStatus())
	r := reply{requestID: e.GetRequestId(), code: st.Code()}
	for _, d := range st.Details() {
		switch d := d.(type) {
		case *errdetails.BadRequest:
			for _, v := range d.GetFieldViolations() {
				r.fields = append(r.fields, v.GetField())
			}
		case *errdetails.ErrorInfo:
			r.reason = d.GetReason()
		}
	}
	return r
}

// assertStatus checks the status err carries and, when reason is set, its
// ErrorInfo reason in domain chat.v1.
func assertStatus(t *testing.T, err error, code codes.Code, reason string) {
	t.Helper()
	st := status.Convert(err)
	if st.Code() != code {
		t.Fatalf("status = %v, want code %v", st, code)
	}
	if reason == "" {
		return
	}
	for _, d := range st.Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok && info.GetReason() == reason && info.GetDomain() == "chat.v1" {
			return
		}
	}
	t.Errorf("status details = %v, want ErrorInfo reason %s in domain chat.v1", st.Details(), reason)
}
