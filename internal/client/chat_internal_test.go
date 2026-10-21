package client

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	chatv1 "github.com/vladSlipushkin/go-grpc-chat/gen/chat/v1"
)

func TestBackoffDoublesWithinJitter(t *testing.T) {
	t.Parallel()

	b := backoff{min: 100 * time.Millisecond, max: time.Second}
	tests := []struct {
		attempt int
		ceiling time.Duration
	}{
		{attempt: 0, ceiling: 100 * time.Millisecond},
		{attempt: 1, ceiling: 200 * time.Millisecond},
		{attempt: 3, ceiling: 800 * time.Millisecond},
		{attempt: 4, ceiling: time.Second},
		{attempt: 63, ceiling: time.Second},
	}
	for _, tt := range tests {
		// The jitter is random: many draws make a wrong bound show up.
		for range 1000 {
			if d := b.delay(tt.attempt); d < tt.ceiling/2 || d > tt.ceiling {
				t.Fatalf("delay(%d) = %v, want within [%v, %v]", tt.attempt, d, tt.ceiling/2, tt.ceiling)
			}
		}
	}
}

func TestSeqTrackerCountsGapsOnly(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		seqs []int64
		want []int64 // missed before each seq
	}{
		{name: "first message after joining", seqs: []int64{41}, want: []int64{0}},
		{name: "consecutive", seqs: []int64{1, 2, 3}, want: []int64{0, 0, 0}},
		{name: "gap", seqs: []int64{1, 2, 6}, want: []int64{0, 0, 3}},
		// The room emptied and started again from 1 (ADR-0003).
		{name: "restart", seqs: []int64{7, 8, 1, 2}, want: []int64{0, 0, 0, 0}},
		{name: "gap after restart", seqs: []int64{7, 1, 3}, want: []int64{0, 0, 1}},
		{name: "repeat", seqs: []int64{5, 5}, want: []int64{0, 0}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			seqs := make(seqTracker)
			for i, seq := range tt.seqs {
				if got := seqs.observe("general", seq); got != tt.want[i] {
					t.Errorf("observe(%d) after %v = %d, want %d", seq, tt.seqs[:i], got, tt.want[i])
				}
			}
		})
	}
}

func TestSeqTrackerForgetsLeftRoom(t *testing.T) {
	t.Parallel()

	seqs := make(seqTracker)
	seqs.observe("general", 3)
	seqs.observe("random", 3)
	seqs.forget("general")
	// Messages sent while the session was out of the room were not missed.
	if got := seqs.observe("general", 10); got != 0 {
		t.Errorf("observe(general, 10) after rejoining = %d, want 0", got)
	}
	if got := seqs.observe("random", 5); got != 1 {
		t.Errorf("observe(random, 5) = %d, want 1: other rooms keep their place", got)
	}
}

// TestTokenNeedsTLSOnATLSConnection checks the guard behind the token: a
// session that believes its connection is TLS refuses to send the token
// over one that is not.
func TestTokenNeedsTLSOnATLSConnection(t *testing.T) {
	t.Parallel()

	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	chatv1.RegisterChatServiceServer(gs, echoServer{})
	served := make(chan error, 1)
	go func() { served <- gs.Serve(lis) }()
	defer func() {
		gs.Stop()
		if err := <-served; err != nil {
			t.Errorf("Serve() = %v", err)
		}
	}()

	conn, err := Dial("passthrough:///fake", Options{
		Insecure: true,
		DialOptions: []grpc.DialOption{grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		})},
	})
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	defer conn.Close()
	conn.secure = true

	chat := conn.NewChat("token", ChatOptions{})
	go func() {
		for range chat.Events() {
		}
	}()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := chat.Run(ctx); status.Code(err) != codes.Unauthenticated {
		t.Errorf("Run() = %v, want UNAUTHENTICATED before anything is sent", err)
	}
}

// echoServer acknowledges every request.
type echoServer struct {
	chatv1.UnimplementedChatServiceServer
}

func (echoServer) Connect(stream chatv1.ChatService_ConnectServer) error {
	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := stream.Send(&chatv1.ConnectResponse{Event: &chatv1.ConnectResponse_Ack{Ack: &chatv1.Ack{RequestId: req.GetRequestId()}}}); err != nil {
			return err
		}
	}
}
