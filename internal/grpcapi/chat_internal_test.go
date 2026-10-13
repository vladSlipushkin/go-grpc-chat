package grpcapi

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/vladSlipushkin/go-grpc-chat/internal/chat"
)

func TestEndedMapsCauseToStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		cause    error
		fromPeer bool // the cause ends the stream's own context
		want     error
	}{
		{name: "slow client", cause: chat.ErrSlowClient, want: errSlowClient},
		{name: "hub closed", cause: chat.ErrHubClosed, want: errShuttingDown},
		{name: "server stopping", cause: errServerStopping, want: errShuttingDown},
		{name: "room failed", cause: chat.ErrRoomFailed, want: errInternal},
		{name: "goroutine panicked", cause: errPanicked, want: errInternal},
		{name: "unexpected failure", cause: errors.New("generate message id: boom"), want: errInternal},
		{name: "client cancelled", cause: context.Canceled, fromPeer: true, want: status.Error(codes.Canceled, context.Canceled.Error())},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			streamCtx, cancelStream := context.WithCancelCause(t.Context())
			defer cancelStream(nil)
			ctx, end := context.WithCancelCause(streamCtx)
			defer end(nil)
			if tt.fromPeer {
				cancelStream(tt.cause)
			} else {
				end(tt.cause)
			}

			got := newTestChatServer().ended(ctx, streamCtx)
			if diff := cmp.Diff(status.Convert(tt.want).Proto(), status.Convert(got).Proto(), protocmp.Transform()); diff != "" {
				t.Errorf("ended() with cause %v mismatch (-want +got):\n%s", tt.cause, diff)
			}
		})
	}
}

// TestEndedKeepsFirstCause pins the order of a drop and a token expiry that
// both happen before the handler wakes up: whichever came first decides the
// status. A stream deadline stands in for the token's.
func TestEndedKeepsFirstCause(t *testing.T) {
	t.Parallel()

	errExpired := errors.New("access token expired")
	tests := []struct {
		name      string
		dropFirst bool
		wantCode  codes.Code
	}{
		{name: "drop then expiry", dropFirst: true, wantCode: codes.ResourceExhausted},
		// DEADLINE_EXCEEDED is what the auth interceptor turns into
		// TOKEN_EXPIRED.
		{name: "expiry then drop", dropFirst: false, wantCode: codes.DeadlineExceeded},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				streamCtx, cancel := context.WithDeadlineCause(t.Context(), time.Now().Add(time.Hour), errExpired)
				defer cancel()
				ctx, end := context.WithCancelCause(streamCtx)
				defer end(nil)

				if tt.dropFirst {
					end(chat.ErrSlowClient)
					time.Sleep(2 * time.Hour)
				} else {
					time.Sleep(2 * time.Hour)
					end(chat.ErrSlowClient)
				}

				if got := status.Code(newTestChatServer().ended(ctx, streamCtx)); got != tt.wantCode {
					t.Errorf("ended() code = %v, want %v", got, tt.wantCode)
				}
			})
		})
	}
}

func newTestChatServer() *ChatServer {
	return NewChatServer(nil, nil, slog.New(slog.DiscardHandler))
}
