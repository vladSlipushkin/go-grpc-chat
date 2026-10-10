package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

func TestUnaryRecoveryTurnsPanicIntoInternal(t *testing.T) {
	t.Parallel()

	logger, logs := newTestLogger()
	info := &grpc.UnaryServerInfo{FullMethod: "/test.v1.Test/Boom"}
	panicking := func(context.Context, any) (any, error) { panic("boom") }

	_, err := unaryRecovery(logger)(t.Context(), nil, info, panicking)

	if got := status.Code(err); got != codes.Internal {
		t.Errorf("code = %v, want %v", got, codes.Internal)
	}
	rec := lastRecord(t, logs)
	if rec["msg"] != "handler panicked" || rec["method"] != info.FullMethod || rec["panic"] != "boom" {
		t.Errorf("log record = %v, want a panic record for %s", rec, info.FullMethod)
	}
}

func TestStreamRecoveryTurnsPanicIntoInternal(t *testing.T) {
	t.Parallel()

	logger, logs := newTestLogger()
	info := &grpc.StreamServerInfo{FullMethod: "/test.v1.Test/BoomStream"}
	panicking := func(any, grpc.ServerStream) error { panic("boom") }

	err := streamRecovery(logger)(nil, fakeStream{ctx: t.Context()}, info, panicking)

	if got := status.Code(err); got != codes.Internal {
		t.Errorf("code = %v, want %v", got, codes.Internal)
	}
	if rec := lastRecord(t, logs); rec["msg"] != "handler panicked" {
		t.Errorf("log record = %v, want a panic record", rec)
	}
}

func TestRecoveryPassesThroughNormalResults(t *testing.T) {
	t.Parallel()

	logger, _ := newTestLogger()
	info := &grpc.UnaryServerInfo{FullMethod: "/test.v1.Test/Ok"}
	want := status.Error(codes.NotFound, "no such thing")
	handler := func(context.Context, any) (any, error) { return "resp", want }

	resp, err := unaryRecovery(logger)(t.Context(), nil, info, handler)
	if resp != "resp" || !errors.Is(err, want) {
		t.Errorf("unaryRecovery() = (%v, %v), want (resp, %v)", resp, err, want)
	}
}

func TestLoggingRecordsOutcome(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		method    string
		err       error
		wantLevel string
		wantCode  string
	}{
		{
			name:      "success",
			method:    "/chat.v1.ChatService/Connect",
			wantLevel: "INFO",
			wantCode:  "OK",
		},
		{
			name:      "client error",
			method:    "/auth.v1.AuthService/Login",
			err:       status.Error(codes.Unauthenticated, "bad credentials"),
			wantLevel: "WARN",
			wantCode:  "Unauthenticated",
		},
		{
			name:      "server error",
			method:    "/auth.v1.AuthService/Register",
			err:       errors.New("database is down"),
			wantLevel: "ERROR",
			wantCode:  "Unknown",
		},
		{
			name:      "client cancelled",
			method:    "/chat.v1.ChatService/Connect",
			err:       context.Canceled,
			wantLevel: "INFO",
			wantCode:  "Canceled",
		},
		{
			name:      "client deadline",
			method:    "/auth.v1.AuthService/Login",
			err:       fmt.Errorf("wait for room: %w", context.DeadlineExceeded),
			wantLevel: "WARN",
			wantCode:  "DeadlineExceeded",
		},
		{
			name:      "health probe",
			method:    "/grpc.health.v1.Health/Check",
			wantLevel: "DEBUG",
			wantCode:  "OK",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			logger, logs := newTestLogger()
			ctx := peer.NewContext(t.Context(), &peer.Peer{Addr: &net.TCPAddr{IP: net.IPv4(10, 0, 0, 7), Port: 4242}})
			info := &grpc.UnaryServerInfo{FullMethod: tt.method}
			handler := func(context.Context, any) (any, error) { return nil, tt.err }

			_, err := unaryLogging(logger)(ctx, nil, info, handler)
			if !errors.Is(err, tt.err) {
				t.Fatalf("unaryLogging() error = %v, want %v", err, tt.err)
			}

			rec := lastRecord(t, logs)
			if _, ok := rec["duration"]; !ok {
				t.Errorf("log record = %v, want a duration", rec)
			}
			got := map[string]any{"level": rec["level"], "msg": rec["msg"], "method": rec["method"], "code": rec["code"], "peer": rec["peer"]}
			want := map[string]any{"level": tt.wantLevel, "msg": "rpc finished", "method": tt.method, "code": tt.wantCode, "peer": "10.0.0.7:4242"}
			if diff := cmp.Diff(want, got); diff != "" {
				t.Errorf("log record mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestStreamLoggingUsesStreamContext(t *testing.T) {
	t.Parallel()

	logger, logs := newTestLogger()
	ctx := peer.NewContext(t.Context(), &peer.Peer{Addr: &net.TCPAddr{IP: net.IPv4(10, 0, 0, 8), Port: 1}})
	info := &grpc.StreamServerInfo{FullMethod: "/chat.v1.ChatService/Connect"}
	handler := func(any, grpc.ServerStream) error { return status.Error(codes.ResourceExhausted, "slow client") }

	if err := streamLogging(logger)(nil, fakeStream{ctx: ctx}, info, handler); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("streamLogging() error = %v, want RESOURCE_EXHAUSTED", err)
	}
	rec := lastRecord(t, logs)
	if rec["code"] != "ResourceExhausted" || rec["peer"] != "10.0.0.8:1" {
		t.Errorf("log record = %v, want code ResourceExhausted from peer 10.0.0.8:1", rec)
	}
}

// fakeStream is the part of grpc.ServerStream the interceptors use.
type fakeStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s fakeStream) Context() context.Context { return s.ctx }

func newTestLogger() (*slog.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	return slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})), &buf
}

func lastRecord(t *testing.T, logs *bytes.Buffer) map[string]any {
	t.Helper()
	lines := bytes.Split(bytes.TrimSpace(logs.Bytes()), []byte("\n"))
	var rec map[string]any
	if err := json.Unmarshal(lines[len(lines)-1], &rec); err != nil {
		t.Fatalf("decode log record %q: %v", lines[len(lines)-1], err)
	}
	return rec
}
