package main

import (
	"bytes"
	"context"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"
	"golang.org/x/crypto/bcrypt"

	"github.com/vladSlipushkin/go-grpc-chat/internal/certtest"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestRunServesUntilCancelled(t *testing.T) {
	t.Parallel()

	env := map[string]string{
		"CHAT_GRPC_ADDR":  "127.0.0.1:0",
		"CHAT_JWT_SECRET": "0123456789abcdef0123456789abcdef",
	}
	lookup := func(key string) (string, bool) { v, ok := env[key]; return v, ok }

	ctx, cancel := context.WithCancel(t.Context())
	var out lockedBuffer
	errc := make(chan error, 1)
	go func() { errc <- run(ctx, nil, lookup, &out, bcrypt.MinCost) }()

	// Cancel once the listener is bound, so the test covers a start that got
	// past configuration and listening before it stops.
	waitFor(t, 5*time.Second, func() bool { return strings.Contains(out.String(), "server started") })
	cancel()

	select {
	case err := <-errc:
		if err != nil {
			t.Fatalf("run() = %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("run() did not return after the context was cancelled")
	}

	for _, msg := range []string{"server started", "shutting down", "server stopped"} {
		if !strings.Contains(out.String(), msg) {
			t.Errorf("log output = %q, want it to contain %q", out.String(), msg)
		}
	}
}

func TestHealthcheck(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		tls  bool
	}{
		{name: "plaintext"},
		{name: "TLS", tls: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// The check finds the server by its configured address, so the
			// port must be known before the server starts.
			env := map[string]string{
				"CHAT_GRPC_ADDR":  "127.0.0.1:" + freePort(t),
				"CHAT_JWT_SECRET": "0123456789abcdef0123456789abcdef",
			}
			if tt.tls {
				files := certtest.SelfSigned(t, "localhost")
				env["CHAT_TLS_CERT_FILE"] = files.Cert
				env["CHAT_TLS_KEY_FILE"] = files.Key
			}
			lookup := func(key string) (string, bool) { v, ok := env[key]; return v, ok }
			healthcheck := func() error { return run(t.Context(), []string{"-healthcheck"}, lookup, io.Discard, bcrypt.MinCost) }

			if err := healthcheck(); err == nil {
				t.Error("-healthcheck before the server started = nil, want an error")
			}

			ctx, cancel := context.WithCancel(t.Context())
			var out lockedBuffer
			errc := make(chan error, 1)
			go func() { errc <- run(ctx, nil, lookup, &out, bcrypt.MinCost) }()
			waitFor(t, 5*time.Second, func() bool { return strings.Contains(out.String(), "server started") })
			if err := healthcheck(); err != nil {
				t.Errorf("-healthcheck = %v, want nil", err)
			}

			cancel()
			if err := <-errc; err != nil {
				t.Errorf("run() = %v, want nil", err)
			}
		})
	}
}

// freePort returns a loopback port that was free a moment ago.
func freePort(t *testing.T) string {
	t.Helper()
	var lc net.ListenConfig
	lis, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer lis.Close()
	_, port, err := net.SplitHostPort(lis.Addr().String())
	if err != nil {
		t.Fatalf("split %s: %v", lis.Addr(), err)
	}
	return port
}

func TestRunRejectsInvalidConfig(t *testing.T) {
	t.Parallel()

	lookup := func(key string) (string, bool) {
		if key == "CHAT_LOG_LEVEL" {
			return "loud", true
		}
		return "", false
	}

	err := run(t.Context(), nil, lookup, &bytes.Buffer{}, bcrypt.MinCost)
	if err == nil || !strings.Contains(err.Error(), "CHAT_LOG_LEVEL") {
		t.Errorf("run() = %v, want a config error mentioning CHAT_LOG_LEVEL", err)
	}
}

// lockedBuffer lets the test read log output while run is still writing it.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func waitFor(t *testing.T, limit time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("condition not met within %v", limit)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
