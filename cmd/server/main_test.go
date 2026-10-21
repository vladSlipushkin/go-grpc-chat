package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"maps"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"
	"golang.org/x/crypto/bcrypt"

	"github.com/vladSlipushkin/go-grpc-chat/internal/certtest"
	"github.com/vladSlipushkin/go-grpc-chat/internal/config"
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
	ctx, cancel := context.WithCancel(t.Context())
	var out lockedBuffer
	errc := make(chan error, 1)
	go func() { errc <- run(ctx, nil, lookupFrom(env), &out, bcrypt.MinCost) }()

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

			env := map[string]string{
				"CHAT_GRPC_ADDR":  "127.0.0.1:0",
				"CHAT_JWT_SECRET": "0123456789abcdef0123456789abcdef",
			}
			if tt.tls {
				files := certtest.SelfSigned(t, "localhost")
				env["CHAT_TLS_CERT_FILE"] = files.Cert
				env["CHAT_TLS_KEY_FILE"] = files.Key
			}
			addr := startServer(t, env)

			// The check reads the same environment, with the port the
			// server got.
			checkEnv := maps.Clone(env)
			checkEnv["CHAT_GRPC_ADDR"] = addr
			if err := run(t.Context(), []string{"-healthcheck"}, lookupFrom(checkEnv), io.Discard, bcrypt.MinCost); err != nil {
				t.Errorf("-healthcheck = %v, want nil", err)
			}
		})
	}
}

// startServer runs the server with env until the test ends and returns the
// address it listens on, read from its "server started" record.
func startServer(t *testing.T, env map[string]string) string {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	var out lockedBuffer
	errc := make(chan error, 1)
	go func() { errc <- run(ctx, nil, lookupFrom(env), &out, bcrypt.MinCost) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-errc:
			if err != nil {
				t.Errorf("run() = %v, want nil", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("run() did not return after the context was cancelled")
		}
	})

	var addr string
	waitFor(t, 5*time.Second, func() bool {
		for line := range strings.Lines(out.String()) {
			var rec struct {
				Msg  string `json:"msg"`
				Addr string `json:"addr"`
			}
			if json.Unmarshal([]byte(line), &rec) == nil && rec.Msg == "server started" {
				addr = rec.Addr
				return true
			}
		}
		return false
	})
	return addr
}

func lookupFrom(env map[string]string) config.LookupFunc {
	return func(key string) (string, bool) {
		v, ok := env[key]
		return v, ok
	}
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
