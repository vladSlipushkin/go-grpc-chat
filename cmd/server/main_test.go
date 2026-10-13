package main

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"
	"golang.org/x/crypto/bcrypt"
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
	go func() { errc <- run(ctx, lookup, &out, bcrypt.MinCost) }()

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

func TestRunRejectsInvalidConfig(t *testing.T) {
	t.Parallel()

	lookup := func(key string) (string, bool) {
		if key == "CHAT_LOG_LEVEL" {
			return "loud", true
		}
		return "", false
	}

	err := run(t.Context(), lookup, &bytes.Buffer{}, bcrypt.MinCost)
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
