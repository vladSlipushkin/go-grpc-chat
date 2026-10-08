package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"
)

func TestRunLogsLifecycleAndReturnsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())

	var out bytes.Buffer
	errc := make(chan error, 1)
	go func() { errc <- run(ctx, &out) }()

	cancel()

	select {
	case err := <-errc:
		if err != nil {
			t.Fatalf("run() = %v, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("run() did not return after the context was cancelled")
	}

	// Reading out is safe: run has returned, so its goroutine no longer writes.
	for _, msg := range []string{"server started", "server stopped"} {
		if !strings.Contains(out.String(), msg) {
			t.Errorf("log output = %q, want it to contain %q", out.String(), msg)
		}
	}
}
