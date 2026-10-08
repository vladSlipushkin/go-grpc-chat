package main

import (
	"context"
	"io"
	"testing"
	"time"
)

func TestRunReturnsWhenContextIsCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())

	errc := make(chan error, 1)
	go func() { errc <- run(ctx, io.Discard) }()

	cancel()

	select {
	case err := <-errc:
		if err != nil {
			t.Fatalf("run() = %v, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("run() did not return after the context was cancelled")
	}
}
