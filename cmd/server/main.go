// Command server runs the chat server.
package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "server: %v\n", err)
		os.Exit(1)
	}
}

// run is the composition root: it wires dependencies and blocks until ctx is
// cancelled or the process receives SIGINT/SIGTERM. Keeping it separate from
// main makes it testable and guarantees deferred cleanup runs before exit.
func run(ctx context.Context, stdout io.Writer) error {
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger := slog.New(slog.NewJSONHandler(stdout, nil))

	logger.InfoContext(ctx, "server started")
	<-ctx.Done()
	logger.InfoContext(ctx, "server stopped")

	return nil
}
