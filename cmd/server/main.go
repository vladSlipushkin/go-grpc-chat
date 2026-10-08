// Command server runs the chat server.
package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/vladSlipushkin/go-grpc-chat/internal/config"
	"github.com/vladSlipushkin/go-grpc-chat/internal/server"
)

func main() {
	if err := run(context.Background(), os.LookupEnv, os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "server: %v\n", err)
		os.Exit(1)
	}
}

// run is the composition root: it wires dependencies and serves until ctx is
// cancelled or the process receives SIGINT/SIGTERM. Keeping it separate from
// main makes it testable and guarantees deferred cleanup runs before exit.
func run(ctx context.Context, lookupEnv config.LookupFunc, stdout io.Writer) error {
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load(lookupEnv)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	logger := slog.New(slog.NewJSONHandler(stdout, &slog.HandlerOptions{Level: cfg.LogLevel}))

	var lc net.ListenConfig
	lis, err := lc.Listen(ctx, "tcp", cfg.GRPCAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", cfg.GRPCAddr, err)
	}

	srv := server.New(logger, server.Options{
		Reflection:      cfg.Reflection,
		ShutdownTimeout: cfg.ShutdownTimeout,
	})

	logger.InfoContext(ctx, "server started",
		slog.String("addr", lis.Addr().String()),
		slog.Bool("reflection", cfg.Reflection),
	)
	if err := srv.Serve(ctx, lis); err != nil {
		return err
	}
	logger.InfoContext(ctx, "server stopped")
	return nil
}
