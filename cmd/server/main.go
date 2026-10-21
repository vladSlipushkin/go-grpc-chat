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

	"github.com/vladSlipushkin/go-grpc-chat/internal/app"
	"github.com/vladSlipushkin/go-grpc-chat/internal/auth"
	"github.com/vladSlipushkin/go-grpc-chat/internal/config"
)

func main() {
	if err := run(context.Background(), os.LookupEnv, os.Stdout, auth.DefaultBcryptCost); err != nil {
		fmt.Fprintf(os.Stderr, "server: %v\n", err)
		os.Exit(1)
	}
}

// run reads the configuration and serves the chat server (internal/app)
// until ctx is cancelled or the process receives SIGINT/SIGTERM. Keeping it
// separate from main makes it testable and guarantees deferred cleanup runs
// before exit. bcryptCost is a parameter only so that tests can lower it.
func run(ctx context.Context, lookupEnv config.LookupFunc, stdout io.Writer, bcryptCost int) error {
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()
	// After the first signal, restore default handling so a second one kills
	// the process if graceful shutdown hangs.
	context.AfterFunc(ctx, stop)

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
	return app.Serve(ctx, logger, lis, app.Options{
		JWTSecret:       cfg.JWTSecret,
		TokenTTL:        cfg.TokenTTL,
		BcryptCost:      bcryptCost,
		Reflection:      cfg.Reflection,
		ShutdownTimeout: cfg.ShutdownTimeout,
	})
}
