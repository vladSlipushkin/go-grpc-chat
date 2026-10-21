// Command server runs the chat server.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/vladSlipushkin/go-grpc-chat/internal/app"
	"github.com/vladSlipushkin/go-grpc-chat/internal/auth"
	"github.com/vladSlipushkin/go-grpc-chat/internal/config"
	"github.com/vladSlipushkin/go-grpc-chat/internal/healthcheck"
)

// healthcheckTimeout bounds -healthcheck below the 3 seconds the container's
// health check allows it.
const healthcheckTimeout = 2 * time.Second

func main() {
	if err := run(context.Background(), os.Args[1:], os.LookupEnv, os.Stdout, auth.DefaultBcryptCost); err != nil {
		fmt.Fprintf(os.Stderr, "server: %v\n", err)
		os.Exit(1)
	}
}

// run reads the configuration and serves the chat server (internal/app)
// until ctx is cancelled or the process receives SIGINT/SIGTERM. With
// -healthcheck it instead checks the server already running next to it.
// Keeping it separate from main makes it testable and guarantees deferred
// cleanup runs before exit. bcryptCost is a parameter only so that tests
// can lower it.
func run(ctx context.Context, args []string, lookupEnv config.LookupFunc, stdout io.Writer, bcryptCost int) error {
	fs := flag.NewFlagSet("server", flag.ContinueOnError)
	check := fs.Bool("healthcheck", false, "check that the server on this host is serving, and exit")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if fs.NArg() > 0 {
		return fmt.Errorf("unexpected argument %q", fs.Arg(0))
	}
	if *check {
		return checkHealth(ctx, lookupEnv)
	}

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
		TLSCertFile:     cfg.TLSCertFile,
		TLSKeyFile:      cfg.TLSKeyFile,
	})
}

// checkHealth reads where the server listens from the same environment the
// server reads and asks it whether it is serving.
func checkHealth(ctx context.Context, lookupEnv config.LookupFunc) error {
	hc, err := config.LoadHealthcheck(lookupEnv)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	ctx, cancel := context.WithTimeout(ctx, healthcheckTimeout)
	defer cancel()
	return healthcheck.Check(ctx, hc.Addr, hc.TLSCertFile)
}
