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

	authv1 "github.com/vladSlipushkin/go-grpc-chat/gen/auth/v1"
	chatv1 "github.com/vladSlipushkin/go-grpc-chat/gen/chat/v1"
	"github.com/vladSlipushkin/go-grpc-chat/internal/auth"
	"github.com/vladSlipushkin/go-grpc-chat/internal/chat"
	"github.com/vladSlipushkin/go-grpc-chat/internal/config"
	"github.com/vladSlipushkin/go-grpc-chat/internal/grpcapi"
	"github.com/vladSlipushkin/go-grpc-chat/internal/memory"
	"github.com/vladSlipushkin/go-grpc-chat/internal/server"
)

func main() {
	if err := run(context.Background(), os.LookupEnv, os.Stdout, auth.DefaultBcryptCost); err != nil {
		fmt.Fprintf(os.Stderr, "server: %v\n", err)
		os.Exit(1)
	}
}

// run is the composition root: it wires dependencies and serves until ctx is
// cancelled or the process receives SIGINT/SIGTERM. Keeping it separate from
// main makes it testable and guarantees deferred cleanup runs before exit.
// bcryptCost is a parameter only so that tests can lower it.
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

	tokens, err := auth.NewTokens(cfg.JWTSecret, cfg.TokenTTL)
	if err != nil {
		return fmt.Errorf("create token issuer: %w", err)
	}
	accounts, err := auth.NewService(memory.NewUsers(), tokens, auth.Options{BcryptCost: bcryptCost})
	if err != nil {
		return fmt.Errorf("create auth service: %w", err)
	}

	var lc net.ListenConfig
	lis, err := lc.Listen(ctx, "tcp", cfg.GRPCAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", cfg.GRPCAddr, err)
	}

	// Interceptors built by New take each call's context from the call itself.
	srv := server.New(logger, tokens, server.Options{ //nolint:contextcheck // false positive, see above
		Reflection:      cfg.Reflection,
		ShutdownTimeout: cfg.ShutdownTimeout,
		PublicMethods: []string{
			authv1.AuthService_Register_FullMethodName,
			authv1.AuthService_Login_FullMethodName,
		},
	})
	hub := chat.NewHub(logger)
	// Every service is registered here, before Serve (ADR-0006).
	authv1.RegisterAuthServiceServer(srv, grpcapi.NewAuthServer(accounts, logger))
	chatv1.RegisterChatServiceServer(srv, grpcapi.NewChatServer(hub, ctx.Done(), logger))

	logger.InfoContext(ctx, "server started",
		slog.String("addr", lis.Addr().String()),
		slog.Bool("reflection", cfg.Reflection),
	)
	serveErr := srv.Serve(ctx, lis)
	// Serve has waited for every handler, and each closed its session, so
	// only rooms nobody holds can be left (ADR-0008).
	hub.Close()
	if serveErr != nil {
		return serveErr
	}
	logger.InfoContext(ctx, "server stopped")
	return nil
}
