// Package app assembles the chat server: accounts, access tokens, the hub
// and both services on one gRPC server. cmd/server runs it with the
// process's configuration and internal/chattest with test settings, so
// client tests run against the server that ships.
package app

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"time"

	authv1 "github.com/vladSlipushkin/go-grpc-chat/gen/auth/v1"
	chatv1 "github.com/vladSlipushkin/go-grpc-chat/gen/chat/v1"
	"github.com/vladSlipushkin/go-grpc-chat/internal/auth"
	"github.com/vladSlipushkin/go-grpc-chat/internal/chat"
	"github.com/vladSlipushkin/go-grpc-chat/internal/grpcapi"
	"github.com/vladSlipushkin/go-grpc-chat/internal/memory"
	"github.com/vladSlipushkin/go-grpc-chat/internal/server"
)

// Options configures Serve.
type Options struct {
	// JWTSecret signs and verifies access tokens.
	JWTSecret []byte
	// TokenTTL is how long an access token stays valid.
	TokenTTL time.Duration
	// BcryptCost is the cost of password hashes; tests lower it.
	BcryptCost int
	// Reflection registers the gRPC reflection service.
	Reflection bool
	// ShutdownTimeout bounds graceful shutdown (ADR-0006).
	ShutdownTimeout time.Duration
	// TLSCertFile and TLSKeyFile, if set, make the server serve TLS 1.3
	// with that certificate chain and key (ADR-0012).
	TLSCertFile, TLSKeyFile string
}

// Serve runs the chat server on lis until ctx is cancelled, then shuts it
// down and returns once every handler has. It closes lis, also when it
// fails to start. Accounts are kept in memory and end with it.
func Serve(ctx context.Context, logger *slog.Logger, lis net.Listener, opts Options) error {
	// Interceptors built by New take each call's context from the call itself.
	srv, hub, err := build(logger, opts) //nolint:contextcheck // false positive, see above
	if err != nil {
		_ = lis.Close()
		return err
	}
	// Runs after Serve, which waits for every handler to close its session,
	// so only rooms nobody holds can be left (ADR-0008).
	defer hub.Close()

	logger.InfoContext(ctx, "server started",
		slog.String("addr", lis.Addr().String()),
		slog.Bool("reflection", opts.Reflection),
		slog.Bool("tls", opts.TLSCertFile != ""),
	)
	if err := srv.Serve(ctx, lis); err != nil {
		return err
	}
	logger.InfoContext(ctx, "server stopped")
	return nil
}

// build creates the server with every service registered.
func build(logger *slog.Logger, opts Options) (*server.Server, *chat.Hub, error) {
	tokens, err := auth.NewTokens(opts.JWTSecret, opts.TokenTTL)
	if err != nil {
		return nil, nil, fmt.Errorf("create token issuer: %w", err)
	}
	accounts, err := auth.NewService(memory.NewUsers(), tokens, auth.Options{BcryptCost: opts.BcryptCost})
	if err != nil {
		return nil, nil, fmt.Errorf("create auth service: %w", err)
	}
	var tlsConfig *tls.Config
	if opts.TLSCertFile != "" {
		cert, err := tls.LoadX509KeyPair(opts.TLSCertFile, opts.TLSKeyFile)
		if err != nil {
			return nil, nil, fmt.Errorf("load TLS certificate: %w", err)
		}
		tlsConfig = &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS13}
	}

	// Chat streams end as soon as shutdown starts, whether a signal or a
	// failing listener started it (ADR-0006).
	shutdown := make(chan struct{})
	srv := server.New(logger, tokens, server.Options{
		Reflection:      opts.Reflection,
		ShutdownTimeout: opts.ShutdownTimeout,
		PublicMethods: []string{
			authv1.AuthService_Register_FullMethodName,
			authv1.AuthService_Login_FullMethodName,
		},
		TLS:        tlsConfig,
		OnShutdown: func() { close(shutdown) },
	})
	hub := chat.NewHub(logger)
	// Every service is registered here, before Serve (ADR-0006).
	authv1.RegisterAuthServiceServer(srv, grpcapi.NewAuthServer(accounts, logger))
	chatv1.RegisterChatServiceServer(srv, grpcapi.NewChatServer(hub, shutdown, logger))
	return srv, hub, nil
}
