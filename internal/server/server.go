// Package server builds the gRPC server and runs it until shutdown. Transport
// limits and the shutdown sequence are explained in ADR-0006.
package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
)

const (
	// maxRecvMsgSize leaves headroom over the largest valid request (a chat
	// message of 4 KiB plus envelope) while staying far below the gRPC
	// default of 4 MiB.
	maxRecvMsgSize = 16 << 10

	// keepaliveTime is how long a connection may stay silent before the
	// server pings it; keepaliveTimeout is how long it waits for the ack
	// before closing the connection as dead.
	keepaliveTime    = 2 * time.Minute
	keepaliveTimeout = 20 * time.Second

	// minClientPingInterval is the most often a client may ping. Clients that
	// ping more often are disconnected with GOAWAY "too_many_pings".
	minClientPingInterval = time.Minute
)

// Options configures a Server.
type Options struct {
	// Reflection registers the gRPC reflection service.
	Reflection bool
	// ShutdownTimeout bounds graceful shutdown; RPCs still running after it
	// are cancelled.
	ShutdownTimeout time.Duration
}

// Server is a gRPC server with health checking, logging and panic recovery.
// It implements grpc.ServiceRegistrar, so generated Register functions accept
// it directly.
type Server struct {
	grpc            *grpc.Server
	health          *health.Server
	logger          *slog.Logger
	shutdownTimeout time.Duration
}

// New creates a Server. Services are registered with RegisterService before
// Serve is called.
func New(logger *slog.Logger, opts Options) *Server {
	g := grpc.NewServer(
		grpc.MaxRecvMsgSize(maxRecvMsgSize),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    keepaliveTime,
			Timeout: keepaliveTimeout,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             minClientPingInterval,
			PermitWithoutStream: true,
		}),
		// Logging runs outside recovery so a recovered panic is logged with
		// the INTERNAL code the client receives.
		grpc.ChainUnaryInterceptor(unaryLogging(logger), unaryRecovery(logger)),
		grpc.ChainStreamInterceptor(streamLogging(logger), streamRecovery(logger)),
	)

	h := health.NewServer()
	healthpb.RegisterHealthServer(g, h)
	if opts.Reflection {
		reflection.Register(g)
	}

	return &Server{
		grpc:            g,
		health:          h,
		logger:          logger,
		shutdownTimeout: opts.ShutdownTimeout,
	}
}

// RegisterService registers a service implementation. It must be called
// before Serve.
func (s *Server) RegisterService(desc *grpc.ServiceDesc, impl any) {
	s.grpc.RegisterService(desc, impl)
}

// Serve accepts connections on lis until ctx is cancelled, then shuts down:
// health checks report NOT_SERVING, new RPCs are refused, and running RPCs
// get the shutdown timeout to finish before they are cancelled. It returns
// nil after a shutdown and an error if serving fails on its own.
func (s *Server) Serve(ctx context.Context, lis net.Listener) error {
	serveErr := make(chan error, 1)
	go func() { serveErr <- s.grpc.Serve(lis) }()

	select {
	case err := <-serveErr:
		return fmt.Errorf("serve: %w", err)
	case <-ctx.Done():
	}

	s.shutdown(ctx)

	// Serve returns nil once stopped, or ErrServerStopped if the stop won the
	// race against it starting.
	if err := <-serveErr; err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		return fmt.Errorf("serve: %w", err)
	}
	return nil
}

func (s *Server) shutdown(ctx context.Context) {
	// Load balancers watching health stop routing new clients here before
	// the listener closes.
	s.health.Shutdown()
	s.logger.InfoContext(ctx, "shutting down", slog.Duration("timeout", s.shutdownTimeout))

	stopped := make(chan struct{})
	go func() {
		s.grpc.GracefulStop()
		close(stopped)
	}()

	select {
	case <-stopped:
	case <-time.After(s.shutdownTimeout):
		s.logger.WarnContext(ctx, "graceful shutdown timed out, cancelling remaining RPCs")
		s.grpc.Stop()
		<-stopped
	}
}
