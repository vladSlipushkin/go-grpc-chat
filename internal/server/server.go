// Package server builds the gRPC server and runs it until shutdown. Transport
// limits and the shutdown sequence are explained in ADR-0006.
package server

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
	reflectionpb "google.golang.org/grpc/reflection/grpc_reflection_v1"
	reflectionv1alphapb "google.golang.org/grpc/reflection/grpc_reflection_v1alpha"
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

	// maxConcurrentStreams caps the RPCs, chat streams included, that one
	// connection can have open at once. 100 is the lowest value RFC 9113
	// recommends; grpc-go's default is unlimited.
	maxConcurrentStreams = 100
)

// Options configures a Server.
type Options struct {
	// Reflection registers the gRPC reflection service.
	Reflection bool
	// ShutdownTimeout bounds graceful shutdown; RPCs still running after it
	// are cancelled. Zero cancels running RPCs as soon as shutdown starts.
	ShutdownTimeout time.Duration
	// PublicMethods are full method names callable without an access
	// token, such as "/auth.v1.AuthService/Login". Health checking and
	// reflection are always public.
	PublicMethods []string
	// TLS, if set, makes the server accept only TLS connections.
	TLS *tls.Config
	// OnShutdown, if set, is called once shutdown starts, whatever started
	// it, before running RPCs are waited for. Services whose RPCs do not
	// end on their own, such as chat streams, end them here (ADR-0006).
	OnShutdown func()
}

// Server is a gRPC server with health checking, logging, panic recovery,
// authentication and request validation.
// It implements grpc.ServiceRegistrar, so generated Register functions accept
// it directly.
type Server struct {
	grpc            *grpc.Server
	health          *health.Server
	logger          *slog.Logger
	shutdownTimeout time.Duration
	onShutdown      func()
}

var _ grpc.ServiceRegistrar = (*Server)(nil)

// New creates a Server that checks access tokens with verifier. Services are
// registered with RegisterService before Serve is called.
func New(logger *slog.Logger, verifier TokenVerifier, opts Options) *Server {
	public := publicMethods{
		healthpb.Health_Check_FullMethodName:                                     true,
		healthpb.Health_List_FullMethodName:                                      true,
		healthpb.Health_Watch_FullMethodName:                                     true,
		reflectionpb.ServerReflection_ServerReflectionInfo_FullMethodName:        true,
		reflectionv1alphapb.ServerReflection_ServerReflectionInfo_FullMethodName: true,
	}
	for _, name := range opts.PublicMethods {
		public[name] = true
	}

	serverOpts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(maxRecvMsgSize),
		grpc.MaxConcurrentStreams(maxConcurrentStreams),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    keepaliveTime,
			Timeout: keepaliveTimeout,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             minClientPingInterval,
			PermitWithoutStream: true,
		}),
		// The first interceptor is the outermost. Logging runs outside
		// recovery so a recovered panic is logged with the INTERNAL code the
		// client receives, and outside auth so rejected calls are logged
		// too. Validation runs after auth, so unauthenticated callers learn
		// nothing about request rules of protected methods (ADR-0007).
		grpc.ChainUnaryInterceptor(
			unaryLogging(logger),
			unaryRecovery(logger),
			unaryAuth(verifier, public),
			unaryValidation(logger),
		),
		grpc.ChainStreamInterceptor(
			streamLogging(logger),
			streamRecovery(logger),
			streamAuth(verifier, public),
		),
	}
	if opts.TLS != nil {
		serverOpts = append(serverOpts, grpc.Creds(credentials.NewTLS(opts.TLS)))
	}
	g := grpc.NewServer(serverOpts...)

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
		onShutdown:      opts.OnShutdown,
	}
}

// RegisterService registers a service implementation. It must be called
// before Serve.
func (s *Server) RegisterService(desc *grpc.ServiceDesc, impl any) {
	s.grpc.RegisterService(desc, impl)
}

// Serve accepts connections on lis until ctx is cancelled, then shuts down:
// health checks report NOT_SERVING, new RPCs are refused, and running RPCs
// get the shutdown timeout to finish before they are cancelled. If accepting
// connections fails on its own, Serve shuts down the same way and returns the
// error. Either way, Serve returns only after every handler has returned, so
// handlers must return once their context is cancelled.
func (s *Server) Serve(ctx context.Context, lis net.Listener) error {
	serveErr := make(chan error, 1)
	go func() { serveErr <- s.grpc.Serve(lis) }()

	// Shutdown runs after ctx is cancelled; it only needs ctx's values.
	shutdownCtx := context.WithoutCancel(ctx)

	select {
	case err := <-serveErr:
		// grpc.Server.Serve closes only the listener; connections it already
		// accepted keep serving until the server is stopped.
		s.shutdown(shutdownCtx)
		return fmt.Errorf("serve: %w", err)
	case <-ctx.Done():
	}

	s.shutdown(shutdownCtx)

	// Serve returns nil once stopped, or ErrServerStopped if the stop won the
	// race against it starting.
	if err := <-serveErr; err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		return fmt.Errorf("serve: %w", err)
	}
	return nil
}

func (s *Server) shutdown(ctx context.Context) {
	// Health watchers learn first. Clients polling Check see the listener
	// close at about the same moment; there is no drain delay.
	s.health.Shutdown()
	s.logger.InfoContext(ctx, "shutting down", slog.Duration("timeout", s.shutdownTimeout))
	if s.onShutdown != nil {
		s.onShutdown()
	}

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
		// GracefulStop returns only after every handler has returned, even
		// once Stop has cancelled them. Waiting here keeps that guarantee for
		// callers of Serve; Stop alone does not wait for handlers.
		<-stopped
	}
}
