// Package chattest runs the whole chat server, wired as cmd/server wires it,
// over an in-memory listener for tests of its clients, the way
// net/http/httptest does for HTTP.
package chattest

import (
	"context"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"

	authv1 "github.com/vladSlipushkin/go-grpc-chat/gen/auth/v1"
	chatv1 "github.com/vladSlipushkin/go-grpc-chat/gen/chat/v1"
	"github.com/vladSlipushkin/go-grpc-chat/internal/auth"
	"github.com/vladSlipushkin/go-grpc-chat/internal/chat"
	"github.com/vladSlipushkin/go-grpc-chat/internal/grpcapi"
	"github.com/vladSlipushkin/go-grpc-chat/internal/memory"
	"github.com/vladSlipushkin/go-grpc-chat/internal/server"
)

// Target is the address clients dial; DialOptions route it to the server.
const Target = "passthrough:///chattest"

// Options configures Start.
type Options struct {
	// TokenTTL is how long access tokens stay valid; zero means an hour.
	TokenTTL time.Duration
}

// Server is a running chat server. Users and the token secret survive
// Restart, so tokens issued before it stay valid.
type Server struct {
	// tb is the test that started the server; failures after a restart
	// are reported to it, since a subtest that restarted it may be over.
	tb       testing.TB
	tokens   *auth.Tokens
	accounts *auth.Service
	logger   *slog.Logger

	lis  atomic.Pointer[bufconn.Listener]
	mu   sync.Mutex
	stop func()
}

// Start runs a server until the test ends.
func Start(tb testing.TB, opts Options) *Server {
	tb.Helper()

	ttl := opts.TokenTTL
	if ttl == 0 {
		ttl = time.Hour
	}
	tokens, err := auth.NewTokens([]byte("chattest-secret-0123456789abcdef"), ttl)
	if err != nil {
		tb.Fatalf("NewTokens() error = %v", err)
	}
	// The minimum cost keeps registration and login fast (ADR-0002).
	accounts, err := auth.NewService(memory.NewUsers(), tokens, auth.Options{BcryptCost: bcrypt.MinCost})
	if err != nil {
		tb.Fatalf("NewService() error = %v", err)
	}
	s := &Server{tb: tb, tokens: tokens, accounts: accounts, logger: slog.New(slog.DiscardHandler)}
	s.serve()
	tb.Cleanup(s.Stop)
	return s
}

// serve starts a new server on a new listener and makes it the one clients
// dial.
func (s *Server) serve() {
	srv := server.New(s.logger, s.tokens, server.Options{
		ShutdownTimeout: time.Second,
		PublicMethods: []string{
			authv1.AuthService_Register_FullMethodName,
			authv1.AuthService_Login_FullMethodName,
		},
	})
	// Not a test context: it is cancelled before cleanups run.
	ctx, cancel := context.WithCancel(context.Background())
	hub := chat.NewHub(s.logger)
	authv1.RegisterAuthServiceServer(srv, grpcapi.NewAuthServer(s.accounts, s.logger))
	chatv1.RegisterChatServiceServer(srv, grpcapi.NewChatServer(hub, ctx.Done(), s.logger))

	lis := bufconn.Listen(1 << 20)
	served := make(chan error, 1)
	go func() {
		err := srv.Serve(ctx, lis)
		hub.Close()
		served <- err
	}()
	s.lis.Store(lis)

	s.mu.Lock()
	defer s.mu.Unlock()
	s.stop = sync.OnceFunc(func() {
		cancel()
		if err := <-served; err != nil {
			s.tb.Errorf("Serve() = %v", err)
		}
	})
}

// DialOptions route Target to the running server, across restarts.
func (s *Server) DialOptions() []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return s.lis.Load().DialContext(ctx)
		}),
	}
}

// Restart shuts the server down, which ends open chat streams with
// UNAVAILABLE, and starts a new one with empty rooms.
func (s *Server) Restart() {
	s.Stop()
	s.serve()
}

// Stop shuts the server down and waits for it. Further calls do nothing.
func (s *Server) Stop() {
	s.mu.Lock()
	stop := s.stop
	s.mu.Unlock()
	stop()
}

// Register creates an account.
func (s *Server) Register(tb testing.TB, username, password string) {
	tb.Helper()
	if _, err := s.accounts.Register(tb.Context(), username, password); err != nil {
		tb.Fatalf("Register(%q) error = %v", username, err)
	}
}
