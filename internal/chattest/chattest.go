// Package chattest runs the chat server that cmd/server runs (internal/app)
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
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	authv1 "github.com/vladSlipushkin/go-grpc-chat/gen/auth/v1"
	"github.com/vladSlipushkin/go-grpc-chat/internal/app"
)

// Target is the address clients dial; DialOptions route it to the server.
const Target = "passthrough:///chattest"

// Options configures Start.
type Options struct {
	// TokenTTL is how long access tokens stay valid; zero means an hour.
	TokenTTL time.Duration
}

// Server is a running chat server. The token secret survives Restart, so
// tokens issued before it stay valid; accounts do not, as with cmd/server
// until accounts are stored.
type Server struct {
	// tb is the test that started the server; failures after a restart
	// are reported to it, since a subtest that restarted it may be over.
	tb   testing.TB
	opts app.Options

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
	s := &Server{tb: tb, opts: app.Options{
		JWTSecret: []byte("chattest-secret-0123456789abcdef"),
		TokenTTL:  ttl,
		// The minimum cost keeps registration and login fast (ADR-0002).
		BcryptCost:      bcrypt.MinCost,
		ShutdownTimeout: time.Second,
	}}
	s.serve()
	tb.Cleanup(s.Stop)
	return s
}

// serve starts a new server on a new listener and makes it the one clients
// dial.
func (s *Server) serve() {
	// Not a test context: it is cancelled before cleanups run.
	ctx, cancel := context.WithCancel(context.Background())
	lis := bufconn.Listen(1 << 20)
	served := make(chan error, 1)
	go func() { served <- app.Serve(ctx, slog.New(slog.DiscardHandler), lis, s.opts) }()
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
// UNAVAILABLE, and starts a new one with empty rooms and no accounts.
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

// Register creates an account through the server's API.
func (s *Server) Register(tb testing.TB, username, password string) {
	tb.Helper()

	opts := append(s.DialOptions(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	conn, err := grpc.NewClient(Target, opts...)
	if err != nil {
		tb.Fatalf("NewClient() error = %v", err)
	}
	defer conn.Close()
	req := &authv1.RegisterRequest{Username: username, Password: password}
	if _, err := authv1.NewAuthServiceClient(conn).Register(tb.Context(), req); err != nil {
		tb.Fatalf("Register(%q) error = %v", username, err)
	}
}
