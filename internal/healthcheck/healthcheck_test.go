package healthcheck_test

import (
	"context"
	"log/slog"
	"net"
	"testing"
	"time"

	"go.uber.org/goleak"
	"golang.org/x/crypto/bcrypt"

	"github.com/vladSlipushkin/go-grpc-chat/internal/app"
	"github.com/vladSlipushkin/go-grpc-chat/internal/certtest"
	"github.com/vladSlipushkin/go-grpc-chat/internal/healthcheck"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestCheck(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		issue func(testing.TB, ...string) certtest.Files // nil for plaintext
		host  string
	}{
		{name: "plaintext"},
		{name: "self-signed", issue: certtest.SelfSigned, host: "localhost"},
		// Issued for a public name only, and sent with its intermediate:
		// the check still connects locally.
		{name: "issued by a CA", issue: certtest.Issued, host: "chat.example.com"},
		{name: "issued for an address", issue: certtest.Issued, host: "127.0.0.1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var files certtest.Files
			if tt.issue != nil {
				files = tt.issue(t, tt.host)
			}
			addr := serve(t, files)
			if err := healthcheck.Check(ctxFor(t), addr, files.Cert); err != nil {
				t.Errorf("Check() = %v, want nil", err)
			}
		})
	}
}

func TestCheckFails(t *testing.T) {
	t.Parallel()

	tlsFiles := certtest.SelfSigned(t, "localhost")
	tlsAddr := serve(t, tlsFiles)
	other := certtest.SelfSigned(t, "localhost")
	stopped := stoppedAddr(t)

	tests := []struct {
		name     string
		addr     string
		certFile string
	}{
		{name: "plaintext to a TLS server", addr: tlsAddr},
		{name: "another certificate", addr: tlsAddr, certFile: other.Cert},
		{name: "a key instead of a certificate", addr: tlsAddr, certFile: tlsFiles.Key},
		{name: "nothing listening", addr: stopped},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if err := healthcheck.Check(ctxFor(t), tt.addr, tt.certFile); err == nil {
				t.Error("Check() = nil, want an error")
			}
		})
	}
}

// serve runs the chat server on a loopback port, with TLS if files has a
// certificate, until the test ends, and returns its address.
func serve(t *testing.T, files certtest.Files) string {
	t.Helper()

	var lc net.ListenConfig
	lis, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	// Not t.Context(): it is cancelled before cleanups run.
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() {
		served <- app.Serve(ctx, slog.New(slog.DiscardHandler), lis, app.Options{
			JWTSecret:       []byte("healthcheck-secret-0123456789abcdef"),
			TokenTTL:        time.Hour,
			BcryptCost:      bcrypt.MinCost,
			ShutdownTimeout: time.Second,
			TLSCertFile:     files.Cert,
			TLSKeyFile:      files.Key,
		})
	}()
	t.Cleanup(func() {
		cancel()
		if err := <-served; err != nil {
			t.Errorf("Serve() = %v", err)
		}
	})
	return lis.Addr().String()
}

// stoppedAddr returns a loopback address nothing listens on any more. The
// kernel could hand the port to another listener in the meantime, such as a
// parallel test's server; it rarely reuses one at once, and a failure then
// shows as this check passing, not as a hang.
func stoppedAddr(t *testing.T) string {
	t.Helper()

	var lc net.ListenConfig
	lis, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := lis.Addr().String()
	if err := lis.Close(); err != nil {
		t.Fatalf("close listener: %v", err)
	}
	return addr
}

func ctxFor(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}
