package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	reflectionpb "google.golang.org/grpc/reflection/grpc_reflection_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/vladSlipushkin/go-grpc-chat/internal/server"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestHealthReportsServing(t *testing.T) {
	t.Parallel()

	ts := startServer(t, newServer(server.Options{ShutdownTimeout: time.Second}))

	if got := checkHealth(t, ts.conn); got != healthpb.HealthCheckResponse_SERVING {
		t.Errorf("Check() status = %v, want SERVING", got)
	}
}

func TestRejectsOversizedRequests(t *testing.T) {
	t.Parallel()

	ts := startServer(t, newServer(server.Options{ShutdownTimeout: time.Second}))

	req := &healthpb.HealthCheckRequest{Service: strings.Repeat("a", 17<<10)}
	_, err := healthpb.NewHealthClient(ts.conn).Check(t.Context(), req)
	if got := status.Code(err); got != codes.ResourceExhausted {
		t.Errorf("Check() with a 17 KiB request: code = %v, want %v", got, codes.ResourceExhausted)
	}
}

func TestReflectionFollowsOption(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		reflection bool
		wantCode   codes.Code
	}{
		{name: "enabled", reflection: true, wantCode: codes.OK},
		{name: "disabled", reflection: false, wantCode: codes.Unimplemented},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ts := startServer(t, newServer(server.Options{Reflection: tt.reflection, ShutdownTimeout: time.Second}))
			services, err := listServices(t.Context(), ts.conn)
			if got := status.Code(err); got != tt.wantCode {
				t.Fatalf("list services: code = %v, want %v (err = %v)", got, tt.wantCode, err)
			}
			if tt.reflection && !slices.Contains(services, healthpb.Health_ServiceDesc.ServiceName) {
				t.Errorf("services = %v, want them to include %s", services, healthpb.Health_ServiceDesc.ServiceName)
			}
		})
	}
}

func TestPanicInRegisteredServiceIsRecoveredAndLogged(t *testing.T) {
	t.Parallel()

	var logs lockedBuffer
	srv := server.New(slog.New(slog.NewJSONHandler(&logs, nil)), server.Options{ShutdownTimeout: time.Second})
	srv.RegisterService(&panicServiceDesc, struct{}{})
	ts := startServer(t, srv)

	err := ts.conn.Invoke(t.Context(), "/test.v1.Panic/Boom", &emptypb.Empty{}, &emptypb.Empty{})
	if got := status.Code(err); got != codes.Internal {
		t.Fatalf("Invoke() code = %v, want %v", got, codes.Internal)
	}

	// The log record proves the interceptor order: logging sees the INTERNAL
	// code that recovery produced, not the panic.
	var finished map[string]any
	for _, rec := range logs.records(t) {
		if rec["msg"] == "rpc finished" && rec["method"] == "/test.v1.Panic/Boom" {
			finished = rec
		}
	}
	if finished["code"] != "Internal" || finished["level"] != "ERROR" {
		t.Errorf("rpc finished record = %v, want code Internal at level ERROR", finished)
	}
}

func TestShutdownWithIdleConnectionIsImmediate(t *testing.T) {
	t.Parallel()

	ts := startServer(t, newServer(server.Options{ShutdownTimeout: time.Minute}))
	checkHealth(t, ts.conn) // establish a connection that then sits idle

	start := time.Now()
	if err := ts.stop(); err != nil {
		t.Fatalf("Serve() = %v, want nil", err)
	}
	// Far below the timeout: an idle connection is closed, not waited for.
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("shutdown took %v with only an idle connection", elapsed)
	}
}

func TestShutdownNotifiesWatchersThenCancelsLongRPCs(t *testing.T) {
	t.Parallel()

	ts := startServer(t, newServer(server.Options{ShutdownTimeout: 200 * time.Millisecond}))

	// A health watch never ends on its own, standing in for a chat stream.
	watch, err := healthpb.NewHealthClient(ts.conn).Watch(t.Context(), &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("Watch() error = %v", err)
	}
	if resp, err := watch.Recv(); err != nil || resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("first Watch update = (%v, %v), want SERVING", resp, err)
	}

	stopped := make(chan error, 1)
	go func() { stopped <- ts.stop() }()

	if resp, err := watch.Recv(); err != nil || resp.GetStatus() != healthpb.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("Watch update after shutdown = (%v, %v), want NOT_SERVING", resp, err)
	}
	if _, err := watch.Recv(); status.Code(err) != codes.Unavailable {
		t.Fatalf("Watch after the shutdown timeout: err = %v, want UNAVAILABLE", err)
	}
	if err := <-stopped; err != nil {
		t.Errorf("Serve() = %v, want nil", err)
	}
}

func TestListenerFailureShutsDownAcceptedConnections(t *testing.T) {
	t.Parallel()

	ts := startServer(t, newServer(server.Options{ShutdownTimeout: 200 * time.Millisecond}))

	watch, err := healthpb.NewHealthClient(ts.conn).Watch(t.Context(), &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("Watch() error = %v", err)
	}
	if _, err := watch.Recv(); err != nil {
		t.Fatalf("first Watch update: %v", err)
	}

	// Accept now fails, but the connection above was accepted earlier.
	if err := ts.lis.Close(); err != nil {
		t.Fatalf("close listener: %v", err)
	}

	if resp, err := watch.Recv(); err != nil || resp.GetStatus() != healthpb.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("Watch update after listener failure = (%v, %v), want NOT_SERVING", resp, err)
	}
	if err := ts.stop(); err == nil {
		t.Error("Serve() = nil after the listener failed, want an error")
	}
}

func TestServeFailsWhenListenerIsClosed(t *testing.T) {
	t.Parallel()

	lis := bufconn.Listen(1)
	if err := lis.Close(); err != nil {
		t.Fatalf("close listener: %v", err)
	}

	if err := newServer(server.Options{ShutdownTimeout: time.Second}).Serve(t.Context(), lis); err == nil {
		t.Error("Serve() on a closed listener = nil, want an error")
	}
}

// panicServiceDesc describes a one-method service whose handler panics, to
// exercise the interceptor chain through a real call.
var panicServiceDesc = grpc.ServiceDesc{
	ServiceName: "test.v1.Panic",
	HandlerType: (*any)(nil),
	Methods: []grpc.MethodDesc{{
		MethodName: "Boom",
		Handler: func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
			req := new(emptypb.Empty)
			if err := dec(req); err != nil {
				return nil, err
			}
			info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/test.v1.Panic/Boom"}
			return interceptor(ctx, req, info, func(context.Context, any) (any, error) { panic("boom") })
		},
	}},
}

type testServer struct {
	conn *grpc.ClientConn
	lis  *bufconn.Listener
	stop func() error
}

func newServer(opts server.Options) *server.Server {
	return server.New(slog.New(slog.DiscardHandler), opts)
}

// startServer serves srv over an in-memory listener. stop cancels it and
// returns what Serve returned; cleanup also calls it, so every test ends with
// the server stopped and its goroutines gone.
func startServer(t *testing.T, srv *server.Server) testServer {
	t.Helper()

	lis := bufconn.Listen(1 << 20)
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() { served <- srv.Serve(ctx, lis) }()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		cancel()
		t.Fatalf("NewClient() error = %v", err)
	}

	stop := sync.OnceValue(func() error {
		cancel()
		return <-served
	})
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Errorf("close client: %v", err)
		}
		_ = stop() // tests that care about the result check it themselves
	})
	return testServer{conn: conn, lis: lis, stop: stop}
}

func checkHealth(t *testing.T, conn *grpc.ClientConn) healthpb.HealthCheckResponse_ServingStatus {
	t.Helper()
	resp, err := healthpb.NewHealthClient(conn).Check(t.Context(), &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("Check() error = %v", err)
	}
	return resp.GetStatus()
}

func listServices(ctx context.Context, conn *grpc.ClientConn) ([]string, error) {
	stream, err := reflectionpb.NewServerReflectionClient(conn).ServerReflectionInfo(ctx)
	if err != nil {
		return nil, err
	}
	req := &reflectionpb.ServerReflectionRequest{
		MessageRequest: &reflectionpb.ServerReflectionRequest_ListServices{},
	}
	// Send returns io.EOF when the server has already ended the stream, for
	// example with UNIMPLEMENTED; the status itself comes from Recv.
	if err := stream.Send(req); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	resp, err := stream.Recv()
	if err != nil {
		return nil, err
	}
	if err := stream.CloseSend(); err != nil {
		return nil, err
	}
	var names []string
	for _, s := range resp.GetListServicesResponse().GetService() {
		names = append(names, s.GetName())
	}
	return names, nil
}

// lockedBuffer collects log output written by server goroutines.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) records(t *testing.T) []map[string]any {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()

	var recs []map[string]any
	for line := range bytes.Lines(b.buf.Bytes()) {
		var rec map[string]any
		if err := json.Unmarshal(line, &rec); err != nil {
			t.Fatalf("decode log record %q: %v", line, err)
		}
		recs = append(recs, rec)
	}
	return recs
}
