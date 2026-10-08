package server_test

import (
	"context"
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

	"github.com/vladSlipushkin/go-grpc-chat/internal/server"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestHealthReportsServing(t *testing.T) {
	t.Parallel()

	ts := startServer(t, server.Options{ShutdownTimeout: time.Second})

	resp, err := healthpb.NewHealthClient(ts.conn).Check(t.Context(), &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("Check() error = %v", err)
	}
	if got := resp.GetStatus(); got != healthpb.HealthCheckResponse_SERVING {
		t.Errorf("Check() status = %v, want SERVING", got)
	}
}

func TestRejectsOversizedRequests(t *testing.T) {
	t.Parallel()

	ts := startServer(t, server.Options{ShutdownTimeout: time.Second})

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

			ts := startServer(t, server.Options{Reflection: tt.reflection, ShutdownTimeout: time.Second})
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

func TestShutdownWithoutRunningRPCsIsImmediate(t *testing.T) {
	t.Parallel()

	ts := startServer(t, server.Options{ShutdownTimeout: time.Minute})

	start := time.Now()
	if err := ts.stop(); err != nil {
		t.Fatalf("Serve() = %v, want nil", err)
	}
	// Far below the timeout: nothing was running, so nothing was waited for.
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("shutdown took %v with no RPCs running", elapsed)
	}
}

func TestShutdownNotifiesWatchersThenCancelsLongRPCs(t *testing.T) {
	t.Parallel()

	ts := startServer(t, server.Options{ShutdownTimeout: 200 * time.Millisecond})

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
	if _, err := watch.Recv(); err == nil {
		t.Fatal("Watch kept running after the shutdown timeout, want it cancelled")
	}
	if err := <-stopped; err != nil {
		t.Errorf("Serve() = %v, want nil", err)
	}
}

func TestServeFailsWhenListenerFails(t *testing.T) {
	t.Parallel()

	lis := bufconn.Listen(1)
	if err := lis.Close(); err != nil {
		t.Fatalf("close listener: %v", err)
	}
	srv := server.New(slog.New(slog.DiscardHandler), server.Options{ShutdownTimeout: time.Second})

	if err := srv.Serve(t.Context(), lis); err == nil {
		t.Error("Serve() on a closed listener = nil, want an error")
	}
}

type testServer struct {
	conn *grpc.ClientConn
	stop func() error
}

// startServer runs a Server over an in-memory listener. stop cancels it and
// waits for Serve to return; it is also called on cleanup.
func startServer(t *testing.T, opts server.Options) testServer {
	t.Helper()

	lis := bufconn.Listen(1 << 20)
	srv := server.New(slog.New(slog.DiscardHandler), opts)

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
		if err := stop(); err != nil {
			t.Errorf("Serve() = %v", err)
		}
	})
	return testServer{conn: conn, stop: stop}
}

func listServices(ctx context.Context, conn *grpc.ClientConn) ([]string, error) {
	stream, err := reflectionpb.NewServerReflectionClient(conn).ServerReflectionInfo(ctx)
	if err != nil {
		return nil, err
	}
	req := &reflectionpb.ServerReflectionRequest{
		MessageRequest: &reflectionpb.ServerReflectionRequest_ListServices{},
	}
	if err := stream.Send(req); err != nil {
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
