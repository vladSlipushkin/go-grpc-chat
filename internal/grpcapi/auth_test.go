package grpcapi_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"

	authv1 "github.com/vladSlipushkin/go-grpc-chat/gen/auth/v1"
	"github.com/vladSlipushkin/go-grpc-chat/internal/auth"
	"github.com/vladSlipushkin/go-grpc-chat/internal/grpcapi"
	"github.com/vladSlipushkin/go-grpc-chat/internal/memory"
	"github.com/vladSlipushkin/go-grpc-chat/internal/server"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

const (
	username = "alice"
	password = "correct horse battery"
)

func TestRegisterLoginAndCallProtectedMethods(t *testing.T) {
	t.Parallel()

	env := startServer(t)
	client := authv1.NewAuthServiceClient(env.conn)

	reg, err := client.Register(t.Context(), &authv1.RegisterRequest{Username: username, Password: password})
	if err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if reg.GetUserId() == "" {
		t.Fatal("Register() returned an empty user id")
	}
	login, err := client.Login(t.Context(), &authv1.LoginRequest{Username: username, Password: password})
	if err != nil {
		t.Fatalf("Login() error = %v", err)
	}
	if ttl := time.Until(login.GetExpiresAt().AsTime()); ttl <= 0 || ttl > time.Hour {
		t.Errorf("Login() expires in %v, want within an hour", ttl)
	}

	tests := []struct {
		name     string
		md       metadata.MD
		wantCode codes.Code
	}{
		{name: "with token", md: metadata.Pairs("authorization", "Bearer "+login.GetAccessToken()), wantCode: codes.OK},
		{name: "without token", md: metadata.MD{}, wantCode: codes.Unauthenticated},
		{name: "with forged token", md: metadata.Pairs("authorization", "Bearer "+login.GetAccessToken()+"x"), wantCode: codes.Unauthenticated},
	}
	for _, tt := range tests {
		// Sequential: the check after the loop needs every call finished.
		t.Run(tt.name, func(t *testing.T) {
			ctx := metadata.NewOutgoingContext(t.Context(), tt.md)

			var whoami emptypb.Empty
			err := env.conn.Invoke(ctx, "/test.v1.Private/WhoAmI", &emptypb.Empty{}, &whoami)
			if code := status.Code(err); code != tt.wantCode {
				t.Errorf("unary call: code = %v, want %v (err = %v)", code, tt.wantCode, err)
			}

			if code := status.Code(callStream(ctx, env.conn)); code != tt.wantCode {
				t.Errorf("stream: code = %v, want %v", code, tt.wantCode)
			}
		})
	}

	if got := env.private.lastUserID(); got != reg.GetUserId() {
		t.Errorf("protected handler saw user %q, want %q", got, reg.GetUserId())
	}
}

func TestAuthErrors(t *testing.T) {
	t.Parallel()

	env := startServer(t)
	client := authv1.NewAuthServiceClient(env.conn)
	if _, err := client.Register(t.Context(), &authv1.RegisterRequest{Username: username, Password: password}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	_, err := client.Register(t.Context(), &authv1.RegisterRequest{Username: username, Password: "another password"})
	if code := status.Code(err); code != codes.AlreadyExists {
		t.Errorf("Register() with a taken name: code = %v, want %v", code, codes.AlreadyExists)
	}

	logins := []struct {
		name string
		req  *authv1.LoginRequest
	}{
		{name: "wrong password", req: &authv1.LoginRequest{Username: username, Password: "wrong password"}},
		{name: "unknown user", req: &authv1.LoginRequest{Username: "nobody", Password: password}},
	}
	for _, tt := range logins {
		t.Run(tt.name, func(t *testing.T) {
			_, err := client.Login(t.Context(), tt.req)
			if st := status.Convert(err); st.Code() != codes.Unauthenticated || st.Message() != "invalid username or password" {
				t.Errorf("Login() = %v, want UNAUTHENTICATED with the shared message", err)
			}
		})
	}

	_, err = client.Register(t.Context(), &authv1.RegisterRequest{Username: "Bad Name", Password: password})
	if code := status.Code(err); code != codes.InvalidArgument {
		t.Errorf("Register() with an invalid name: code = %v, want %v", code, codes.InvalidArgument)
	}

	// Fits the contract as sent, exceeds 72 bytes after NFKC (ADR-0005).
	expanding := strings.Repeat("\ufdfa", 3) + "password"
	_, err = client.Register(t.Context(), &authv1.RegisterRequest{Username: "bob", Password: expanding})
	st := status.Convert(err)
	if st.Code() != codes.InvalidArgument || len(st.Details()) != 1 {
		t.Fatalf("Register() with an expanding password = %v, want INVALID_ARGUMENT with one detail", err)
	}
	br, ok := st.Details()[0].(*errdetails.BadRequest)
	if !ok || len(br.GetFieldViolations()) != 1 || br.GetFieldViolations()[0].GetField() != "password" {
		t.Errorf("Register() details = %v, want a BadRequest on password", st.Details())
	}
}

// fakeAccounts fails every call with err.
type fakeAccounts struct{ err error }

func (a fakeAccounts) Register(context.Context, string, string) (auth.User, error) {
	return auth.User{}, a.err
}

func (a fakeAccounts) Login(context.Context, string, string) (auth.Token, error) {
	return auth.Token{}, a.err
}

func TestContextErrors(t *testing.T) {
	t.Parallel()

	cancelled := func(t *testing.T) context.Context {
		t.Helper()
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		return ctx
	}
	expired := func(t *testing.T) context.Context {
		t.Helper()
		ctx, cancel := context.WithDeadline(t.Context(), time.Unix(0, 0))
		t.Cleanup(cancel)
		return ctx
	}
	live := func(t *testing.T) context.Context {
		t.Helper()
		return t.Context()
	}

	tests := []struct {
		name    string
		ctx     func(t *testing.T) context.Context
		err     error
		want    codes.Code
		wantLog bool
	}{
		{name: "caller cancelled", ctx: cancelled, err: context.Canceled, want: codes.Canceled},
		{name: "caller deadline", ctx: expired, err: fmt.Errorf("find user: %w", context.DeadlineExceeded), want: codes.DeadlineExceeded},
		{
			name:    "server timeout on a live call",
			ctx:     live,
			err:     fmt.Errorf("find user: query users: %w", context.DeadlineExceeded),
			want:    codes.Internal,
			wantLog: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var logs bytes.Buffer
			srv := grpcapi.NewAuthServer(fakeAccounts{err: tt.err}, slog.New(slog.NewJSONHandler(&logs, nil)))
			ctx := tt.ctx(t)

			_, err := srv.Register(ctx, &authv1.RegisterRequest{Username: username, Password: password})
			if code := status.Code(err); code != tt.want {
				t.Errorf("Register() code = %v, want %v", code, tt.want)
			}
			_, err = srv.Login(ctx, &authv1.LoginRequest{Username: username, Password: password})
			if st := status.Convert(err); st.Code() != tt.want || strings.Contains(st.Message(), "find user") {
				t.Errorf("Login() = %v, want code %v without internal error text", err, tt.want)
			}
			if logged := logs.Len() != 0; logged != tt.wantLog {
				t.Errorf("logged %q, want a record: %v", logs.String(), tt.wantLog)
			}
		})
	}
}

func TestSecretsStayOutOfLogs(t *testing.T) {
	t.Parallel()

	env := startServer(t)
	client := authv1.NewAuthServiceClient(env.conn)

	if _, err := client.Register(t.Context(), &authv1.RegisterRequest{Username: username, Password: password}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	_, _ = client.Login(t.Context(), &authv1.LoginRequest{Username: username, Password: "wrong " + password})
	login, err := client.Login(t.Context(), &authv1.LoginRequest{Username: username, Password: password})
	if err != nil {
		t.Fatalf("Login() error = %v", err)
	}
	token := login.GetAccessToken()
	for _, md := range []metadata.MD{
		metadata.Pairs("authorization", "Bearer "+token),
		metadata.Pairs("authorization", "Bearer "+token+"x"),
	} {
		ctx := metadata.NewOutgoingContext(t.Context(), md)
		_ = env.conn.Invoke(ctx, "/test.v1.Private/WhoAmI", &emptypb.Empty{}, &emptypb.Empty{})
		_ = callStream(ctx, env.conn)
	}

	logs := env.logs.String()
	if !strings.Contains(logs, "rpc finished") {
		t.Fatalf("logs = %q, want call records", logs)
	}
	for _, secret := range []string{password, token, "authorization", "Bearer"} {
		if strings.Contains(logs, secret) {
			t.Errorf("logs contain %q:\n%s", secret, logs)
		}
	}
}

// callStream opens the protected test stream, closes its side and waits for
// the status the server ends it with.
func callStream(ctx context.Context, conn *grpc.ClientConn) error {
	stream, err := conn.NewStream(ctx, &privateServiceDesc.Streams[0], "/test.v1.Private/Echo")
	if err != nil {
		return err
	}
	if err := stream.CloseSend(); err != nil {
		return err
	}
	if err := stream.RecvMsg(&emptypb.Empty{}); !errors.Is(err, io.EOF) {
		return err
	}
	return nil
}

// privateService stands in for the chat service until it exists: one
// unary method and one stream, both requiring a token.
type privateService struct {
	mu     sync.Mutex
	userID string
}

func (p *privateService) record(ctx context.Context) error {
	claims, ok := auth.FromContext(ctx)
	if !ok {
		return status.Error(codes.Internal, "no claims in context")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.userID = claims.UserID
	return nil
}

func (p *privateService) lastUserID() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.userID
}

var privateServiceDesc = grpc.ServiceDesc{
	ServiceName: "test.v1.Private",
	HandlerType: (*any)(nil),
	Methods: []grpc.MethodDesc{{
		MethodName: "WhoAmI",
		Handler: func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
			req := new(emptypb.Empty)
			if err := dec(req); err != nil {
				return nil, err
			}
			info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/test.v1.Private/WhoAmI"}
			return interceptor(ctx, req, info, func(ctx context.Context, _ any) (any, error) {
				return &emptypb.Empty{}, srv.(*privateService).record(ctx)
			})
		},
	}},
	Streams: []grpc.StreamDesc{{
		StreamName:    "Echo",
		ClientStreams: true,
		ServerStreams: true,
		// Echo does not watch its context; it ends when the client closes
		// its side. A real handler must also return on ctx.Done().
		Handler: func(srv any, stream grpc.ServerStream) error {
			if err := srv.(*privateService).record(stream.Context()); err != nil {
				return err
			}
			for {
				err := stream.RecvMsg(&emptypb.Empty{})
				if errors.Is(err, io.EOF) {
					return nil
				}
				if err != nil {
					return err
				}
			}
		},
	}},
}

type testEnv struct {
	conn    *grpc.ClientConn
	private *privateService
	logs    *lockedBuffer
}

// startServer runs the server as cmd/server wires it, plus the private test
// service, over an in-memory listener.
func startServer(t *testing.T) testEnv {
	t.Helper()

	logs := &lockedBuffer{}
	logger := slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))

	tokens, err := auth.NewTokens([]byte("0123456789abcdef0123456789abcdef"), time.Hour)
	if err != nil {
		t.Fatalf("NewTokens() error = %v", err)
	}
	accounts, err := auth.NewService(memory.NewUsers(), tokens, auth.Options{BcryptCost: bcrypt.MinCost})
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	srv := server.New(logger, tokens, server.Options{
		ShutdownTimeout: time.Second,
		PublicMethods: []string{
			authv1.AuthService_Register_FullMethodName,
			authv1.AuthService_Login_FullMethodName,
		},
	})
	authv1.RegisterAuthServiceServer(srv, grpcapi.NewAuthServer(accounts, logger))
	private := &privateService{}
	srv.RegisterService(&privateServiceDesc, private)

	lis := bufconn.Listen(1 << 20)
	// Not t.Context(): it is cancelled before cleanups run, which would stop
	// the server while the client is still open.
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() { served <- srv.Serve(ctx, lis) }()
	t.Cleanup(func() {
		cancel()
		if err := <-served; err != nil {
			t.Errorf("Serve() = %v", err)
		}
	})

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	// Cleanups run last-in first-out: the client closes before the server
	// stops, which also ends streams like Echo that ignore their context.
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Errorf("close client: %v", err)
		}
	})
	return testEnv{conn: conn, private: private, logs: logs}
}

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
