package cli_test

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/goleak"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"

	authv1 "github.com/vladSlipushkin/go-grpc-chat/gen/auth/v1"
	chatv1 "github.com/vladSlipushkin/go-grpc-chat/gen/chat/v1"
	"github.com/vladSlipushkin/go-grpc-chat/internal/certtest"
	"github.com/vladSlipushkin/go-grpc-chat/internal/chattest"
	"github.com/vladSlipushkin/go-grpc-chat/internal/cli"
	"github.com/vladSlipushkin/go-grpc-chat/internal/client"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

const password = "correct horse"

func TestTwoTerminals(t *testing.T) {
	t.Parallel()

	srv := chattest.Start(t, chattest.Options{})
	register(t, srv, "alice")
	register(t, srv, "bob")
	alice := open(t, srv, "alice", password)
	bob := open(t, srv, "bob", password)
	alice.waitFor(t, "-- connected as alice; /help lists commands --")
	bob.waitFor(t, "-- connected as bob; /help lists commands --")

	alice.typeLine(t, "/join general")
	alice.waitFor(t, "-- joined general --")
	bob.typeLine(t, "/join general")
	bob.waitFor(t, "-- joined general --")

	alice.typeLine(t, "hello, bob")
	bob.waitFor(t, "[general] alice: hello, bob")
	alice.waitFor(t, "[general] alice: hello, bob")
	bob.typeLine(t, "hi alice")
	alice.waitFor(t, "[general] bob: hi alice")

	alice.typeLine(t, "/join random")
	alice.waitFor(t, "-- joined random --")
	alice.typeLine(t, "/rooms")
	alice.waitFor(t, "-- rooms: general, random (current) --")
	alice.typeLine(t, "/use general")
	alice.waitFor(t, "-- talking in general --")

	bob.typeLine(t, "/leave")
	bob.waitFor(t, "-- left general --")
	alice.typeLine(t, "anyone?")
	alice.waitFor(t, "[general] alice: anyone?")

	bob.typeLine(t, "/quit")
	if err := bob.wait(t); err != nil {
		t.Errorf("bob's Run() = %v, want nil after /quit", err)
	}
	if strings.Contains(bob.out.String(), "anyone?") {
		t.Errorf("bob saw a message sent after he left:\n%s", bob.out.String())
	}
}

func TestCommandsNeedARoom(t *testing.T) {
	t.Parallel()

	srv := chattest.Start(t, chattest.Options{})
	register(t, srv, "alice")
	alice := open(t, srv, "alice", password)
	alice.waitFor(t, "-- connected as alice")

	steps := []struct{ line, want string }{
		{"hello?", "-- not in a room; /join ROOM first --"},
		{"/leave", "-- usage: /leave [ROOM] --"},
		{"/use random", "-- not in random; /join random first --"},
		{"/rooms", "-- no rooms; /join ROOM --"},
		{"/join Not-A-Room", "-- cannot join Not-A-Room: invalid join_room.room_id"},
		{"/shout hi", "-- unknown command /shout; /help lists them --"},
		{"/help", "/join ROOM     join a room and talk in it"},
	}
	for _, s := range steps {
		alice.typeLine(t, s.line)
		alice.waitFor(t, s.want)
	}
}

func TestEndOfInputQuits(t *testing.T) {
	t.Parallel()

	srv := chattest.Start(t, chattest.Options{})
	register(t, srv, "alice")
	alice := open(t, srv, "alice", password)
	alice.typeLine(t, "/join general")
	alice.waitFor(t, "-- joined general --")
	alice.in.Close()
	if err := alice.wait(t); err != nil {
		t.Errorf("Run() at end of input = %v, want nil", err)
	}
}

func TestInterruptQuits(t *testing.T) {
	t.Parallel()

	srv := chattest.Start(t, chattest.Options{})
	register(t, srv, "alice")
	alice := open(t, srv, "alice", password)
	alice.waitFor(t, "-- connected as alice")
	alice.cancel()
	if err := alice.wait(t); err != nil {
		t.Errorf("Run() after Ctrl+C = %v, want nil", err)
	}
}

func TestWrongPasswordIsAskedAgain(t *testing.T) {
	t.Parallel()

	srv := chattest.Start(t, chattest.Options{})
	register(t, srv, "alice")
	alice := open(t, srv, "alice", "wrong password", password)
	alice.waitFor(t, "password for alice: \ninvalid username or password\npassword for alice: \n-- connected as alice")
}

func TestLoginAgainAfterTokenExpiry(t *testing.T) {
	t.Parallel()

	// A two-second token: the expiry races nothing but in-process work
	// (ADR-0002).
	srv := chattest.Start(t, chattest.Options{TokenTTL: 2 * time.Second})
	register(t, srv, "alice")
	alice := open(t, srv, "alice", password, password)
	alice.typeLine(t, "/join general")
	alice.waitFor(t, "-- joined general --")

	alice.waitFor(t, "-- session expired; press Enter to log in again --")
	alice.typeLine(t, "lost words")
	alice.waitFor(t, "-- not sent: log in first --\npassword for alice: \n")
	alice.waitFor(t, "-- reconnected; rejoined general --")
	alice.typeLine(t, "back")
	alice.waitFor(t, "[general] alice: back")
}

func TestCurrentRoomNotRejoined(t *testing.T) {
	t.Parallel()

	alice := openFake(t, &refusingServer{refuse: "general"}, "alice", password)
	alice.typeLine(t, "/join general")
	alice.waitFor(t, "-- joined general --")
	// The stream breaks on the message; the next one refuses general.
	alice.typeLine(t, "hello")
	alice.waitFor(t, "-- reconnected --\n-- could not rejoin general --")
	alice.typeLine(t, "again")
	alice.waitFor(t, "-- not sent: no longer in general; /join general --")
	alice.typeLine(t, "/rooms")
	alice.waitFor(t, "-- no rooms; /join ROOM --")
}

// refusingServer logs anyone in. On its first chat stream it acknowledges
// joins and ends the stream on the first message without replying; on
// later streams it refuses to join refuse, as a server at its room limit
// would, and acknowledges the rest.
type refusingServer struct {
	authv1.UnimplementedAuthServiceServer
	chatv1.UnimplementedChatServiceServer
	refuse string

	streams atomic.Int32
}

func (*refusingServer) Login(context.Context, *authv1.LoginRequest) (*authv1.LoginResponse, error) {
	return &authv1.LoginResponse{AccessToken: "token", ExpiresAt: timestamppb.New(time.Now().Add(time.Hour))}, nil
}

func (s *refusingServer) Connect(stream chatv1.ChatService_ConnectServer) error {
	first := s.streams.Add(1) == 1
	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if first && req.GetSendMessage() != nil {
			return status.Error(codes.Unavailable, "going down")
		}
		resp := &chatv1.ConnectResponse{Event: &chatv1.ConnectResponse_Ack{Ack: &chatv1.Ack{RequestId: req.GetRequestId()}}}
		if !first && req.GetJoinRoom().GetRoomId() == s.refuse {
			st, err := status.New(codes.ResourceExhausted, "room limit reached").WithDetails(
				&errdetails.ErrorInfo{Reason: client.ReasonRoomLimit, Domain: "chat.v1"},
			)
			if err != nil {
				return err
			}
			resp = &chatv1.ConnectResponse{Event: &chatv1.ConnectResponse_Error{Error: &chatv1.Error{
				RequestId: req.GetRequestId(),
				Status:    st.Proto(),
			}}}
		}
		if err := stream.Send(resp); err != nil {
			return err
		}
	}
}

func TestTLS(t *testing.T) {
	t.Parallel()

	files := certtest.SelfSigned(t, chattest.Host)
	srv := chattest.Start(t, chattest.Options{TLS: files})
	register(t, srv, "alice")
	alice := openWith(t, srv.DialOptions(), []string{"-ca", files.CA}, "alice", password)
	alice.typeLine(t, "/join general")
	alice.waitFor(t, "-- joined general --")
	alice.typeLine(t, "hello")
	alice.waitFor(t, "[general] alice: hello")
}

func TestRefusesPlaintextToRemoteAddress(t *testing.T) {
	t.Parallel()

	err := cli.Run(t.Context(), []string{"-user", "alice", "-addr", "chat.example.com:50051"}, cli.Env{
		Stdin:  strings.NewReader(""),
		Stdout: io.Discard,
		Stderr: io.Discard,
		ReadPassword: func() (string, error) {
			t.Error("password asked for before the transport was checked")
			return password, nil
		},
	})
	if !errors.Is(err, client.ErrPlaintext) || !strings.Contains(err.Error(), "-insecure") {
		t.Errorf("Run() to a remote address without TLS = %v, want ErrPlaintext naming -insecure", err)
	}
}

func TestUsage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		args       []string
		wantErr    error
		wantStderr string
	}{
		{name: "no user", args: nil, wantErr: cli.ErrUsage, wantStderr: "-user is required"},
		{name: "extra argument", args: []string{"-user", "alice", "now"}, wantErr: cli.ErrUsage, wantStderr: `unexpected argument "now"`},
		{name: "TLS and insecure", args: []string{"-user", "alice", "-tls", "-insecure"}, wantErr: cli.ErrUsage, wantStderr: "-insecure cannot be combined with -tls or -ca"},
		{name: "TLS and CA", args: []string{"-user", "alice", "-tls", "-ca", "ca.pem"}, wantErr: cli.ErrUsage, wantStderr: "-tls and -ca cannot be combined"},
		{name: "unknown flag", args: []string{"register", "-name", "alice"}, wantErr: cli.ErrUsage, wantStderr: "flag provided but not defined: -name"},
		{name: "help", args: []string{"-h"}, wantErr: flag.ErrHelp, wantStderr: "chat register -user NAME"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var stderr bytes.Buffer
			err := cli.Run(t.Context(), tt.args, cli.Env{Stdin: strings.NewReader(""), Stdout: io.Discard, Stderr: &stderr})
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("Run(%q) = %v, want %v", tt.args, err, tt.wantErr)
			}
			if !strings.Contains(stderr.String(), tt.wantStderr) {
				t.Errorf("Run(%q) printed %q, want it to contain %q", tt.args, stderr.String(), tt.wantStderr)
			}
		})
	}
}

func TestRegister(t *testing.T) {
	t.Parallel()

	srv := chattest.Start(t, chattest.Options{})
	tests := []struct {
		name      string
		passwords []string
		wantOut   string
		wantErr   string
	}{
		{name: "ok", passwords: []string{password, password}, wantOut: "registered alice\n"},
		{name: "taken", passwords: []string{password, password}, wantErr: "username already taken"},
		{name: "mismatch", passwords: []string{password, "correct horse!"}, wantErr: "passwords do not match"},
	}
	// Sequential: the second case needs the first one's account.
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stdout bytes.Buffer
			err := cli.Run(t.Context(), []string{"register", "-user", "alice", "-addr", chattest.Target, "-insecure"}, cli.Env{
				Stdin:        strings.NewReader(""),
				Stdout:       &stdout,
				Stderr:       io.Discard,
				ReadPassword: passwordsFor(t, tt.passwords),
				DialOptions:  srv.DialOptions(),
			})
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Errorf("Run(register) = %v, want an error containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Run(register) error = %v", err)
			}
			if want := "password: \nrepeat password: \n" + tt.wantOut; stdout.String() != want {
				t.Errorf("Run(register) printed %q, want %q", stdout.String(), want)
			}
		})
	}
}

func register(t *testing.T, srv *chattest.Server, name string) {
	t.Helper()
	srv.Register(t, name, password)
}

// terminal is one running chat command, typed into through a pipe.
type terminal struct {
	in     *io.PipeWriter
	out    *lockedBuffer
	cancel context.CancelFunc
	result chan error
	err    error
	once   sync.Once
}

// open runs the chat command for name, answering password prompts with
// passwords in order.
func open(t *testing.T, srv *chattest.Server, name string, passwords ...string) *terminal {
	t.Helper()
	return openWith(t, srv.DialOptions(), []string{"-insecure"}, name, passwords...)
}

// fakeServer is a server written for a test, implementing both services.
type fakeServer interface {
	authv1.AuthServiceServer
	chatv1.ChatServiceServer
}

// openFake runs the chat command for name against impl.
func openFake(t *testing.T, impl fakeServer, name string, passwords ...string) *terminal {
	t.Helper()

	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	authv1.RegisterAuthServiceServer(gs, impl)
	chatv1.RegisterChatServiceServer(gs, impl)
	served := make(chan error, 1)
	go func() { served <- gs.Serve(lis) }()
	t.Cleanup(func() {
		gs.Stop()
		if err := <-served; err != nil {
			t.Errorf("Serve() = %v", err)
		}
	})
	dialer := grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return lis.DialContext(ctx)
	})
	return openWith(t, []grpc.DialOption{dialer}, []string{"-insecure"}, name, passwords...)
}

// openWith runs the chat command for name with the transport flags given.
func openWith(t *testing.T, dialOpts []grpc.DialOption, transport []string, name string, passwords ...string) *terminal {
	t.Helper()

	stdin, in := io.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	term := &terminal{in: in, out: &lockedBuffer{}, cancel: cancel, result: make(chan error, 1)}
	go func() {
		args := append([]string{"-user", name, "-addr", chattest.Target}, transport...)
		term.result <- cli.Run(ctx, args, cli.Env{
			Stdin:        stdin,
			Stdout:       term.out,
			Stderr:       term.out,
			ReadPassword: passwordsFor(t, passwords),
			DialOptions:  dialOpts,
		})
	}()
	// Registered after the server's cleanup, so it runs first.
	t.Cleanup(func() {
		term.in.Close()
		if err := term.wait(t); err != nil {
			t.Errorf("%s's Run() = %v", name, err)
		}
		cancel()
	})
	return term
}

func (term *terminal) typeLine(t *testing.T, line string) {
	t.Helper()
	if _, err := io.WriteString(term.in, line+"\n"); err != nil {
		t.Fatalf("type %q: %v", line, err)
	}
}

// waitFor waits until the output contains want.
func (term *terminal) waitFor(t *testing.T, want string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !strings.Contains(term.out.String(), want) {
		if time.Now().After(deadline) {
			t.Fatalf("output does not contain %q within 5s:\n%s", want, term.out.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// wait returns what Run returned.
func (term *terminal) wait(t *testing.T) error {
	t.Helper()
	term.once.Do(func() {
		select {
		case term.err = <-term.result:
		case <-time.After(10 * time.Second):
			t.Fatal("Run() did not return within 10s")
		}
	})
	return term.err
}

// passwordsFor answers password prompts in order and fails the test if
// asked more often.
func passwordsFor(t *testing.T, answers []string) func() (string, error) {
	t.Helper()
	var mu sync.Mutex
	return func() (string, error) {
		mu.Lock()
		defer mu.Unlock()
		if len(answers) == 0 {
			t.Error("password asked for more often than expected")
			return "", errors.New("no more passwords")
		}
		p := answers[0]
		answers = answers[1:]
		return p, nil
	}
}

// lockedBuffer is a bytes.Buffer safe for the concurrent writes of the
// command and reads of the test.
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
