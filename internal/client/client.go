// Package client talks to the chat server: it registers users, logs them in
// and keeps a chat session alive across reconnects (ADR-0010). It knows
// nothing about terminals; internal/cli builds the interactive client on
// top of it.
package client

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strings"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	grpcbackoff "google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"

	authv1 "github.com/vladSlipushkin/go-grpc-chat/gen/auth/v1"
	chatv1 "github.com/vladSlipushkin/go-grpc-chat/gen/chat/v1"
)

const (
	// keepaliveTime is how long the connection may stay silent before the
	// client pings the server. The server disconnects clients that ping
	// more often than once a minute (ADR-0006); the margin keeps network
	// jitter from making a ping look early.
	keepaliveTime    = 90 * time.Second
	keepaliveTimeout = 20 * time.Second
)

var (
	// ErrPlaintext reports an attempt to connect without TLS to an address
	// that is not loopback: the password and the token would cross the
	// network in the clear.
	ErrPlaintext = errors.New("plaintext connection to a non-loopback address")
	// ErrInvalidCredentials reports a failed login.
	ErrInvalidCredentials = errors.New("invalid username or password")
	// ErrUsernameTaken reports a registration under a name already in use.
	ErrUsernameTaken = errors.New("username already taken")
)

// Options configures Dial.
type Options struct {
	// TLS, if set, connects with TLS. Without it the connection is
	// plaintext, which Dial allows only to a loopback address unless
	// Insecure is set.
	TLS *tls.Config
	// Insecure allows a plaintext connection to an address that is not
	// loopback.
	Insecure bool
	// DialOptions are applied after the client's own; tests use them to
	// reach an in-memory server.
	DialOptions []grpc.DialOption
}

// Conn is a connection to the chat server. It is safe for concurrent use.
type Conn struct {
	cc   *grpc.ClientConn
	auth authv1.AuthServiceClient
	chat chatv1.ChatServiceClient
	// secure reports a TLS connection; the token then goes only over one
	// (see bearer).
	secure bool
}

// Dial returns a connection to addr, a "host:port" address. It does not
// wait for the server: the first call connects.
func Dial(addr string, opts Options) (*Conn, error) {
	creds := insecure.NewCredentials()
	switch {
	case opts.TLS != nil:
		creds = credentials.NewTLS(opts.TLS)
	case !opts.Insecure && !isLoopback(addr):
		return nil, fmt.Errorf("%w: %s", ErrPlaintext, addr)
	}
	dialOpts := append([]grpc.DialOption{
		grpc.WithTransportCredentials(creds),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:    keepaliveTime,
			Timeout: keepaliveTimeout,
		}),
		// While the server is down, a new stream fails at once until gRPC
		// connects again, so gRPC's own backoff, up to two minutes by
		// default, would decide when a session comes back. Its base and
		// cap match the session's; its jitter is ±50%, so it can wait up
		// to 45 s (ADR-0010).
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: grpcbackoff.Config{
				BaseDelay:  defaultMinBackoff,
				Multiplier: 2,
				Jitter:     0.5,
				MaxDelay:   defaultMaxBackoff,
			},
			// gRPC's default; without it an attempt would get only the
			// backoff delay, 500 ms at first, to connect.
			MinConnectTimeout: 20 * time.Second,
		}),
	}, opts.DialOptions...)
	cc, err := grpc.NewClient(addr, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	return &Conn{
		cc:     cc,
		auth:   authv1.NewAuthServiceClient(cc),
		chat:   chatv1.NewChatServiceClient(cc),
		secure: opts.TLS != nil,
	}, nil
}

// bearer puts the access token in a call's metadata. On a TLS connection
// it requires transport security, so gRPC refuses to send the token over
// anything less; a plaintext connection exists only where Dial allowed one.
type bearer struct {
	token  string
	secure bool
}

func (b bearer) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + b.token}, nil
}

func (b bearer) RequireTransportSecurity() bool {
	return b.secure
}

// isLoopback reports whether addr names this machine: localhost or a
// loopback IP such as 127.0.0.1 or ::1.
func isLoopback(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip, err := netip.ParseAddr(host)
	return err == nil && ip.IsLoopback()
}

// Close closes the connection. A Chat on it must be closed first.
func (c *Conn) Close() error {
	return c.cc.Close()
}

// Register creates an account.
func (c *Conn) Register(ctx context.Context, username, password string) error {
	_, err := c.auth.Register(ctx, &authv1.RegisterRequest{Username: username, Password: password})
	if status.Code(err) == codes.AlreadyExists {
		return ErrUsernameTaken
	}
	if err != nil {
		return fmt.Errorf("register: %w", describe(err))
	}
	return nil
}

// Token is an access token and the moment it expires.
type Token struct {
	Value     string
	ExpiresAt time.Time
}

// Login exchanges credentials for an access token.
func (c *Conn) Login(ctx context.Context, username, password string) (Token, error) {
	resp, err := c.auth.Login(ctx, &authv1.LoginRequest{Username: username, Password: password})
	if status.Code(err) == codes.Unauthenticated {
		return Token{}, ErrInvalidCredentials
	}
	if err != nil {
		return Token{}, fmt.Errorf("login: %w", describe(err))
	}
	return Token{Value: resp.GetAccessToken(), ExpiresAt: resp.GetExpiresAt().AsTime()}, nil
}

// describe turns a status that failed validation into an error naming the
// fields and their rules, since the status message alone says only
// "invalid request". Other errors are returned as they are.
func describe(err error) error {
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.InvalidArgument {
		return err
	}
	var fields []string
	for _, d := range st.Details() {
		if br, ok := d.(*errdetails.BadRequest); ok {
			for _, v := range br.GetFieldViolations() {
				fields = append(fields, v.GetField()+": "+v.GetDescription())
			}
		}
	}
	if len(fields) == 0 {
		return err
	}
	return &InvalidError{Fields: fields, err: err}
}

// InvalidError reports a request the server rejected as invalid.
type InvalidError struct {
	// Fields describes each violation as "field: rule".
	Fields []string
	err    error
}

func (e *InvalidError) Error() string { return "invalid " + strings.Join(e.Fields, "; ") }

// Unwrap returns the status error the server sent.
func (e *InvalidError) Unwrap() error { return e.err }

// Reason returns the ErrorInfo reason of a status error from the chat API,
// such as "SLOW_CLIENT" or "TOKEN_EXPIRED", or "" if it has none.
func Reason(err error) string {
	st, ok := status.FromError(err)
	if !ok {
		return ""
	}
	for _, d := range st.Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok && info.GetDomain() == errorDomain {
			return info.GetReason()
		}
	}
	return ""
}

// errorDomain is the ErrorInfo domain of the chat API (ADR-0003).
const errorDomain = "chat.v1"

// ErrorInfo reasons of the chat API (ADR-0003), as Reason returns them.
const (
	ReasonTokenExpired = "TOKEN_EXPIRED"
	ReasonSlowClient   = "SLOW_CLIENT"
	ReasonRoomLimit    = "ROOM_LIMIT_EXCEEDED"
	ReasonRateLimited  = "RATE_LIMITED"
)
