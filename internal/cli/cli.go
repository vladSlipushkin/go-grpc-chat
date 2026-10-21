// Package cli implements the chat command: subcommands and flags, password
// prompts and a line-based REPL on top of internal/client (ADR-0010).
package cli

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"google.golang.org/grpc"

	"github.com/vladSlipushkin/go-grpc-chat/internal/client"
)

const (
	// requestTimeout bounds every call to the server made on the user's
	// behalf, including the wait for a reconnect.
	requestTimeout = 10 * time.Second
	// closeTimeout bounds waiting for replies and the end of the stream
	// on the way out.
	closeTimeout = 5 * time.Second
	// loginAttempts is how many times a wrong password is asked again.
	loginAttempts = 3
)

// ErrUsage reports arguments the command cannot run with; the usage has
// been printed.
var ErrUsage = errors.New("invalid usage")

// Env is what the command reads and writes. cmd/chat passes the process's
// own streams and terminal; tests pass pipes and buffers.
type Env struct {
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
	// ReadPassword reads a line from the terminal without echoing it. The
	// prompt is printed before it is called.
	ReadPassword func() (string, error)
	// DialOptions are passed to client.Dial; tests use them to reach an
	// in-memory server.
	DialOptions []grpc.DialOption
}

const usage = `Usage:
  chat register -user NAME [-addr HOST:PORT] [-tls | -ca FILE | -insecure]
  chat -user NAME [-addr HOST:PORT] [-tls | -ca FILE | -insecure]

register creates an account; without it, chat logs in and starts a session.
Both ask for the password.

Flags:
`

// Run runs the command with args, the command line without the program
// name. It returns flag.ErrHelp if help was asked for.
func Run(ctx context.Context, args []string, env Env) error {
	if len(args) > 0 && args[0] == "register" {
		return register(ctx, args[1:], env)
	}
	return session(ctx, args, env)
}

type options struct {
	user     string
	addr     string
	tls      bool
	ca       string
	insecure bool
}

func parse(args []string, stderr io.Writer) (options, error) {
	fs := flag.NewFlagSet("chat", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		fmt.Fprint(fs.Output(), usage)
		fs.PrintDefaults()
	}
	var o options
	fs.StringVar(&o.user, "user", "", "username (required)")
	fs.StringVar(&o.addr, "addr", "localhost:50051", "server address")
	fs.BoolVar(&o.tls, "tls", false, "connect with TLS, trusting the system's root certificates")
	fs.StringVar(&o.ca, "ca", "", "connect with TLS, trusting the CA certificate in `FILE`")
	fs.BoolVar(&o.insecure, "insecure", false, "allow a connection without TLS to an address that is not loopback")

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return o, err
		}
		return o, ErrUsage
	}
	switch {
	case fs.NArg() > 0:
		fmt.Fprintf(stderr, "unexpected argument %q\n", fs.Arg(0))
	case o.user == "":
		fmt.Fprintln(stderr, "-user is required")
	case o.insecure && (o.tls || o.ca != ""):
		fmt.Fprintln(stderr, "-insecure cannot be combined with -tls or -ca")
	default:
		return o, nil
	}
	fs.Usage()
	return o, ErrUsage
}

func dial(o options, env Env) (*client.Conn, error) {
	tlsConfig, err := clientTLS(o)
	if err != nil {
		return nil, err
	}
	conn, err := client.Dial(o.addr, client.Options{TLS: tlsConfig, Insecure: o.insecure, DialOptions: env.DialOptions})
	if errors.Is(err, client.ErrPlaintext) {
		return nil, fmt.Errorf("%w: without TLS the password and token would cross the network in plaintext; pass -tls or -ca FILE, or -insecure to connect anyway", err)
	}
	return conn, err
}

// clientTLS returns the TLS configuration the flags ask for, or nil for
// plaintext.
func clientTLS(o options) (*tls.Config, error) {
	switch {
	case o.ca != "":
		data, err := os.ReadFile(o.ca)
		if err != nil {
			return nil, fmt.Errorf("read CA certificate: %w", err)
		}
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(data) {
			return nil, fmt.Errorf("%s: no PEM certificate", o.ca)
		}
		return &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS13}, nil
	case o.tls:
		return &tls.Config{MinVersion: tls.VersionTLS13}, nil
	default:
		return nil, nil
	}
}

func register(ctx context.Context, args []string, env Env) error {
	o, err := parse(args, env.Stderr)
	if err != nil {
		return err
	}
	conn, err := dial(o, env)
	if err != nil {
		return err
	}
	defer conn.Close()

	out := &printer{w: env.Stdout}
	password, err := readPassword(out, env, "password: ")
	if err != nil {
		return err
	}
	again, err := readPassword(out, env, "repeat password: ")
	if err != nil {
		return err
	}
	if password != again {
		return errors.New("passwords do not match")
	}

	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	if err := conn.Register(ctx, o.user, password); err != nil {
		return err
	}
	out.println("registered " + o.user)
	return nil
}

func session(ctx context.Context, args []string, env Env) error {
	o, err := parse(args, env.Stderr)
	if err != nil {
		return err
	}
	conn, err := dial(o, env)
	if err != nil {
		return err
	}
	defer conn.Close()

	r := newREPL(o.user, conn, env)
	token, err := r.login(ctx)
	if errors.Is(err, errQuit) {
		return nil
	}
	if err != nil {
		return err
	}
	return r.run(ctx, token)
}

// readPassword prompts for a password. The prompt goes through out so it
// does not land in the middle of a message being printed.
func readPassword(out *printer, env Env, prompt string) (string, error) {
	out.print(prompt)
	password, err := env.ReadPassword()
	// The Enter that ended the password was not echoed either.
	out.println("")
	if err != nil {
		return "", fmt.Errorf("read password: %w", err)
	}
	return password, nil
}
