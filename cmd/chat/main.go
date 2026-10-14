// Command chat is the command-line chat client.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"

	"golang.org/x/term"

	"github.com/vladSlipushkin/go-grpc-chat/internal/cli"
)

func main() {
	os.Exit(run())
}

func run() int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	// After the first Ctrl+C, restore default handling so a second one ends
	// the process if closing the session hangs.
	context.AfterFunc(ctx, stop)

	err := cli.Run(ctx, os.Args[1:], cli.Env{
		Stdin:        os.Stdin,
		Stdout:       os.Stdout,
		Stderr:       os.Stderr,
		ReadPassword: readPassword,
	})
	switch {
	case err == nil, errors.Is(err, flag.ErrHelp):
		return 0
	case errors.Is(err, cli.ErrUsage):
		return 2
	default:
		fmt.Fprintf(os.Stderr, "chat: %v\n", err)
		return 1
	}
}

// readPassword reads from the terminal with echo off. A password is never
// read from a pipe or a file, where it would be stored somewhere already.
func readPassword() (string, error) {
	fd := int(os.Stdin.Fd())
	if !term.IsTerminal(fd) {
		return "", errors.New("the password must be typed in a terminal")
	}
	b, err := term.ReadPassword(fd)
	return string(b), err
}
