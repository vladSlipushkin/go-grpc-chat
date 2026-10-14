package cli

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/vladSlipushkin/go-grpc-chat/internal/client"
)

const help = `-- commands:
   /join ROOM     join a room and talk in it
   /leave [ROOM]  leave a room, the current one by default
   /use ROOM      talk in a room already joined
   /rooms         list the rooms joined
   /help          show this list
   /quit          leave the chat (so does Ctrl+C)
   any other line is sent to the current room --`

// errQuit tells a pending re-login that the user has left.
var errQuit = errors.New("quit")

// repl reads commands and messages line by line and prints what happens in
// the session. Its loop owns stdin: the line reader reads a line only when
// the loop asks for one, so a password prompt never competes with it for
// the terminal.
type repl struct {
	user string
	conn *client.Conn
	chat *client.Chat
	env  Env
	out  *printer

	room string // the current room; only the loop uses it

	want   chan struct{}         // the loop asks the reader for a line
	lines  chan string           // the reader's answer; closed at end of input
	logins chan chan loginResult // Run asks the loop for a new token
	done   chan struct{}         // closed when the loop has returned
}

type loginResult struct {
	token string
	err   error
}

func newREPL(user string, conn *client.Conn, env Env) *repl {
	return &repl{
		user:   user,
		conn:   conn,
		env:    env,
		out:    &printer{w: env.Stdout},
		want:   make(chan struct{}),
		lines:  make(chan string),
		logins: make(chan chan loginResult),
		done:   make(chan struct{}),
	}
}

// run serves the session until /quit, the end of input, ctx (Ctrl+C) or a
// failure that ends the session, and then closes it the graceful way.
func (r *repl) run(ctx context.Context, token string) error {
	r.chat = r.conn.NewChat(token, client.ChatOptions{Relogin: r.relogin})

	printed := make(chan struct{})
	go func() {
		defer close(printed)
		r.print(r.chat.Events())
	}()
	var runErr error
	ran := make(chan struct{})
	go func() {
		defer close(ran)
		// Ctrl+C must not cut the session short: Close ends it below.
		runErr = r.chat.Run(context.WithoutCancel(ctx))
	}()
	go readLines(r.env.Stdin, r.want, r.lines, r.done)

	r.loop(ctx, ran)
	close(r.done)

	closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), closeTimeout)
	defer cancel()
	closeErr := r.chat.Close(closeCtx)
	<-ran
	<-printed
	// A re-login the user walked away from is a way to quit.
	if runErr != nil && !errors.Is(runErr, errQuit) {
		return runErr
	}
	return closeErr
}

// loop handles lines until the user quits or the session ends.
func (r *repl) loop(ctx context.Context, ran <-chan struct{}) {
	for {
		select {
		case r.want <- struct{}{}:
		case <-ctx.Done():
			return
		case <-ran:
			return
		}

		select {
		case line, ok := <-r.lines:
			if !ok || !r.handle(ctx, line) {
				return
			}
		case reply := <-r.logins:
			// The reader is waiting for a line with echo on; it has to get
			// one before the password can be read without echo.
			r.out.println("-- session expired; press Enter to log in again --")
			var line string
			var ok bool
			select {
			case line, ok = <-r.lines:
			case <-ctx.Done():
			}
			if !ok || ctx.Err() != nil {
				reply <- loginResult{err: errQuit}
				return
			}
			if strings.TrimSpace(line) != "" {
				r.out.println("-- not sent: log in first --")
			}
			token, err := r.login(ctx)
			reply <- loginResult{token: token, err: err}
		case <-ctx.Done():
			return
		case <-ran:
			return
		}
	}
}

// relogin is the chat's Relogin: it runs on the chat's goroutine and asks
// the loop, which owns the terminal, for a new token.
func (r *repl) relogin(ctx context.Context) (string, error) {
	reply := make(chan loginResult, 1)
	select {
	case r.logins <- reply:
	case <-r.done:
		return "", errQuit
	case <-ctx.Done():
		return "", ctx.Err()
	}
	res := <-reply
	return res.token, res.err
}

// login asks for the password until the server accepts it or attempts run
// out, and returns an access token. The password is not kept. Ctrl+C
// during the prompt is errQuit.
func (r *repl) login(ctx context.Context) (string, error) {
	for range loginAttempts {
		password, err := readPassword(r.out, r.env, "password for "+r.user+": ")
		if ctx.Err() != nil {
			return "", errQuit
		}
		if err != nil {
			return "", err
		}
		callCtx, cancel := context.WithTimeout(ctx, requestTimeout)
		token, err := r.conn.Login(callCtx, r.user, password)
		cancel()
		if ctx.Err() != nil {
			return "", errQuit
		}
		if errors.Is(err, client.ErrInvalidCredentials) {
			r.out.println(err.Error())
			continue
		}
		if err != nil {
			return "", err
		}
		return token.Value, nil
	}
	return "", fmt.Errorf("login failed %d times", loginAttempts)
}

// handle runs one line and reports whether to go on.
func (r *repl) handle(ctx context.Context, line string) bool {
	line = strings.TrimSpace(line)
	if line == "" {
		return true
	}
	if !strings.HasPrefix(line, "/") {
		r.say(ctx, line)
		return true
	}

	cmd, arg, _ := strings.Cut(line, " ")
	arg = strings.TrimSpace(arg)
	switch cmd {
	case "/quit":
		return false
	case "/join":
		r.join(ctx, arg)
	case "/leave":
		r.leave(ctx, arg)
	case "/use":
		r.use(arg)
	case "/rooms":
		r.rooms()
	case "/help":
		r.out.println(help)
	default:
		r.out.printf("-- unknown command %s; /help lists them --", cmd)
	}
	return true
}

func (r *repl) say(ctx context.Context, text string) {
	if r.room == "" {
		r.out.println("-- not in a room; /join ROOM first --")
		return
	}
	ctx, cancel := requestContext(ctx)
	defer cancel()
	// The message itself is printed when the room delivers it.
	if err := r.chat.Send(ctx, r.room, text); err != nil {
		r.out.printf("-- not sent: %s --", describe(err))
	}
}

func (r *repl) join(ctx context.Context, room string) {
	if room == "" {
		r.out.println("-- usage: /join ROOM --")
		return
	}
	ctx, cancel := requestContext(ctx)
	defer cancel()
	if err := r.chat.Join(ctx, room); err != nil {
		r.out.printf("-- cannot join %s: %s --", room, describe(err))
		return
	}
	r.room = room
	r.out.printf("-- joined %s --", room)
}

func (r *repl) leave(ctx context.Context, room string) {
	if room == "" {
		room = r.room
	}
	if room == "" {
		r.out.println("-- usage: /leave [ROOM] --")
		return
	}
	ctx, cancel := requestContext(ctx)
	defer cancel()
	if err := r.chat.Leave(ctx, room); err != nil {
		r.out.printf("-- cannot leave %s: %s --", room, describe(err))
		return
	}
	if r.room == room {
		r.room = ""
	}
	r.out.printf("-- left %s --", room)
}

// requestContext bounds a request by requestTimeout but not by Ctrl+C: a
// request already sent is answered before the session closes, so the
// screen does not say "not sent" about a message the room has delivered.
// A second Ctrl+C ends the process.
func requestContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), requestTimeout)
}

func (r *repl) use(room string) {
	switch {
	case room == "":
		r.out.println("-- usage: /use ROOM --")
	case !slices.Contains(r.chat.Rooms(), room):
		r.out.printf("-- not in %s; /join %s first --", room, room)
	default:
		r.room = room
		r.out.printf("-- talking in %s --", room)
	}
}

func (r *repl) rooms() {
	rooms := r.chat.Rooms()
	if len(rooms) == 0 {
		r.out.println("-- no rooms; /join ROOM --")
		return
	}
	for i, room := range rooms {
		if room == r.room {
			rooms[i] += " (current)"
		}
	}
	r.out.printf("-- rooms: %s --", strings.Join(rooms, ", "))
}

// print shows the session's events until Run closes the channel.
func (r *repl) print(events <-chan client.Event) {
	connected := false
	for e := range events {
		switch e := e.(type) {
		case client.Message:
			r.out.printf("[%s] %s: %s", e.Room, e.Sender, e.Text)
		case client.Missed:
			noun := "messages"
			if e.Count == 1 {
				noun = "message"
			}
			r.out.printf("-- missed %d %s in %s --", e.Count, noun, e.Room)
		case client.Disconnected:
			if client.Reason(e.Err) == client.ReasonTokenExpired {
				// The login prompt says it.
				continue
			}
			r.out.printf("-- connection lost (%s); reconnecting in %s --", describe(e.Err), e.RetryIn.Round(100*time.Millisecond))
		case client.Connected:
			switch {
			case !connected:
				connected = true
				r.out.printf("-- connected as %s; /help lists commands --", r.user)
			case len(e.Rooms) == 0:
				r.out.println("-- reconnected --")
			default:
				r.out.printf("-- reconnected; rejoined %s --", strings.Join(e.Rooms, ", "))
			}
		}
	}
}

// describe says what went wrong in a few words.
func describe(err error) string {
	switch {
	case errors.Is(err, client.ErrNotConnected):
		return "not connected, still trying"
	case errors.Is(err, client.ErrRateLimited):
		return "sending too fast; wait a moment"
	case errors.Is(err, client.ErrConnectionLost), errors.Is(err, client.ErrNoReply):
		return "no reply from the server; it may have gone through"
	case client.Reason(err) == client.ReasonSlowClient:
		return "dropped for reading too slowly"
	case status.Code(err) == codes.Unavailable:
		return "server unavailable"
	}
	if st, ok := status.FromError(err); ok {
		return st.Message()
	}
	return err.Error()
}

// readLines reads a line each time it is asked, never ahead of the loop.
// It returns at the end of input or once done is closed; a read in
// progress then ends with the process.
func readLines(in io.Reader, want <-chan struct{}, lines chan<- string, done <-chan struct{}) {
	defer close(lines)
	sc := bufio.NewScanner(in)
	for {
		select {
		case <-want:
		case <-done:
			return
		}
		if !sc.Scan() {
			return
		}
		select {
		case lines <- sc.Text():
		case <-done:
			return
		}
	}
}

// printer serializes output from the loop and the event printer, so
// lines never interleave.
type printer struct {
	mu sync.Mutex
	w  io.Writer
}

func (p *printer) print(s string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	// Nothing useful can be done if the terminal is gone.
	_, _ = io.WriteString(p.w, s)
}

func (p *printer) println(s string) {
	p.print(s + "\n")
}

func (p *printer) printf(format string, args ...any) {
	p.println(fmt.Sprintf(format, args...))
}
