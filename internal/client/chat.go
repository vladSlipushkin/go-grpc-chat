package client

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"strconv"
	"sync"
	"time"

	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	chatv1 "github.com/vladSlipushkin/go-grpc-chat/gen/chat/v1"
)

var (
	// ErrClosed reports a request made after Close was called or Run
	// returned.
	ErrClosed = errors.New("chat closed")
	// ErrNotRunning reports a Close whose context ended before Run was
	// called.
	ErrNotRunning = errors.New("chat not running")
	// ErrNotConnected reports a request that gave up waiting for a
	// reconnect.
	ErrNotConnected = errors.New("not connected")
	// ErrConnectionLost reports a message whose stream broke before the
	// server replied. It may or may not have been delivered.
	ErrConnectionLost = errors.New("connection lost before the server replied")
	// ErrNoReply reports a request sent but not answered before its
	// context ended. It may or may not have been applied.
	ErrNoReply = errors.New("no reply from the server")
	// ErrRoomLimit reports a join past the server's room limit.
	ErrRoomLimit = errors.New("room limit reached")
	// ErrNotMember reports a message sent to a room not joined.
	ErrNotMember = errors.New("not a member of the room")
	// ErrRateLimited reports a message sent faster than the server allows.
	// It was not posted; the stream stays open.
	ErrRateLimited = errors.New("sending too fast")
)

const (
	defaultMinBackoff = 500 * time.Millisecond
	defaultMaxBackoff = 30 * time.Second
)

// ChatOptions configures a Chat.
type ChatOptions struct {
	// Relogin is called when the access token has expired and returns a
	// new one; the chat stays disconnected while it runs. If it is nil or
	// fails, Run returns. It must return once ctx is done.
	Relogin func(ctx context.Context) (string, error)
	// MinBackoff and MaxBackoff bound the wait between reconnect attempts.
	// The wait grows with each attempt and starts again from MinBackoff
	// after a stream that stayed open longer than MaxBackoff. Zero values
	// mean 500 ms and 30 s.
	MinBackoff, MaxBackoff time.Duration
}

type chatStream = grpc.BidiStreamingClient[chatv1.ConnectRequest, chatv1.ConnectResponse]

// Chat is a chat session that outlives its streams: Run keeps a stream
// open, reconnects when it breaks and joins the same rooms again
// (ADR-0010). Join, Leave, Send and Rooms are safe for concurrent use.
type Chat struct {
	api     chatv1.ChatServiceClient
	secure  bool // the connection is TLS
	token   string
	relogin func(context.Context) (string, error)
	backoff backoff

	events   chan Event
	quit     chan struct{} // closed when the session is to end
	quitOnce sync.Once
	started  chan struct{} // closed when Run starts
	done     chan struct{} // closed when Run returns

	// sendMu serializes Send and CloseSend: gRPC allows one sender at a
	// time on a stream.
	sendMu sync.Mutex

	mu        sync.Mutex
	stream    chatStream    // the open stream, nil between streams
	ready     chan struct{} // closed once stream is open and rooms rejoined
	isReady   bool
	closing   bool
	nextID    uint64
	pending   map[string]pending
	rooms     map[string]bool // rooms the server has acknowledged joining
	inflight  sync.WaitGroup  // requests sent and not yet answered
	cancelRun context.CancelFunc
}

// pending is a request waiting for its Ack or Error.
type pending struct {
	req   *chatv1.ConnectRequest
	reply chan error // buffered: the receiver never waits for the caller
}

// NewChat returns a chat session that authenticates with token. Nothing
// happens until Run is called.
func (c *Conn) NewChat(token string, opts ChatOptions) *Chat {
	b := backoff{min: opts.MinBackoff, max: opts.MaxBackoff}
	if b.min <= 0 {
		b.min = defaultMinBackoff
	}
	if b.max <= 0 {
		b.max = defaultMaxBackoff
	}
	return &Chat{
		api:     c.chat,
		secure:  c.secure,
		token:   token,
		relogin: opts.Relogin,
		backoff: b,
		events:  make(chan Event),
		quit:    make(chan struct{}),
		started: make(chan struct{}),
		done:    make(chan struct{}),
		ready:   make(chan struct{}),
		pending: make(map[string]pending),
		rooms:   make(map[string]bool),
	}
}

// Events delivers what happens in the session. The caller must keep
// reading it until it is closed, when Run returns: a reader that stops
// stops the stream too, and the server then drops the session as slow.
func (c *Chat) Events() <-chan Event {
	return c.events
}

// Rooms returns the rooms joined, sorted.
func (c *Chat) Rooms() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Sorted(maps.Keys(c.rooms))
}

// Join joins room. While the chat is reconnecting it waits for the new
// stream, until ctx ends. Joins and leaves are idempotent (ADR-0003), so
// one whose stream breaks before the reply is sent again on the next.
func (c *Chat) Join(ctx context.Context, room string) error {
	return c.call(ctx, joinRequest(room), true)
}

// Leave leaves room. It is retried like Join.
func (c *Chat) Leave(ctx context.Context, room string) error {
	return c.call(ctx, &chatv1.ConnectRequest{Action: &chatv1.ConnectRequest_LeaveRoom{
		LeaveRoom: &chatv1.LeaveRoom{RoomId: room},
	}}, true)
}

// Send posts text to room. The message comes back as an event like
// everyone else's. A send that fails with ErrConnectionLost or ErrNoReply
// may still have been delivered; it is not resent, since that could post
// it twice (ADR-0003).
func (c *Chat) Send(ctx context.Context, room, text string) error {
	return c.call(ctx, &chatv1.ConnectRequest{Action: &chatv1.ConnectRequest_SendMessage{
		SendMessage: &chatv1.SendMessage{RoomId: room, Text: text},
	}}, false)
}

func joinRequest(room string) *chatv1.ConnectRequest {
	return &chatv1.ConnectRequest{Action: &chatv1.ConnectRequest_JoinRoom{
		JoinRoom: &chatv1.JoinRoom{RoomId: room},
	}}
}

// Run serves the session until Close is called, ctx ends or the server
// ends it in a way a reconnect cannot fix, such as a token it rejects. It
// closes Events when it returns. Run must be called exactly once.
func (c *Chat) Run(ctx context.Context) error {
	defer close(c.done)
	defer close(c.events)
	// Requests still waiting for a stream learn that none will come.
	defer c.stop()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	// Under the lock, so that a Close either sees Run started or is seen
	// by it.
	c.mu.Lock()
	closing := c.closing
	c.cancelRun = cancel
	close(c.started)
	c.mu.Unlock()
	// A Close that came first is waiting for Run to start, or has given
	// up: either way there is nothing to serve.
	if closing {
		return nil
	}

	token := c.token
	attempt := 0
	for {
		if c.quitting() {
			return nil
		}
		lasted, err := c.serve(ctx, token)
		// A stream that outlived the longest wait shows the server is
		// working again; one that ends soon after the rejoin does not, and
		// resetting on it would come back every few hundred milliseconds
		// to a server that keeps failing.
		if lasted > c.backoff.max {
			attempt = 0
		}

		switch {
		case c.quitting():
			return nil
		case ctx.Err() != nil:
			return ctx.Err()
		case Reason(err) == ReasonTokenExpired:
			if c.relogin == nil {
				return err
			}
			if !c.emit(ctx, Disconnected{Err: err}) {
				return c.ended(ctx)
			}
			if token, err = c.relogin(ctx); err != nil {
				if c.quitting() {
					return nil
				}
				return fmt.Errorf("log in again: %w", err)
			}
		case retryable(err):
			delay := c.backoff.delay(attempt)
			attempt++
			if !c.emit(ctx, Disconnected{Err: err, RetryIn: delay}) || !c.sleep(ctx, delay) {
				return c.ended(ctx)
			}
		default:
			return err
		}
	}
}

// ended is what Run returns when a wait was cut short: nothing if Close
// asked for it, the context's error otherwise.
func (c *Chat) ended(ctx context.Context) error {
	if c.quitting() {
		return nil
	}
	return ctx.Err()
}

// serve runs one stream: it opens it, joins the rooms again and keeps it
// until it ends or Close asks for the end. lasted is how long the stream
// was open, zero if it never opened.
func (c *Chat) serve(ctx context.Context, token string) (lasted time.Duration, err error) {
	ctx, cancel := context.WithCancel(ctx)
	// Cancelling the stream's context also ends it on the server, so a
	// stream given up here does not linger there.
	defer cancel()

	stream, err := c.api.Connect(ctx, grpc.PerRPCCredentials(bearer{token: token, secure: c.secure}))
	if err != nil {
		return 0, err
	}
	opened := time.Now()
	c.mu.Lock()
	c.stream = stream
	c.mu.Unlock()

	// Written by the receiver and read only after received is closed.
	var recvErr error
	received := make(chan struct{})
	go func() {
		defer close(received)
		recvErr = c.receive(ctx, stream)
		c.detach(stream)
	}()

	err = c.hold(ctx, stream, received)
	// The receiver must be gone before the next stream is attached.
	cancel()
	<-received
	if err == nil {
		err = recvErr
	}
	return time.Since(opened), err
}

// hold joins the rooms again on a new stream and keeps the stream until
// the receiver stops or Close asks for the end. A nil result leaves the
// reason to the receiver.
func (c *Chat) hold(ctx context.Context, stream chatStream, received <-chan struct{}) error {
	dropped, err := c.rejoin(ctx, stream)
	if err != nil {
		if errors.Is(err, ErrConnectionLost) {
			return nil
		}
		return err
	}
	if !c.setReady(stream) {
		// The stream broke right after the rejoin.
		return nil
	}
	if !c.emit(ctx, Connected{Rooms: c.Rooms(), Dropped: dropped}) {
		return ctx.Err()
	}

	select {
	case <-received:
		return nil
	case <-c.quit:
		// Close has waited for the replies to every request, so the
		// stream can end without losing any (ADR-0009).
		c.sendMu.Lock()
		err := stream.CloseSend()
		c.sendMu.Unlock()
		if err != nil {
			return err
		}
		// The server ends the stream with OK, which the receiver sees as
		// io.EOF.
		<-received
		return nil
	}
}

// rejoin joins the rooms of the previous stream on a new one and returns
// the rooms the server refused, which are dropped. A broken stream stops
// it.
func (c *Chat) rejoin(ctx context.Context, stream chatStream) (dropped []string, err error) {
	for _, room := range c.Rooms() {
		err := c.roundTrip(ctx, stream, joinRequest(room))
		switch {
		case err == nil:
		case errors.Is(err, ErrConnectionLost), ctx.Err() != nil:
			return nil, err
		default:
			c.mu.Lock()
			delete(c.rooms, room)
			c.mu.Unlock()
			dropped = append(dropped, room)
		}
	}
	return dropped, nil
}

// receive reads the stream until it ends: replies go to the requests
// waiting for them, messages to Events.
func (c *Chat) receive(ctx context.Context, stream chatStream) error {
	seqs := make(seqTracker)
	for {
		resp, err := stream.Recv()
		if err != nil {
			return err
		}
		switch e := resp.GetEvent().(type) {
		case *chatv1.ConnectResponse_Ack:
			if req := c.resolve(e.Ack.GetRequestId(), nil); req.GetLeaveRoom() != nil {
				// Rejoining later is a new run of the room's seq.
				seqs.forget(req.GetLeaveRoom().GetRoomId())
			}
		case *chatv1.ConnectResponse_Error:
			c.resolve(e.Error.GetRequestId(), requestError(e.Error.GetStatus()))
		case *chatv1.ConnectResponse_Message:
			m := toMessage(e.Message)
			if n := seqs.observe(m.Room, m.Seq); n > 0 {
				if !c.emit(ctx, Missed{Room: m.Room, Count: n}) {
					return ctx.Err()
				}
			}
			if !c.emit(ctx, m) {
				return ctx.Err()
			}
		default:
			// An event added to the contract after this client was
			// built (ADR-0003).
		}
	}
}

// call sends req once the chat is connected and waits for its reply. An
// idempotent request whose stream breaks is sent again on the next one.
func (c *Chat) call(ctx context.Context, req *chatv1.ConnectRequest, idempotent bool) error {
	for {
		stream, err := c.awaitReady(ctx)
		if err != nil {
			return err
		}
		err = c.roundTrip(ctx, stream, req)
		c.inflight.Done()
		if !idempotent || !errors.Is(err, ErrConnectionLost) {
			return err
		}
	}
}

// awaitReady returns the open stream and counts the caller's request as in
// flight, or waits until there is one.
func (c *Chat) awaitReady(ctx context.Context) (chatStream, error) {
	for {
		c.mu.Lock()
		if c.closing {
			c.mu.Unlock()
			return nil, ErrClosed
		}
		if c.isReady {
			stream := c.stream
			c.inflight.Add(1)
			c.mu.Unlock()
			return stream, nil
		}
		ready := c.ready
		c.mu.Unlock()

		select {
		case <-ready:
		case <-c.quit:
			return nil, ErrClosed
		case <-ctx.Done():
			return nil, fmt.Errorf("%w: %w", ErrNotConnected, ctx.Err())
		}
	}
}

// roundTrip sends a copy of req with a fresh request_id on stream and waits
// for its Ack or Error. req itself is left alone: gRPC does not allow a
// message to change once it has been sent, and a retry sends req again.
func (c *Chat) roundTrip(ctx context.Context, stream chatStream, req *chatv1.ConnectRequest) error {
	req = proto.CloneOf(req)
	reply := make(chan error, 1)
	c.mu.Lock()
	if c.stream != stream {
		c.mu.Unlock()
		return ErrConnectionLost
	}
	c.nextID++
	req.RequestId = strconv.FormatUint(c.nextID, 10)
	c.pending[req.GetRequestId()] = pending{req: req, reply: reply}
	c.mu.Unlock()

	// Send waits only for the stream's context, not ctx: a stream stuck on
	// flow control holds the caller until keepalive declares the
	// connection dead.
	c.sendMu.Lock()
	// A failed Send means the stream has ended; the receiver learns why
	// from Recv and fails every pending request, this one included.
	_ = stream.Send(req)
	c.sendMu.Unlock()

	select {
	case err := <-reply:
		return err
	case <-ctx.Done():
		// The reply, if it comes, still updates the rooms.
		return fmt.Errorf("%w: %w", ErrNoReply, ctx.Err())
	}
}

// resolve hands a reply to the request waiting for it and returns that
// request, or nil if none is waiting.
func (c *Chat) resolve(id string, err error) *chatv1.ConnectRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	p, ok := c.pending[id]
	if !ok {
		return nil
	}
	delete(c.pending, id)
	if err == nil {
		switch a := p.req.GetAction().(type) {
		case *chatv1.ConnectRequest_JoinRoom:
			c.rooms[a.JoinRoom.GetRoomId()] = true
		case *chatv1.ConnectRequest_LeaveRoom:
			delete(c.rooms, a.LeaveRoom.GetRoomId())
		}
	}
	p.reply <- err
	return p.req
}

// setReady lets requests use stream and reports whether it is still open.
func (c *Chat) setReady(stream chatStream) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stream != stream {
		return false
	}
	if !c.isReady {
		c.isReady = true
		close(c.ready)
	}
	return true
}

// detach forgets stream once it has ended and fails the requests still
// waiting on it.
func (c *Chat) detach(stream chatStream) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stream != stream {
		return
	}
	c.stream = nil
	if c.isReady {
		c.isReady = false
		c.ready = make(chan struct{})
	}
	for id, p := range c.pending {
		delete(c.pending, id)
		// A leave about to be resent must not be undone by the rejoin in
		// the meantime.
		if leave := p.req.GetLeaveRoom(); leave != nil {
			delete(c.rooms, leave.GetRoomId())
		}
		p.reply <- ErrConnectionLost
	}
}

// Close ends the session the way ADR-0009 asks of clients: it stops taking
// requests, waits for the replies to those already sent, half-closes the
// stream and waits for the server to end it. If ctx ends first, it cancels
// the stream instead. Events must still be read meanwhile. Run returns
// before Close does. Close may be called more than once.
//
// Close may come first when Run is started on another goroutine. It then
// waits for Run to be called, until ctx ends, and that Run returns at once.
// If ctx ends first, Close returns ErrNotRunning, and a later Run returns at
// once.
func (c *Chat) Close(ctx context.Context) error {
	c.mu.Lock()
	c.closing = true
	c.mu.Unlock()
	select {
	case <-c.started:
	case <-ctx.Done():
		select {
		case <-c.started:
		default:
			// A Run that starts from now on sees closing.
			c.stop()
			return fmt.Errorf("close chat: %w: %w", ErrNotRunning, ctx.Err())
		}
	}

	answered := make(chan struct{})
	go func() {
		c.inflight.Wait()
		close(answered)
	}()
	var err error
	select {
	case <-answered:
	case <-ctx.Done():
		err = ctx.Err()
	}
	c.stop()
	if err == nil {
		select {
		case <-c.done:
			return nil
		case <-ctx.Done():
			err = ctx.Err()
		}
	}

	c.mu.Lock()
	cancel := c.cancelRun
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	<-c.done
	return fmt.Errorf("close chat: %w", err)
}

// stop marks the session as ending: no new requests, and Run stops.
func (c *Chat) stop() {
	c.mu.Lock()
	c.closing = true
	c.mu.Unlock()
	c.quitOnce.Do(func() { close(c.quit) })
}

func (c *Chat) quitting() bool {
	select {
	case <-c.quit:
		return true
	default:
		return false
	}
}

// emit delivers e unless ctx ends first.
func (c *Chat) emit(ctx context.Context, e Event) bool {
	select {
	case c.events <- e:
		return true
	case <-ctx.Done():
		return false
	}
}

// sleep waits for d and reports whether it did, rather than being cut
// short by ctx or Close.
func (c *Chat) sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
	case <-c.quit:
	}
	return false
}

// retryable reports whether a stream that ended with err is worth opening
// again. Errors that would come back unchanged, such as a rejected token,
// are not.
func retryable(err error) bool {
	switch status.Code(err) {
	case codes.Unavailable, codes.Internal, codes.Unknown, codes.Canceled, codes.DeadlineExceeded, codes.Aborted:
		return true
	case codes.ResourceExhausted:
		return Reason(err) == ReasonSlowClient
	default:
		return false
	}
}

// requestError turns the status in an Error event into the error its
// request returns.
func requestError(sp *spb.Status) error {
	err := status.ErrorProto(sp)
	switch {
	case Reason(err) == ReasonRoomLimit:
		return ErrRoomLimit
	case Reason(err) == ReasonRateLimited:
		return ErrRateLimited
	case status.Code(err) == codes.FailedPrecondition:
		// The only precondition in the contract (ADR-0003).
		return ErrNotMember
	default:
		return describe(err)
	}
}

// backoff spaces out reconnect attempts.
type backoff struct {
	min, max time.Duration
}

// delay returns the wait before reconnect attempt n, counted from 0. It
// doubles from min up to max, and a random half of it is jitter, so that
// clients cut off together, as at a server restart, do not all come back
// together.
func (b backoff) delay(n int) time.Duration {
	d := b.max
	if n < 32 {
		if v := b.min << n; v > 0 && v < b.max {
			d = v
		}
	}
	half := d / 2
	return half + rand.N(d-half+1) //nolint:gosec // jitter needs no unpredictability
}
