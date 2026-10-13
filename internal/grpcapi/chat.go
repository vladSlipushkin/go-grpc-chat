package grpcapi

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"runtime/debug"

	"buf.build/go/protovalidate"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	chatv1 "github.com/vladSlipushkin/go-grpc-chat/gen/chat/v1"
	"github.com/vladSlipushkin/go-grpc-chat/internal/chat"
)

// Hub opens chat sessions. *chat.Hub implements it.
type Hub interface {
	Connect(ctx context.Context, end context.CancelCauseFunc) (*chat.Session, error)
}

// ChatServer implements chat.v1.ChatService. ADR-0009 describes how a stream
// is served: the handler owns the session, a reader and a writer goroutine
// move messages between it and the stream.
type ChatServer struct {
	chatv1.UnimplementedChatServiceServer

	hub      Hub
	shutdown <-chan struct{}
	logger   *slog.Logger
}

// NewChatServer returns a ChatServer backed by hub. When shutdown is closed
// (the server passes the Done channel of its root context), every stream
// ends with UNAVAILABLE; graceful stop alone would wait for clients to hang
// up (ADR-0006).
func NewChatServer(hub Hub, shutdown <-chan struct{}, logger *slog.Logger) *ChatServer {
	return &ChatServer{hub: hub, shutdown: shutdown, logger: logger}
}

// errorDomain is the ErrorInfo domain of the chat API (ADR-0003).
const errorDomain = "chat.v1"

var (
	errSlowClient        = mustErrorInfo(codes.ResourceExhausted, "outbound queue full", "SLOW_CLIENT")
	errRoomLimit         = mustErrorInfo(codes.ResourceExhausted, "room limit reached", "ROOM_LIMIT_EXCEEDED")
	errNotMember         = status.Error(codes.FailedPrecondition, "not a member of the room")
	errUnsupportedAction = status.Error(codes.Unimplemented, "action not supported")
	errShuttingDown      = status.Error(codes.Unavailable, "server shutting down")

	// errServerStopping is the cause a session ends with when the server
	// shuts down.
	errServerStopping = errors.New("server stopping")
	// errPanicked is the cause a session ends with when its reader or
	// writer panicked.
	errPanicked = errors.New("stream goroutine panicked")
)

// Connect serves one chat stream until the client half-closes it, the
// stream's context ends, the hub drops the session or the server shuts
// down.
//
// The handler returns without waiting for the reader and the writer: either
// may be blocked in Recv or Send, and only the handler's return releases
// them (ADR-0009).
func (s *ChatServer) Connect(stream chatv1.ChatService_ConnectServer) error {
	ctx, end := context.WithCancelCause(stream.Context())
	defer end(nil)

	sess, err := s.hub.Connect(ctx, end)
	if err != nil {
		return s.connectFailed(ctx, err)
	}
	defer sess.Close()

	reqs := make(chan *chatv1.ConnectRequest)
	// Buffered so that the reader never blocks on reporting its last error.
	recvErr := make(chan error, 1)
	go s.read(ctx, end, stream, reqs, recvErr)
	go s.write(ctx, end, stream, sess.Events())

	for {
		select {
		case <-ctx.Done():
			return s.ended(ctx, stream.Context())
		case <-s.shutdown:
			end(errServerStopping)
			return s.ended(ctx, stream.Context())
		case err := <-recvErr:
			// Half-close ends the session at once; replies still queued
			// are dropped (ADR-0009).
			if errors.Is(err, io.EOF) {
				return nil
			}
			// RecvMsg has already ended the stream with this status.
			return err
		case req := <-reqs:
			// select picks at random among ready cases, so a request may
			// win over a session that has just ended; it is not applied.
			if ctx.Err() != nil {
				return s.ended(ctx, stream.Context())
			}
			s.apply(ctx, end, sess, req)
		}
	}
}

func (s *ChatServer) connectFailed(ctx context.Context, err error) error {
	if errors.Is(err, chat.ErrHubClosed) {
		return errShuttingDown
	}
	// ErrNoUser: the auth interceptor did not run for this method.
	s.logger.ErrorContext(ctx, "open chat session", slog.Any("error", err))
	return errInternal
}

// read hands requests over to the handler. It stops after the first Recv
// error, which it reports on errc, or once ctx ends; a Recv blocked when the
// handler returns is released by gRPC closing the stream.
func (s *ChatServer) read(ctx context.Context, end context.CancelCauseFunc, stream chatv1.ChatService_ConnectServer, reqs chan<- *chatv1.ConnectRequest, errc chan<- error) {
	defer s.recoverPanic(ctx, end, "reader")
	for {
		req, err := stream.Recv()
		if err != nil {
			errc <- err
			return
		}
		select {
		case reqs <- req:
		case <-ctx.Done():
			return
		}
	}
}

// write sends the session's events. It is the only goroutine that calls
// Send on the stream, as gRPC requires.
func (s *ChatServer) write(ctx context.Context, end context.CancelCauseFunc, stream chatv1.ChatService_ConnectServer, events <-chan chat.Event) {
	defer s.recoverPanic(ctx, end, "writer")
	for {
		select {
		case <-ctx.Done():
			return
		case e := <-events:
			// An event queued after a SLOW_CLIENT drop can only be taken
			// after the drop cancelled ctx, so this check keeps seq free
			// of gaps up to the final status.
			if ctx.Err() != nil {
				return
			}
			if err := stream.Send(toResponse(e)); err != nil {
				// A failed Send ends the stream with its own status,
				// which cancels the stream's context and the handler
				// with it.
				return
			}
		}
	}
}

// recoverPanic keeps a panic in the reader or the writer from taking the
// process down: the interceptor's recover only covers the handler's own
// goroutine. The session ends and the stream with INTERNAL.
func (s *ChatServer) recoverPanic(ctx context.Context, end context.CancelCauseFunc, goroutine string) {
	if r := recover(); r != nil {
		s.logger.ErrorContext(ctx, "chat stream goroutine panicked",
			slog.String("goroutine", goroutine),
			slog.Any("panic", r),
			slog.String("stack", string(debug.Stack())),
		)
		end(errPanicked)
	}
}

// apply runs one request and queues its Ack or Error. Errors that concern
// the whole connection end the session instead.
func (s *ChatServer) apply(ctx context.Context, end context.CancelCauseFunc, sess *chat.Session, req *chatv1.ConnectRequest) {
	if err := protovalidate.Validate(req); err != nil {
		sess.Enqueue(s.invalid(ctx, req, err))
		return
	}

	id := req.GetRequestId()
	var (
		messageID string
		err       error
	)
	switch a := req.GetAction().(type) {
	case *chatv1.ConnectRequest_JoinRoom:
		err = sess.Join(ctx, a.JoinRoom.GetRoomId())
	case *chatv1.ConnectRequest_LeaveRoom:
		err = sess.Leave(ctx, a.LeaveRoom.GetRoomId())
	case *chatv1.ConnectRequest_SendMessage:
		messageID, err = sess.Send(ctx, a.SendMessage.GetRoomId(), a.SendMessage.GetText())
	default:
		// An action added to the contract but not handled here yet.
		sess.Enqueue(chat.Rejection{RequestID: id, Err: errUnsupportedAction})
		return
	}

	switch {
	case err == nil:
		sess.Enqueue(chat.Ack{RequestID: id, MessageID: messageID})
	case errors.Is(err, chat.ErrRoomLimit):
		sess.Enqueue(chat.Rejection{RequestID: id, Err: errRoomLimit})
	case errors.Is(err, chat.ErrNotMember):
		sess.Enqueue(chat.Rejection{RequestID: id, Err: errNotMember})
	case ctx.Err() != nil:
		// The session is ending; the handler reports why.
	default:
		// ErrHubClosed, ErrRoomFailed or a failure inside the hub.
		end(err)
	}
}

// invalid builds the Error for a request that fails validation. The
// request id is echoed only if it is itself valid.
func (s *ChatServer) invalid(ctx context.Context, req *chatv1.ConnectRequest, err error) chat.Rejection {
	var verr *protovalidate.ValidationError
	if !errors.As(err, &verr) {
		// The rules failed to compile or evaluate: a bug in the contract.
		s.logger.ErrorContext(ctx, "request validation failed", slog.Any("error", err))
		return chat.Rejection{RequestID: req.GetRequestId(), Err: errInternal}
	}
	id := req.GetRequestId()
	for _, v := range verr.Violations {
		if protovalidate.FieldPathString(v.Proto.GetField()) == "request_id" {
			id = ""
			break
		}
	}
	return chat.Rejection{RequestID: id, Err: badRequest(verr)}
}

// ended returns the status a stream ends with once its session's context
// has ended. The first cause wins (ADR-0008), so a session dropped as slow
// just before its token expired still reports SLOW_CLIENT.
func (s *ChatServer) ended(ctx, streamCtx context.Context) error {
	cause := context.Cause(ctx)
	s.logger.InfoContext(ctx, "chat session ended", slog.Any("cause", cause))
	switch {
	case errors.Is(cause, chat.ErrSlowClient):
		return errSlowClient
	case errors.Is(cause, chat.ErrHubClosed), errors.Is(cause, errServerStopping):
		return errShuttingDown
	case errors.Is(cause, chat.ErrRoomFailed), errors.Is(cause, errPanicked):
		// Logged with the stack where it happened.
		return errInternal
	case errors.Is(cause, context.Cause(streamCtx)):
		// The stream itself ended: the client cancelled, its deadline
		// passed or its token expired. The auth interceptor turns the
		// last into TOKEN_EXPIRED.
		return status.FromContextError(ctx.Err()).Err()
	default:
		s.logger.ErrorContext(ctx, "chat session failed", slog.Any("error", cause))
		return errInternal
	}
}

// toResponse translates a session event into the protocol. The Message is
// built per recipient: chat shares one *chat.Message among all of them.
func toResponse(e chat.Event) *chatv1.ConnectResponse {
	switch e := e.(type) {
	case *chat.Message:
		return &chatv1.ConnectResponse{Event: &chatv1.ConnectResponse_Message{Message: &chatv1.Message{
			Id:             e.ID,
			RoomId:         e.RoomID,
			SenderId:       e.SenderID,
			SenderUsername: e.SenderUsername,
			Text:           e.Text,
			SentAt:         timestamppb.New(e.SentAt),
			Seq:            e.Seq,
		}}}
	case chat.Ack:
		return &chatv1.ConnectResponse{Event: &chatv1.ConnectResponse_Ack{Ack: &chatv1.Ack{
			RequestId: e.RequestID,
			MessageId: e.MessageID,
		}}}
	case chat.Rejection:
		return &chatv1.ConnectResponse{Event: &chatv1.ConnectResponse_Error{Error: &chatv1.Error{
			RequestId: e.RequestID,
			Status:    status.Convert(e.Err).Proto(),
		}}}
	default:
		panic(fmt.Sprintf("grpcapi: unknown chat event %T", e))
	}
}

// badRequest reports validation errors the way the unary validator does,
// so clients handle request errors alike in both places.
func badRequest(verr *protovalidate.ValidationError) error {
	br := &errdetails.BadRequest{}
	for _, v := range verr.Violations {
		br.FieldViolations = append(br.FieldViolations, &errdetails.BadRequest_FieldViolation{
			Field:       protovalidate.FieldPathString(v.Proto.GetField()),
			Description: v.Proto.GetMessage(),
			Reason:      v.Proto.GetRuleId(),
		})
	}
	st, err := status.New(codes.InvalidArgument, "invalid request").WithDetails(br)
	if err != nil {
		return status.Error(codes.InvalidArgument, "invalid request")
	}
	return st.Err()
}

// mustErrorInfo builds a status with an ErrorInfo detail for package-level
// errors and panics if it cannot, like regexp.MustCompile.
func mustErrorInfo(code codes.Code, msg, reason string) error {
	st, err := status.New(code, msg).WithDetails(&errdetails.ErrorInfo{Reason: reason, Domain: errorDomain})
	if err != nil {
		// WithDetails fails only if the detail cannot be marshalled, which
		// a static ErrorInfo always can.
		panic(err)
	}
	return st.Err()
}
