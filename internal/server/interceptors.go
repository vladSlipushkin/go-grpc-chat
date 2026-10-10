package server

import (
	"context"
	"log/slog"
	"runtime/debug"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// errInternal is what a client sees when a handler panics. Details stay in
// the server log.
var errInternal = status.Error(codes.Internal, "internal error")

var healthMethodPrefix = "/" + healthpb.Health_ServiceDesc.ServiceName + "/"

// unaryRecovery turns a panic in a unary handler into an INTERNAL status so
// one faulty request cannot take the process down.
func unaryRecovery(logger *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if r := recover(); r != nil {
				logPanic(ctx, logger, info.FullMethod, r)
				err = errInternal
			}
		}()
		return handler(ctx, req)
	}
}

// streamRecovery is unaryRecovery for streaming handlers.
func streamRecovery(logger *slog.Logger) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
		defer func() {
			if r := recover(); r != nil {
				logPanic(ss.Context(), logger, info.FullMethod, r)
				err = errInternal
			}
		}()
		return handler(srv, ss)
	}
}

func logPanic(ctx context.Context, logger *slog.Logger, method string, r any) {
	logger.ErrorContext(ctx, "handler panicked",
		slog.String("method", method),
		slog.Any("panic", r),
		slog.String("stack", string(debug.Stack())),
	)
}

// callInfo collects what inner interceptors learn about a call, so the
// record logging writes when the call ends can include it. Interceptors run
// on the call's goroutine before and after the handler, so no locking is
// needed.
type callInfo struct {
	userID string
}

type callInfoKey struct{}

func withCallInfo(ctx context.Context) (context.Context, *callInfo) {
	ci := &callInfo{}
	return context.WithValue(ctx, callInfoKey{}, ci), ci
}

func callInfoFrom(ctx context.Context) *callInfo {
	ci, _ := ctx.Value(callInfoKey{}).(*callInfo)
	return ci
}

// unaryLogging records the outcome of every unary call.
func unaryLogging(logger *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		ctx, ci := withCallInfo(ctx)
		resp, err := handler(ctx, req)
		logCall(ctx, logger, info.FullMethod, start, ci, err)
		return resp, err
	}
}

// streamLogging records the outcome of every stream when it ends.
func streamLogging(logger *slog.Logger) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		start := time.Now()
		ctx, ci := withCallInfo(ss.Context())
		err := handler(srv, &serverStream{ServerStream: ss, ctx: ctx})
		logCall(ctx, logger, info.FullMethod, start, ci, err)
		return err
	}
}

// logCall never logs request contents or metadata: they carry passwords
// and access tokens.
func logCall(ctx context.Context, logger *slog.Logger, method string, start time.Time, ci *callInfo, err error) {
	code := statusCode(err)
	attrs := []slog.Attr{
		slog.String("method", method),
		slog.String("code", code.String()),
		slog.Duration("duration", time.Since(start)),
		slog.String("peer", peerAddr(ctx)),
	}
	if ci.userID != "" {
		attrs = append(attrs, slog.String("user_id", ci.userID))
	}
	logger.LogAttrs(ctx, logLevel(method, code), "rpc finished", attrs...)
}

// statusCode returns the code the client receives for err. A handler that
// returns a bare context error is reported as CANCELLED or
// DEADLINE_EXCEEDED: grpc-go converts it after the interceptors run, and
// status.Code alone would see UNKNOWN.
func statusCode(err error) codes.Code {
	if st, ok := status.FromError(err); ok {
		return st.Code()
	}
	return status.FromContextError(err).Code()
}

// logLevel keeps health probes out of normal logs and makes server-side
// failures stand out from client errors.
func logLevel(method string, code codes.Code) slog.Level {
	if strings.HasPrefix(method, healthMethodPrefix) {
		return slog.LevelDebug
	}
	switch code {
	case codes.OK, codes.Canceled:
		return slog.LevelInfo
	case codes.Internal, codes.Unknown, codes.DataLoss, codes.Unimplemented:
		return slog.LevelError
	default:
		return slog.LevelWarn
	}
}

func peerAddr(ctx context.Context) string {
	if p, ok := peer.FromContext(ctx); ok && p.Addr != nil {
		return p.Addr.String()
	}
	return ""
}
