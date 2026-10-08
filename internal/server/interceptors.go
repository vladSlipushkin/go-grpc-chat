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

// unaryLogging records the outcome of every unary call.
func unaryLogging(logger *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		logCall(ctx, logger, info.FullMethod, start, err)
		return resp, err
	}
}

// streamLogging records the outcome of every stream when it ends.
func streamLogging(logger *slog.Logger) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		start := time.Now()
		err := handler(srv, ss)
		logCall(ss.Context(), logger, info.FullMethod, start, err)
		return err
	}
}

func logCall(ctx context.Context, logger *slog.Logger, method string, start time.Time, err error) {
	code := status.Code(err)
	logger.LogAttrs(ctx, logLevel(method, code), "rpc finished",
		slog.String("method", method),
		slog.String("code", code.String()),
		slog.Duration("duration", time.Since(start)),
		slog.String("peer", peerAddr(ctx)),
	)
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
