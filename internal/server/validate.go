package server

import (
	"context"
	"errors"
	"log/slog"

	"buf.build/go/protovalidate"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// unaryValidation checks unary requests against the protovalidate rules in
// the contracts and rejects invalid ones with INVALID_ARGUMENT and a
// google.rpc.BadRequest detail listing every violated field. Stream
// requests are validated in their handlers instead (ADR-0003).
func unaryValidation(logger *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		msg, ok := req.(proto.Message)
		if !ok {
			return handler(ctx, req)
		}
		err := protovalidate.Validate(msg)
		var verr *protovalidate.ValidationError
		switch {
		case errors.As(err, &verr):
			return nil, invalidArgument(verr)
		case err != nil:
			// The rules failed to compile or evaluate: a bug in the
			// contract, not in the request.
			logger.ErrorContext(ctx, "request validation failed",
				slog.String("method", info.FullMethod),
				slog.Any("error", err),
			)
			return nil, errInternal
		}
		return handler(ctx, req)
	}
}

func invalidArgument(verr *protovalidate.ValidationError) error {
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
