// Package grpcapi implements the gRPC services on top of the domain
// packages: it maps requests to domain calls and domain errors to statuses.
package grpcapi

import (
	"context"
	"errors"
	"log/slog"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	authv1 "github.com/vladSlipushkin/go-grpc-chat/gen/auth/v1"
	"github.com/vladSlipushkin/go-grpc-chat/internal/auth"
)

// Accounts registers users and logs them in. *auth.Service implements it.
type Accounts interface {
	Register(ctx context.Context, username, password string) (auth.User, error)
	Login(ctx context.Context, username, password string) (auth.Token, error)
}

// AuthServer implements auth.v1.AuthService. Requests reach it already
// validated against the contract by the server's interceptors.
type AuthServer struct {
	authv1.UnimplementedAuthServiceServer

	accounts Accounts
	logger   *slog.Logger
}

// NewAuthServer returns an AuthServer backed by accounts.
func NewAuthServer(accounts Accounts, logger *slog.Logger) *AuthServer {
	return &AuthServer{accounts: accounts, logger: logger}
}

var (
	errUsernameTaken      = status.Error(codes.AlreadyExists, "username already taken")
	errInvalidCredentials = status.Error(codes.Unauthenticated, "invalid username or password")
	errInternal           = status.Error(codes.Internal, "internal error")
)

// Register creates an account. A taken username is reported as
// ALREADY_EXISTS, which reveals that the name is registered; ADR-0007
// accepts that.
func (s *AuthServer) Register(ctx context.Context, req *authv1.RegisterRequest) (*authv1.RegisterResponse, error) {
	u, err := s.accounts.Register(ctx, req.GetUsername(), req.GetPassword())
	switch {
	case errors.Is(err, auth.ErrUsernameTaken):
		return nil, errUsernameTaken
	case errors.Is(err, auth.ErrPasswordTooLong):
		return nil, passwordTooLong()
	case err != nil:
		return nil, s.internal(ctx, "register", err)
	}
	return &authv1.RegisterResponse{UserId: u.ID}, nil
}

// Login exchanges credentials for an access token.
func (s *AuthServer) Login(ctx context.Context, req *authv1.LoginRequest) (*authv1.LoginResponse, error) {
	token, err := s.accounts.Login(ctx, req.GetUsername(), req.GetPassword())
	switch {
	case errors.Is(err, auth.ErrInvalidCredentials):
		return nil, errInvalidCredentials
	case err != nil:
		return nil, s.internal(ctx, "login", err)
	}
	return &authv1.LoginResponse{
		AccessToken: token.Value,
		ExpiresAt:   timestamppb.New(token.ExpiresAt),
	}, nil
}

// internal logs err, which may name the storage or other internals, and
// returns a generic INTERNAL status for the client.
func (s *AuthServer) internal(ctx context.Context, op string, err error) error {
	s.logger.ErrorContext(ctx, "auth request failed", slog.String("op", op), slog.Any("error", err))
	return errInternal
}

// passwordTooLong reports the 72-byte limit the way the request validator
// reports the same limit before normalization, so clients handle both alike.
func passwordTooLong() error {
	st, err := status.New(codes.InvalidArgument, "invalid request").WithDetails(&errdetails.BadRequest{
		FieldViolations: []*errdetails.BadRequest_FieldViolation{{
			Field:       "password",
			Description: "value length must be at most 72 bytes after Unicode normalization",
			Reason:      "string.max_bytes",
		}},
	})
	if err != nil {
		return status.Error(codes.InvalidArgument, "invalid request")
	}
	return st.Err()
}
