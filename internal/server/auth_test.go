package server

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/vladSlipushkin/go-grpc-chat/internal/auth"
)

// fakeVerifier accepts exactly one token.
type fakeVerifier struct {
	token  string
	claims auth.Claims
	err    error
}

func (v fakeVerifier) Verify(token string) (auth.Claims, error) {
	if v.err != nil {
		return auth.Claims{}, v.err
	}
	if token != v.token {
		return auth.Claims{}, auth.ErrInvalidToken
	}
	return v.claims, nil
}

func withAuthorization(ctx context.Context, values ...string) context.Context {
	md := metadata.MD{}
	for _, v := range values {
		md.Append("authorization", v)
	}
	return metadata.NewIncomingContext(ctx, md)
}

func TestUnaryAuth(t *testing.T) {
	t.Parallel()

	claims := auth.Claims{UserID: "user-1", Username: "alice", ExpiresAt: time.Now().Add(time.Hour)}
	verifier := fakeVerifier{token: "good", claims: claims}
	public := publicMethods{"/auth.v1.AuthService/Login": true}

	tests := []struct {
		name     string
		method   string
		auth     []string
		verifier TokenVerifier
		wantCode codes.Code
	}{
		{name: "valid token", method: "/test.v1.Private/Do", auth: []string{"Bearer good"}, wantCode: codes.OK},
		{name: "scheme is case-insensitive", method: "/test.v1.Private/Do", auth: []string{"bearer good"}, wantCode: codes.OK},
		{name: "public method without token", method: "/auth.v1.AuthService/Login", wantCode: codes.OK},
		{name: "no metadata", method: "/test.v1.Private/Do", wantCode: codes.Unauthenticated},
		{name: "wrong scheme", method: "/test.v1.Private/Do", auth: []string{"Basic good"}, wantCode: codes.Unauthenticated},
		{name: "empty token", method: "/test.v1.Private/Do", auth: []string{"Bearer "}, wantCode: codes.Unauthenticated},
		{name: "two tokens", method: "/test.v1.Private/Do", auth: []string{"Bearer good", "Bearer good"}, wantCode: codes.Unauthenticated},
		{name: "unknown token", method: "/test.v1.Private/Do", auth: []string{"Bearer forged"}, wantCode: codes.Unauthenticated},
		{name: "service name prefix is not public", method: "/auth.v1.AuthServiceX/Login", wantCode: codes.Unauthenticated},
		{name: "unlisted method of a public service", method: "/auth.v1.AuthService/ChangePassword", wantCode: codes.Unauthenticated},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := withAuthorization(t.Context(), tt.auth...)
			var got auth.Claims
			var gotOK bool
			handler := func(ctx context.Context, _ any) (any, error) {
				got, gotOK = auth.FromContext(ctx)
				return nil, nil
			}

			_, err := unaryAuth(verifier, public)(ctx, nil, &grpc.UnaryServerInfo{FullMethod: tt.method}, handler)
			if code := status.Code(err); code != tt.wantCode {
				t.Fatalf("unaryAuth() code = %v, want %v (err = %v)", code, tt.wantCode, err)
			}
			if tt.wantCode == codes.OK && len(tt.auth) > 0 && (!gotOK || got != claims) {
				t.Errorf("handler claims = %+v, %v; want %+v", got, gotOK, claims)
			}
		})
	}
}

func TestUnaryAuthReportsExpiredTokenReason(t *testing.T) {
	t.Parallel()

	verifier := fakeVerifier{err: auth.ErrTokenExpired}
	ctx := withAuthorization(t.Context(), "Bearer old")
	info := &grpc.UnaryServerInfo{FullMethod: "/test.v1.Private/Do"}

	_, err := unaryAuth(verifier, nil)(ctx, nil, info, func(context.Context, any) (any, error) {
		t.Error("handler called with an expired token")
		return nil, nil
	})
	assertTokenExpired(t, err)
}

func TestStreamAuthEndsStreamAtTokenExpiry(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		exp := start.Add(time.Hour)
		verifier := fakeVerifier{token: "good", claims: auth.Claims{UserID: "user-1", Username: "alice", ExpiresAt: exp}}
		ss := fakeStream{ctx: withAuthorization(t.Context(), "Bearer good")}
		info := &grpc.StreamServerInfo{FullMethod: "/chat.v1.ChatService/Connect"}

		var endedAt time.Time
		var cause error
		handler := func(_ any, ss grpc.ServerStream) error {
			ctx := ss.Context()
			if c, ok := auth.FromContext(ctx); !ok || c.UserID != "user-1" {
				t.Errorf("stream context claims = %+v, %v; want user-1", c, ok)
			}
			<-ctx.Done()
			endedAt, cause = time.Now(), context.Cause(ctx)
			return ctx.Err()
		}

		err := streamAuth(verifier, nil)(nil, ss, info, handler)

		if !endedAt.Equal(exp) {
			t.Errorf("stream context ended %v after start, want exactly %v", endedAt.Sub(start), time.Hour)
		}
		if !errors.Is(cause, errTokenExpired) {
			t.Errorf("context.Cause() = %v, want errTokenExpired", cause)
		}
		assertTokenExpired(t, err)
	})
}

func TestStreamAuthKeepsOtherCancellations(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		verifier := fakeVerifier{token: "good", claims: auth.Claims{UserID: "user-1", ExpiresAt: time.Now().Add(time.Hour)}}
		// The client goes away after a minute, long before the token expires.
		ctx, cancel := context.WithTimeout(withAuthorization(t.Context(), "Bearer good"), time.Minute)
		defer cancel()
		info := &grpc.StreamServerInfo{FullMethod: "/chat.v1.ChatService/Connect"}

		err := streamAuth(verifier, nil)(nil, fakeStream{ctx: ctx}, info, func(_ any, ss grpc.ServerStream) error {
			<-ss.Context().Done()
			return ss.Context().Err()
		})

		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("streamAuth() = %v, want the handler's context.DeadlineExceeded", err)
		}
	})
}

func TestStreamAuthKeepsHandlerErrorsAtExpiry(t *testing.T) {
	t.Parallel()

	internal := status.Error(codes.Internal, "room failed")
	tests := []struct {
		name    string
		result  func(ctx context.Context) error
		wantErr error
	}{
		{name: "nil", result: func(context.Context) error { return nil }, wantErr: errExpiredToken},
		{name: "context error", result: func(ctx context.Context) error { return ctx.Err() }, wantErr: errExpiredToken},
		{
			name:    "context error as status",
			result:  func(ctx context.Context) error { return status.FromContextError(ctx.Err()).Err() },
			wantErr: errExpiredToken,
		},
		{name: "handler's own error", result: func(context.Context) error { return internal }, wantErr: internal},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				verifier := fakeVerifier{token: "good", claims: auth.Claims{UserID: "user-1", ExpiresAt: time.Now().Add(time.Hour)}}
				ss := fakeStream{ctx: withAuthorization(t.Context(), "Bearer good")}
				info := &grpc.StreamServerInfo{FullMethod: "/chat.v1.ChatService/Connect"}

				err := streamAuth(verifier, nil)(nil, ss, info, func(_ any, ss grpc.ServerStream) error {
					<-ss.Context().Done()
					return tt.result(ss.Context())
				})

				if !errors.Is(err, tt.wantErr) {
					t.Errorf("streamAuth() = %v, want %v", err, tt.wantErr)
				}
			})
		})
	}
}

func TestStreamAuthRejectsMissingToken(t *testing.T) {
	t.Parallel()

	info := &grpc.StreamServerInfo{FullMethod: "/chat.v1.ChatService/Connect"}
	err := streamAuth(fakeVerifier{}, nil)(nil, fakeStream{ctx: t.Context()}, info, func(any, grpc.ServerStream) error {
		t.Error("handler called without a token")
		return nil
	})
	if code := status.Code(err); code != codes.Unauthenticated {
		t.Errorf("streamAuth() code = %v, want %v", code, codes.Unauthenticated)
	}
}

func TestLoggingRecordsAuthenticatedUser(t *testing.T) {
	t.Parallel()

	logger, logs := newTestLogger()
	verifier := fakeVerifier{token: "good", claims: auth.Claims{UserID: "user-1", ExpiresAt: time.Now().Add(time.Hour)}}
	// Logging wraps auth as in the server's chain; the holder logging puts in
	// the context is how the user id reaches its record.
	info := &grpc.UnaryServerInfo{FullMethod: "/test.v1.Private/Do"}
	ctx := withAuthorization(t.Context(), "Bearer good")
	_, err := unaryLogging(logger)(ctx, nil, info, func(ctx context.Context, req any) (any, error) {
		return unaryAuth(verifier, nil)(ctx, req, info, func(context.Context, any) (any, error) { return nil, nil })
	})
	if err != nil {
		t.Fatalf("call error = %v", err)
	}

	if rec := lastRecord(t, logs); rec["user_id"] != "user-1" {
		t.Errorf("log record = %v, want user_id user-1", rec)
	}
}

func assertTokenExpired(t *testing.T, err error) {
	t.Helper()
	st := status.Convert(err)
	if st.Code() != codes.Unauthenticated {
		t.Fatalf("code = %v, want %v (err = %v)", st.Code(), codes.Unauthenticated, err)
	}
	want := []any{&errdetails.ErrorInfo{Reason: "TOKEN_EXPIRED", Domain: "chat.v1"}}
	if diff := cmp.Diff(want, st.Details(), protocmp.Transform()); diff != "" {
		t.Errorf("status details mismatch (-want +got):\n%s", diff)
	}
}
