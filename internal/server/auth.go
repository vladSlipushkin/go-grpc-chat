package server

import (
	"context"
	"errors"
	"strings"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/vladSlipushkin/go-grpc-chat/internal/auth"
)

// TokenVerifier checks an access token and returns its claims.
// *auth.Tokens implements it.
type TokenVerifier interface {
	Verify(token string) (auth.Claims, error)
}

// errTokenExpired is the cancellation cause of a stream whose access token
// expired, telling expiry apart from the client cancelling or from a
// deadline the client set itself.
var errTokenExpired = errors.New("access token expired")

var (
	errMissingToken = status.Error(codes.Unauthenticated, "missing bearer token in authorization metadata")
	errInvalidToken = status.Error(codes.Unauthenticated, "invalid access token")
	// errExpiredToken carries the TOKEN_EXPIRED reason from ADR-0003, so a
	// client knows to log in again rather than treat its token as forged.
	errExpiredToken = mustErrorInfo(codes.Unauthenticated, "access token expired", "TOKEN_EXPIRED")
)

// errorDomain is the ErrorInfo domain of the chat API (ADR-0003).
const errorDomain = "chat.v1"

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

// publicMethods lists full method names, such as
// "/auth.v1.AuthService/Login", callable without an access token. Listing
// methods rather than services keeps a method added to a public service
// protected until it is listed too.
type publicMethods map[string]bool

func (p publicMethods) allows(fullMethod string) bool {
	return p[fullMethod]
}

// unaryAuth rejects calls to non-public methods that lack a valid access
// token and passes the token's claims to the handler in the context.
func unaryAuth(verifier TokenVerifier, public publicMethods) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if public.allows(info.FullMethod) {
			return handler(ctx, req)
		}
		claims, err := authenticate(ctx, verifier)
		if err != nil {
			return nil, err
		}
		return handler(auth.NewContext(ctx, claims), req)
	}
}

// streamAuth is unaryAuth for streams. The token is checked once, when the
// stream opens; the stream's context then ends when the token expires, and
// the stream ends with UNAUTHENTICATED and reason TOKEN_EXPIRED (ADR-0003).
//
// Expiry only cancels the context: RecvMsg and SendMsg blocked on the
// transport do not return until the handler does. Handlers must therefore
// watch ss.Context().Done() and return when it fires, without first waiting
// for a goroutine blocked in RecvMsg; that goroutine is released once the
// handler has returned and the stream is closed.
func streamAuth(verifier TokenVerifier, public publicMethods) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if public.allows(info.FullMethod) {
			return handler(srv, ss)
		}
		claims, err := authenticate(ss.Context(), verifier)
		if err != nil {
			return err
		}

		ctx, cancel := context.WithDeadlineCause(auth.NewContext(ss.Context(), claims), claims.ExpiresAt, errTokenExpired)
		defer cancel()
		err = handler(srv, &serverStream{ServerStream: ss, ctx: ctx})
		// A parent cancellation or a shorter client deadline keeps its own
		// cause, so only a real expiry is reported as one.
		if errors.Is(context.Cause(ctx), errTokenExpired) && endedByDeadline(err) {
			return errExpiredToken
		}
		return err
	}
}

// endedByDeadline reports whether a stream handler's result says no more
// than that its context ended: nil, the context's error, or its status
// equivalent. Any other error, such as INTERNAL, is the handler's own
// outcome and reaches the client unchanged even if the token expired too.
func endedByDeadline(err error) bool {
	return err == nil ||
		errors.Is(err, context.DeadlineExceeded) ||
		status.Code(err) == codes.DeadlineExceeded
}

func authenticate(ctx context.Context, verifier TokenVerifier) (auth.Claims, error) {
	token, ok := bearerToken(ctx)
	if !ok {
		return auth.Claims{}, errMissingToken
	}
	claims, err := verifier.Verify(token)
	switch {
	case errors.Is(err, auth.ErrTokenExpired):
		return auth.Claims{}, errExpiredToken
	case err != nil:
		return auth.Claims{}, errInvalidToken
	}
	if ci := callInfoFrom(ctx); ci != nil {
		ci.userID = claims.UserID
	}
	return claims, nil
}

// bearerToken extracts the token from "authorization: Bearer <token>"
// metadata. The scheme is case-insensitive (RFC 9110, section 11.1).
func bearerToken(ctx context.Context) (string, bool) {
	values := metadata.ValueFromIncomingContext(ctx, "authorization")
	if len(values) != 1 {
		return "", false
	}
	scheme, token, ok := strings.Cut(values[0], " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") || token == "" {
		return "", false
	}
	return token, true
}
