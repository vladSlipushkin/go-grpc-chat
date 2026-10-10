package auth

import "context"

type claimsKey struct{}

// NewContext returns a copy of ctx that carries the claims of the caller's
// verified access token.
func NewContext(ctx context.Context, c Claims) context.Context {
	return context.WithValue(ctx, claimsKey{}, c)
}

// FromContext returns the claims stored in ctx by NewContext.
func FromContext(ctx context.Context) (Claims, bool) {
	c, ok := ctx.Value(claimsKey{}).(Claims)
	return c, ok
}
