package auth

import (
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// MinSecretBytes is the shortest accepted signing secret: HS256 needs a key
// at least as long as its 256-bit output (RFC 7518, section 3.2).
const MinSecretBytes = 32

var (
	// ErrInvalidToken reports a token that is malformed, not signed by this
	// server, or signed with another algorithm.
	ErrInvalidToken = errors.New("invalid access token")
	// ErrTokenExpired reports a valid token whose expiry has passed.
	ErrTokenExpired = errors.New("access token expired")
)

// signingMethod is the only accepted algorithm. Pinning it rejects tokens
// with "alg": "none" or any algorithm an attacker picks.
var signingMethod = jwt.SigningMethodHS256

// Claims identify the user an access token was issued to.
type Claims struct {
	UserID    string
	Username  string
	ExpiresAt time.Time
}

type tokenClaims struct {
	Username string `json:"username"`
	jwt.RegisteredClaims
}

// Tokens issues and verifies access tokens signed with HS256. The issuing
// server is also the only verifier, so a shared secret is enough and no key
// distribution is needed (ADR-0007).
type Tokens struct {
	secret []byte
	ttl    time.Duration
	parser *jwt.Parser
}

// NewTokens returns Tokens that sign with secret and issue tokens valid for
// ttl.
func NewTokens(secret []byte, ttl time.Duration) (*Tokens, error) {
	if len(secret) < MinSecretBytes {
		return nil, fmt.Errorf("token secret is %d bytes, need at least %d", len(secret), MinSecretBytes)
	}
	if ttl <= 0 {
		return nil, fmt.Errorf("token lifetime must be positive, got %s", ttl)
	}
	return &Tokens{
		secret: secret,
		ttl:    ttl,
		parser: jwt.NewParser(
			jwt.WithValidMethods([]string{signingMethod.Alg()}),
			jwt.WithExpirationRequired(),
			jwt.WithIssuedAt(),
		),
	}, nil
}

// Issue returns a signed token for the user and the time it expires.
func (t *Tokens) Issue(userID, username string) (string, time.Time, error) {
	// JWT times have one-second precision. Truncating here keeps the
	// returned expiry equal to the one in the token, which is also where a
	// stream's deadline is set.
	now := time.Now().Truncate(time.Second)
	exp := now.Add(t.ttl)
	claims := tokenClaims{
		Username: username,
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   userID,
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(exp),
		},
	}
	token, err := jwt.NewWithClaims(signingMethod, claims).SignedString(t.secret)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("sign token: %w", err)
	}
	return token, exp, nil
}

// Verify checks the token's signature, algorithm and expiry and returns its
// claims. A token is expired from the instant of its expiry on; there is no
// clock-skew leeway (ADR-0003).
func (t *Tokens) Verify(token string) (Claims, error) {
	var claims tokenClaims
	_, err := t.parser.ParseWithClaims(token, &claims, func(*jwt.Token) (any, error) {
		return t.secret, nil
	})
	switch {
	case errors.Is(err, jwt.ErrTokenExpired):
		return Claims{}, ErrTokenExpired
	case err != nil:
		return Claims{}, fmt.Errorf("%w: %w", ErrInvalidToken, err)
	case claims.Subject == "" || claims.Username == "":
		return Claims{}, fmt.Errorf("%w: missing subject or username", ErrInvalidToken)
	}
	return Claims{
		UserID:    claims.Subject,
		Username:  claims.Username,
		ExpiresAt: claims.ExpiresAt.Time,
	}, nil
}
