package auth

import (
	"encoding/base64"
	"errors"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/go-cmp/cmp"
)

var testSecret = []byte("0123456789abcdef0123456789abcdef")

func newTestTokens(t *testing.T, ttl time.Duration) *Tokens {
	t.Helper()
	tokens, err := NewTokens(testSecret, ttl)
	if err != nil {
		t.Fatalf("NewTokens() error = %v", err)
	}
	return tokens
}

func TestTokenRoundTrip(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		tokens := newTestTokens(t, time.Hour)

		token, exp, err := tokens.Issue("user-1", "alice")
		if err != nil {
			t.Fatalf("Issue() error = %v", err)
		}
		if want := time.Now().Add(time.Hour); !exp.Equal(want) {
			t.Errorf("Issue() expiry = %v, want %v", exp, want)
		}

		got, err := tokens.Verify(token)
		if err != nil {
			t.Fatalf("Verify() error = %v", err)
		}
		want := Claims{UserID: "user-1", Username: "alice", ExpiresAt: exp}
		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("Verify() claims mismatch (-want +got):\n%s", diff)
		}
	})
}

func TestTokenExpiresExactlyAtExp(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		tokens := newTestTokens(t, time.Hour)
		token, _, err := tokens.Issue("user-1", "alice")
		if err != nil {
			t.Fatalf("Issue() error = %v", err)
		}

		time.Sleep(time.Hour - time.Nanosecond)
		if _, err := tokens.Verify(token); err != nil {
			t.Errorf("Verify() just before expiry = %v, want nil", err)
		}

		time.Sleep(time.Nanosecond)
		if _, err := tokens.Verify(token); !errors.Is(err, ErrTokenExpired) {
			t.Errorf("Verify() at expiry = %v, want ErrTokenExpired", err)
		}
	})
}

func TestTokenRejectsForgeries(t *testing.T) {
	t.Parallel()

	tokens := newTestTokens(t, time.Hour)
	valid, _, err := tokens.Issue("user-1", "alice")
	if err != nil {
		t.Fatalf("Issue() error = %v", err)
	}
	claims := tokenClaims{
		Username: "alice",
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   "user-1",
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
		},
	}

	tests := []struct {
		name  string
		token string
	}{
		{name: "empty", token: ""},
		{name: "garbage", token: "not.a.token"},
		{name: "tampered payload", token: withPayload(t, valid, `{"sub":"user-2","username":"mallory","exp":4102444800}`)},
		{name: "another secret", token: sign(t, jwt.SigningMethodHS256, claims, []byte("another-secret-of-at-least-32-bytes"))},
		{name: "alg none", token: sign(t, jwt.SigningMethodNone, claims, jwt.UnsafeAllowNoneSignatureType)},
		{name: "HS512 with the same secret", token: sign(t, jwt.SigningMethodHS512, claims, testSecret)},
		{name: "no expiry", token: sign(t, jwt.SigningMethodHS256, tokenClaims{
			Username:         "alice",
			RegisteredClaims: jwt.RegisteredClaims{Subject: "user-1"},
		}, testSecret)},
		{name: "no subject", token: sign(t, jwt.SigningMethodHS256, tokenClaims{
			Username:         "alice",
			RegisteredClaims: jwt.RegisteredClaims{ExpiresAt: claims.ExpiresAt},
		}, testSecret)},
		{name: "issued in the future", token: sign(t, jwt.SigningMethodHS256, tokenClaims{
			Username: "alice",
			RegisteredClaims: jwt.RegisteredClaims{
				Subject:   "user-1",
				IssuedAt:  jwt.NewNumericDate(time.Now().Add(time.Minute)),
				ExpiresAt: claims.ExpiresAt,
			},
		}, testSecret)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if _, err := tokens.Verify(tt.token); !errors.Is(err, ErrInvalidToken) {
				t.Errorf("Verify() = %v, want ErrInvalidToken", err)
			}
		})
	}
}

func TestNewTokensRejectsWeakSettings(t *testing.T) {
	t.Parallel()

	if _, err := NewTokens(testSecret[:MinSecretBytes-1], time.Hour); err == nil {
		t.Errorf("NewTokens() with a %d-byte secret = nil error, want an error", MinSecretBytes-1)
	}
	if _, err := NewTokens(testSecret, 0); err == nil {
		t.Error("NewTokens() with zero lifetime = nil error, want an error")
	}
}

func sign(t *testing.T, method jwt.SigningMethod, claims jwt.Claims, key any) string {
	t.Helper()
	token, err := jwt.NewWithClaims(method, claims).SignedString(key)
	if err != nil {
		t.Fatalf("sign test token: %v", err)
	}
	return token
}

// withPayload replaces the payload of token and keeps its signature.
func withPayload(t *testing.T, token, payload string) string {
	t.Helper()
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatalf("token has %d parts, want 3", len(parts))
	}
	parts[1] = base64.RawURLEncoding.EncodeToString([]byte(payload))
	return strings.Join(parts, ".")
}
