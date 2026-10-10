package auth

import (
	"errors"
	"fmt"

	"golang.org/x/crypto/bcrypt"
	"golang.org/x/text/unicode/norm"
)

// maxPasswordBytes is how much of its input bcrypt reads; longer passwords
// are rejected rather than silently truncated.
const maxPasswordBytes = 72

// ErrPasswordTooLong reports a password longer than 72 bytes after
// normalization. It can only happen for passwords that NFKC expands, since
// the contract limits the raw input to 72 bytes.
var ErrPasswordTooLong = errors.New("password exceeds 72 bytes after normalization")

// normalizePassword returns the NFKC form of password, so the same visible
// password hashes the same from any device or input method (ADR-0005).
func normalizePassword(password string) []byte {
	return norm.NFKC.Bytes([]byte(password))
}

func hashPassword(password string, cost int) ([]byte, error) {
	normalized := normalizePassword(password)
	if len(normalized) > maxPasswordBytes {
		return nil, ErrPasswordTooLong
	}
	hash, err := bcrypt.GenerateFromPassword(normalized, cost)
	if err != nil {
		return nil, fmt.Errorf("hash password: %w", err)
	}
	return hash, nil
}

// comparePassword reports whether password matches hash, returning
// bcrypt.ErrMismatchedHashAndPassword if it does not.
func comparePassword(hash []byte, password string) error {
	normalized := normalizePassword(password)
	if len(normalized) > maxPasswordBytes {
		// No stored hash was made from a password this long. Truncating
		// could match a registered password that is a prefix, so compare
		// against the hash anyway only to spend the same time, and fail.
		_ = bcrypt.CompareHashAndPassword(hash, normalized[:maxPasswordBytes])
		return bcrypt.ErrMismatchedHashAndPassword
	}
	return bcrypt.CompareHashAndPassword(hash, normalized)
}
