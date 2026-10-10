package auth

import (
	"errors"
	"strings"
	"testing"

	"golang.org/x/crypto/bcrypt"
	"golang.org/x/text/unicode/norm"
)

func TestPasswordMatchesAcrossNormalizationForms(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		register string
		login    string
	}{
		{name: "NFC and NFD", register: "café-pâss", login: "café-pâss"},
		{name: "full-width and ASCII", register: "ｐａｓｓｗｏｒｄ", login: "password"},
		{name: "ligature and letters", register: "ﬁrefly-pass", login: "firefly-pass"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if tt.register == tt.login {
				t.Fatal("test passwords must differ byte-wise")
			}
			hash, err := hashPassword(tt.register, bcrypt.MinCost)
			if err != nil {
				t.Fatalf("hashPassword(%q) error = %v", tt.register, err)
			}
			if err := comparePassword(hash, tt.login); err != nil {
				t.Errorf("comparePassword(hash of %q, %q) = %v, want nil", tt.register, tt.login, err)
			}
		})
	}
}

func TestPasswordRejectsDifferentPassword(t *testing.T) {
	t.Parallel()

	hash, err := hashPassword("correct horse", bcrypt.MinCost)
	if err != nil {
		t.Fatalf("hashPassword() error = %v", err)
	}
	if err := comparePassword(hash, "correct horsE"); !errors.Is(err, bcrypt.ErrMismatchedHashAndPassword) {
		t.Errorf("comparePassword() with another password = %v, want ErrMismatchedHashAndPassword", err)
	}
}

func TestPasswordExpandingBeyond72BytesIsRejected(t *testing.T) {
	t.Parallel()

	// U+FDFA is one code point of 3 bytes that NFKC expands into 18 code
	// points, so this password fits the contract's 72-byte limit as sent and
	// exceeds it after normalization.
	password := strings.Repeat("ﷺ", 3) + "password"
	if len(password) > maxPasswordBytes || len(norm.NFKC.String(password)) <= maxPasswordBytes {
		t.Fatalf("test password: %d bytes raw, %d normalized; want <= %d raw and more normalized",
			len(password), len(norm.NFKC.String(password)), maxPasswordBytes)
	}

	if _, err := hashPassword(password, bcrypt.MinCost); !errors.Is(err, ErrPasswordTooLong) {
		t.Errorf("hashPassword() = %v, want ErrPasswordTooLong", err)
	}
}

func TestPasswordLongerThanStoredPrefixDoesNotMatch(t *testing.T) {
	t.Parallel()

	// U+33C2 SQUARE AM is 3 bytes and normalizes to "a.m.", 4 bytes. The
	// login fits the contract as sent, and its first 72 normalized bytes are
	// exactly the stored password.
	login := strings.Repeat("㏂", 23)
	stored := strings.Repeat("a.m.", 18)
	if len(login) > maxPasswordBytes || !strings.HasPrefix(norm.NFKC.String(login), stored) {
		t.Fatalf("test passwords: login is %d bytes, want <= %d with the stored password as normalized prefix",
			len(login), maxPasswordBytes)
	}

	hash, err := hashPassword(stored, bcrypt.MinCost)
	if err != nil {
		t.Fatalf("hashPassword() error = %v", err)
	}
	if err := comparePassword(hash, login); !errors.Is(err, bcrypt.ErrMismatchedHashAndPassword) {
		t.Errorf("comparePassword() = %v, want ErrMismatchedHashAndPassword", err)
	}
}

func TestPasswordAtLimitAfterNormalizationIsAccepted(t *testing.T) {
	t.Parallel()

	password := strings.Repeat("a", maxPasswordBytes)
	if _, err := hashPassword(password, bcrypt.MinCost); err != nil {
		t.Errorf("hashPassword() of %d ASCII bytes = %v, want nil", maxPasswordBytes, err)
	}
}
