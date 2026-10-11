// Package auth registers users, checks their passwords and issues and
// verifies access tokens. Design decisions are in ADR-0005 and ADR-0007.
package auth

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"golang.org/x/crypto/bcrypt"
)

// DefaultBcryptCost is the bcrypt work factor for new password hashes.
const DefaultBcryptCost = 12

var (
	// ErrUsernameTaken reports a registration with a username that already
	// exists. UserStore implementations return it from Create.
	ErrUsernameTaken = errors.New("username already taken")
	// ErrUserNotFound is returned by UserStore.UserByName for an unknown
	// username.
	ErrUserNotFound = errors.New("user not found")
	// ErrInvalidCredentials reports a login with an unknown username or a
	// wrong password; callers cannot tell which.
	ErrInvalidCredentials = errors.New("invalid username or password")
)

// User is a registered account.
type User struct {
	ID           string
	Username     string
	PasswordHash []byte
	CreatedAt    time.Time
}

// UserStore persists users. Usernames are unique.
type UserStore interface {
	// Create stores u, or returns ErrUsernameTaken if its username exists.
	Create(ctx context.Context, u User) error
	// UserByName returns the user with username, or ErrUserNotFound.
	UserByName(ctx context.Context, username string) (User, error)
}

// Options configures a Service.
type Options struct {
	// BcryptCost is the bcrypt work factor; zero means DefaultBcryptCost.
	// Tests lower it to bcrypt.MinCost.
	BcryptCost int
}

// Service registers users and logs them in.
type Service struct {
	users  UserStore
	tokens *Tokens
	cost   int
	// dummyHash is compared against when the username is unknown, so a
	// login takes as long whether or not the user exists.
	dummyHash []byte
}

// NewService returns a Service that stores users in users and issues tokens
// with tokens.
func NewService(users UserStore, tokens *Tokens, opts Options) (*Service, error) {
	cost := opts.BcryptCost
	if cost == 0 {
		cost = DefaultBcryptCost
	}
	dummy, err := bcrypt.GenerateFromPassword([]byte("unused password for timing"), cost)
	if err != nil {
		return nil, fmt.Errorf("generate dummy hash: %w", err)
	}
	return &Service{users: users, tokens: tokens, cost: cost, dummyHash: dummy}, nil
}

// Register creates a user. Username format and password length are
// validated by the API contract; Register only enforces the limit that
// depends on normalization and returns ErrPasswordTooLong for it.
//
// Register and Login return ctx.Err() without hashing if ctx is already
// done: bcrypt cannot be interrupted, and a caller that has gone away is not
// worth a quarter of a second of CPU.
func (s *Service) Register(ctx context.Context, username, password string) (User, error) {
	if err := ctx.Err(); err != nil {
		return User{}, err
	}
	hash, err := hashPassword(password, s.cost)
	if err != nil {
		return User{}, err
	}
	// Hashing takes long enough for a short client deadline to pass; do not
	// create an account the caller will never hear about.
	if err := ctx.Err(); err != nil {
		return User{}, err
	}
	id, err := uuid.NewV7()
	if err != nil {
		return User{}, fmt.Errorf("generate user id: %w", err)
	}
	u := User{
		ID:           id.String(),
		Username:     username,
		PasswordHash: hash,
		CreatedAt:    time.Now(),
	}
	if err := s.users.Create(ctx, u); err != nil {
		return User{}, fmt.Errorf("create user: %w", err)
	}
	return u, nil
}

// Token is a signed access token and the time it expires.
type Token struct {
	Value     string
	ExpiresAt time.Time
}

// Login checks the credentials and issues an access token. An unknown
// username and a wrong password both return ErrInvalidCredentials after the
// same amount of bcrypt work.
func (s *Service) Login(ctx context.Context, username, password string) (Token, error) {
	u, err := s.users.UserByName(ctx, username)
	notFound := errors.Is(err, ErrUserNotFound)
	if err != nil && !notFound {
		return Token{}, fmt.Errorf("find user: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return Token{}, err
	}
	if notFound {
		_ = comparePassword(s.dummyHash, password)
		return Token{}, ErrInvalidCredentials
	}

	switch err := comparePassword(u.PasswordHash, password); {
	case errors.Is(err, bcrypt.ErrMismatchedHashAndPassword):
		return Token{}, ErrInvalidCredentials
	case err != nil:
		return Token{}, fmt.Errorf("compare password: %w", err)
	}

	value, exp, err := s.tokens.Issue(u.ID, u.Username)
	if err != nil {
		return Token{}, fmt.Errorf("issue token: %w", err)
	}
	return Token{Value: value, ExpiresAt: exp}, nil
}
