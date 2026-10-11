// Package memory provides in-memory storage used until the PostgreSQL
// implementation exists. Everything it holds is lost on restart.
package memory

import (
	"context"
	"slices"
	"sync"

	"github.com/vladSlipushkin/go-grpc-chat/internal/auth"
)

// Users is an auth.UserStore backed by a map. It is safe for concurrent use.
// Password hashes are copied in and out, so callers never share a slice with
// the store, as they would not with a database.
type Users struct {
	mu     sync.RWMutex
	byName map[string]auth.User
}

var _ auth.UserStore = (*Users)(nil)

// NewUsers returns an empty store.
func NewUsers() *Users {
	return &Users{byName: make(map[string]auth.User)}
}

// Create stores u, or returns auth.ErrUsernameTaken if its username exists.
// The check and the insert happen under one lock, so of several concurrent
// registrations of one name exactly one succeeds.
func (s *Users) Create(_ context.Context, u auth.User) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.byName[u.Username]; ok {
		return auth.ErrUsernameTaken
	}
	u.PasswordHash = slices.Clone(u.PasswordHash)
	s.byName[u.Username] = u
	return nil
}

// UserByName returns the user with username, or auth.ErrUserNotFound.
func (s *Users) UserByName(_ context.Context, username string) (auth.User, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	u, ok := s.byName[username]
	if !ok {
		return auth.User{}, auth.ErrUserNotFound
	}
	u.PasswordHash = slices.Clone(u.PasswordHash)
	return u, nil
}
