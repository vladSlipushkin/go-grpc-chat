package grpcapi

import (
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// messageInterval and messageBurst limit how fast one user posts, across
// all of the user's streams: a message every 200 ms on average, five a
// second, and ten at once (ADR-0008).
const (
	messageInterval = 200 * time.Millisecond
	messageBurst    = 10
)

// userLimiters gives each user one message rate limiter, shared by the
// user's open streams and dropped with the last of them (ADR-0008). It is
// safe for concurrent use.
type userLimiters struct {
	mu    sync.Mutex
	users map[string]*userLimiter
}

type userLimiter struct {
	limiter *rate.Limiter
	streams int
}

func newUserLimiters() *userLimiters {
	return &userLimiters{users: make(map[string]*userLimiter)}
}

// acquire returns the limiter of userID and counts the caller's stream as
// holding it until release is called, exactly once.
func (l *userLimiters) acquire(userID string) (limiter *rate.Limiter, release func()) {
	l.mu.Lock()
	defer l.mu.Unlock()
	u, ok := l.users[userID]
	if !ok {
		u = &userLimiter{limiter: rate.NewLimiter(rate.Every(messageInterval), messageBurst)}
		l.users[userID] = u
	}
	u.streams++
	return u.limiter, func() { l.release(userID, u) }
}

func (l *userLimiters) release(userID string, u *userLimiter) {
	l.mu.Lock()
	defer l.mu.Unlock()
	u.streams--
	if u.streams == 0 {
		delete(l.users, userID)
	}
}

// len returns the number of users with an open stream.
func (l *userLimiters) len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.users)
}
