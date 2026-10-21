package grpcapi

// The message rate limit, for tests that pin its behaviour.
const (
	MessageBurst    = messageBurst
	MessageInterval = messageInterval
)

// LimitedUsers returns the number of users srv keeps a rate limiter for.
func LimitedUsers(srv *ChatServer) int {
	return srv.limiters.len()
}
