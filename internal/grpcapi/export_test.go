package grpcapi

import "time"

// The message rate limit, for tests that pin its behaviour.
var (
	MessageBurst    = messageBurst
	MessageInterval = time.Duration(float64(time.Second) / float64(messageRate))
)
