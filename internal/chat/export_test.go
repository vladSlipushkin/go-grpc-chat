package chat

import "context"

// RoomCount reports how many rooms the hub is running.
func RoomCount(h *Hub) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.rooms)
}

// Crash makes the goroutine of a room s is in panic, as a bug in it would.
func Crash(ctx context.Context, s *Session, roomID string) error {
	r := s.rooms[roomID]
	return r.do(ctx, func() { panic("test crash") })
}
