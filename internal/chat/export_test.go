package chat

// RoomCount reports how many rooms the hub is running.
func RoomCount(h *Hub) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.byID)
}
