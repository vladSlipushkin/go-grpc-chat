package chat_test

import (
	"fmt"
	"log/slog"
	"testing"

	"github.com/vladSlipushkin/go-grpc-chat/internal/auth"
	"github.com/vladSlipushkin/go-grpc-chat/internal/chat"
)

// BenchmarkSend measures one message fanned out to every member of a room,
// the cost ADR-0008 weighs against a member limit.
func BenchmarkSend(b *testing.B) {
	for _, members := range []int{10, 1000} {
		b.Run(fmt.Sprintf("members=%d", members), func(b *testing.B) {
			h := chat.NewHub(slog.New(slog.DiscardHandler))
			defer h.Close()
			sessions := make([]*chat.Session, members)
			for i := range sessions {
				ctx := auth.NewContext(b.Context(), auth.Claims{UserID: fmt.Sprint(i), Username: fmt.Sprint(i)})
				s, err := h.Connect(ctx, func(error) {})
				if err != nil {
					b.Fatal(err)
				}
				if err := s.Join(ctx, "general"); err != nil {
					b.Fatal(err)
				}
				sessions[i] = s
			}

			sent := 0
			for b.Loop() {
				if _, err := sessions[0].Send(b.Context(), "general", "hello"); err != nil {
					b.Fatal(err)
				}
				// Empty the queues before they fill up, off the clock.
				if sent++; sent == chat.QueueLen {
					b.StopTimer()
					for _, s := range sessions {
						for range sent {
							<-s.Events()
						}
					}
					sent = 0
					b.StartTimer()
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(members), "ns/member")
		})
	}
}
