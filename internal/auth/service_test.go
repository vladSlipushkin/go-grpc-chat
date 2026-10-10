package auth_test

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/goleak"
	"golang.org/x/crypto/bcrypt"

	"github.com/vladSlipushkin/go-grpc-chat/internal/auth"
	"github.com/vladSlipushkin/go-grpc-chat/internal/memory"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func newService(t *testing.T) *auth.Service {
	t.Helper()
	tokens, err := auth.NewTokens([]byte("0123456789abcdef0123456789abcdef"), time.Hour)
	if err != nil {
		t.Fatalf("NewTokens() error = %v", err)
	}
	svc, err := auth.NewService(memory.NewUsers(), tokens, auth.Options{BcryptCost: bcrypt.MinCost})
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	return svc
}

func TestRegisterThenLogin(t *testing.T) {
	t.Parallel()

	svc := newService(t)
	u, err := svc.Register(t.Context(), "alice", "correct horse")
	if err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if u.ID == "" || u.Username != "alice" {
		t.Errorf("Register() = %+v, want an id and username alice", u)
	}

	token, err := svc.Login(t.Context(), "alice", "correct horse")
	if err != nil {
		t.Fatalf("Login() error = %v", err)
	}
	if token.Value == "" || token.ExpiresAt.IsZero() {
		t.Errorf("Login() = %+v, want a token with an expiry", token)
	}
}

func TestRegisterRejectsTakenUsername(t *testing.T) {
	t.Parallel()

	svc := newService(t)
	if _, err := svc.Register(t.Context(), "alice", "correct horse"); err != nil {
		t.Fatalf("first Register() error = %v", err)
	}
	if _, err := svc.Register(t.Context(), "alice", "another password"); !errors.Is(err, auth.ErrUsernameTaken) {
		t.Errorf("second Register() = %v, want ErrUsernameTaken", err)
	}
}

func TestConcurrentRegistrationOfOneNameHasOneWinner(t *testing.T) {
	t.Parallel()

	svc := newService(t)
	const attempts = 16
	var succeeded, taken atomic.Int32
	var wg sync.WaitGroup
	for range attempts {
		wg.Go(func() {
			_, err := svc.Register(t.Context(), "alice", "correct horse")
			switch {
			case err == nil:
				succeeded.Add(1)
			case errors.Is(err, auth.ErrUsernameTaken):
				taken.Add(1)
			default:
				t.Errorf("Register() = %v, want nil or ErrUsernameTaken", err)
			}
		})
	}
	wg.Wait()

	if succeeded.Load() != 1 || taken.Load() != attempts-1 {
		t.Errorf("%d concurrent registrations: %d succeeded, %d taken; want 1 and %d",
			attempts, succeeded.Load(), taken.Load(), attempts-1)
	}
}

func TestLoginFailuresAreIndistinguishable(t *testing.T) {
	t.Parallel()

	svc := newService(t)
	if _, err := svc.Register(t.Context(), "alice", "correct horse"); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	tests := []struct {
		name     string
		username string
		password string
	}{
		{name: "wrong password", username: "alice", password: "wrong horse"},
		{name: "unknown user", username: "bob", password: "correct horse"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if _, err := svc.Login(t.Context(), tt.username, tt.password); !errors.Is(err, auth.ErrInvalidCredentials) {
				t.Errorf("Login(%q) = %v, want ErrInvalidCredentials", tt.username, err)
			}
		})
	}
}

func TestClaimsRoundTripThroughContext(t *testing.T) {
	t.Parallel()

	if _, ok := auth.FromContext(t.Context()); ok {
		t.Error("FromContext() on a bare context reported claims")
	}
	want := auth.Claims{UserID: "user-1", Username: "alice"}
	got, ok := auth.FromContext(auth.NewContext(t.Context(), want))
	if !ok || got != want {
		t.Errorf("FromContext() = %+v, %v; want %+v, true", got, ok, want)
	}
}
