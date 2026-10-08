package config_test

import (
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/vladSlipushkin/go-grpc-chat/internal/config"
)

func TestLoad(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		env  map[string]string
		want config.Config
	}{
		{
			name: "defaults",
			want: config.Config{
				GRPCAddr:        ":50051",
				LogLevel:        slog.LevelInfo,
				ShutdownTimeout: 10 * time.Second,
			},
		},
		{
			name: "all set",
			env: map[string]string{
				"CHAT_GRPC_ADDR":        "127.0.0.1:9000",
				"CHAT_LOG_LEVEL":        "debug",
				"CHAT_SHUTDOWN_TIMEOUT": "3s",
				"CHAT_REFLECTION":       "true",
			},
			want: config.Config{
				GRPCAddr:        "127.0.0.1:9000",
				LogLevel:        slog.LevelDebug,
				ShutdownTimeout: 3 * time.Second,
				Reflection:      true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := config.Load(lookupFrom(tt.env))
			if err != nil {
				t.Fatalf("Load() error = %v, want nil", err)
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("Load() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestLoadRejectsInvalidValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		env     map[string]string
		wantErr []string
	}{
		{
			name:    "empty address",
			env:     map[string]string{"CHAT_GRPC_ADDR": ""},
			wantErr: []string{"CHAT_GRPC_ADDR: must not be empty"},
		},
		{
			name:    "unknown log level",
			env:     map[string]string{"CHAT_LOG_LEVEL": "verbose"},
			wantErr: []string{"CHAT_LOG_LEVEL"},
		},
		{
			name:    "unparsable timeout",
			env:     map[string]string{"CHAT_SHUTDOWN_TIMEOUT": "ten seconds"},
			wantErr: []string{"CHAT_SHUTDOWN_TIMEOUT"},
		},
		{
			name:    "zero timeout",
			env:     map[string]string{"CHAT_SHUTDOWN_TIMEOUT": "0s"},
			wantErr: []string{"CHAT_SHUTDOWN_TIMEOUT: must be positive"},
		},
		{
			name:    "reflection is not a bool",
			env:     map[string]string{"CHAT_REFLECTION": "yes please"},
			wantErr: []string{"CHAT_REFLECTION"},
		},
		{
			name: "every invalid variable is reported",
			env: map[string]string{
				"CHAT_LOG_LEVEL":        "loud",
				"CHAT_SHUTDOWN_TIMEOUT": "-1s",
			},
			wantErr: []string{"CHAT_LOG_LEVEL", "CHAT_SHUTDOWN_TIMEOUT"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := config.Load(lookupFrom(tt.env))
			if err == nil {
				t.Fatal("Load() error = nil, want an error")
			}
			for _, want := range tt.wantErr {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("Load() error = %q, want it to mention %q", err, want)
				}
			}
		})
	}
}

func lookupFrom(env map[string]string) config.LookupFunc {
	return func(key string) (string, bool) {
		v, ok := env[key]
		return v, ok
	}
}
