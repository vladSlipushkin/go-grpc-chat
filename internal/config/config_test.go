package config_test

import (
	"log/slog"
	"maps"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/vladSlipushkin/go-grpc-chat/internal/config"
)

// testSecret is 32 bytes, the shortest accepted secret.
const testSecret = "0123456789abcdef0123456789abcdef"

func TestLoad(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		env  map[string]string
		want config.Config
	}{
		{
			name: "defaults",
			env:  map[string]string{"CHAT_JWT_SECRET": testSecret},
			want: config.Config{
				GRPCAddr:        ":50051",
				LogLevel:        slog.LevelInfo,
				ShutdownTimeout: 10 * time.Second,
				JWTSecret:       []byte(testSecret),
				TokenTTL:        time.Hour,
			},
		},
		{
			name: "all set",
			env: map[string]string{
				"CHAT_GRPC_ADDR":        "127.0.0.1:9000",
				"CHAT_LOG_LEVEL":        "debug",
				"CHAT_SHUTDOWN_TIMEOUT": "3s",
				"CHAT_REFLECTION":       "true",
				"CHAT_JWT_SECRET":       testSecret,
				"CHAT_TOKEN_TTL":        "15m",
				"CHAT_TLS_CERT_FILE":    "/certs/server.pem",
				"CHAT_TLS_KEY_FILE":     "/certs/server.key",
			},
			want: config.Config{
				GRPCAddr:        "127.0.0.1:9000",
				LogLevel:        slog.LevelDebug,
				ShutdownTimeout: 3 * time.Second,
				Reflection:      true,
				JWTSecret:       []byte(testSecret),
				TokenTTL:        15 * time.Minute,
				TLSCertFile:     "/certs/server.pem",
				TLSKeyFile:      "/certs/server.key",
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
			env:     withSecret(map[string]string{"CHAT_GRPC_ADDR": ""}),
			wantErr: []string{"CHAT_GRPC_ADDR: must not be empty"},
		},
		{
			name:    "unknown log level",
			env:     withSecret(map[string]string{"CHAT_LOG_LEVEL": "verbose"}),
			wantErr: []string{"CHAT_LOG_LEVEL"},
		},
		{
			name:    "unparsable timeout",
			env:     withSecret(map[string]string{"CHAT_SHUTDOWN_TIMEOUT": "ten seconds"}),
			wantErr: []string{"CHAT_SHUTDOWN_TIMEOUT"},
		},
		{
			name:    "zero timeout",
			env:     withSecret(map[string]string{"CHAT_SHUTDOWN_TIMEOUT": "0s"}),
			wantErr: []string{"CHAT_SHUTDOWN_TIMEOUT: must be positive"},
		},
		{
			name:    "reflection is not a bool",
			env:     withSecret(map[string]string{"CHAT_REFLECTION": "yes please"}),
			wantErr: []string{"CHAT_REFLECTION"},
		},
		{
			name:    "missing secret",
			env:     map[string]string{},
			wantErr: []string{"CHAT_JWT_SECRET: must be set"},
		},
		{
			name:    "short secret",
			env:     map[string]string{"CHAT_JWT_SECRET": testSecret[:31]},
			wantErr: []string{"CHAT_JWT_SECRET: must be at least 32 bytes"},
		},
		{
			name:    "zero token lifetime",
			env:     withSecret(map[string]string{"CHAT_TOKEN_TTL": "0s"}),
			wantErr: []string{"CHAT_TOKEN_TTL: must be positive"},
		},
		{
			name:    "certificate without key",
			env:     withSecret(map[string]string{"CHAT_TLS_CERT_FILE": "/certs/server.pem"}),
			wantErr: []string{"CHAT_TLS_CERT_FILE and CHAT_TLS_KEY_FILE: set both or neither"},
		},
		{
			name:    "key without certificate",
			env:     withSecret(map[string]string{"CHAT_TLS_KEY_FILE": "/certs/server.key"}),
			wantErr: []string{"CHAT_TLS_CERT_FILE and CHAT_TLS_KEY_FILE: set both or neither"},
		},
		{
			name: "every invalid variable is reported",
			env: withSecret(map[string]string{
				"CHAT_LOG_LEVEL":        "loud",
				"CHAT_SHUTDOWN_TIMEOUT": "-1s",
			}),
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
			if s := tt.env["CHAT_JWT_SECRET"]; s != "" && strings.Contains(err.Error(), s) {
				t.Errorf("Load() error = %q, want it not to echo the secret", err)
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

// withSecret adds a valid CHAT_JWT_SECRET to env, so a case fails only on
// the variables it is about.
func withSecret(env map[string]string) map[string]string {
	out := map[string]string{"CHAT_JWT_SECRET": testSecret}
	maps.Copy(out, env)
	return out
}

func TestLoadHealthcheck(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		env     map[string]string
		want    config.Healthcheck
		wantErr string
	}{
		// The check runs with the server's environment but needs none of
		// its secrets.
		{name: "defaults", env: map[string]string{}, want: config.Healthcheck{Addr: "127.0.0.1:50051"}},
		{name: "any address", env: map[string]string{"CHAT_GRPC_ADDR": "0.0.0.0:9000"}, want: config.Healthcheck{Addr: "127.0.0.1:9000"}},
		{name: "any IPv6 address", env: map[string]string{"CHAT_GRPC_ADDR": "[::]:9000"}, want: config.Healthcheck{Addr: "127.0.0.1:9000"}},
		{name: "one address", env: map[string]string{"CHAT_GRPC_ADDR": "10.0.0.7:9000"}, want: config.Healthcheck{Addr: "10.0.0.7:9000"}},
		{
			name: "TLS",
			env:  map[string]string{"CHAT_TLS_CERT_FILE": "/certs/server.pem", "CHAT_TLS_KEY_FILE": "/certs/server.key"},
			want: config.Healthcheck{Addr: "127.0.0.1:50051", TLSCertFile: "/certs/server.pem"},
		},
		{name: "no port", env: map[string]string{"CHAT_GRPC_ADDR": "localhost"}, wantErr: "CHAT_GRPC_ADDR"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := config.LoadHealthcheck(lookupFrom(tt.env))
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Errorf("LoadHealthcheck() error = %v, want one mentioning %s", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("LoadHealthcheck() error = %v", err)
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("LoadHealthcheck() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
