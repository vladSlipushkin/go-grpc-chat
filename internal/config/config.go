// Package config loads server settings from environment variables.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"
)

// Config holds the server settings.
type Config struct {
	// GRPCAddr is the address the gRPC server listens on.
	GRPCAddr string
	// LogLevel is the minimum level of emitted log records.
	LogLevel slog.Level
	// ShutdownTimeout bounds how long in-flight RPCs may run after a stop
	// signal before they are cancelled.
	ShutdownTimeout time.Duration
	// Reflection enables the gRPC reflection service.
	Reflection bool
	// JWTSecret signs and verifies access tokens. It is required and must
	// never be logged.
	JWTSecret []byte
	// TokenTTL is how long an access token stays valid.
	TokenTTL time.Duration
}

// minJWTSecretBytes matches auth.MinSecretBytes: HS256 needs a key at least
// as long as its 256-bit output.
const minJWTSecretBytes = 32

// LookupFunc reports the value of an environment variable and whether it is
// set. os.LookupEnv satisfies it.
type LookupFunc func(key string) (string, bool)

// Load reads the configuration through lookup, applying defaults for unset
// variables. It reports every invalid variable at once.
func Load(lookup LookupFunc) (Config, error) {
	cfg := Config{
		GRPCAddr:        ":50051",
		LogLevel:        slog.LevelInfo,
		ShutdownTimeout: 10 * time.Second,
		TokenTTL:        time.Hour,
	}

	var errs []error
	if v, ok := lookup("CHAT_GRPC_ADDR"); ok {
		// An empty address would make the server listen on a random port.
		if v == "" {
			errs = append(errs, errors.New("CHAT_GRPC_ADDR: must not be empty"))
		}
		cfg.GRPCAddr = v
	}
	if v, ok := lookup("CHAT_LOG_LEVEL"); ok {
		if err := cfg.LogLevel.UnmarshalText([]byte(v)); err != nil {
			errs = append(errs, fmt.Errorf("CHAT_LOG_LEVEL: %w", err))
		}
	}
	if v, ok := lookup("CHAT_SHUTDOWN_TIMEOUT"); ok {
		if d, err := positiveDuration(v); err != nil {
			errs = append(errs, fmt.Errorf("CHAT_SHUTDOWN_TIMEOUT: %w", err))
		} else {
			cfg.ShutdownTimeout = d
		}
	}
	if v, ok := lookup("CHAT_TOKEN_TTL"); ok {
		if d, err := positiveDuration(v); err != nil {
			errs = append(errs, fmt.Errorf("CHAT_TOKEN_TTL: %w", err))
		} else {
			cfg.TokenTTL = d
		}
	}
	// No default: a built-in secret would let anyone mint tokens for a
	// deployment that forgot to set one. The value is never echoed back.
	switch v, _ := lookup("CHAT_JWT_SECRET"); {
	case v == "":
		errs = append(errs, errors.New("CHAT_JWT_SECRET: must be set"))
	case len(v) < minJWTSecretBytes:
		errs = append(errs, fmt.Errorf("CHAT_JWT_SECRET: must be at least %d bytes, got %d", minJWTSecretBytes, len(v)))
	default:
		cfg.JWTSecret = []byte(v)
	}
	if v, ok := lookup("CHAT_REFLECTION"); ok {
		b, err := strconv.ParseBool(v)
		if err != nil {
			errs = append(errs, fmt.Errorf("CHAT_REFLECTION: %w", err))
		}
		cfg.Reflection = b
	}

	if err := errors.Join(errs...); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func positiveDuration(s string) (time.Duration, error) {
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, err
	}
	if d <= 0 {
		return 0, fmt.Errorf("must be positive, got %s", d)
	}
	return d, nil
}
