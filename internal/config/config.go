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
}

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
	}

	var errs []error
	if v, ok := lookup("CHAT_GRPC_ADDR"); ok {
		cfg.GRPCAddr = v
	}
	if v, ok := lookup("CHAT_LOG_LEVEL"); ok {
		if err := cfg.LogLevel.UnmarshalText([]byte(v)); err != nil {
			errs = append(errs, fmt.Errorf("CHAT_LOG_LEVEL: %w", err))
		}
	}
	if v, ok := lookup("CHAT_SHUTDOWN_TIMEOUT"); ok {
		d, err := time.ParseDuration(v)
		switch {
		case err != nil:
			errs = append(errs, fmt.Errorf("CHAT_SHUTDOWN_TIMEOUT: %w", err))
		case d <= 0:
			errs = append(errs, fmt.Errorf("CHAT_SHUTDOWN_TIMEOUT: must be positive, got %s", d))
		default:
			cfg.ShutdownTimeout = d
		}
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
