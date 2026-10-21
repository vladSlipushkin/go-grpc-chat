// Package config loads server settings from environment variables.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/netip"
	"strconv"
	"time"

	"github.com/vladSlipushkin/go-grpc-chat/internal/auth"
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
	// TLSCertFile and TLSKeyFile are PEM files with the server's
	// certificate, followed by any intermediates, and its private key.
	// Both are set or neither; without them the server serves plaintext.
	TLSCertFile, TLSKeyFile string
}

// defaultGRPCAddr is where the server listens unless CHAT_GRPC_ADDR says
// otherwise.
const defaultGRPCAddr = ":50051"

// LookupFunc reports the value of an environment variable and whether it is
// set. os.LookupEnv satisfies it.
type LookupFunc func(key string) (string, bool)

// Load reads the configuration through lookup, applying defaults for unset
// variables. It reports every invalid variable at once.
func Load(lookup LookupFunc) (Config, error) {
	cfg := Config{
		GRPCAddr:        defaultGRPCAddr,
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
	case len(v) < auth.MinSecretBytes:
		errs = append(errs, fmt.Errorf("CHAT_JWT_SECRET: must be at least %d bytes, got %d", auth.MinSecretBytes, len(v)))
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

	cfg.TLSCertFile, _ = lookup("CHAT_TLS_CERT_FILE")
	cfg.TLSKeyFile, _ = lookup("CHAT_TLS_KEY_FILE")
	if (cfg.TLSCertFile == "") != (cfg.TLSKeyFile == "") {
		errs = append(errs, errors.New("CHAT_TLS_CERT_FILE and CHAT_TLS_KEY_FILE: set both or neither"))
	}

	if err := errors.Join(errs...); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// Healthcheck is what the health check needs to reach the server it runs
// next to.
type Healthcheck struct {
	// Addr is the server's address as seen from its own host.
	Addr string
	// TLSCertFile is the server's certificate file, empty without TLS.
	TLSCertFile string
}

// LoadHealthcheck reads, from the server's own environment, where the
// server listens and whether it serves TLS. It needs no secrets.
func LoadHealthcheck(lookup LookupFunc) (Healthcheck, error) {
	addr := defaultGRPCAddr
	if v, ok := lookup("CHAT_GRPC_ADDR"); ok {
		addr = v
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return Healthcheck{}, fmt.Errorf("CHAT_GRPC_ADDR: %w", err)
	}
	// A server listening on every address is reached on loopback.
	if ip, err := netip.ParseAddr(host); host == "" || err == nil && ip.IsUnspecified() {
		host = "127.0.0.1"
	}
	cert, _ := lookup("CHAT_TLS_CERT_FILE")
	return Healthcheck{Addr: net.JoinHostPort(host, port), TLSCertFile: cert}, nil
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
