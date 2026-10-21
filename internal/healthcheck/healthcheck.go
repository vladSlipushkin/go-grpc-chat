// Package healthcheck asks a chat server on the same host whether it is
// serving. The container image runs it as its health check, so it needs
// nothing the image does not have (ADR-0012).
package healthcheck

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"slices"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// Check calls grpc.health.v1.Health/Check on the server at addr and
// returns nil if the server is SERVING. With certFile, the server's own
// certificate file, it connects with TLS and trusts that certificate alone.
func Check(ctx context.Context, addr, certFile string) error {
	creds := insecure.NewCredentials()
	if certFile != "" {
		cfg, err := pinned(certFile)
		if err != nil {
			return err
		}
		creds = credentials.NewTLS(cfg)
	}
	conn, err := grpc.NewClient("passthrough:///"+addr, grpc.WithTransportCredentials(creds))
	if err != nil {
		return fmt.Errorf("dial %s: %w", addr, err)
	}
	defer conn.Close()

	resp, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		return fmt.Errorf("check health of %s: %w", addr, err)
	}
	if s := resp.GetStatus(); s != healthpb.HealthCheckResponse_SERVING {
		return fmt.Errorf("server at %s is %s", addr, s)
	}
	return nil
}

// pinned returns a TLS configuration that trusts only the first
// certificate in certFile, the server's own, under a name it was issued
// for. Go accepts a certificate found among the roots as it is, so this
// works for a self-signed certificate and for one a CA issued alike, and
// needs no CA file.
func pinned(certFile string) (*tls.Config, error) {
	data, err := os.ReadFile(certFile)
	if err != nil {
		return nil, fmt.Errorf("read certificate: %w", err)
	}
	block, _ := pem.Decode(data)
	if block == nil || block.Type != "CERTIFICATE" {
		return nil, fmt.Errorf("%s: no PEM certificate", certFile)
	}
	leaf, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse certificate: %w", err)
	}
	name, err := serverName(leaf)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", certFile, err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(leaf)
	return &tls.Config{RootCAs: roots, ServerName: name, MinVersion: tls.VersionTLS13}, nil
}

// serverName picks a name the certificate is valid for: localhost if it
// has it, since the check connects locally, or else the first it names.
func serverName(leaf *x509.Certificate) (string, error) {
	switch {
	case slices.Contains(leaf.DNSNames, "localhost"):
		return "localhost", nil
	case len(leaf.DNSNames) > 0:
		return leaf.DNSNames[0], nil
	case len(leaf.IPAddresses) > 0:
		return leaf.IPAddresses[0].String(), nil
	default:
		return "", errors.New("certificate names no host")
	}
}
