// Command devcert writes a development CA and a server certificate it
// issued, for trying the server with TLS locally (ADR-0012). Nothing it
// writes belongs in a deployment.
//
//	go -C tools run ./devcert -dir ../certs
package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"flag"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "devcert: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	dir := flag.String("dir", "certs", "directory to write ca.pem, server.pem and server.key to")
	// server is the compose service's name, for clients in other containers.
	hosts := flag.String("hosts", "localhost,127.0.0.1,::1,server", "comma-separated names and addresses the server certificate is for")
	flag.Parse()

	if err := os.MkdirAll(*dir, 0o755); err != nil {
		return err
	}
	now := time.Now()

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	caTmpl := &x509.Certificate{
		SerialNumber:          serial(),
		Subject:               pkix.Name{CommonName: "go-grpc-chat development CA"},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.AddDate(1, 0, 0),
		IsCA:                  true,
		BasicConstraintsValid: true,
		MaxPathLenZero:        true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		return fmt.Errorf("create CA: %w", err)
	}

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	tmpl := &x509.Certificate{
		SerialNumber: serial(),
		Subject:      pkix.Name{CommonName: "go-grpc-chat development server"},
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     now.AddDate(0, 3, 0),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	for h := range strings.SplitSeq(*hosts, ",") {
		if ip := net.ParseIP(h); ip != nil {
			tmpl.IPAddresses = append(tmpl.IPAddresses, ip)
		} else if h != "" {
			tmpl.DNSNames = append(tmpl.DNSNames, h)
		}
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, caTmpl, &key.PublicKey, caKey)
	if err != nil {
		return fmt.Errorf("create server certificate: %w", err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return err
	}

	// The key is readable by everyone: the server in the container runs
	// as a user that does not own files on the host.
	files := []struct {
		name  string
		block *pem.Block
	}{
		{"ca.pem", &pem.Block{Type: "CERTIFICATE", Bytes: caDER}},
		{"server.pem", &pem.Block{Type: "CERTIFICATE", Bytes: der}},
		{"server.key", &pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}},
	}
	for _, f := range files {
		path := filepath.Join(*dir, f.name)
		if err := os.WriteFile(path, pem.EncodeToMemory(f.block), 0o644); err != nil { //nolint:gosec // a development key, see above
			return err
		}
		fmt.Println("wrote", path)
	}
	return nil
}

func serial() *big.Int {
	n, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 127))
	if err != nil {
		panic(err)
	}
	return n
}
