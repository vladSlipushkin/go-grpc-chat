// Package certtest issues throwaway certificates for tests of TLS: a
// self-signed one, or one issued by a root through an intermediate, the
// shape of a certificate from a real CA.
package certtest

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Files are PEM files in a test's temporary directory.
type Files struct {
	// Cert holds the server's certificate followed by any intermediates.
	Cert string
	// Key holds the server's private key.
	Key string
	// CA holds the root a client must trust: the issuing root, or the
	// certificate itself if it is self-signed.
	CA string
}

// SelfSigned returns a certificate for hosts, DNS names or IP addresses,
// that signs itself.
func SelfSigned(tb testing.TB, hosts ...string) Files {
	tb.Helper()

	key := newKey(tb)
	tmpl := leafTemplate(hosts)
	cert := create(tb, tmpl, tmpl, &key.PublicKey, key)
	return write(tb, [][]byte{cert}, key, cert)
}

// Issued returns a certificate for hosts issued by an intermediate that a
// root issued. The certificate file holds the leaf and the intermediate,
// as a server sends them; the CA file holds the root.
func Issued(tb testing.TB, hosts ...string) Files {
	tb.Helper()

	rootKey, interKey, leafKey := newKey(tb), newKey(tb), newKey(tb)
	rootTmpl := caTemplate("certtest root", 1)
	root := create(tb, rootTmpl, rootTmpl, &rootKey.PublicKey, rootKey)
	interTmpl := caTemplate("certtest intermediate", 2)
	inter := create(tb, interTmpl, parse(tb, root), &interKey.PublicKey, rootKey)
	leaf := create(tb, leafTemplate(hosts), parse(tb, inter), &leafKey.PublicKey, interKey)
	return write(tb, [][]byte{leaf, inter}, leafKey, root)
}

func newKey(tb testing.TB) *ecdsa.PrivateKey {
	tb.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		tb.Fatalf("generate key: %v", err)
	}
	return key
}

func caTemplate(name string, serial int64) *x509.Certificate {
	now := time.Now()
	return &x509.Certificate{
		SerialNumber:          big.NewInt(serial),
		Subject:               pkix.Name{CommonName: name},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
}

func leafTemplate(hosts []string) *x509.Certificate {
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject:      pkix.Name{CommonName: "certtest server"},
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     now.Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	for _, h := range hosts {
		if ip := net.ParseIP(h); ip != nil {
			tmpl.IPAddresses = append(tmpl.IPAddresses, ip)
		} else {
			tmpl.DNSNames = append(tmpl.DNSNames, h)
		}
	}
	return tmpl
}

func create(tb testing.TB, tmpl, parent *x509.Certificate, pub *ecdsa.PublicKey, signer *ecdsa.PrivateKey) []byte {
	tb.Helper()
	der, err := x509.CreateCertificate(rand.Reader, tmpl, parent, pub, signer)
	if err != nil {
		tb.Fatalf("create certificate %q: %v", tmpl.Subject.CommonName, err)
	}
	return der
}

func parse(tb testing.TB, der []byte) *x509.Certificate {
	tb.Helper()
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		tb.Fatalf("parse certificate: %v", err)
	}
	return cert
}

func write(tb testing.TB, chain [][]byte, key *ecdsa.PrivateKey, ca []byte) Files {
	tb.Helper()

	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		tb.Fatalf("marshal key: %v", err)
	}
	dir := tb.TempDir()
	files := Files{
		Cert: filepath.Join(dir, "server.pem"),
		Key:  filepath.Join(dir, "server.key"),
		CA:   filepath.Join(dir, "ca.pem"),
	}
	var certPEM []byte
	for _, der := range chain {
		certPEM = append(certPEM, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})...)
	}
	for name, data := range map[string][]byte{
		files.Cert: certPEM,
		files.Key:  pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}),
		files.CA:   pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca}),
	} {
		if err := os.WriteFile(name, data, 0o600); err != nil {
			tb.Fatalf("write %s: %v", name, err)
		}
	}
	return files
}
