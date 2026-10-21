# 12. Packaging and transport security

Date: 2026-10-21

## Status

Accepted

## Context

The server has to run as a container that Docker Compose, or any
orchestrator, can start, check and stop cleanly. ADR-0006 left two things
to this stage: a grace period longer than the server's own shutdown
timeout, and a server that handles SIGTERM as PID 1.

Until now the client refused plaintext to anything but loopback, and the
server had no TLS at all (ADR-0010). A login sends the password and every
stream sends the access token, so a deployment needs encryption somewhere:
at an ingress (ADR-0011) or on the server.

## Decision

### Image

A two-stage build: `golang:1.27.3` compiles static binaries
(`CGO_ENABLED=0`, `-trimpath`, `-ldflags="-s -w"`), and the runtime image is
`gcr.io/distroless/static-debian13:nonroot`, pinned by digest.

- distroless has CA certificates, `/etc/passwd` with a non-root user and
  time zone data, and no shell or package manager. There is nothing in it
  to log into or to patch apart from the binaries.
- Rejected: `scratch`, which needs the user and the certificates added by
  hand for the same result; `alpine`, which brings a shell and a package
  manager that only debugging needs. `docker debug` covers that.
- The image holds the client as well, for a client in another container;
  the entry point is the server.
- The entry point is the binary itself, in exec form, so the server is PID
  1 and receives SIGTERM directly. Go handles it as PID 1 without an init
  process (ADR-0006).
- The tag is rebuilt in place by its publisher, so the digest keeps builds
  reproducible; updating the base image is a deliberate change.

### Health check

`server -healthcheck` asks the server next to it for
`grpc.health.v1.Health/Check` and exits 0 only on `SERVING`. It reads the
address and the certificate from the same environment the server reads, so
it needs no configuration of its own and no secret.

- Rejected: adding `grpc_health_probe` to the image, a second binary to
  version and patch for one call; and no health check at all, which leaves
  `docker compose up --wait` and `depends_on` nothing to wait for.
- During shutdown the health service reports `NOT_SERVING` and the
  listener closes at about the same moment (no drain delay, ADR-0006), so
  a check run then fails. Docker marks the container unhealthy only after
  three failures in a row, which a stop that takes under a second never
  reaches; an orchestrator stopping the container already knows.
- The check gives itself 2 seconds, inside the 3 the container allows.

With TLS, the check connects to the server's own address and trusts
exactly one certificate, the first in the server's certificate file. It
checks the name against that certificate: `localhost` if listed, or else
the first name or address in it. Go accepts a certificate found among its
roots as it is, so this works for a self-signed certificate and for one a
CA issued, with or without intermediates in the file. Tests cover a
self-signed certificate and one issued through an intermediate; the
development certificates are issued by a CA directly.
It needs no CA file, and it does not skip verification:
`InsecureSkipVerify` would also accept a different server on that port.

### Compose

- `stop_grace_period` is 15 seconds, over the 10-second
  `CHAT_SHUTDOWN_TIMEOUT`, so the server finishes its own shutdown before
  Docker sends SIGKILL. `docker stop` with a client connected takes under a
  second: chat streams end at once with `UNAVAILABLE` (ADR-0009).
- The port is published on `127.0.0.1` only. The client on the host then
  connects without TLS, as a loopback address allows.
- `compose.tls.yaml`, added on top, mounts `certs/` and turns TLS on.
- The JWT secret has a public development default, like `make run`; a real
  deployment sets its own.

### TLS on the server

`CHAT_TLS_CERT_FILE` and `CHAT_TLS_KEY_FILE` turn TLS on. Both are set or
neither; one alone fails at start rather than serving plaintext by
mistake. The certificate file holds the server's certificate followed by
any intermediates.

- TLS 1.3 only. Every client is this repository's Go client or grpcurl,
  both of which speak it, and 1.3 leaves no cipher suites to choose.
- The certificate is read at start; renewing it means a restart. Rejected
  for now: reloading it through `GetCertificate`, which a deployment with
  short-lived certificates needs and one with an ingress does not.
- Rejected: mutual TLS. Users authenticate with a password and a token;
  client certificates would add a second identity to manage.

### TLS on the client

`-tls` connects with TLS and the system's roots; `-ca FILE` trusts the CA in
that file instead, for a private or development CA. `-insecure` cannot be
combined with either.

- Without TLS the client still connects only to a loopback address unless
  `-insecure` is given (ADR-0010).
- The token goes as per-RPC credentials. On a TLS connection they require
  transport security, so gRPC refuses to send the token over anything
  less. On a plaintext connection they cannot require it, or the token
  could not be sent at all, but such a connection exists only where the
  loopback rule or `-insecure` allowed it.

### Development certificates

`make certs` runs `tools/devcert`, which writes a CA and a server
certificate it issued, for `localhost`, `127.0.0.1`, `::1` and `server`, the
compose service name, into `certs/` (ignored by git). The key is readable by
everyone: the server in the container runs as a user that does not own
files on the host. The CA's key is not kept, so running it again replaces
both.

Rejected: mkcert, which installs its CA into the system's trust store, and
openssl, which Windows does not ship.

### Demo

`docs/demo.gif` is recorded by VHS from `docs/demo/demo.tape`, in a Docker
image with tmux, the server and two clients (`make demo`). Anyone can
record it again and get the same result; nothing depends on the host's
terminal. Its base images are pinned like the server's.

## Consequences

- `docker compose up` starts the server healthy, with or without TLS, and
  `docker stop` ends it cleanly within the grace period.
- A deployment without an ingress can still encrypt: certificate and key in
  two variables.
- Certificate renewal requires a restart, which the reconnect wave of
  ADR-0011 makes visible to users for under a second.
