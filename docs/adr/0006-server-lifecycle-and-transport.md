# 6. Server lifecycle and transport settings

Date: 2026-10-09

## Status

Accepted

## Context

The server holds long-lived bidirectional streams. That shapes how it shuts
down, how it notices clients that vanished without closing their connection,
and how much a single client can make it buffer. It also needs logging and
panic recovery on every call, and a way to inspect it with tools such as
grpcurl.

## Decision

### Configuration

Settings come from environment variables (`CHAT_GRPC_ADDR`,
`CHAT_LOG_LEVEL`, `CHAT_SHUTDOWN_TIMEOUT`, `CHAT_REFLECTION`), parsed with the
standard library. Every invalid variable is reported at once and the process
refuses to start. A configuration library would save a few lines for four
settings at the cost of a dependency.

### Shutdown

On SIGINT or SIGTERM:

1. The health service switches to `NOT_SERVING`, so load balancers and
   watchers stop sending new clients here.
2. `GracefulStop` stops accepting connections and RPCs and waits for running
   ones to finish.
3. If they have not finished within the shutdown timeout (10 seconds by
   default), `Stop` cancels them and closes every connection.

`GracefulStop` alone would never return while a chat stream is open, because
a stream only ends when the client or the handler ends it. The timeout is the
backstop. Chat stream handlers will also watch for shutdown themselves and
end their streams with `UNAVAILABLE` (ADR-0003), so clients get a clear status
and can reconnect elsewhere instead of being cut off by `Stop`; that part
arrives with the stream handler.

### Transport limits

- Maximum received message size is 16 KiB instead of the gRPC default of
  4 MiB; ADR-0003 explains the figure.
- Keepalive: after 2 minutes without traffic the server pings the client and
  closes the connection if the ping is not answered within 20 seconds. This
  finds clients that disappeared without closing their TCP connection, which
  would otherwise keep their streams and room memberships alive.
- Clients may ping at most once a minute, also when they have no active RPC.
  A client that pings more often is disconnected with `GOAWAY` and
  `too_many_pings`. Every client must therefore set its keepalive time to at
  least one minute; the CLI client must respect this.
- No maximum connection age: rotating connections would cut chat streams on
  a timer.

### Interceptors

Every unary and streaming call passes through two interceptors written for
this project:

- recovery turns a panic into `INTERNAL` with a generic message for the
  client and logs the panic with its stack;
- logging records method, status code, duration and peer when the call ends.
  Calls ending with server-side codes (`INTERNAL`, `UNKNOWN`, `DATA_LOSS`,
  `UNIMPLEMENTED`) are logged as errors, other failures as warnings, and
  health checks only at debug level so frequent probes do not flood the log.

Logging wraps recovery, so a recovered panic is logged with the `INTERNAL`
code the client actually receives.

go-grpc-middleware is the standard collection of such interceptors. It is
not used because the two needed here are short, and writing them keeps the
interceptor mechanics visible in the code that the authentication
interceptors will follow.

### Reflection

The reflection service is registered only when `CHAT_REFLECTION` is true. It
is off by default so a deployment does not expose schema discovery unless it
asks to; `make run` turns it on for local use with grpcurl.

## Consequences

- A stop signal finishes within the shutdown timeout even with streams open.
- Dead clients are detected within about two and a half minutes.
- Client implementations have to configure keepalive within the server's
  policy or be disconnected.
- Interceptor behaviour is covered by unit tests; the shutdown sequence and
  the receive limit are tested against a real server over an in-memory
  connection.
