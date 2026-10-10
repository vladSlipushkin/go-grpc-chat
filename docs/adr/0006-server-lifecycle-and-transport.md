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
`CHAT_LOG_LEVEL`, `CHAT_SHUTDOWN_TIMEOUT`, `CHAT_REFLECTION`, and
`CHAT_JWT_SECRET` and `CHAT_TOKEN_TTL` from ADR-0007), parsed with the
standard library. Every invalid variable is reported at once and the process
refuses to start. A configuration library would save a few lines for six
settings at the cost of a dependency.

### Shutdown

On SIGINT or SIGTERM:

1. The health service switches to `NOT_SERVING`. Clients watching health
   learn it first; clients polling `Check` see the listener close at about
   the same moment, since there is no drain delay before the next step.
2. `GracefulStop` stops accepting connections and RPCs and waits for running
   ones to finish.
3. If they have not finished within the shutdown timeout (10 seconds by
   default), `Stop` cancels them and closes every connection.

`GracefulStop` alone would never return while a chat stream is open, because
a stream only ends when the client or the handler ends it; a health `Watch`
from grpcurl or a client's health checking holds it the same way. The timeout
is the backstop.

`GracefulStop` does not cancel the contexts of running streams; it only stops
new RPCs. Chat stream handlers will therefore get an explicit shutdown signal
from the process's root context, not from their stream context, and end
their streams with `UNAVAILABLE` (ADR-0003). Clients then get a clear status
and can reconnect elsewhere instead of being cut off by `Stop`; this arrives
with the stream handler.

`Serve` returns only after every handler has returned, including after
`Stop`. Every handler must therefore return once its context is cancelled;
a handler that ignores cancellation would hang shutdown. As a safety net, the
first SIGINT or SIGTERM restores default signal handling, so a second one
kills the process.

This holds when the server runs as PID 1 in a container. The kernel does
not deliver signals with the default action to PID 1, but the Go runtime
keeps its own handler for SIGINT and SIGTERM; when no channel is notified,
it re-raises the signal and, if that did nothing, exits with status
128 + signal (`dieFromSignal` in `runtime/signal_unix.go`). No init process
is needed for signal handling.

The orchestrator's grace period must be longer than the shutdown timeout,
otherwise it kills the process before `Stop` gets to cancel running RPCs:
`docker stop` waits 10 seconds by default, exactly the default timeout here.
The packaging stage sets `stop_grace_period` in Compose (and
`terminationGracePeriodSeconds` for Kubernetes) with a margin over
`CHAT_SHUTDOWN_TIMEOUT`. Behind a load balancer the missing drain delay
means clients can still be routed to the server for a moment after it
stops listening; a pause before `GracefulStop` would cover that and is
deferred to the same stage.

Rejected: `grpc.WaitForHandlers(true)` with a single `Stop`. It makes `Stop`
wait for handlers, which `GracefulStop` already does, and it is still marked
experimental. `Stop` alone also skips the drain phase, cutting off streams
that would have finished within the timeout.

If accepting connections fails on its own, the server shuts down the same
way before reporting the error: closing the listener alone would leave
already accepted connections running.

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
  least one minute; the CLI client must respect this. grpc-go tolerates a
  couple of early pings before it disconnects, but clients should not rely
  on that.
- No maximum connection age: rotating connections would cut chat streams on
  a timer.

### Interceptors

Every unary and streaming call passes through two interceptors written for
this project, followed by authentication and request validation
(ADR-0007):

- recovery turns a panic into `INTERNAL` with a generic message for the
  client and logs the panic with its stack;
- logging records method, status code, duration, peer and, for
  authenticated calls, the user id when the call ends. A handler that
  returns a bare context error is logged as `CANCELLED` or
  `DEADLINE_EXCEEDED`, the code grpc-go sends the client for it, not as
  `UNKNOWN`.
  Calls ending with server-side codes (`INTERNAL`, `UNKNOWN`, `DATA_LOSS`,
  `UNIMPLEMENTED`) are logged as errors, cancelled calls as information like
  successful ones, other failures as warnings, and health checks only at
  debug level so frequent probes do not flood the log.

Logging wraps recovery, so a recovered panic is logged with the `INTERNAL`
code the client actually receives.

go-grpc-middleware is the standard collection of such interceptors. It is
not used because the two needed here are short, and writing them keeps the
interceptor mechanics visible in the code that the authentication
interceptors will follow.

### Service registration

Services are registered on the `Server` before `Serve` is called. grpc-go
treats a registration after `Serve`, or a duplicate one, as a programming
error and calls `log.Fatalf`, which exits the process without running
deferred functions. All registration therefore happens in the composition
root (`cmd/server`) between `New` and `Serve`, never from handlers or
background goroutines.

### Reflection

The reflection service is registered only when `CHAT_REFLECTION` is true. It
is off by default so a deployment does not expose schema discovery unless it
asks to; `make run` turns it on for local use with grpcurl.

## Consequences

- A stop signal finishes within the shutdown timeout even with streams open,
  provided every handler returns once its context is cancelled.
- Dead clients are detected within 2 minutes 20 seconds of going silent.
- Client implementations have to configure keepalive within the server's
  policy or be disconnected.
- Interceptor behaviour is covered by unit tests; the shutdown sequence and
  the receive limit are tested against a real server over an in-memory
  connection.
