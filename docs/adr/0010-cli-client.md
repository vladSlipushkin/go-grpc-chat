# 10. CLI client: a line-based REPL over a reconnecting session

Date: 2026-10-14

## Status

Accepted

## Context

The chat needs a client people can use from a terminal. Earlier decisions
already say what any client must do:

- hold one `Connect` stream, ignore events it does not know, and use `seq`
  to notice missed messages (ADR-0003);
- reconnect after `UNAVAILABLE` or `SLOW_CLIENT` and join its rooms again,
  and log in again after `TOKEN_EXPIRED` (ADR-0003, ADR-0009);
- wait for the replies it needs before half-closing, since the server ends
  the stream at once and drops what it has not sent (ADR-0009);
- ping no more often than once a minute (ADR-0006).

The server has no TLS yet, and a login sends the password.

## Decision

### Layout

- `cmd/chat` only wires the process: signals, standard streams and the
  terminal's password reader (ADR-0001).
- `internal/client` speaks the protocol: register, log in, and `Chat`, a
  session that survives reconnects. It knows nothing about terminals, so it
  is tested directly against a real server.
- `internal/cli` is the command: subcommands and flags, password prompts and
  the REPL. It reads an `io.Reader` and writes an `io.Writer`, so whole
  terminal sessions are tested through pipes.
- `internal/chattest` runs the full server over `bufconn` for both, the way
  `net/http/httptest` serves HTTP tests (ADR-0002). It runs `internal/app`,
  the same assembly `cmd/server` runs, so an interceptor or a limit added
  to the server is in the client tests too.

### A line-based REPL

Each line is a command (`/join`, `/leave`, `/use`, `/rooms`, `/help`,
`/quit`) or a message to the current room. Events are printed as lines as
they arrive: `[general] alice: hello`, and `-- ... --` for everything else.

Rejected: a full-screen TUI (bubbletea). It would keep typed input apart from
incoming messages, but it is polish, which is the first thing to cut, and it
is much harder to test than lines on a pipe.

### The session

`Chat.Run` supervises streams one at a time; each stream has a receiver
goroutine that reads it until it ends.

- `Join`, `Leave` and `Send` are round trips: each sends its request with a
  fresh `request_id` and returns once the matching `Ack` or `Error` arrives.
  They are safe for concurrent use; a mutex keeps sends to one at a time, as
  gRPC requires.
- The rooms to rejoin are the rooms whose join the server acknowledged.
- Events reach the caller through one unbuffered channel. A caller that
  stops reading stops the receiver, the server's queue fills, and the server
  drops the session as `SLOW_CLIENT`, after which it reconnects. Replies
  travel on the same stream behind the messages, so a stalled reader also
  holds them up: `Send` fails with `ErrNoReply`, and `Close` ends by
  cancelling the stream. In the CLI the reader is the printer, which stalls
  only when the terminal stops taking output, as the Windows console does
  while a QuickEdit selection is open.
- The client adds no buffer of its own. An unbounded one would hide a
  stalled reader until memory ran out. A bounded one that drops on overflow
  and reports the drop as `Missed` would keep replies flowing, but it
  duplicates the server's own bounded queue: a reader stalled long enough
  to fill it loses the same messages either way, and the server's drop at
  least ends in a reconnect the user sees.
- While the session is reconnecting, requests wait for the new stream until
  their context ends (10 seconds in the CLI) instead of failing at once.
- Joins and leaves are idempotent (ADR-0003), so one whose stream breaks
  before the reply is sent again on the next stream, after the rejoin, and
  returns once that one is answered, unless its context ends or the session
  closes first. A lost leave also takes its room off the rejoin list at
  once, so the rejoin does not return to it; a lost join is not silently
  dropped. A leave that then fails, because its context ended before the
  next stream, has left the room all the same: the new stream never joins
  it. The CLI reports it as left.
- Each attempt sends a copy of the request with its own `request_id`. gRPC
  does not allow a message to change after `SendMsg`, since interceptors
  and stats handlers may still hold it, and the retry would otherwise set
  a new id on the message already sent.
- A send whose stream breaks before the reply fails with
  `ErrConnectionLost`, and one whose context ends first fails with
  `ErrNoReply`. Neither is retried: the message may have been delivered, and
  sending it again could post it twice (ADR-0003). The CLI says it may have
  gone through.
- A send over the server's rate limit fails with `ErrRateLimited` and is not
  retried either: the user decides whether to send it again (ADR-0008).

### Reconnecting

The session reconnects after `UNAVAILABLE`, `SLOW_CLIENT`, `INTERNAL`,
`UNKNOWN`, `CANCELLED`, `DEADLINE_EXCEEDED` and `ABORTED`, the codes a
server restart, a dropped connection or a transient failure produce. Any
other status ends `Run` with that error, since reconnecting would only
repeat it: a rejected token, or `RESOURCE_EXHAUSTED` other than
`SLOW_CLIENT`, which means a request the client should never have sent.

- The wait before attempt n doubles from 500 ms up to 30 s, and half of it is
  random. A server restart ends every stream at once with `UNAVAILABLE`
  (ADR-0009); without jitter every client would come back in the same
  instant. Keeping half of the wait fixed means no client retries
  immediately.
- The count starts again only after a stream that stayed open longer than
  the longest wait, 30 seconds. A server that fails right after the rejoin
  has still answered it; resetting on any reply would retry such a server
  every 250 to 500 ms for as long as it keeps failing.
- gRPC reconnects the connection underneath with a backoff of its own, and
  while it waits a new stream fails at once. Its default grows to two
  minutes, which would decide when a session returns after a long outage,
  so the client gives it the same base of 500 ms, factor of 2 and cap of
  30 s. Its jitter is symmetric, ±50%, so its wait reaches up to 45 s where
  the session's stops at 30 s: after a long outage a session can be back
  up to 15 s later than its own backoff says.
- Rejected: `WaitForReady` on `Connect`, which would leave the connection
  to gRPC alone and keep the session's backoff for errors on a live
  connection, such as `SLOW_CLIENT`. The stream would then block until the
  connection is ready, with no event per attempt: the CLI could no longer
  say that it is reconnecting and when it tries next.
- The stream given up is cancelled through its context before the next one
  opens, so it does not linger on the server holding a stream slot
  (ADR-0006).
- The rooms are joined again before requests are taken: a `/leave` typed
  during the rejoin waits for it, so it cannot be undone by it. Rooms the
  server refuses are dropped: `Connected` lists the rooms rejoined and,
  apart, the rooms dropped. The CLI says which it could not rejoin, and a
  message typed for a dropped current room is not sent.

### Missed messages

The client tracks the last `seq` per room on the current stream. A jump
prints `-- missed N messages in ROOM --` before the message after the gap.

- A `seq` at or below the last one means the room started counting again,
  not a gap: until messages are stored, a room that empties starts over from
  1 (ADR-0003).
- Leaving a room forgets its `seq`, so messages sent while the client was not
  a member are not reported as missed.
- A new stream starts with nothing tracked. ADR-0003 forbids comparing `seq`
  across a reconnect, because the room may have emptied or the server
  restarted in between. Messages sent while the client was away are
  therefore not counted; the `-- reconnected --` line is the signal.
- With the current server, `Missed` cannot happen. A room delivers every
  message to every member in order, and the only way it skips one is the
  `SLOW_CLIENT` drop, which ends the stream, and with it the tracking. Only
  a unit test reaches it. It becomes useful once messages are stored: `seq`
  then survives restarts and empty rooms, the tracking can continue across
  a reconnect, and a gap is filled from `ListMessages` (stage 7).

### Credentials

The password is asked for at start and is not kept; the session holds only
the access token, in memory. When the token expires, the session calls back
into the REPL, which asks for the password again and reconnects with the new
token.

The REPL owns standard input: its line reader reads a line only when the REPL
asks for one. A password prompt therefore never competes with a pending read
for the terminal. When the token expires while a line read is already waiting,
that read cannot be cancelled portably (a console read on Windows cannot be
interrupted), so the REPL asks the user to press Enter first and then reads
the password with echo off.

That relies on one read from a terminal returning one line, which is what
a terminal in line mode does: the Enter ends that read, and the password is
read next, without echo. Several lines pasted at once at that prompt break
the assumption: the lines after the first may be read as the password, or
the password may be read as a message. This is a known limit of the
line-based REPL.

Passwords are read with `golang.org/x/term` and only from a terminal, never
from a pipe or a file. `register` asks twice. Echo was checked to stay off
in PowerShell in the Windows console and in Windows Terminal.

Rejected: keeping the password in memory to log in again silently, which
leaves it in the process for the whole session; storing the token in a file
with 0600 permissions, which puts a secret on disk for convenience; refresh
tokens, a non-goal of ADR-0003.

Not done yet: logging in again before the token expires. `Login` returns
`expires_at`, and ADR-0003 allows reconnecting ahead of it. The REPL would
ask for the password a minute or so before expiry, open a new stream with
the new token and only then let the old one end, so no message falls
between the two. It interrupts the user on a timer rather than on a
failure, and it needs two streams of one session at once; with tokens that
last an hour, the extra Enter after expiry is the cheaper trade for now.
Messages that arrive while the prompt waits are lost to the session.

### Quitting

`/quit`, the end of input and Ctrl+C all close the session the same way:
stop taking requests, wait for the replies to requests in flight, half-close
the stream and wait for the server to end it with `OK`. The whole sequence is
bounded at 5 seconds, after which the stream is cancelled. A second Ctrl+C
ends the process at once.

Ctrl+C does not cancel the request in progress, so that the session closes
only after its reply, as ADR-0009 asks: a message the server applies before
it sees the half-close must not be reported as lost. The request is bounded
by its own 10-second timeout instead, which includes waiting for a reconnect
while the server is down. Ctrl+C can therefore take up to 10 seconds plus
the 5 of closing; a second Ctrl+C ends the process at once.

### Keepalive

The client pings after 90 seconds without traffic and gives up on the
connection 20 seconds after an unanswered ping. That is above the server's
one-minute minimum (ADR-0006) with margin: a ping that network jitter makes
look early counts as a strike against the client. It notices a server that
vanished within about two minutes, even in a quiet room.

### Transport security

Without TLS the password and the token cross the network in plaintext.
The client therefore connects without TLS only to a loopback address
(`localhost`, `127.0.0.0/8`, `::1`) unless `-insecure` is given; anything
else fails before a byte is sent, with an error naming the flag.

- The check reads the address, not the connection. `localhost` is trusted
  by name: a hosts file that maps it elsewhere would send plaintext there.
  Resolving it first would close that, at the cost of a lookup before
  every dial.
- Rejected: `credentials/local` from grpc-go. It checks the connection
  itself, which is better, but it refuses a remote address outright, so
  `-insecure` would need a second credential anyway, and it reports a TCP
  loopback connection as `NoSecurity`, which per-RPC credentials that
  require transport security refuse.
- A client in a container that dials the server's container (`server:50051`
  in compose) is not on loopback: it needs TLS or `-insecure`. Clients on
  the host reach a published port on `localhost` and need neither.

In a real deployment, TLS is either terminated at the ingress in front of
the server or enabled on the server itself. The packaging stage adds the
latter as an option: a certificate and key in the server's configuration,
and `-tls` with a CA file on the client.

## Consequences

- Incoming messages are printed while the user may be typing, so they can
  break up a half-typed line on the screen; the line itself is not affected.
- Messages sent while a client reconnects are lost to it until
  `ListMessages` exists (ADR-0003).
- Logging in again after expiry takes an extra Enter.
- Tests against the real server cover: join, send and leave between two
  sessions; reconnecting after a server restart and after `SLOW_CLIENT`,
  with the rooms joined again; logging in again after `TOKEN_EXPIRED`;
  `Close` waiting for every reply; a room's `seq` starting over without a
  false gap; a rejected token ending the session. Hand-written servers
  cover what the real one does not do on demand: a stream that breaks
  before replying (a join or leave is sent again, a send is not), a request
  never answered, and events from a newer server. The CLI is tested as whole
  terminal sessions, including the re-login prompt and a current room the
  rejoin dropped. Further hand-written servers cover the backoff growing
  and starting again, a resend leaving the sent request unchanged, rooms
  refused on rejoin, and `Close` giving up at its deadline on a server that
  ignores the half-close or never replies. `docs/manual-testing.md`
  describes the same checks by hand.
