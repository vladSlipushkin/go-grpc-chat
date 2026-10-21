# 8. Chat hub: a mutex for the room registry, a goroutine per room

Date: 2026-10-11

## Status

Accepted

## Context

ADR-0003 fixes what the chat must guarantee: every member of a room sees its
messages in the same order with gapless `seq`; join and leave are applied
before they are acknowledged; nothing arrives from a room after its leave is
acknowledged; a connection is in at most 16 rooms; rooms are created on first
join and, until storage exists, disappear with their last member; nothing
ever waits for a slow client. It also says the room's goroutine issues `seq`.

That leaves two separate concurrency problems. The registry of rooms is
shared by every connection and changes when rooms appear and disappear. A
room's own state, its members and its counter, changes with every join,
leave and message, and its changes must happen in one order. Package
`internal/chat` solves each with the tool that fits it.

## Decision

### Registry: a map under a mutex

`Hub` keeps `map[string]*room` under a `sync.Mutex`. Nothing done under the
lock blocks: map operations, counter changes, closing a room's stop channel
and starting a room's goroutine; no I/O, no channel sends or receives. A
plain mutex is the simplest correct tool for that, and contention is
limited to joins and leaves, never messages.

The hub alone decides when a room stops. Every session that is in a room, or
is about to join it, holds it: `acquire` (look up or start the room, count a
holder) before a join, `release` after a leave or a failed join. When the
last holder releases, the hub removes the room from the map and tells its
goroutine to exit, all under the same lock. A join therefore either finds a
room that will stay up until it releases it, or starts a new one; it can
never be handed to a room that is shutting down. A room recreated this way
starts `seq` at 1 again, as ADR-0003 describes.

### Room: one goroutine

Each room runs one goroutine that owns its member set and `seq`. Callers
send it commands, closures over the room, through an unbuffered channel and
wait until the command has run. Because only that goroutine touches the
room's state, the order in which it runs commands is the room's order:
`seq` needs no atomics and every member's queue receives messages in the
same order.

- A caller gives up on its context only before the room accepts a
  command; a context that has already ended is checked first, so it always
  wins. Once accepted, it waits for the command to finish
  regardless of the context: commands never block, so the wait is short,
  and abandoning it would leave the caller unsure whether its join was
  applied.
- "Commands never block" holds only while rooms live in memory. Storing a
  message puts I/O inside a command, and then a stalled database would
  stall `Send`, the leave in `Session.Close` and `Hub.Close` with it. The
  rule for that change: every I/O inside a command runs under a deadline
  of its own, so an accepted command finishes in bounded time, and every
  wait on the room inherits that bound.
- Join, leave and send all return after the room applied them. A sender's
  own `Message` is therefore always queued before its `Ack`, which is
  stricter than ADR-0003 promises; clients still must not rely on it.

The goroutine is chosen over a mutex inside each room for what comes next
rather than for today's in-memory code, where a mutex would work as well:

- With storage, a message is inserted before it is broadcast (ADR-0003).
  A mutex would then be held across a database round trip, and joins and
  leaves would queue on it with no way to give up; with a goroutine they
  wait on a channel send that honours their context.
- A panic is contained in one place, and the room's state is thrown away
  with the goroutine instead of being left half-updated behind a lock.

The cost is a goroutine per active room, two channel operations per
command, and a lifecycle to manage: starting, stopping and waiting for room
goroutines. That lifecycle is exactly what the registry lock takes care of.

### Sessions and the outbound queue

A `Session` is one connection's membership. `Join`, `Leave`, `Send` and
`Close` are called only by the stream handler's goroutine (ADR-0009), so
the session's own room map needs no lock. The outbound queue is a channel of 64 events
read by the stream's writer.

- `Enqueue` never waits. A producer that finds the queue full, a room
  broadcasting or the handler adding an `Ack`, ends the session with
  `ErrSlowClient` (ADR-0003). Events queued after that are not sent: the
  writer checks the session's context before each send, so the client sees
  no gap in `seq` before the final status (ADR-0009).
- An ended session still answers calls until `Close`, and not always
  truthfully: a join of a room whose goroutine has failed reports success,
  because the session counts that room as joined. The handler stops
  applying requests once the session has ended, so no client sees it.
- Ending a session calls the `context.CancelCauseFunc` the handler passed
  to `Connect`. The handler watches that context and reads the reason from
  `context.Cause`. The first cause wins: a session dropped as slow before
  its token expires reports `SLOW_CLIENT`, one whose token expired first
  reports `TOKEN_EXPIRED`, and the later event changes nothing.
- The sender of a message is the user authenticated in the context passed
  to `Connect`; nothing in a request can change it.
- Queue items are domain types (`*Message`, `Ack`, `Rejection`); `grpcapi`
  translates them to `chat.v1` messages, so `chat` does not import
  generated code. One `*Message` is shared by all recipients.

### Failure of a room

A panic in a room's goroutine is recovered there. The room logs it with the
stack, removes itself from the registry and ends every member's session
with `ErrRoomFailed`, which the stream handler reports as `INTERNAL`. Calls
waiting on it return `ErrRoomFailed`, unless their command had already run.
Other rooms are unaffected, and the next join of the same id starts a fresh
room; a join that found the room a moment before it failed gets
`ErrRoomFailed` too. Letting the panic crash the
process would drop every connection for a bug in one room.

### Shutdown order

The server's `Serve` returns only after every handler has returned
(ADR-0006), and each handler closes its session on the way out, which
leaves its rooms. `Hub.Close` is called after that, when no room should be
left; it stops whatever remains, ending sessions still inside with
`ErrHubClosed`, and waits for every room goroutine to exit. Joins and new
sessions after `Close` fail with `ErrHubClosed`. `cmd/server` calls
`Hub.Close` right after `Serve` returns.

### Members per room: not limited

A room has no member limit. Each member costs the room one non-blocking
channel send of a shared pointer per message; text is not copied, and a
member's memory is bounded by its own queue, which is counted per
connection, not per room. A large room only slows its own goroutine:
`BenchmarkSend` measures about 40 nanoseconds per member plus a few
microseconds per message on a development machine, so ten thousand members
cost under half a millisecond per message. That is a best case: the
benchmark has no writers competing for the CPU. A limit would also need a
new error reason in the contract. Fan-out time per room is the metric to
watch once metrics exist.

### Message rate: limited per user

Without a limit, one client could post a few thousand messages a second
into a large room, since each send waits only for its room. Members whose
writers fall a few tens of milliseconds behind would then overflow their
queues and be dropped as `SLOW_CLIENT`, although the sender is the cause.
An ingress cannot help: it limits connections and requests, not messages
inside a long-lived stream.

The stream handler therefore limits each user to five messages a second
on average and ten at once, with a token bucket (`golang.org/x/time/rate`).
A message over the limit is rejected in the stream like any request error:
`RESOURCE_EXHAUSTED` with reason `RATE_LIMITED` and a `google.rpc.RetryInfo`
saying when the next token is free. The stream stays open. Only messages
actually posted use up a token: an invalid one and one to a room not joined
are rejected before the limiter, and the limiter's `AllowN` takes nothing
when it refuses.

Rejected: reserving a token and cancelling the reservation when the message
is not posted. `x/time/rate` gives a token back only while the reservation's
time to act has not passed, which for a token taken at once is the moment
of the reservation; any time spent on the request after it makes the cancel
do nothing. On Windows, where the clock often does not move between the
two, the loss does not even show in tests. With a bucket shared by several
streams, reservations of two streams can also interleave and leave one
refused message charged.

- Five a second is far above anyone typing, and a burst of ten lets a
  pasted block of lines through. One user then adds at most ten messages
  at once and five a second to a member's queue of 64; a member that keeps
  reading drains that long before it fills, so one user alone can no
  longer get members dropped.
- The bucket belongs to the user, not to the stream. A client could
  otherwise multiply its allowance by opening streams: up to 100 on one
  HTTP/2 connection (ADR-0006) and any number of connections. All of a
  user's streams draw on one limiter, kept in a map by user id under a
  mutex in the `ChatServer`. The first stream of a user creates it and the
  last one to close removes it, so the map holds only users with an open
  stream.
- The limit lives in the handler, next to validation, rather than in the
  hub: the room's goroutine stays free of per-sender state, and the hub
  keeps no state between a user's sessions.
- Joins and leaves are not limited. They post nothing to other members,
  and each one waits for its room.
- The client is told how long to wait, but the CLI only asks the user to
  slow down: a person reads "wait a moment" faster than a delay in
  milliseconds.

What remains is a user who closes every stream and opens a new one: the new
stream starts with a full bucket, ten messages for the price of a stream
and a join. Keeping an emptied limiter until it has refilled would close
that, at the cost of entries that outlive their streams and a sweep to
remove them. Until it is needed, the cost of opening streams, which the
ingress's connection and request limits bound, is the limit.

## Rejected

- **One goroutine for the whole hub.** Simple, but it serializes every
  room: a busy room delays all others, and with storage one slow insert
  stalls the whole server.
- **A mutex per room and no goroutines.** Works for in-memory rooms but
  holds the lock across storage I/O later, and ADR-0003 already gives
  ordering to the room's goroutine.
- **Rooms that stop themselves when empty.** A join that found the room in
  the map a moment earlier would be sent to a goroutine that is exiting.
  Closing that gap needs a handshake with the registry, which is the
  registry's lock again.
- **`sync.Map` for the registry.** Lookup-or-create plus a holder count is
  a compound operation that `sync.Map` cannot make atomic.
- **Blocking or dropping on a full queue.** See ADR-0003.

## Consequences

- One goroutine per active room, none per idle room name.
- Session methods other than `Enqueue` and `Events` are not safe for
  concurrent use; the stream handler keeps them on its own goroutine.
- The tests run every scenario in `testing/synctest` bubbles under
  `goleak`, including concurrent joins and leaves on a room that keeps
  emptying and the order of a drop and a token expiry.
- With several server instances the registry and room ordering move to a
  shared message bus; that is a separate decision.
