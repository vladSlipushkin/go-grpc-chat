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
`Close` are called only by the stream's reader goroutine, so the session's
own room map needs no lock. The outbound queue is a channel of 64 events
read by the stream's writer.

- `Enqueue` never waits. A producer that finds the queue full, a room
  broadcasting or the reader adding an `Ack`, ends the session with
  `ErrSlowClient` (ADR-0003).
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
sessions after `Close` fail with `ErrHubClosed`. `cmd/server` wires this
order together with the stream handler.

### Members per room: not limited

A room has no member limit. Each member costs the room one non-blocking
channel send of a shared pointer per message; text is not copied, and a
member's memory is bounded by its own queue, which is counted per
connection, not per room. A large room only slows its own goroutine:
`BenchmarkSend` measures about 40 nanoseconds per member plus a few
microseconds per message on a development machine, so ten thousand members
cost under half a millisecond per message. A limit would also need a new
error reason in the contract. What bounds the load is the number of
connections, which belongs to the ingress and to rate limiting. Fan-out
time per room is the metric to watch once metrics exist.

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
  concurrent use; the stream handler must keep them on its reader.
- The tests run every scenario in `testing/synctest` bubbles under
  `goleak`, including concurrent joins and leaves on a room that keeps
  emptying and the order of a drop and a token expiry.
- With several server instances the registry and room ordering move to a
  shared message bus; that is a separate decision.
