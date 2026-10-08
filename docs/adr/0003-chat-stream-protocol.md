# 3. Chat stream protocol

Date: 2026-10-08

## Status

Accepted

## Context

Clients join rooms, leave them, send messages and receive messages from other
members in real time. The protocol has to define the shape of the RPC, how
requests are processed, how replies are matched to requests, what ordering and
delivery guarantees hold, how errors are reported, what happens when the
access token expires and how the server protects itself from clients that
read too slowly.

## Decision

### Shape

One bidirectional RPC per client: `ChatService.Connect`. Each direction carries
an envelope (`ConnectRequest`, `ConnectResponse`) with a `oneof`. A client
switches rooms without reconnecting, authenticates once per connection, and
new actions or events are added as new `oneof` fields without breaking
existing clients: clients must ignore events they do not recognise, and an
older server rejects an unknown action as an invalid request. Envelope names
follow the buf `STANDARD` lint rules.

Rejected: a server stream for events plus unary `JoinRoom`, `LeaveRoom` and
`SendMessage`. Room membership belongs to a connection, so every unary call
would need a session id binding it to the right stream, with a race when a
join arrives before the stream is open. Authentication would happen on every
call instead of once, and an `Ack` could not be ordered relative to events on
the stream at all.

### Scope of v1

Actions: join a room, leave a room, send a message. Rooms are created
implicitly on first join. Join and leave describe a desired state, so both are
idempotent: joining a room twice or leaving a room the connection is not in is
acknowledged as a no-op, and a client can retry either one blindly after a
reconnect.

Server rules that the schema cannot express:

- A connection can be in at most 16 rooms; the next join fails with
  `RESOURCE_EXHAUSTED`. Membership is checked before the limit, so re-joining
  a room at the limit is still a no-op.
- The sender is taken from the access token, never from the request.
- The sender receives its own message as a regular `Message` event; the
  `message_id` in its `Ack` links the two.
- Message text is limited to 4096 bytes, not characters, so the memory a
  queued message takes does not depend on the script it is written in. It
  must contain at least one character that is not whitespace, a separator, or
  a control, format, private-use or unassigned code point. That rejects blank
  messages and those made only of such characters; visible-looking fillers
  such as U+3164 HANGUL FILLER still pass, which a test records.

Non-goals for v1, each addable later without breaking the contract:

- message history in the stream (planned as a separate unary `ListMessages`);
- typing indicators;
- member lists and join/leave presence events;
- room management: creating, listing, deleting rooms, permissions;
- editing and deleting messages;
- refresh tokens and token revocation.

### Processing model

- Requests of one stream are handled one at a time, in the order received.
  Join and leave are synchronous: the room applies them before the reader
  enqueues the `Ack` and reads the next request, so join-then-send never
  fails with `FAILED_PRECONDITION`.
- Each connection has one first-in, first-out outbound queue and one
  goroutine that writes to the stream; gRPC does not allow concurrent sends
  on a stream. The connection's reader enqueues `Ack` and `Error`; rooms
  enqueue `Message`. Because they are enqueued by different goroutines:
  - a room's `Message` may arrive before the `Ack` for joining that room;
  - the sender's own `Message` may arrive before or after the `Ack` for
    sending it;
  - nothing from a room arrives after the `Ack` for leaving it.
- Requests are validated in the handler, not by a protovalidate stream
  interceptor. An interceptor reports a violation as a `RecvMsg` error, so the
  handler would have to tell it apart from transport errors by code and then
  read `request_id` from a message it was told is invalid. Validating in the
  handler keeps request errors request-scoped by construction.

### Request correlation

`request_id` is chosen by the client and echoed in the `Ack` or `Error` for
that request. It only correlates replies with requests and is not an
idempotency key: resending a `SendMessage` after a reconnect creates a
duplicate. Deduplication would need a per-message idempotency key from the
client and a bounded per-sender record of recent keys on the server, answering
a repeat with the original `message_id`.

### Ordering and sequence numbers

All members of a room see its messages in the same order; there is no
ordering across rooms. Every `Message` carries `seq`, its position in the
room: 1, 2, 3 and so on without gaps. A client that sees a jump knows it
missed messages, and `seq` is the cursor `ListMessages` will page by.

- The room's goroutine issues `seq`. It is the only writer of the room's
  counter, so issuing is atomic without locks, and the same serialization is
  what gives the room a single order.
- With storage, `seq` is allocated in the transaction that stores the
  message: `UPDATE rooms SET last_seq = last_seq + 1 WHERE id = $1 RETURNING
  last_seq`, with `UNIQUE (room_id, seq)` on messages. The row lock serializes
  allocation per room even across processes, and a rolled-back transaction
  leaves no gap. The counter lives in the database, so a restart resumes where
  it stopped.
- Until storage exists, a room's goroutine stops once its last member leaves,
  and its counter goes with it; keeping counters for every room name ever
  used would let clients grow server memory without bound. A room recreated
  later, or any room after a server restart, starts from 1 again. Messages do
  not outlive the room either, so clients must not compare `seq` across a
  room becoming empty or a server restart before storage lands.
- With storage, the room waits for the insert before broadcasting, so one
  room's throughput is bounded by database latency. Rooms are independent of
  each other, and the bound is far above chat traffic.
- `seq` is an `int64` to match PostgreSQL `bigint` without conversion.

Rejected: a sortable message id (UUIDv7, ULID) as the cursor. It works for
paging but has gaps by design, so a client could not tell that it missed a
message.

### Delivery guarantees

At-most-once. A message reaches the members connected to the room at the
moment the room processes it. Messages sent while a client is disconnected are
lost for that client in v1; `ListMessages` will let it catch up.

All ordering and delivery guarantees hold within one server process. Running
several instances needs a shared message bus and moves room ordering there;
that is a separate decision.

### Errors

Errors are split by what they affect:

| Scope | Reported as | Codes |
|---|---|---|
| One request | `Error{request_id, status}` in the stream; the stream stays open | `INVALID_ARGUMENT` (request fails validation), `FAILED_PRECONDITION` (send to a room not joined), `RESOURCE_EXHAUSTED` (room limit) |
| The connection | the stream ends with a gRPC status | `UNAUTHENTICATED` (missing, invalid or expired token), `RESOURCE_EXHAUSTED` (client too slow), `UNAVAILABLE` (server shutting down), `INTERNAL` (unexpected failure) |

`Error.status` is a `google.rpc.Status`, the same model gRPC uses for call
status, so a code means the same thing in both places. Where one code covers
several causes, the status carries a `google.rpc.ErrorInfo` detail with
domain `chat.v1` and one of these reasons:

- `ROOM_LIMIT_EXCEEDED`: the join would exceed 16 rooms (in the stream);
- `SLOW_CLIENT`: the outbound queue was full (ends the stream);
- `TOKEN_EXPIRED`: the access token expired (ends the stream).

The domain is the protobuf package rather than a DNS name as AIP-193
suggests, because the service has no public hostname.

Only requests that decode successfully reach this model. A message that is
not valid protobuf, has invalid UTF-8 in a string field or exceeds the
receive limit fails inside gRPC before the handler sees it, and the stream
ends with `INTERNAL` or `RESOURCE_EXHAUSTED`. The receive limit is 16 KiB: the
largest valid request is just over 4 KiB, and the gRPC default of 4 MiB would
let a client make the server buffer far more than it ever needs.

Rejected: ending the stream on every error, because one malformed request
would cost a reconnect and re-joining every room; reporting everything in the
stream, because interceptors and metrics only see the status a call ends with.

### Access token expiry

The token is verified once, when the stream opens. The stream's context then
gets a deadline at the token's expiry, set with
`context.WithDeadlineCause(ctx, exp, errTokenExpired)`. When it fires, the
connection leaves all rooms and the stream ends with `UNAUTHENTICATED` and
reason `TOKEN_EXPIRED`; `context.Cause` distinguishes expiry from the client
cancelling or from a shorter deadline the client set itself, which keeps its
own code. The client reconnects with a fresh token. An access token lifetime
of about an hour keeps reconnects rare.

Each reconnect still has a cost: without refresh tokens the client has to log
in again with the password, and messages sent while it reconnects are lost
until `ListMessages` exists. Clients can use `expires_at` from `Login` to
reconnect before expiry instead of after it.

No clock-skew leeway is applied: the same server issues and verifies tokens,
so they share one clock. With several instances, verification would accept a
small leeway (about 30 seconds) while the stream deadline stays at the exact
expiry.

Rejected: verifying the token at open with no deadline on the stream, which
lets a session outlive its token indefinitely; renewing the token inside the
stream, which adds protocol surface for little gain in v1 and can still be
added later as a new action.

### Slow clients

Each connection has an outbound queue of 64 events. Whoever finds that queue
full, a room broadcasting a `Message` or the reader enqueueing an `Ack` or
`Error`, closes the connection with `RESOURCE_EXHAUSTED` and reason
`SLOW_CLIENT`. One rule for every producer means nothing ever waits on a
client.

- Not blocking: one slow reader would stall the room's broadcast for every
  member, and a client could do it on purpose. Blocking only the reader would
  leave a client that stopped reading in a quiet room connected until its
  token expires.
- Not dropping: dropped events are gaps the client cannot detect. With
  at-most-once delivery, an explicit disconnect is the only signal that it
  missed something.
- 64 absorbs short bursts in a busy room for a client on a normal network,
  while bounding queued message text at 256 KiB per connection (64 × 4 KiB,
  the byte limit on message text). Envelope fields and gRPC and HTTP/2
  buffers behind the writer come on top. It is a starting point to revisit
  with metrics.

## Consequences

- Clients hold one connection and authenticate once for any number of rooms.
- Clients must handle reconnects: re-join their rooms and accept message loss
  until `ListMessages` exists; `seq` tells them when it happened.
- The room implementation must never block on a single connection, a
  connection must leave all rooms when its token expires, and kicking a client
  must not leak goroutines. These are covered by tests when the hub and the
  stream handler are implemented.
