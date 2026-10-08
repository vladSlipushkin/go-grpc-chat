# 3. Chat stream protocol

Date: 2026-10-08

## Status

Accepted

## Context

Clients join rooms, leave them, send messages and receive messages from other
members in real time. The protocol has to define the shape of the RPC, how
replies are matched to requests, what delivery guarantees hold, how errors are
reported, what happens when the access token expires and how the server
protects itself from clients that read too slowly.

## Decision

### Shape

One bidirectional RPC per client: `ChatService.Connect`. Each direction carries
an envelope (`ConnectRequest`, `ConnectResponse`) with a `oneof`. A client
switches rooms without reconnecting, authenticates once per connection, and
new actions or events are added as new `oneof` fields without breaking
existing clients: clients must ignore events they do not recognise, and an
older server rejects an unknown action as an invalid request. Envelope names
follow the buf `STANDARD` lint rules.

### Scope of v1

Actions: join a room, leave a room, send a message. Rooms are created
implicitly on first join. Join and leave describe a desired state, so both are
idempotent: joining a room twice or leaving a room the connection is not in is
acknowledged as a no-op, and a client can retry either one blindly after a
reconnect.

Server rules that the schema cannot express:

- A connection can be in at most 16 rooms; the next join fails with
  `RESOURCE_EXHAUSTED`.
- The sender is taken from the access token, never from the request.
- The sender receives its own message as a regular `Message` event; the
  `message_id` in its `Ack` links the two. The two may arrive in either
  order: the `Message` goes through the room, the `Ack` straight back.
- Message text is limited to 4096 bytes, not characters, so the memory a
  queued message takes does not depend on the script it is written in.

Non-goals for v1, each addable later without breaking the contract:

- message history in the stream (planned as a separate unary `ListMessages`);
- typing indicators;
- member lists and join/leave presence events;
- room management: creating, listing, deleting rooms, permissions;
- editing and deleting messages;
- refresh tokens and token revocation.

### Request correlation

`request_id` is chosen by the client and echoed in the `Ack` or `Error` for
that request. It only correlates replies with requests and is not an
idempotency key: resending a `SendMessage` after a reconnect creates a
duplicate. Deduplication would need a per-message idempotency key from the
client and a bounded per-sender record of recent keys on the server, answering
a repeat with the original `message_id`.

### Delivery guarantees

At-most-once. A message reaches the members connected to the room at the
moment the room processes it. All members of a room see its messages in the
same order; there is no ordering across rooms. Messages sent while a client is
disconnected are lost for that client in v1; `ListMessages` will let it catch
up.

### Errors

Errors are split by what they affect:

| Scope | Reported as | Codes |
|---|---|---|
| One request | `Error{request_id, status}` in the stream; the stream stays open | `INVALID_ARGUMENT` (request fails validation), `FAILED_PRECONDITION` (send to a room not joined), `RESOURCE_EXHAUSTED` (room limit) |
| The connection | the stream ends with a gRPC status | `UNAUTHENTICATED` (missing, invalid or expired token), `RESOURCE_EXHAUSTED` (client too slow), `UNAVAILABLE` (server shutting down), `INTERNAL` (unexpected failure) |

`Error.status` is a `google.rpc.Status`, the same model gRPC uses for call
status, so a code means the same thing in both places.

Rejected: ending the stream on every error, because one malformed request
would cost a reconnect and re-joining every room; reporting everything in the
stream, because connection-level status codes are what interceptors, metrics
and client retry policies act on.

### Access token expiry

The token is verified once, when the stream opens. The stream's context then
gets a deadline at the token's expiry, set with
`context.WithDeadlineCause(ctx, exp, errTokenExpired)`. When it fires, the
connection leaves all rooms and the stream ends with `UNAUTHENTICATED`;
`context.Cause` distinguishes expiry from other cancellations. The client
reconnects with a fresh token. An access token lifetime of about an hour keeps
reconnects rare.

Rejected: verifying the token at open with no deadline on the stream, which
lets a session outlive its token indefinitely; renewing the token inside the stream, which adds protocol
surface for little gain in v1 and can still be added later as a new action.

### Slow clients

Each connection has an outbound queue of 64 events. If a room finds that queue
full when broadcasting, the connection is closed with `RESOURCE_EXHAUSTED`.

- Not blocking: one slow reader would stall the room's broadcast for every
  member, and a client could do it on purpose.
- Not dropping: dropped events are gaps the client cannot detect. With
  at-most-once delivery, an explicit disconnect is the only signal that it
  missed something.
- 64 absorbs short bursts in a busy room for a client on a normal network,
  while bounding queued text at 256 KiB per connection (64 × 4 KiB, the
  byte limit on message text).
  It is a starting point to revisit with metrics.

## Consequences

- Clients hold one connection and authenticate once for any number of rooms.
- Clients must handle reconnects: re-join their rooms and accept message loss
  until `ListMessages` exists.
- The room implementation must never block on a single connection; this is
  covered by tests when the hub is implemented.
