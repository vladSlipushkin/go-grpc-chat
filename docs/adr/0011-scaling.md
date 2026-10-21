# 11. Scaling: one instance, and what several would take

Date: 2026-10-21

## Status

Accepted

## Context

The server keeps every room in its own memory: the hub's registry, one
goroutine per room that orders its messages, and each room's `seq` counter
(ADR-0003, ADR-0008). Accounts are in memory too until storage exists, and
the message rate limit is a token bucket per user in the process
(ADR-0008). Access tokens are HS256 JWTs that any process with the secret
can verify (ADR-0007).

A chat stream lives for as long as the user is connected, often hours. A
replica that stops ends every stream on it at once with `UNAVAILABLE`
(ADR-0006, ADR-0009), and every one of those clients reconnects and joins
its rooms again (ADR-0010).

## Decision

### One instance

The server runs as a single instance, and the compose file starts one.

Two replicas behind a load balancer would split a room. Members connected
to replica A would never see messages posted on replica B, because each
replica has its own room goroutine for the same room id, with its own
`seq`. Sticky sessions do not help: they keep one client on one replica,
but a room is shared by many clients, which land on different replicas.

### What several instances need

- **A shared fan-out.** A message bus with a subject per room: NATS, Redis
  Pub/Sub, or PostgreSQL `LISTEN/NOTIFY` for small loads. A replica's room
  goroutine subscribes while the replica has members in the room, and
  publishes each message after storing it. Local delivery stays as it is:
  one queue per session, `SLOW_CLIENT` on overflow.
- **`seq` from the database.** ADR-0003 already allocates it in the
  transaction that stores the message, with a row lock per room, which
  holds across processes. Messages from different replicas can reach the
  bus out of order, so a replica delivers by `seq`: it holds a message back
  while an earlier one is missing, for a short time, and then reads the gap
  from the database. Clients already treat a gap as missed messages
  (ADR-0010).
- **Accounts in PostgreSQL** (stage 7) and the same JWT secret on every
  replica. Tokens need nothing else.
- **A shared rate limit.** The bucket is per user in each process, so a
  user with streams on two replicas gets two allowances. A shared bucket
  (Redis, for one) or routing every stream of a user to the same replica
  closes that.
- **A drain delay.** Before `GracefulStop`, a replica that is about to stop
  should report `NOT_SERVING` and keep serving for a few seconds, so the
  load balancer stops routing new connections to it (ADR-0006). With one
  instance there is nowhere else to route, so it is not done yet.

### The reconnect wave on an update

Updating the server ends every stream on the replica being replaced at
the same moment. With N clients on it:

- Each client waits before its first attempt: half of 500 ms fixed, half
  random, so all N come back within 250 to 500 ms (ADR-0010). Each one then
  opens a stream and joins up to 16 rooms. 10,000 clients mean 10,000
  streams and up to 160,000 joins within a quarter of a second, all on the
  replicas still running, or on the new one.
- The fixed half keeps clients from coming back the instant the server
  stops, when nothing is listening yet. Full jitter, a wait anywhere in
  `[0, 500 ms]`, would spread the wave over twice the time, but let some
  clients retry at once.
- A server can flatten the wave itself: end its streams spread over the
  drain window instead of all at once, or send `GOAWAY` and let streams
  end on their own until the shutdown timeout. Either way is needed only
  at a scale where one replica holds thousands of streams.
- A rolling update with two replicas doubles the load on the one left
  running while the other restarts; capacity is planned for that.

### Connection limits at the ingress

The server limits streams per connection (100) but not connections
(ADR-0006): a limit inside the process stalls new clients in the accept
backlog instead of refusing them. The ingress in front of the server is
where connections are limited:

- connections per client address, and new connections per second, which
  bounds both a misbehaving client and the reconnect wave above;
- a total that matches what one replica is sized for;
- idle and read timeouts longer than the keepalive intervals. A quiet chat
  stream sends nothing for minutes: the client pings after 90 seconds
  without traffic and the server after 2 minutes (ADR-0006, ADR-0010). A
  proxy timeout of 60 seconds, nginx's default for `grpc_read_timeout`,
  would cut every quiet stream.

TLS can end at the ingress, with plaintext or a separate certificate
behind it on a private network, or at the server itself (ADR-0012). With
an ingress in front, terminating there is the usual choice: it holds the
public certificate and sees the gRPC requests, which it needs for
per-method limits.

## Consequences

- The deployment is one process: simple to run and reason about, and a
  single point of failure. Its restart drops every stream for about half
  a second plus the time to start.
- Moving to several instances is a planned change, not a rewrite: the
  room's goroutine keeps ordering local delivery, and a bus and the
  database take over ordering across replicas.
- The reconnect wave and the ingress limits are described, not built;
  nothing in the repository runs an ingress.
