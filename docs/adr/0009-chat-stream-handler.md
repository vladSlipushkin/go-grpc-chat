# 9. Chat stream handler: three goroutines, no waiting on return

Date: 2026-10-13

## Status

Accepted

## Context

`ChatService.Connect` serves one client: it reads requests and applies them
in order, and it writes the events of the client's session queue
(ADR-0003, ADR-0008). A stream ends when the client half-closes or cancels,
when its token expires, when the hub drops the session as slow or because
its room failed, or when the server shuts down.

Two rules of grpc-go shape the handler:

- One goroutine may call `SendMsg` while another calls `RecvMsg`, but never
  two goroutines the same method (`ServerStream` in `stream.go`,
  `Documentation/concurrency.md`).
- Neither call returns when a context of ours is cancelled. `Recv` waits for
  the client to send something; `Send` to a client that stopped reading
  waits for HTTP/2 flow control. Only the end of the stream itself releases
  them, and the stream ends when the handler returns.

The second rule means a handler that waits for its own `Recv` or `Send`
cannot be ended by token expiry, a `SLOW_CLIENT` drop or shutdown, which are
exactly the cases where the client is silent or not reading.

## Decision

### Three goroutines

- The handler owns the session. It applies requests one at a time, since
  `Session` methods other than `Enqueue` and `Events` are not safe for
  concurrent use, queues the `Ack` or `Error` for each, and watches the
  session's context and the server's shutdown signal. It returns without
  waiting for the other two.
- The reader calls `Recv` and hands each request to the handler over an
  unbuffered channel. It stops at the first `Recv` error, which it reports
  on a channel with room for one value, or when the session's context ends.
- The writer takes events from the session queue and calls `Send`. It is the
  only goroutine that sends on the stream.

The handler's context is derived from the stream's with
`context.WithCancelCause`, and its cancel function is the `end` function
the hub gets in `Hub.Connect`. Every way a session can end is therefore one
cancellation with a cause, and the first cause wins.

### The stream after the handler returns

What grpc-go v1.84.0 does, by its source:

- When the handler returns, `processRPC` writes the call's status
  (`server.go`, the `writeStatus` calls after the handler). The transport
  then finishes the stream: `finishStream` cancels the stream's context and
  marks it done (`internal/transport/http2_server.go`).
- A `Send` blocked on flow control waits in `writeQuota.get` for the
  stream's done channel (`internal/transport/flowcontrol.go`; the quota is
  set up with the stream's `ctxDone` in `http2_server.go`). A blocked `Recv`
  waits in `recvBufferReader` on the same channel. Both return once the
  stream is finished. The reader and the writer therefore always exit after
  the handler returns; `goleak` in the `grpcapi` tests checks that no
  goroutine outlives a test.
- The documentation says nothing about calling `SendMsg` or `RecvMsg` after
  the handler has returned, and the stats handler has already received
  `stats.End` by then (the deferred function in `processRPC`). Such calls
  are treated as unsupported.

What the handler guarantees on top of that:

1. **No gap in `seq` before the final status.** The writer checks the
   session's context after taking each event and before sending it. When a
   queue overflows, `end(ErrSlowClient)` cancels the context first; an event
   queued after that can only be taken after it, so the check sees the
   cancellation. The client gets an unbroken run of messages and then
   `SLOW_CLIENT`.
2. **The writer never starts a `Send` once it has seen the session end.**
   This cannot be made strict with respect to the handler's return: a
   window remains between the check and the call. Closing it would need the
   handler to wait for the writer, which may be blocked in a `Send` that
   only the handler's return releases. A `Send` that slips through that
   window writes nothing to the client. Usually it fails at once:
   `http2Server.write` checks that the stream is done
   (`http2_server.go`), and `serverStream.writeStatus` sends a status
   only once (`statusWritten` in `stream.go`). If the stream finishes
   between that check and the hand-over to the transport's writer, `Send`
   returns nil, but the frame is queued behind the trailers and discarded
   when the stream is cleaned up (`controlbuf.go`).

   That narrow case has two side effects. A stats handler receives an
   `OutPayload` after `stats.End`, which matters once tracing is added;
   and with binary logging turned on, `SendMsg` and the code after the
   handler write the same flag without synchronization. Binary logging is
   off and there is no stats handler today. This is how grpc-go behaves,
   not something it documents, so it is to be re-checked when grpc-go is
   upgraded and before a stats handler is installed; no test would notice
   a change.

   The reader has the same window with `Recv`: it may call it once more
   after the handler returned. That call returns at once with the
   stream's context error, or a request already buffered, which the
   reader then drops.

Rejected:

- **Writing from the handler.** A client that stops reading blocks `Send`
  on flow control, and nothing but the handler's return releases it: the
  handler would never return, and the stream would never end.
- **Reading from the handler.** Same with a silent client and `Recv`: token
  expiry would cancel a context nobody watches.
- **Waiting for the reader and the writer with a `sync.WaitGroup`.** The
  same deadlock, one step removed.
- **Applying requests in the reader, with a mutex on `Session`.** This is
  where ADR-0008 first put them. The handler must call `Session.Close`
  before it returns, so that every room is empty once `Serve` returns and
  `Hub.Close` only stops rooms nobody is in (ADR-0008); it cannot leave that
  to the reader, which may be blocked in `Recv`. With a mutex, `Close` would
  wait for a `Join` the reader holds for a whole room command, which with
  storage includes a database round trip: the lock held across I/O that
  ADR-0008 rejected for rooms. Applying requests in the handler keeps every
  `Session` call on one goroutine without a lock. The unbuffered channel
  between them also means the reader takes the next request from gRPC only
  after the handler has taken the previous one, so backpressure reaches
  HTTP/2 flow control and no unbounded queue sits in between.
- **`context.AfterFunc` on the root context instead of a shutdown
  channel.** `AfterFunc(root, func() { end(errServerStopping) })` in
  `Connect` would make shutdown one more cancellation of the session's
  context, and the separate `select` case and the `stopping` check after
  taking a request would go. It needs the root context itself in
  `ChatServer`, a context stored in a struct; the done channel carries only
  the signal, which is all the handler uses. Each stream would also register
  a callback on the root context and has to remove it with the returned
  `stop` on the way out. With in-memory rooms nothing in `apply` blocks, so
  seeing shutdown inside a room command earlier gains nothing yet. Worth
  revisiting if more places in the handler come to need the signal.

### How a stream ends

| Cause | Status |
|---|---|
| The client half-closes (`Recv` returns `io.EOF`) | `OK` |
| The client cancels, or its deadline passes | `CANCELLED`, `DEADLINE_EXCEEDED` |
| The token expires | `UNAUTHENTICATED`, reason `TOKEN_EXPIRED` (set by the auth interceptor, ADR-0007) |
| The session queue overflows | `RESOURCE_EXHAUSTED`, reason `SLOW_CLIENT` |
| The server shuts down, or the hub is closed | `UNAVAILABLE` |
| A room fails, the reader or the writer panics, or the hub fails unexpectedly | `INTERNAL` |
| A request cannot be decoded or exceeds 16 KiB | the status gRPC sends itself: `INTERNAL` or `RESOURCE_EXHAUSTED` |

- Half-close ends the session at once: leaving rooms is what a client that
  is done wants, and keeping the stream open only to receive would make
  half-close a second way of subscribing. Every request read before it is
  applied: the reader hands each one over on an unbuffered channel before
  it calls `Recv` again, so `io.EOF` reaches the handler only after the
  last request. Their replies may be lost: the writer races the handler's
  return, and what it has not sent by then is discarded. A client that
  needs them waits for them before closing. A tool that half-closes at the
  end of its input, such as grpcurl reading requests from a pipe, may show
  no replies at all.
- A half-close after the session has ended keeps the session's cause. One
  that arrives after the token expired ends with `TOKEN_EXPIRED`, not `OK`:
  the auth interceptor cannot tell a nil result after expiry from one
  caused by it (ADR-0007).
- `select` picks at random among ready cases, so a request can be taken in
  the same moment the session ends. The handler checks the session's
  context after taking a request and does not apply it then. That also
  covers ADR-0008's note that a join of a failed room reports success
  until the session closes: by then the handler no longer applies joins.
- Every end through the session's context is logged as `chat session ended`
  with its cause. A half-close is not: it is the normal way out, and the
  logging interceptor's `rpc finished` record with code `OK` covers it.
  When gRPC ends the stream itself, for an oversized or
  undecodable request, the client gets the right status, but the log
  usually shows `CANCELLED`: `RecvMsg` writes the status, which cancels the
  stream's context, before the reader can report the error, and the
  handler wakes on the cancellation first. Logging the real status would
  need the handler to wait for the reader in that case.

### Request errors

Requests are validated in the handler (ADR-0003) with the same
`google.rpc.BadRequest` shape the unary validator uses. The `request_id` is
echoed in the `Error` only if it is itself valid. `ROOM_LIMIT_EXCEEDED` and
`FAILED_PRECONDITION` (send to a room not joined) are request errors; a
hub or room failure during a request ends the session instead.

### Panics

The recovery interceptor only covers the handler's goroutine: a panic in
the reader or the writer would take the process down. Both recover, log the
panic with its stack and end the session, so the stream ends with
`INTERNAL`. A panic in the handler itself is the interceptor's, and the
handler's deferred cleanup (cancel, `Session.Close`) still runs.

### Logging

The handler, the reader and the writer log with `user_id` and `peer`, the
attributes the logging interceptor puts in the call's `rpc finished`
record, so `chat session ended` and the other records of a stream can be
matched with its final code. `Connect` adds them once with `logger.With`.

Rejected: a `slog.Handler` that reads them from the context. Every record in
the process would pay for the lookups, and the interceptor's record, which
sets them itself, would carry them twice.

### Shutdown

`grpcapi.NewChatServer` gets the done channel of the process's root
context. When it closes, every handler ends its session and returns
`UNAVAILABLE`, which lets `GracefulStop` finish without waiting for the
timeout (ADR-0006). `cmd/server` calls `Hub.Close` after `Serve` returns,
when every handler has left its rooms (ADR-0008).

The exception is a client that has stopped reading. Its status is queued
behind data waiting for the client's flow-control window, the stream stays
open in the transport until it is written, and the connection holds
`GracefulStop` until the shutdown timeout, when `Stop` closes it. The same
holds for a client dropped as slow: it sees `SLOW_CLIENT` only if it
starts reading again. Nothing in the handler can change this; only
closing the connection discards the queued data.

If `Serve` fails on its own, for example because the listener broke, the
root context is not cancelled: open chat streams last until the shutdown
timeout and end with `CANCELLED` when `Stop` closes them. That path is rare
enough not to warrant a second signal.

### A client dropped as slow

Dropping a client as slow releases what the chat service holds at once: the
handler returns, the session leaves its rooms and its queue is freed. What
remains is in the transport, for a client that stopped reading but keeps
its connection:

- the events the writer had already handed to `Send`, at most about 64 KiB
  per stream (grpc-go's per-stream write quota), and the trailers queued
  behind them, waiting for the client's flow-control window;
- the stream itself, which keeps one of the connection's 100 stream slots
  (ADR-0006) until its trailers are written.

Keepalive does not end this: the client's transport still answers pings.
It lasts until the client reads again or its connection closes; a client
that is gone is found by keepalive within 2 minutes 20 seconds. The cost is
bounded per stream, and per process only as far as the number of
connections is (ADR-0006).

## Consequences

- Up to two goroutines per stream outlive its handler, for as long as gRPC
  takes to finish the stream.
- Correctness after the handler returns rests on grpc-go internals as of
  v1.84.0, listed above.
- End-to-end tests over `bufconn` cover join, send and leave, request errors
  that keep the stream open, the room limit, half-close, token expiry with
  a silent client, a slow client dropped without a gap while others keep
  sending, shutdown and an oversized request. A hand-written stream covers
  what a real client cannot cause: a panic in `Recv` or `Send`, and the
  writer inside `Send` while its session is dropped.

## References

grpc-go v1.84.0, the version in `go.mod` when this was written:

- `ServerStream`, which calls are safe concurrently:
  [stream.go#L1670-L1714](https://github.com/grpc/grpc-go/blob/v1.84.0/stream.go#L1670-L1714),
  [Documentation/concurrency.md](https://github.com/grpc/grpc-go/blob/v1.84.0/Documentation/concurrency.md)
- status written after the handler returns:
  [server.go#L1494](https://github.com/grpc/grpc-go/blob/v1.84.0/server.go#L1494),
  [server.go#L1513](https://github.com/grpc/grpc-go/blob/v1.84.0/server.go#L1513); `stats.End` in the deferred
  function: [server.go#L1330-L1358](https://github.com/grpc/grpc-go/blob/v1.84.0/server.go#L1330-L1358)
- the stream finished and its context cancelled:
  [internal/transport/http2_server.go#L1338-L1347](https://github.com/grpc/grpc-go/blob/v1.84.0/internal/transport/http2_server.go#L1338-L1347)
- a blocked `Send` released:
  [internal/transport/flowcontrol.go#L54-L67](https://github.com/grpc/grpc-go/blob/v1.84.0/internal/transport/flowcontrol.go#L54-L67),
  quota bound to the stream's done channel at
  [internal/transport/http2_server.go#L625](https://github.com/grpc/grpc-go/blob/v1.84.0/internal/transport/http2_server.go#L625)
- a blocked `Recv` released:
  [internal/transport/transport.go#L280-L285](https://github.com/grpc/grpc-go/blob/v1.84.0/internal/transport/transport.go#L280-L285)
- a `Send` after the stream is done fails without writing:
  [internal/transport/http2_server.go#L1160-L1170](https://github.com/grpc/grpc-go/blob/v1.84.0/internal/transport/http2_server.go#L1160-L1170),
  and its status is not written twice:
  [stream.go#L1801-L1806](https://github.com/grpc/grpc-go/blob/v1.84.0/stream.go#L1801-L1806)
- trailers queued behind data still waiting for flow control:
  [internal/transport/controlbuf.go#L705-L709](https://github.com/grpc/grpc-go/blob/v1.84.0/internal/transport/controlbuf.go#L705-L709);
  queued frames discarded when the stream is cleaned up:
  [internal/transport/controlbuf.go#L817-L832](https://github.com/grpc/grpc-go/blob/v1.84.0/internal/transport/controlbuf.go#L817-L832);
  a draining connection closes only once no stream is left:
  [internal/transport/controlbuf.go#L839-L842](https://github.com/grpc/grpc-go/blob/v1.84.0/internal/transport/controlbuf.go#L839-L842)
- the per-stream write quota:
  [internal/transport/defaults.go#L46](https://github.com/grpc/grpc-go/blob/v1.84.0/internal/transport/defaults.go#L46);
  a finished stream leaves the active set only when its trailers are
  written:
  [internal/transport/http2_server.go#L1317-L1359](https://github.com/grpc/grpc-go/blob/v1.84.0/internal/transport/http2_server.go#L1317-L1359)
