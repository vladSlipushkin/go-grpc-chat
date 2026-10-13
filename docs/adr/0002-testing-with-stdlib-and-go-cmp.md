# 2. Testing with the standard library and go-cmp

Date: 2026-10-08

## Status

Accepted

## Context

Tests need a consistent assertion style. The two common options are
`stretchr/testify` and the standard `testing` package with
`google/go-cmp` for structural comparison.

testify gives short `assert`/`require` calls, but it is an extra dependency,
the difference between `assert` (continue) and `require` (stop) is easy to get
wrong, and `assert.Equal` compares protobuf messages field by field including
internal state, which produces false failures. Protobuf messages must be
compared with `proto.Equal` or `cmp` plus `protocmp.Transform()`.

The Go team's review guidance (go.dev/wiki/TestComments) and "Learning Go"
recommend plain `testing` with clear failure messages, using go-cmp when
values are too complex for `==`.

## Decision

- Use the standard `testing` package. Table-driven tests where there are
  several cases; `t.Helper()` in helpers; `t.Context()` for contexts.
- Compare structured values with `cmp.Diff(want, got)` and report the result
  as `(-want +got)`; add `protocmp.Transform()` for protobuf messages.
- Failure messages follow the `Func(args) = got, want want` form.
- No mocking framework. Dependencies are small interfaces satisfied by
  hand-written fakes in tests.

### Concurrency, transport and time

- Every package whose tests start goroutines runs
  `goleak.VerifyTestMain`. A goroutine still running after the last test
  fails the package, which catches handlers, rooms or server loops that do
  not stop.
- gRPC behaviour is tested against a real server over `bufconn`: an
  in-memory listener with real HTTP/2 framing, interceptors, status codes
  and metadata, but no ports and no network. Tests call methods through
  generated clients, or through hand-written `grpc.ServiceDesc` values when
  a method exists only for the test.
- Code of our own that depends on time (room goroutines, token expiry) is
  tested inside `testing/synctest` bubbles. Time there is fake and advances
  only when every goroutine in the bubble is blocked, so a test of a
  one-hour expiry runs instantly and asserts exact instants instead of
  ranges.
- Faults a real client cannot cause, such as a panic inside `Send`, are
  tested by calling the handler directly with a hand-written stream.
- Tests that run a grpc-go server use real time: grpc-go starts goroutines
  and timers that cannot be moved into a bubble. There:
  - nothing is synchronized with `time.Sleep`; tests wait on channels,
    stream events or a polled condition;
  - waits for something that must happen are capped generously (5 seconds)
    and only fail a test that hangs. Heavy work is kept out of such waits
    rather than given a longer cap: a test that starts the whole server
    passes the minimum bcrypt cost, since startup hashes a dummy password;
  - a short timeout (200 ms, or a one-second token lifetime) is allowed
    only to cut a wait short when what it races against is in-process work
    taking microseconds, a margin of several orders of magnitude even under
    the race detector;
  - a short pause (100 ms) is allowed where a test checks that something
    does not happen too early, such as `Serve` returning while a handler is
    still running. It can only let a bug slip through, never fail correct
    code.

## Consequences

- Checks are more verbose than testify one-liners.
- Failures show a precise diff instead of a generic "not equal".
- Protobuf comparisons are correct by construction.
- The test dependency surface is a single, widely used module.
