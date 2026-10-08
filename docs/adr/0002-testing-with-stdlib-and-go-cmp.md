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
- Compare structured values with `cmp.Diff(want, got)`; add
  `protocmp.Transform()` for protobuf messages.
- Failure messages follow the `Func(args) = got, want want` form.
- No mocking framework. Dependencies are small interfaces satisfied by
  hand-written fakes in tests.

## Consequences

- Checks are more verbose than testify one-liners.
- Failures show a precise diff instead of a generic "not equal".
- Protobuf comparisons are correct by construction.
- The test dependency surface is a single, widely used module.
