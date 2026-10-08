# 1. Project layout

Date: 2026-10-08

## Status

Accepted

## Context

Go has no official project template. The only layout rule the toolchain
enforces is `internal/`: packages under it cannot be imported from outside the
module. The popular `golang-standards/project-layout` repository is a community
collection of conventions, not a standard, and copying all of its directories
adds structure without adding meaning.

The official guide "Organizing a Go module" (go.dev/doc/modules/layout)
recommends, for a server project, keeping commands in `cmd/` and everything
else in `internal/`.

## Decision

- `cmd/<binary>/` holds thin `main` packages. `main` only calls `run`, which
  wires dependencies and owns the process lifecycle.
- All application code lives in `internal/`, split by responsibility
  (domain, transport, storage, auth) rather than by technical layer names.
- No `pkg/`: nothing in this repository is meant to be imported by other
  modules. If that changes, a package is moved out of `internal/` deliberately.
- Protobuf contracts live in `api/`, separate from their implementation.
- Directories are created when the first file needs them; no placeholder
  directories or `.gitkeep` files.

## Consequences

- The compiler guarantees that no one depends on internal packages.
- Top-level directories map to things a reader cares about: binaries, API
  contracts, application code, infrastructure.
- Exposing a public library later requires an explicit move, which is the
  intended friction.
