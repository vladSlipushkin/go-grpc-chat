# 1. Project layout

Date: 2026-10-08

## Status

Accepted

## Context

Go has no official project template. The only import-visibility rule the go
command enforces is `internal/`: packages under it can be imported only from
code rooted at the parent of `internal/`, which here means only from this
module. The popular `golang-standards/project-layout` repository is a community
collection of conventions, not a standard, and copying all of its directories
adds structure without adding meaning.

The official guide "Organizing a Go module" (go.dev/doc/modules/layout)
recommends, for a server project, keeping commands in `cmd/` and everything
else in `internal/`.

## Decision

- `cmd/<binary>/` holds thin `main` packages. `main` only calls `run`, which
  wires dependencies and owns the process lifecycle.
- All application code lives in `internal/`. Packages are named after what
  they provide (`chat`, `auth`, `postgres`, `grpcapi`) rather than generic
  layer buckets such as `models`, `services` or `controllers`.
- No `pkg/`: nothing in this repository is meant to be imported by other
  modules. If that changes, a package is moved out of `internal/` deliberately.
- Protobuf contracts live in `api/`, separate from their implementation.
- Directories are created when the first file needs them; no placeholder
  directories or `.gitkeep` files.

## Consequences

- The go command guarantees that code outside this module cannot import
  internal packages.
- Top-level directories map to things a reader cares about: binaries, API
  contracts, application code, infrastructure.
- Exposing a public library later requires an explicit move, which is the
  intended friction.
