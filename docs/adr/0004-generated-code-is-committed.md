# 4. Generated code is committed

Date: 2026-10-08

## Status

Accepted

## Context

Go code generated from the protobuf contracts can either be committed or
produced on every build. Generating on build keeps the repository smaller but
means nobody can `go build` or `go get` the module without buf and the protoc
plugins installed. Committing it risks the code drifting from the `.proto`
files.

## Decision

- Generated code lives in `gen/` and is committed. It stays outside
  `internal/` on purpose: the stubs are the Go client SDK for this API, the
  one part of the module other modules are expected to import. This refines
  ADR-0001.
- Generation is reproducible: buf and the protoc plugins are pinned in
  `tools/go.mod`, protobuf dependencies in `buf.lock`. `buf.gen.yaml` runs the
  plugins through `go tool`, so nothing has to be installed globally.
- `buf.gen.yaml` uses managed mode for `go_package`, keeping the `.proto`
  files free of Go-specific options, and `clean: true`, so removing a contract
  also removes its generated code.
- CI regenerates the code and fails if `gen/` changes.
- `.gitattributes` marks `gen/` as `linguist-generated`, so GitHub collapses
  it in diffs and leaves it out of language statistics.
- Breaking-change detection uses buf's `FILE` category, the strictest one.
  Because `gen/` is a Go SDK, renaming a field or message breaks consumers'
  code even when the wire format stays compatible; `WIRE` or `WIRE_JSON`
  would let that through.

Rejected: a separate Go module for `gen/`. It would give SDK users a smaller
dependency graph, since today they also pull in everything the server
requires, but it needs its own versioning and release tags and a `replace`
during development. Worth revisiting if the SDK gets external users.

## Consequences

- The module builds with the plain Go toolchain; reviewers can read the
  generated API on GitHub.
- Changing a contract means running `make gen` and committing the result;
  CI catches it when that is forgotten.
- Pull requests that change contracts show the generated diff collapsed, so
  review focuses on the `.proto` change.
- Dependabot updates of the protoc plugins change the version header of the
  generated files, so those PRs fail the drift check until `make gen` is run
  on their branch.
- `protoc-gen-go` in `tools/go.mod` must not be newer than
  `google.golang.org/protobuf` in the root `go.mod`; generated code fails to
  compile against an older runtime (`protoimpl.EnforceVersion`). Bump the
  runtime first.
