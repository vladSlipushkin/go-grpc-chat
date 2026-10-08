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

- Generated code lives in `gen/` and is committed.
- Generation is reproducible: buf and the protoc plugins are pinned in
  `tools/go.mod`, protobuf dependencies in `buf.lock`. `buf.gen.yaml` runs the
  plugins through `go tool`, so nothing has to be installed globally.
- `buf.gen.yaml` uses managed mode for `go_package`, keeping the `.proto`
  files free of Go-specific options, and `clean: true`, so removing a contract
  also removes its generated code.
- CI regenerates the code and fails if `gen/` changes.
- `.gitattributes` marks `gen/` as `linguist-generated`, so GitHub collapses
  it in diffs and leaves it out of language statistics.

## Consequences

- The module builds with the plain Go toolchain; reviewers can read the
  generated API on GitHub.
- Changing a contract means running `make gen` and committing the result;
  CI catches it when that is forgotten.
- Pull requests that change contracts show the generated diff collapsed, so
  review focuses on the `.proto` change.
