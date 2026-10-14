# go-grpc-chat

Real-time chat server in Go built on gRPC bidirectional streaming.

## Try it

`make run` starts the server; `make build` builds the `chat` client.
[docs/manual-testing.md](docs/manual-testing.md) walks through two users
chatting in two terminals and the API with grpcurl.

## Status

Work in progress.

- Until messages are stored, the per-room message sequence (`seq`) starts
  again from 1 when a room becomes empty.
- Until storage exists, user accounts are kept in memory and lost on
  restart.
