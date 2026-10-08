# go-grpc-chat

Real-time chat server in Go built on gRPC bidirectional streaming.

## Status

Work in progress.

- Until messages are stored, the per-room message sequence (`seq`) starts
  again from 1 when a room becomes empty.
