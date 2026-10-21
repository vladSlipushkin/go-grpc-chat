# syntax=docker/dockerfile:1

# Records docs/demo.gif: VHS drives a terminal with tmux, the server and
# two clients in one container (make demo).

FROM golang:1.27.2 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -o /out/ ./cmd/server ./cmd/chat

FROM ghcr.io/charmbracelet/vhs:v0.12.1
RUN apt-get update && apt-get install -y --no-install-recommends tmux \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/server /out/chat /usr/local/bin/
