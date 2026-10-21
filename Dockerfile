# syntax=docker/dockerfile:1

FROM golang:1.27.2 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
# Static binaries: the runtime image has no C library.
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/server ./cmd/chat

# No shell, no package manager, a non-root user and CA certificates
# (ADR-0012). The digest pins the tag, which is rebuilt in place.
FROM gcr.io/distroless/static-debian13:nonroot@sha256:d62067879de890a1def9da0e7766b7d20810a5e5555cef8506b752437d7a3e88
COPY --from=build /out/server /out/chat /usr/local/bin/
USER nonroot:nonroot
EXPOSE 50051
# The server is PID 1 and handles SIGTERM itself: no shell form, no init.
ENTRYPOINT ["server"]
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 CMD ["server", "-healthcheck"]
