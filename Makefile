# Recipes call only go, docker and git so they behave the same under
# cmd.exe on Windows and sh on Linux/macOS.

GO_IMAGE ?= golang:1.27
BIN_DIR  ?= bin

# Race detector needs cgo and a C toolchain; running it in the official
# golang image keeps the host free of both. Module and build caches live in
# named volumes so repeated runs stay fast.
DOCKER_GO = docker run --rm \
	--mount type=bind,src=$(CURDIR),dst=/src \
	--mount type=volume,src=go-grpc-chat-gomod,dst=/go/pkg/mod \
	--mount type=volume,src=go-grpc-chat-gobuild,dst=/root/.cache/go-build \
	--workdir /src \
	$(GO_IMAGE)

.DEFAULT_GOAL := help

.PHONY: help
help:
	@echo Usage: make TARGET
	@echo   fmt        format code
	@echo   lint       run golangci-lint
	@echo   test       run unit tests
	@echo   test-race  run tests with the race detector in Docker
	@echo   vuln       scan dependencies for known vulnerabilities
	@echo   check      lint, race tests and vulnerability scan
	@echo   build      build binaries into $(BIN_DIR)/
	@echo   up         start local infrastructure and wait until healthy
	@echo   down       stop local infrastructure

.PHONY: fmt
fmt:
	golangci-lint fmt

.PHONY: lint
lint:
	golangci-lint run

.PHONY: test
test:
	go test ./...

.PHONY: test-race
test-race:
	$(DOCKER_GO) go test -race ./...

.PHONY: vuln
vuln:
	go tool govulncheck ./...

.PHONY: check
check: lint test-race vuln

.PHONY: build
build:
	go build -o $(BIN_DIR)/ ./cmd/...

.PHONY: up
up:
	docker compose up --detach --wait

.PHONY: down
down:
	docker compose down
