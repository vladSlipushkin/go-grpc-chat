# Recipes run plain commands without shell syntax so they behave the same
# under cmd.exe on Windows and sh on Linux/macOS.
#
# Requires: Go 1.27, Docker, golangci-lint v2.14.0 (same version as CI).

GO_IMAGE ?= golang:1.27
BIN_DIR  ?= bin

# Development tools are pinned in a separate module so their dependencies
# never leak into the application's go.mod.
TOOL = go tool -modfile=tools/go.mod

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

define HELP
Usage: make TARGET
  hooks           enable git hooks from scripts/githooks
  fmt             format code
  lint            run golangci-lint
  test            run unit tests
  test-race       run tests with the race detector in Docker
  vuln            scan dependencies for known vulnerabilities
  gen             generate Go code from protobuf contracts
  proto-lint      lint and format-check protobuf contracts
  proto-breaking  check contracts for breaking changes against local main
  check           lint, proto-lint, race tests and vulnerability scan
  build           build binaries into $(BIN_DIR)/
  run             run the server locally with reflection and a dev JWT secret
  up              start local infrastructure and wait until healthy
  down            stop local infrastructure
endef

# Printed by make itself: echo keeps spacing in cmd.exe but collapses it in sh.
# "cd ." is a no-op that works in both shells and silences "Nothing to be done".
.PHONY: help
help:
	$(info $(HELP))@cd .

.PHONY: hooks
hooks:
	git config core.hooksPath scripts/githooks

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
	$(TOOL) govulncheck ./...

.PHONY: gen
gen:
	$(TOOL) buf generate

.PHONY: proto-lint
proto-lint:
	$(TOOL) buf lint
	$(TOOL) buf format --diff --exit-code

.PHONY: proto-breaking
proto-breaking:
	$(TOOL) buf breaking --against ".git#branch=main"

.PHONY: check
check: lint proto-lint test-race vuln

.PHONY: build
build:
	go build -o $(BIN_DIR)/ ./cmd/...

# Target-specific export sets the variable for the recipe in any shell, which
# "VAR=value command" syntax would not do under cmd.exe.
.PHONY: run
run: export CHAT_REFLECTION ?= true
# A fixed, public value for local runs only; deployments must set their own.
run: export CHAT_JWT_SECRET ?= insecure-local-development-secret-0123456789
run:
	go run ./cmd/server

.PHONY: up
up:
	docker compose up --detach --wait

.PHONY: down
down:
	docker compose down
