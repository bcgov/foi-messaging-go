# Phase 0: Project Scaffolding & Container Setup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up the `foi-messaging-go` repo skeleton (Go module, PRD §18 package layout, Docker Compose Redis, Testcontainers-go wiring, Makefile, golangci-lint) with no messaging logic or test assertions yet, so later phases have an unambiguous place to put real code and a working local environment to test against.

**Architecture:** A flat top-level `messaging` package with stub files matching PRD §18's file list, plus five near-empty sub-packages (`telemetry/`, `internal/watermill/`, `internal/redis/`, `testing/` as `package messagingtest`, `examples/`). Container tooling is dual: `docker-compose.yml` for manual local dev, and an `internal/testsupport` helper wrapping Testcontainers-go for future automated integration tests.

**Tech Stack:** Go 1.22+ (module targets 1.22, built with whatever toolchain is installed), Docker Compose, `github.com/testcontainers/testcontainers-go` + its `modules/redis` package, `golangci-lint`.

## Global Constraints

- Module path: `github.com/bcgov/foi-messaging-go`
- `go.mod` `go` directive: `1.22` (matches README's stated minimum)
- Redis image: `redis:7.4-alpine`, no persistent volume (disposable dev/test infra)
- No CI workflow in this phase
- No messaging/library logic (envelope, publisher, consumer, DLQ, retry) in this phase — stub files only
- No committed `_test.go` files with assertions in this phase — `internal/testsupport` is plumbing, verified manually, not via a committed test
- The PRD's public `testing/` package (§19) uses package name `messagingtest`, not `testing` — confirmed by the README's `messagingtest.Publisher{}` usage. Our own internal integration-test helper package is named `testsupport` specifically to avoid colliding with both the stdlib `testing` package and the PRD's `messagingtest` package.
- `depguard` must block `github.com/ThreeDotsLabs/watermill` and `github.com/redis/go-redis` imports outside `internal/` (PRD §12, §21)

---

## Task 1: Go module and package skeleton

**Files:**
- Create: `go.mod`
- Create: `doc.go`
- Create: `config.go`, `envelope.go`, `eventdef.go`, `publisher.go`, `consumer.go`, `handler.go`, `validation.go`, `errors.go`, `context.go`, `dlq.go`
- Create: `telemetry/doc.go`
- Create: `internal/watermill/doc.go`
- Create: `internal/redis/doc.go`
- Create: `testing/doc.go`
- Create: `examples/doc.go`

**Interfaces:**
- Consumes: nothing (first task)
- Produces: a buildable Go module `github.com/bcgov/foi-messaging-go` with package `messaging` at the root, importable sub-packages `telemetry`, `internal/watermill`, `internal/redis`, `examples`, and `testing` (package name `messagingtest`). Task 3 will add `go get` dependencies to this `go.mod`; Task 4's Makefile will run `go build ./...` against this tree.

- [ ] **Step 1: Initialize the Go module**

Run:
```bash
go mod init github.com/bcgov/foi-messaging-go
```

This creates `go.mod` with whatever Go version is installed locally as the `go` directive.

- [ ] **Step 2: Pin the go directive to 1.22**

Open the generated `go.mod` and set the `go` line to exactly:
```
go 1.22
```
(Leave the `module` line as `module github.com/bcgov/foi-messaging-go`.)

- [ ] **Step 3: Create the root package doc file**

Create `doc.go`:
```go
// Package messaging provides a transport-agnostic, strongly-typed
// asynchronous messaging library for FOI platform services, built on
// Watermill and Redis Streams.
//
// See docs/foi-messaging-go-prd-v1.1.md for the full design. This is a
// Phase 0 scaffold: the types and functions described in the PRD are not
// yet implemented.
package messaging
```

- [ ] **Step 4: Create the remaining root stub files**

Create `config.go`:
```go
package messaging

// Config will be the library's single configuration object, covering
// Source, Redis, Consumer, Retry, and Telemetry settings.
// See PRD §17. Not yet implemented — Phase 0 scaffolding only.
```

Create `envelope.go`:
```go
package messaging

// Envelope will be the standard event envelope wrapping every published
// and consumed message, generic over the payload type.
// See PRD §5. Not yet implemented — Phase 0 scaffolding only.
```

Create `eventdef.go`:
```go
package messaging

// EventDef will be the typed descriptor (topic + type + version) that
// identifies an event contract, shared by publishers and consumers.
// See PRD §8. Not yet implemented — Phase 0 scaffolding only.
```

Create `publisher.go`:
```go
package messaging

// Publisher and NewPublisher will let applications publish typed
// payloads without importing Watermill or go-redis.
// See PRD §8. Not yet implemented — Phase 0 scaffolding only.
```

Create `consumer.go`:
```go
package messaging

// Consumer, NewConsumer, and RegisterHandler will let applications
// register typed handlers and run a consumer loop.
// See PRD §9. Not yet implemented — Phase 0 scaffolding only.
```

Create `handler.go`:
```go
package messaging

// Handler will be the generic interface applications implement to
// process a typed event payload.
// See PRD §9. Not yet implemented — Phase 0 scaffolding only.
```

Create `validation.go`:
```go
package messaging

// Envelope validation will check required fields, event_type format,
// schema_version semver parsing, and timestamp presence at publish and
// consume time.
// See PRD §18. Not yet implemented — Phase 0 scaffolding only.
```

Create `errors.go`:
```go
package messaging

// AsPermanent, AsRetryable, AsDiscard, and their Is* predicates will
// implement the library's error classification scheme.
// See PRD §15. Not yet implemented — Phase 0 scaffolding only.
```

Create `context.go`:
```go
package messaging

// Correlation ID propagation through context.Context will live here:
// reading an inbound correlation ID and placing it for outbound
// publishes within the same handler chain.
// See PRD §5 (Correlation ID Semantics). Not yet implemented — Phase 0
// scaffolding only.
```

Create `dlq.go`:
```go
package messaging

// DeadLetter will be the exported wrapper contract used to publish
// permanently failed or delivery-cap-exceeded messages to a topic's
// DLQ stream.
// See PRD §14. Not yet implemented — Phase 0 scaffolding only.
```

- [ ] **Step 5: Create the sub-package doc files**

Create `telemetry/doc.go`:
```go
// Package telemetry provides the OpenTelemetry tracing and Prometheus
// metrics integration for the messaging library.
//
// See docs/foi-messaging-go-prd-v1.1.md §16. Not yet implemented — Phase
// 0 scaffolding only.
package telemetry
```

Create `internal/watermill/doc.go`:
```go
// Package watermill wraps Watermill's Publisher, Subscriber, Router, and
// middleware chain. This package is internal: no application code, and
// no other package in this module outside internal/, may import
// Watermill directly (PRD §12, §21).
//
// See docs/foi-messaging-go-prd-v1.1.md §12. Not yet implemented — Phase
// 0 scaffolding only.
package watermill
```

Create `internal/redis/doc.go`:
```go
// Package redis wraps the go-redis client and the Redis Streams
// adapter. This package is internal: no application code, and no other
// package in this module outside internal/, may import go-redis
// directly (PRD §12, §21).
//
// See docs/foi-messaging-go-prd-v1.1.md §12. Not yet implemented — Phase
// 0 scaffolding only.
package redis
```

Create `testing/doc.go`:
```go
// Package messagingtest (imported from the testing/ directory) lets
// applications that consume this library unit-test their publish paths
// and handlers without a running Redis instance.
//
// See docs/foi-messaging-go-prd-v1.1.md §19. Not yet implemented — Phase
// 0 scaffolding only.
package messagingtest
```

Create `examples/doc.go`:
```go
// Package examples will hold runnable example programs demonstrating
// library usage and service migration guides, one subdirectory per
// example.
//
// See docs/foi-messaging-go-prd-v1.1.md §21 (Rollout). Not yet
// implemented — Phase 0 scaffolding only.
package examples
```

- [ ] **Step 6: Verify the module builds**

Run:
```bash
go build ./...
go vet ./...
```
Expected: both commands exit 0 with no output.

- [ ] **Step 7: Commit**

```bash
git add go.mod doc.go config.go envelope.go eventdef.go publisher.go consumer.go handler.go validation.go errors.go context.go dlq.go telemetry/doc.go internal/watermill/doc.go internal/redis/doc.go testing/doc.go examples/doc.go
git commit -m "chore: scaffold go module and PRD §18 package layout"
```

---

## Task 2: Docker Compose Redis service

**Files:**
- Create: `docker-compose.yml`

**Interfaces:**
- Consumes: nothing (independent of Task 1)
- Produces: a `redis` service reachable at `localhost:6379` when running, consumed manually by developers and referenced by Task 4's `make up` / `make down` targets.

- [ ] **Step 1: Create the compose file**

Create `docker-compose.yml`:
```yaml
services:
  redis:
    image: redis:7.4-alpine
    ports:
      - "6379:6379"
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 3s
      retries: 5
```

- [ ] **Step 2: Verify the container starts and is healthy**

Run:
```bash
docker compose up -d
docker compose ps
```
Expected: the `redis` service shows `running (healthy)` (allow a few seconds for the healthcheck to pass; re-run `docker compose ps` if it still shows `starting`).

- [ ] **Step 3: Verify Redis actually responds**

Run:
```bash
docker compose exec redis redis-cli ping
```
Expected: `PONG`

- [ ] **Step 4: Tear the container down**

Run:
```bash
docker compose down
```
Expected: exits 0, container removed.

- [ ] **Step 5: Commit**

```bash
git add docker-compose.yml
git commit -m "chore: add docker-compose Redis service for local dev"
```

---

## Task 3: Testcontainers-go wiring

**Files:**
- Create: `internal/testsupport/redis.go`
- Modify: `go.mod`, `go.sum` (via `go get` / `go mod tidy`)

**Interfaces:**
- Consumes: `go.mod` from Task 1 (module must exist before `go get` can add dependencies to it)
- Produces: `func testsupport.StartRedis(ctx context.Context) (addr string, terminate func(context.Context) error, err error)` — later phases' integration tests will call this to get a live Redis address and a teardown function.

- [ ] **Step 1: Add the Testcontainers dependencies**

Run:
```bash
go get github.com/testcontainers/testcontainers-go@latest
go get github.com/testcontainers/testcontainers-go/modules/redis@latest
```
Expected: both commands exit 0 and add entries to `go.mod`/`go.sum`.

- [ ] **Step 2: Create the helper package**

Create `internal/testsupport/redis.go`:
```go
// Package testsupport provides infrastructure helpers for this
// repository's own integration tests. It is internal plumbing, not the
// PRD's application-facing testing/ (messagingtest) package, and must
// never be imported by application code.
package testsupport

import (
	"context"
	"fmt"

	"github.com/docker/go-connections/nat"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
)

// StartRedis launches a disposable Redis 7 container for integration
// tests and returns its connection address (host:port) and a terminate
// function the caller must invoke to tear the container down.
func StartRedis(ctx context.Context) (addr string, terminate func(context.Context) error, err error) {
	container, err := tcredis.Run(ctx, "redis:7.4-alpine")
	if err != nil {
		return "", nil, fmt.Errorf("starting redis container: %w", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		return "", nil, fmt.Errorf("resolving redis container host: %w", err)
	}

	port, err := container.MappedPort(ctx, nat.Port("6379/tcp"))
	if err != nil {
		return "", nil, fmt.Errorf("resolving redis container port: %w", err)
	}

	terminate = func(ctx context.Context) error {
		return container.Terminate(ctx)
	}

	return fmt.Sprintf("%s:%s", host, port.Port()), terminate, nil
}
```

If `go build` reports a signature mismatch against the installed `testcontainers-go`/`modules/redis` version (these APIs move between minor versions), run `go doc github.com/testcontainers/testcontainers-go/modules/redis.Run` and `go doc github.com/testcontainers/testcontainers-go.Container` to check the current signatures, and adjust the calls to match — the container lifecycle (`Run` returns a container + error; the container exposes `Host`, `MappedPort`, and `Terminate`) is stable across versions even when exact option/return types differ.

- [ ] **Step 3: Verify it compiles**

Run:
```bash
go build ./...
go vet ./...
```
Expected: both exit 0.

- [ ] **Step 4: Manually verify the helper actually starts and stops a container**

This phase commits no test files, so verify by hand with a throwaway `go run`. Create a temporary file `/tmp/verify_testsupport.go`:
```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/bcgov/foi-messaging-go/internal/testsupport"
)

func main() {
	ctx := context.Background()
	addr, terminate, err := testsupport.StartRedis(ctx)
	if err != nil {
		log.Fatalf("StartRedis: %v", err)
	}
	fmt.Println("redis started at:", addr)

	if err := terminate(ctx); err != nil {
		log.Fatalf("terminate: %v", err)
	}
	fmt.Println("redis terminated cleanly")
}
```
Run:
```bash
go run /tmp/verify_testsupport.go
```
Expected output: `redis started at: <host>:<port>` followed by `redis terminated cleanly`. Docker must be running for this to succeed.

Delete the throwaway file afterward — it is not part of the repo:
```bash
rm /tmp/verify_testsupport.go
```

- [ ] **Step 5: Commit**

```bash
git add go.mod go.sum internal/testsupport/redis.go
git commit -m "chore: wire testcontainers-go redis helper for future integration tests"
```

---

## Task 4: Makefile and golangci-lint config

**Files:**
- Create: `Makefile`
- Create: `.golangci.yml`

**Interfaces:**
- Consumes: `go build ./...` target from Task 1's module, `docker-compose.yml` from Task 2, the `internal/testsupport` package existing from Task 3 (so `test-integration` has something to eventually exercise via the `integration` build tag)
- Produces: `make build`, `make test`, `make test-integration`, `make lint`, `make tidy`, `make up`, `make down` — the developer-facing entry points every later phase's contributors will use.

- [ ] **Step 1: Create the Makefile**

Create `Makefile`:
```makefile
.PHONY: build test test-integration lint tidy up down

build:
	go build ./...

test:
	go test ./...

test-integration:
	go test -tags=integration ./...

lint:
	golangci-lint run ./...

tidy:
	go mod tidy

up:
	docker compose up -d

down:
	docker compose down
```

- [ ] **Step 2: Install golangci-lint if it isn't already on PATH**

Run:
```bash
which golangci-lint || go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest
```
Expected: `golangci-lint version` then prints a version string. (`go install` puts the binary in `$(go env GOPATH)/bin` — make sure that's on your `PATH`.)

- [ ] **Step 3: Create the golangci-lint config**

Run `golangci-lint version` first to confirm which major config schema is installed (v2's config starts with `version: "2"`; v1 does not have that key and uses a top-level `linters-settings:` key instead of `linters.settings:`). The config below targets v2; if the installed binary is v1, move the `settings:` block under a top-level `linters-settings:` key instead.

Create `.golangci.yml`:
```yaml
version: "2"
linters:
  enable:
    - govet
    - staticcheck
    - errcheck
    - unused
    - gofmt
    - goimports
  settings:
    depguard:
      rules:
        internal-only:
          files:
            - "!**/internal/**"
          deny:
            - pkg: github.com/ThreeDotsLabs/watermill
              desc: "Watermill must stay inside internal/ (PRD §12, §21)"
            - pkg: github.com/redis/go-redis
              desc: "go-redis must stay inside internal/ (PRD §12, §21)"
```

- [ ] **Step 4: Verify the Makefile targets work**

Run:
```bash
make build
make lint
make up
make down
```
Expected:
- `make build` exits 0 (same as Task 1's `go build ./...`)
- `make lint` exits 0 with no findings (no code yet imports watermill/go-redis, so the `depguard` rule is currently inert — it activates once a later phase adds those imports outside `internal/`)
- `make up` starts the Redis container (same as Task 2's verification)
- `make down` stops it

- [ ] **Step 5: Commit**

```bash
git add Makefile .golangci.yml
git commit -m "chore: add Makefile and golangci-lint config with internal-boundary depguard rule"
```

---

## Post-plan verification

After all four tasks are committed, run from the repo root:
```bash
go build ./...
go vet ./...
make lint
docker compose up -d && docker compose exec redis redis-cli ping && docker compose down
```
All four should succeed, confirming Phase 0's deliverable: a buildable module matching PRD §18's layout, a working local Redis container, and lint tooling with the internal-boundary rule in place — ready for Phase 1 to start implementing real library logic (envelope, config, publisher) against this skeleton.
