# Phase 0: Project Scaffolding & Container Setup — Design

**Date:** 2026-08-10
**Status:** Approved
**Author:** brainstorming session (alvesfc + Claude)

## Purpose

Establish the repo skeleton and local Redis container tooling described by
[PRD v1.1](../../foi-messaging-go-prd-v1.1.md) so later phases have an
unambiguous place to put real code and a working environment to test
against. This phase contains **no messaging logic, no test assertions, and
no CI** — those are later phases.

## Scope

In scope:

- Go module initialization
- Package/directory skeleton matching PRD §18
- Local Redis container via Docker Compose
- Testcontainers-go wiring (helper function only — no test files)
- Makefile with common dev targets
- golangci-lint configuration, including the internal-boundary `depguard`
  rule named in PRD §12/§21

Out of scope (explicitly deferred to later phases):

- CI workflow (GitHub Actions)
- Envelope, publisher, consumer, DLQ, retry, or any other library logic
- Any `_test.go` file with actual test cases/assertions
- `examples/` content beyond a placeholder

## 1. Directory & file layout

Mirrors PRD §18 exactly, so every future phase has an unambiguous home:

```text
foi-messaging-go/
├── go.mod / go.sum
├── Makefile
├── .golangci.yml
├── docker-compose.yml
├── doc.go            # package messaging — top-level package doc comment
├── config.go  envelope.go  eventdef.go  publisher.go  consumer.go
├── handler.go validation.go errors.go   context.go     dlq.go
├── telemetry/
│   └── doc.go
├── internal/
│   ├── watermill/doc.go
│   ├── redis/doc.go
│   └── testsupport/
│       └── redis.go   # Testcontainers helper (see §4)
├── testing/
│   └── doc.go
└── examples/
    └── doc.go
```

Each top-level `.go` file contains only a `package messaging` line and a
doc comment pointing at the relevant PRD section (e.g. `// Envelope type —
see PRD §5. Not yet implemented.`), so `go build ./...` and `go vet ./...`
succeed immediately. Directory-only packages (`telemetry/`,
`internal/watermill/`, `internal/redis/`, `testing/`, `examples/`) get a
`doc.go` so Git tracks the empty directory and the build has something to
compile in each package.

## 2. Go module

- `module github.com/bcgov/foi-messaging-go` — matches the actual GitHub
  org this repo is hosted under (README's `foi-platform` path was a
  placeholder).
- `go 1.22` — matches the minimum the README already commits to (generics
  + `slog`), independent of whatever toolchain version is installed
  locally.

## 3. docker-compose.yml

One disposable service — no volume, since this is dev/test infrastructure,
not data worth persisting:

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

`docker compose up` gives a developer a Redis Streams-capable instance for
manual/local runs against `redis:6379`.

## 4. Testcontainers wiring (no tests yet)

An internal helper package, `internal/testsupport`, wraps
`testcontainers-go`'s Redis module:

```go
package testsupport

func StartRedis(ctx context.Context) (addr string, terminate func(context.Context) error, err error)
```

This is plumbing — a function future integration tests will call — not a
test itself. Deliberately named `testsupport`, not `testing/`, to avoid
colliding with the PRD's public `testing/` package (§19), which is a
different thing: app-facing mocks for *consumers* of this library, not
this repo's own integration-test infra.

## 5. Makefile + golangci-lint

Makefile targets:

- `build` — `go build ./...`
- `test` — unit tests only
- `test-integration` — build-tag gated (`-tags=integration`), uses
  `testsupport`
- `up` / `down` — `docker compose up -d` / `docker compose down`
- `lint` — `golangci-lint run`
- `tidy` — `go mod tidy`

`.golangci.yml` enables standard linters (`govet`, `staticcheck`,
`errcheck`, `unused`, `gofmt`/`goimports`) and a `depguard` rule blocking
`github.com/ThreeDotsLabs/watermill` and `github.com/redis/go-redis`
imports outside `internal/`. This directly implements the boundary PRD
§12/§21 call out; CI enforcement of it is deferred, but the local rule
costs nothing to add now.

## Alternatives considered

- **Testcontainers only, no docker-compose** — rejected: loses the fast
  manual dev loop (`docker compose up` + point a REPL/script at
  `localhost:6379`) that doesn't require writing Go code to inspect state.
- **docker-compose only, no Testcontainers wiring** — rejected: PRD §19
  states integration tests use Testcontainers; deferring the dependency
  wiring would just move this exact work into Phase 1 with no benefit.
- **Single `doc.go` only, create PRD §18 files on demand per phase** —
  rejected: the PRD's file list is a fixed target; stubbing them now
  makes "where does X go" a solved question for every later phase, at
  the cost of ten near-empty files today.
