# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`github.com/bcgov/foi-messaging-go` — a Go library (no binaries) giving FOI platform
services a typed, transport-agnostic messaging API over Watermill + Redis Streams.
Applications import only the root `messaging` package; Watermill, go-redis, and
watermill-redisstream never cross the library boundary.

Design of record: [`docs/foi-messaging-go-prd-v1.1.md`](docs/foi-messaging-go-prd-v1.1.md).
Code comments and plans cite it as "PRD §N" — when changing behaviour, check the cited
section still matches.

## Commands

```bash
make build              # go build ./...
make test               # unit tests only (no Docker needed)
make test-examples      # cd examples/telemetry && go test ./...  (a separate Go module)
make test-all           # make test test-examples — every non-Docker test tier
make test-integration   # go test -tags=integration ./...  (needs Docker: Testcontainers)
make lint               # golangci-lint run ./...
make up / make down     # local Redis on :6379 via docker compose (not used by tests)

go test -run TestConfig_Validate ./...                              # single unit test
go test -tags=integration -run TestConsumer_Run ./ -v               # single integration test
go test -tags=integration -race -count=1 ./...                      # what to run before claiming green
```

Integration tests start their own disposable Redis container per suite via
`internal/testsupport.StartRedis` — they do not use `docker-compose.yml`. `make test`
skips them entirely because of the build tag, so a passing `make test` proves very little
about the consume path. `examples/telemetry` is its own Go module (nested `go.mod`), so
`./...` from the root never runs it — that's what `make test-examples` is for, and it
holds the test pinning the exported Prometheus metric names. `go test ./...` does cover
`testing/` (`messagingtest`), which is part of this module.

CI (`.github/workflows/ci.yml`) runs three jobs on every pull request and push to
`main` — `lint`, `test` (unit + `examples/telemetry` + the changelog script test),
and `integration` — all under `-race -count=1`, via the reusable
`.github/workflows/verify.yml`. `make verify` runs exactly the same set locally,
and is what you should run before pushing a release tag: `release.yml` calls the
same reusable workflow, so a tag whose gates fail produces no GitHub Release —
but the tag itself still exists, and a module version that reaches
proxy.golang.org can never be corrected, only superseded.

## Architecture

Three layers, deliberately separated so the dependency boundary is enforceable:

- **Root package `messaging`** (`config.go`, `publisher.go`, `consumer.go`, `registry.go`,
  `envelope.go`, `validation.go`, `handler.go`, `context.go`, `telemetry.go`) — the entire
  public API.
  Owns the envelope contract, config defaulting/validation, handler registration,
  and dispatch. Must never import Watermill or go-redis, *including types* like
  `message.Message`.
- **`internal/redis`** — `NewClient` plus `StreamReader`, the raw Redis Streams verbs
  (`EnsureGroup`/`ReadNew`/`PendingOverIdle`/`Claim`/`Ack`) returning plain structs.
  Imports go-redis, never Watermill.
- **`internal/watermill`** — `Publisher`, `Subscriber` (our own `message.Subscriber`
  implementation over `StreamReader`), `Router` wrapper, and a slog logger adapter.
  Imports Watermill throughout, and go-redis in exactly one file: `publisher.go`,
  because `redisstream.PublisherConfig` wants a `*goredis.Client` and there is no
  seam to hide it behind. `subscriber.go` needs no such import — it reads through
  `internal/redis.StreamReader`, which is the whole reason that interface exists.
  Its handler seam (`MessageHandler`) takes only `[]byte` + `map[string]string`,
  which is what keeps Watermill types out of the root.

OpenTelemetry is deliberately **outside** this boundary and is imported by the root
package directly: `TelemetryConfig` is public API, so applications hand us `TracerProvider`,
`MeterProvider`, and `Propagator` values. Do not try to hide OTel behind `internal/` — every
fact worth recording (event type, classification, outcome) is only knowable in the root, and
`internal/watermill` importing the root would be an import cycle.

A golangci-lint `depguard` rule in `.golangci.yml` enforces the boundary: those three
modules are denied outside `**/internal/**`. If you find yourself wanting a Watermill
type in the root package, add a plain-typed seam in `internal/watermill` instead.

`internal/testseam` is a fourth, much smaller package: three function variables the
root registers at `init` (`testhooks.go`) so `testing/` can drive the *real* publish and
consume paths. It exists because `Consumer.dispatch`, `contextWithCorrelationID`, and a
dead-letter sink are all unexported with no exported path reaching them — `Consumer.dlq`
is nil until `Run`, and `deadLetter` refuses to run without one. Moving `dispatch` into
`internal/` instead is impossible: it needs `Envelope[T]`, `IsPermanent`, `DeadLetter`,
and the registry, so an internal package holding it would import the root and cycle —
the same constraint that puts OpenTelemetry outside the boundary. `testseam` imports
only `context`, so it cannot cycle, and `internal/` keeps it invisible to applications:
the root's exported API is unchanged by its existence.

The `testseam.Probe` travels on the delivery's **context**, not on the Consumer. That is
what lets `messagingtest.Dispatch` need no lock, run concurrently against one Consumer,
and leave the application's Consumer unmutated once it returns. The price is two
`ctx.Value` lookups per delivery — one in `dispatch`, one in `deadLetter` — both of
which return nil in production. That price is accepted deliberately; do not "optimise"
it into a Consumer field.

### Publish path

`Publisher.Publish` builds an `Envelope[T]` (generating `event_id` as UUIDv7 and
`timestamp`), resolves the correlation ID (explicit `WithCorrelationID` → context →
new UUIDv7), validates, marshals, and writes to stream `{StreamPrefix}:{EventDef.Topic}`
(prefix defaults to `foi`). No buffering, no retry — errors are returned synchronously.

### Consume path

`NewConsumer` opens nothing; `Run` builds the client, subscriber, and router once the
topic set is known, then blocks until ctx is cancelled and drains within
`Consumer.ShutdownTimeout`. Handlers are registered via the generic free function
`RegisterHandler[T]` (Go has no generic methods) or `RegisterRawHandler`; registration
after `Run` starts is an error.

Routing is two-level: topic → Redis stream, then `event_type` + **major** schema version →
handler. Minor/patch deliberately do not participate, so a `1.0.0` handler receives `1.4.2`
events; JSON decoding must stay lenient (never `DisallowUnknownFields`). A topic has
either typed handlers or one raw handler, never both. Unmatched events are ACKed and
logged at debug — topics are shared.

`Consumer.dispatch` states a `deliveryOutcome` on each of its exit paths rather than
recording telemetry at each one; a single deferred `deliveryRecorder.end()` turns that into
the span status, the duration histogram, and exactly one terminal counter. `event_type`
becomes a metric attribute only on a `matchTyped` registry hit — a raw handler takes every
event on its topic, so its event type is an unbounded wire value.

`Consumer.dispatch` is the whole failure path: the delivery-attempt cap fires
before decoding (an over-cap event must not spend four handler invocations, and
its concurrency slot, proving it), then undecodable/invalid/unversioned
envelopes are dead-lettered rather than nacked (all three are permanent by
definition), then `runWithRetry` runs the handler with full-jitter backoff.
Retry is a plain loop in the root package, **not** Watermill middleware: the
classification predicates live in the root, and the root imports
`internal/watermill`, so a middleware there would be an import cycle.
Retries sleep inside the message's per-topic concurrency slot, which is why
`ClaimMinIdle` must exceed `(1+MaxImmediateRetries) × handler + backoff` —
`validateConsumer` rejects a config where the backoff term alone reaches it.

An entry Watermill's own marshaller cannot read never reaches `dispatch`, so
neither the cap nor the DLQ could see it. `internal/watermill`'s one concession
is a plain-typed `OnUndecodable` hook that `Run` wires to the DLQ; it takes only
`[]byte`-ish values, so no Watermill type crosses back.

`internal/watermill.Subscriber` runs a read loop (`XREADGROUP` for new entries) and a
reclaim loop (`XPENDING` over `ClaimMinIdle` → `XCLAIM`) per subscription. Concurrency is
bounded **per subscribed stream**, not per Subscriber — a shared semaphore let one topic's
blocking read throttle another. A reclaimed entry's delivery attempt is
`PendingEntry.RetryCount + 1` because the `XCLAIM` itself is the next delivery.

Several non-obvious lifecycle invariants exist and are documented inline with the bug each
one fixes — read the comment before "simplifying" any of them:

- `Consumer.Run` takes the empty-registry check, running check, and topic snapshot in one
  critical section (TOCTOU with concurrent registration).
- `Consumer.Close` is a no-op while `Run` is in progress; stopping a running consumer means
  cancelling `Run`'s context. Closing the client under a live read loop wedges it forever.
- `sharedSubscriber` in `router.go` suppresses Watermill's per-handler `Close`, because
  every handler shares one Subscriber and Watermill closes it before the drain begins.
- Per-message contexts are `context.WithoutCancel(ctx)`-derived so handlers keep a live
  context through the whole drain; the ack path uses its own detached, timeout-bounded ctx.
- `Run` joins every teardown error rather than returning the first.

### Transport metadata vs envelope

The envelope carries business/workflow metadata only. Delivery attempt and Redis entry ID
travel as in-process message metadata (`_foi_stream_id`, `_foi_delivery_attempt` in
`internal/watermill`), are logged, and are never merged into the envelope (PRD §5).

## Implementation status

Every phase is done: 0 (scaffolding), 1 (publish), 2a (consume), 2b (error
classification, retry, the delivery-attempt cap, DLQ), 3 (spans and metrics), and 4 —
the application-facing `testing/` (`messagingtest`) package, which holds `Publisher`,
`Deliver`, `Dispatch`, `NewEvent`, and `Config`.

`examples/telemetry` is its own Go module, so `go test ./...` from the root does not reach
it — run `make test-all`, or `make test-examples`. It is a separate module so the Prometheus
exporter's `client_golang` dependency stays out of the library's `go.mod`.

Keep the README's "Planned: Phase N" markers and `doc.go` honest when a phase lands — past
review rounds repeatedly caught the docs claiming unimplemented behaviour.

## Conventions

- Go 1.25, Redis 7.0+.
- Tests use the **standard library `testing` package only** — no testify anywhere in
  first-party test code, including test-only deps.
- Integration tests carry `//go:build integration` and live in the `_test` external package
  (`package messaging_test`, `package redis_test`, …), exercising the library as an
  application would.
- Comments explain *why*, especially where a subtle bug motivated the code. This codebase's
  comment density is well above typical Go; match it in the packages you touch.
- Work is spec-driven: a design spec in `docs/superpowers/specs/`, then a task-by-task plan
  in `docs/superpowers/plans/`, then TDD commits (`feat:`/`fix:`/`test:`/`docs:`), then a
  merge commit for the phase branch. Review findings get carried back into the spec.
- No `Co-Authored-By` trailer (`.claude/settings.local.json` sets
  `includeCoAuthoredBy: false`).
