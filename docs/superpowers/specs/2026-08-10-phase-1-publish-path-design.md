# Phase 1: Publish Path — Design

**Date:** 2026-08-10
**Status:** Approved
**Author:** brainstorming session (alvesfc + Claude)

## Purpose

Implement real library logic for the write path described by
[PRD v1.1](../../foi-messaging-go-prd-v1.1.md) against the Phase 0 skeleton:
`Envelope`, `EventDef`, `Config`, and a `Publisher` that genuinely writes to a
Redis stream via Watermill. This phase contains no consumer, retry, DLQ, or
telemetry instrumentation — those are later phases.

## Scope

In scope:

- `EventDef` (PRD §8)
- `Envelope[T]` and its unexported constructor (PRD §5)
- Envelope validation (PRD §18: required fields, `event_type` format,
  `schema_version` format, non-zero timestamp)
- Correlation ID resolution for publish (PRD §5: option → context → generated
  UUIDv7), via unexported context helpers
- `Config` (full PRD §17 shape) and `Config.Validate()`, scoped to what
  `NewPublisher` needs
- `Publisher`, `NewPublisher`, `Publish`, `PublishResult`, `WithCorrelationID`,
  `Close`
- `internal/redis`: go-redis client construction
- `internal/watermill`: Watermill + watermill-redisstream publisher wrapper
- Unit tests for the above
- One integration test proving `Publish` writes a correctly shaped entry to a
  real Redis stream

Out of scope (explicitly deferred to later phases):

- Consumer, `Handler[T]`, `RegisterHandler`, routing/dispatch
- Retry layers, delivery attempt cap, DLQ (`dlq.go`, `errors.go` classification)
- OpenTelemetry span creation and Prometheus metric emission on `Publish`
  (Config still carries `TelemetryConfig` with defaulted providers, but
  `Publish` does not create spans or emit metrics yet)
- `ConsumerConfig` defaulting/validation logic (`Group` required,
  `ClaimMinIdle < ClaimInterval` check) — the struct exists so `Config`
  compiles as documented, but nothing exercises it without a consumer
- `testing/` package (`messagingtest.Publisher`) — pairs naturally with
  `messagingtest.Deliver` once a consumer/handler exists; bundled into a later
  test-support phase instead of revisited twice
- CI workflow

## 1. Data flow

```text
messaging.Publisher.Publish(ctx, def, payload, opts...)
  → resolve correlation ID (option → ctx → generate UUIDv7)
  → construct Envelope[any]{EventID, EventType, Timestamp, SchemaVersion,
                             CorrelationID, Source, Payload}
  → validate envelope (validation.go)
  → json.Marshal envelope
  → internal/watermill.Publisher.Publish(stream, eventID, bytes, metadata{})
       → internal/watermill wraps watermill-redisstream, using a *redis.Client
         built by internal/redis
```

Stream name is `{Config.StreamPrefix}:{EventDef.Topic}` (default prefix
`"foi"`), matching PRD §10's `foi:{topic}` scheme.

**Boundary:** the root `messaging` package never imports `watermill` or
`go-redis` packages directly. `internal/watermill`'s `Publish` method takes
plain `(topic string, id string, payload []byte, metadata map[string]string)`
— no `watermill.Message` or go-redis type crosses into the root package. This
keeps the existing `depguard` rule from Phase 0 meaningful without carve-outs.

## 2. Components

### `eventdef.go`

```go
type EventDef struct {
    Topic   string
    Type    string
    Version string
}
```

Plain struct, no logic.

### `envelope.go`

`Envelope[T any]` exactly as specified in PRD §5's Go representation, plus an
unexported constructor (e.g. `newEnvelope[T](def EventDef, source string,
correlationID string, payload T) Envelope[T]`) used internally by `Publish`.

### `validation.go`

Validates a constructed envelope:

- Required fields (`EventID`, `EventType`, `SchemaVersion`, `Source`,
  `CorrelationID`) non-empty
- `EventType` is 2–3 dot-separated lowercase/underscore segments
  (`^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*){1,2}$`)
- `SchemaVersion` matches `MAJOR.MINOR.PATCH` (non-negative integers) — a
  small hand-rolled regex check, not a new semver dependency; major-version
  extraction for routing is Phase 2's concern
- `Timestamp` is non-zero

Returns a single descriptive error on the first failing rule (or joins all
failures — implementation detail for the plan).

### `context.go`

Unexported context-key helpers: something like
`correlationIDFromContext(ctx) (string, bool)` and
`contextWithCorrelationID(ctx, id) context.Context`. Unexported because the
PRD's only public correlation API this phase is the `WithCorrelationID`
publish option; the consumer will reuse these same helpers to *set* the value
in Phase 2 — no public API is added or removed later, just a new caller.

### `config.go`

Full PRD §17 shape so `Config` compiles as documented:

```go
type Config struct {
    Source       string
    StreamPrefix string
    Redis        RedisConfig
    Consumer     ConsumerConfig
    Retry        RetryConfig
    Telemetry    TelemetryConfig
}
```

`Config.Validate()` is scoped to what `NewPublisher` needs this phase:

- `Source` required (non-empty)
- `Redis.Address` required (non-empty)
- Defaults: `StreamPrefix` → `"foi"`, `Redis.PoolSize` → 10×GOMAXPROCS (capped
  per PRD), `Retry.MaxImmediateRetries` → 3, `Retry.InitialBackoff` → 100ms,
  `Retry.MaxBackoff` → 5s, `Telemetry.TracerProvider`/`MeterProvider` →
  `otel.GetTracerProvider()`/`GetMeterProvider()`, `Telemetry.Logger` →
  `slog.Default()`
- `ConsumerConfig` fields are present on the struct (so the PRD's example
  config literal compiles) but `Validate()` does not require `Consumer.Group`
  or check `ClaimMinIdle < ClaimInterval` — that logic moves into
  `NewConsumer` in Phase 2, since nothing can exercise it without a consumer

### `publisher.go`

```go
type PublishResult struct {
    EventID   string
    Timestamp time.Time
}

type PublishOption func(*publishOptions) // unexported options struct

func WithCorrelationID(id string) PublishOption

type Publisher struct { /* cfg Config, internal watermill publisher */ }

func NewPublisher(cfg Config) (*Publisher, error)
func (p *Publisher) Publish(ctx context.Context, def EventDef, payload any, opts ...PublishOption) (PublishResult, error)
func (p *Publisher) Close() error
```

`Publish` takes `payload any`, not a generic type parameter — marshaling
doesn't need a static type, so this stays a plain method (unlike
`RegisterHandler`, which must be a top-level generic function per PRD §9's
explicit note that Go disallows generic methods). This matches the README's
`publisher.Publish(ctx, def, payload)` call shape.

`NewPublisher`: calls `cfg.Validate()`, builds a `*redis.Client` via
`internal/redis`, builds a Watermill publisher via `internal/watermill`,
returns a ready `*Publisher`.

`Publish`: resolves correlation ID, constructs and validates the envelope,
marshals to JSON, calls `internal/watermill`'s `Publish` with the computed
stream name. Returns `PublishResult` on success. Publish errors (validation,
marshal, or transport) are returned synchronously — no buffering or retry,
per PRD §8.

### `internal/redis/client.go` (new)

Builds a `*redis.Client` (go-redis) from primitive parameters — address,
username, password, TLS config, DB index, pool size. The only place
`github.com/redis/go-redis/v9` is imported outside `internal/watermill`.

### `internal/watermill/publisher.go` (new)

Takes the `*redis.Client`, constructs a `watermill-redisstream` publisher,
exposes:

```go
func NewPublisher(client *redis.Client, logger watermill.LoggerAdapter) (*Publisher, error)
func (p *Publisher) Publish(topic string, id string, payload []byte, metadata map[string]string) error
func (p *Publisher) Close() error
```

Only place `watermill` and `watermill-redisstream` packages are imported.
Exact constructor signatures for `watermill-redisstream` may differ from the
sketch above depending on the installed version — check with `go doc` during
implementation, same approach Phase 0 used for `testcontainers-go`.

## 3. New dependencies

- `github.com/ThreeDotsLabs/watermill`
- `github.com/ThreeDotsLabs/watermill-redisstream`
- `github.com/redis/go-redis/v9`

None of these are currently in `go.mod`. `github.com/google/uuid` is already
an indirect dependency (v1.6.0, has `NewV7()`) and is promoted to direct for
`EventID`/correlation-ID generation.

`.golangci.yml`'s `depguard` `internal-only` rule gets a new deny entry for
`github.com/ThreeDotsLabs/watermill-redisstream` — a separate module path not
covered by the existing `watermill` prefix rule. `github.com/redis/go-redis`
is already covered by prefix match against `.../v9`.

## 4. Error handling

Publish errors return synchronously: `Config.Validate()` and envelope
validation return descriptive errors before any network call; transport
errors from `internal/watermill` propagate unwrapped. No error-classification
wrappers (`AsPermanent`/`AsRetryable`/`AsDiscard`) this phase — those only
matter for the consumer/retry path (Phase 2).

## 5. Testing

**Unit** (package `messaging`, no Redis):

- Envelope construction and field population
- `Config.Validate()` defaults and required-field errors
- Correlation ID resolution order: option → context → generated UUIDv7
- `EventDef`/envelope validation edge cases (bad `event_type` format, bad
  `schema_version`, zero timestamp, empty required fields)

**Integration** (`-tags=integration`, matches Phase 0's `make
test-integration`):

- Uses `internal/testsupport.StartRedis` (existing, from Phase 0) to get a
  live Redis address
- `NewPublisher` + `Publish` against it, then verifies the raw stream entry
  shape
- To keep go-redis fully inside `internal/`, `internal/testsupport` gets a
  small read-back addition (e.g. `ReadStreamEntries`) so the integration test
  calls that helper instead of importing go-redis itself — this avoids a
  depguard carve-out for a root-level `_test.go` file

## Alternatives considered

- **Generic `Publish[T any]`** — rejected: unlike `RegisterHandler`, `Publish`
  doesn't need a static payload type (it only marshals), so a generic method
  would add ceremony (and Go doesn't allow generic methods anyway) for no
  benefit over `payload any`.
- **Semver library (`Masterminds/semver` or `golang.org/x/mod/semver`) for
  `schema_version` validation** — rejected for this phase: the PRD only needs
  a `MAJOR.MINOR.PATCH` shape check here; major-version extraction for
  dispatch is Phase 2's problem and can revisit the choice then if a regex
  stops being enough.
- **`internal/redis` and `internal/watermill` merged into one package** —
  rejected: PRD §18 lists them as separate directories, and keeping go-redis
  client construction separate from the Watermill wrapper matches that
  boundary and keeps each package's single responsibility clear.
- **Root-level integration test importing go-redis directly to verify the
  write** — rejected: would need a depguard carve-out, undermining the
  boundary rule Phase 0 just added. Routing the read-back through
  `internal/testsupport` keeps the rule exception-free.
