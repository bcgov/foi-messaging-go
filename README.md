# foi-messaging-go

Standardized asynchronous messaging for FOI platform services — a transport-agnostic Go library built on [Watermill](https://watermill.io/) and Redis Streams.

> **Status: early development (pre-v1.0).** The public API described here reflects the design in [PRD v1.1](docs/foi-messaging-go-prd-v1.1.md) and is being implemented. It is not yet stable and should not be adopted by production services until v1.0.0 is tagged.

---

## Why this exists

FOI services communicate asynchronously, but today each one implements serialization, routing, retries, correlation, and Redis configuration on its own. This library provides those concerns once, behind a small typed API: serialization, routing, correlation, Redis configuration, retry, and dead-lettering.

Application code interacts only with this library. Watermill, Redis Streams, and go-redis are internal implementation details and never cross the library boundary.

## Features

- Standard, strongly-typed event envelope with generic payloads
- Publisher and consumer APIs keyed on a shared typed `EventDef` (topic + type + version)
- Automatic serialization, routing, and handler dispatch
- At-least-once delivery with three-layer retry and poison-message protection
- Dead Letter Queue with a defined wrapper contract
- Correlation-ID propagation across service chains
- *(Planned: Phase 3)* OpenTelemetry tracing and Prometheus metrics
- Structured logging via `slog`
- *(Planned: Phase 4)* A `testing/` package for unit-testing handlers without Redis

## Requirements

- Go 1.25+ (generics, `slog`)
- Redis 7.0+ (Redis Streams consumer groups)

## Installation

```bash
go get github.com/bcgov/foi-messaging-go
```

## Quick start

### Define the contract

Event contracts live in a shared package and are imported by both producers and consumers:

```go
package contracts

import messaging "github.com/bcgov/foi-messaging-go"

type DocumentCreatedPayload struct {
    EntityID string `json:"entity_id"`
    Name     string `json:"name"`
}

var DocumentCreated = messaging.EventDef{
    Topic:   "documents",
    Type:    "document.created",
    Version: "1.0.0",
}
```

### Publish

```go
cfg := messaging.Config{
    Source: "documents.service",
    Redis:  messaging.RedisConfig{Address: "redis:6379"},
}

publisher, err := messaging.NewPublisher(cfg)
if err != nil {
    log.Fatal(err)
}

result, err := publisher.Publish(ctx,
    contracts.DocumentCreated,
    contracts.DocumentCreatedPayload{EntityID: "42", Name: "Report.pdf"},
)
// result.EventID, result.Timestamp
```

The library generates `event_id` and `timestamp`. Correlation IDs are resolved from publish options, context, or generated as a UUIDv7.

### Consuming events

```go
consumer, err := messaging.NewConsumer(cfg)  // cfg.Consumer.Group is required

err = messaging.RegisterHandler(consumer, contracts.DocumentCreated, documentHandler{})

// Blocks until ctx is cancelled, then drains in-flight handlers.
err = consumer.Run(ctx)
```

A handler is invoked for every event on its topic whose event type matches and
whose **major** schema version matches. Within a major version, payload changes
must be backward-compatible, so a handler registered for `1.0.0` receives
`1.4.2` too; breaking changes require a new major version and a new `EventDef`.

Events on a subscribed topic that no handler matches are acknowledged and
skipped — topics are shared, and services consume only the event types they
care about.

Handlers must be idempotent. Delivery is at-least-once and `EventID` is the
deduplication key.

See [`examples/consumer`](examples/consumer) for a complete service.

## Core concepts

### Envelope

Every event is wrapped in a standard envelope carrying business and workflow metadata only. Transport state (retry counters, delivery attempts, trace context) never appears in the envelope — it travels in message metadata.

```go
type Envelope[T any] struct {
    EventID       string    `json:"event_id"`
    EventType     string    `json:"event_type"`
    Timestamp     time.Time `json:"timestamp"`
    SchemaVersion string    `json:"schema_version"`
    CorrelationID string    `json:"correlation_id"`
    Source        string    `json:"source"`
    Payload       T         `json:"payload"`
}
```

### Routing

Routing is two-level: `EventDef.Topic` maps to a Redis stream, and within that stream messages dispatch to handlers by `event_type` + **major** schema version. A handler registered for `1.0.0` receives `1.x.y` events, so producers can add optional fields without a coordinated consumer release. See [Schema versioning](docs/foi-messaging-go-prd-v1.1.md#11-schema-versioning).

### Delivery semantics

The library is **at-least-once**. Three consequences are application obligations:

- **Handlers must be idempotent.** The same event may be delivered more than once; `event_id` is the deduplication key.
- **Ordering is per-stream and only with `Concurrency: 1`** (the default). Reclaimed messages arrive out of order. `Concurrency` bounds in-flight handlers *per subscribed topic*, so a consumer registered on three topics at `Concurrency: 3` can be running nine handlers. Immediate retries run inside the message's slot, so a retrying message holds its topic's slot for the whole retry window — which is what preserves ordering at `Concurrency: 1`.
- **Publishing is not transactional with your database.** A crash between a DB write and a publish loses the event. Transactional outbox support is on the roadmap, not in the initial release.

## Error handling

A handler that returns an error is retried in-process — `Retry.MaxImmediateRetries` times, with exponential backoff and full jitter — before the message NACKs and is left pending for the reclaim loop. Redelivery is bounded by `Consumer.MaxDeliveryAttempts`: on the delivery whose attempt exceeds it, the event is dead-lettered and ACKed without being decoded or dispatched.

Handlers steer that path by classifying the error they return:

```go
return messaging.AsPermanent(err) // → Dead Letter Queue, then ACK
return messaging.AsRetryable(err) // → retried in-process, then NACKed
return messaging.AsDiscard(err)   // → acknowledged without retry or DLQ
```

An unclassified error is retryable. Classification is re-read on every attempt, so a handler may fail transiently and then return `AsPermanent` once it knows better. `AsPermanent` and `AsDiscard` both skip the remaining retries; `AsPermanent` skips the delivery cap too, since the verdict is already final.

Classification is additive: `errors.Is` and `errors.As` see straight through the wrapper, and a classified error may itself be wrapped with `%w` without losing its verdict.

## Dead Letter Queue

Permanent failures, messages exceeding the delivery cap, and events that could not be deserialized are published to `<topic>.dlq` using the exported `messaging.DeadLetter` wrapper, which carries failure metadata — `reason`, `error`, `delivery_attempts`, `consumer_group`, `consumer_name`, `original_topic`, `dead_lettered_at` — alongside the original event.

The event travels in one of two fields, never both. `event` holds the original bytes verbatim when they were valid JSON, so replay tooling can republish without transformation; `event_raw` holds them when they were not parseable, which is the case for a malformed entry or an envelope that failed validation. Splicing unparseable bytes into `event` would make the dead letter itself invalid JSON, unreadable by the very tooling the DLQ exists for.

A failed DLQ write NACKs rather than ACKs: while the DLQ is unwritable the entry stays pending and the next reclaim sweep retries it, which is preferable to acknowledging an event into nothing.

## Configuration

A minimal config is three fields; every reclaim and concurrency knob has a working default.

```go
cfg := messaging.Config{
    Source:   "billing.service",                          // required
    Redis:    messaging.RedisConfig{Address: "redis:6379"}, // Address required
    Consumer: messaging.ConsumerConfig{Group: "billing-service"}, // required for consumers
}
```

Redis auth/TLS, pool sizing, consumer concurrency, and claim intervals are all configurable with working defaults, as are the delivery cap (`MaxDeliveryAttempts`, default 5) and retry backoff (`RetryConfig`, default 3 retries from 100ms to 5s). Because retries sleep inside the message's concurrency slot, `NewConsumer` rejects a config whose worst-case backoff reaches `Consumer.ClaimMinIdle` — such a config guarantees the entry is reclaimed, and processed a second time by the same process, before the first delivery has finished retrying. See the [full configuration reference](docs/foi-messaging-go-prd-v1.1.md#17-configuration).

## Observability

Consumers emit structured `slog` logs when an envelope cannot be decoded, fails validation, or carries an unparseable schema version, a warning whenever an event is dead-lettered or discarded, and a debug log when no registered handler matches an event. Logged fields include `topic`, `event_id`, `event_type` (when a handler is not found), and `error`. Correlation IDs propagate through handler contexts via `context.Context`.

> **Planned for Phase 3** — OpenTelemetry tracing with linked spans for publish and consume, and Prometheus metrics covering event counts (published/received/processed/failed), retries, and processing latency.

## Testing

> **Planned for Phase 4** — The `testing/` package will let applications unit-test handlers and publish paths without a Redis instance.

Integration tests in this repository use [Testcontainers](https://testcontainers.com/) against real Redis.

## Project layout

```text
foi-messaging-go/
├── config.go        envelope.go     eventdef.go
├── publisher.go     consumer.go     handler.go
├── validation.go    errors.go       context.go     dlq.go
├── telemetry/       testing/        examples/
└── internal/
    ├── watermill/
    └── redis/
```

Only the top-level package is imported by applications today; the `testing/` package joins it in Phase 4 (see [Testing](#testing)). All Watermill and Redis code stays in `internal/`, enforced by a golangci-lint `depguard` rule (CI enforcement is planned for a later phase).

## Roadmap

Planned after v1.0: transactional outbox, a Redis-backed idempotency helper, delayed retry queues, DLQ replay tooling, a schema registry, and additional transports (Kafka, RabbitMQ, and others). Because applications depend only on the library interfaces, these can arrive without application changes.

## Documentation

Full design and rationale live in the [Product Requirements Document](docs/foi-messaging-go-prd-v1.1.md).

## License

Apache License 2.0 — see [LICENSE](LICENSE).
