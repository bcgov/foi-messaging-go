# foi-messaging-go

Standardized asynchronous messaging for FOI platform services — a transport-agnostic Go library built on [Watermill](https://watermill.io/) and Redis Streams.

> **Status: early development (pre-v1.0).** The public API described here reflects the design in [PRD v1.1](docs/foi-messaging-go-prd-v1.1.md) and is being implemented. It is not yet stable and should not be adopted by production services until v1.0.0 is tagged.

---

## Why this exists

FOI services communicate asynchronously, but today each one implements serialization, routing, retries, correlation, and Redis configuration on its own. This library provides those concerns once, behind a small typed API.

Application code interacts only with this library. Watermill, Redis Streams, and go-redis are internal implementation details and never cross the library boundary.

## Features

- Standard, strongly-typed event envelope with generic payloads
- Publisher and consumer APIs keyed on a shared typed `EventDef` (topic + type + version)
- Automatic serialization, routing, and handler dispatch
- *(Planned: Phase 2b)* At-least-once delivery with three-layer retry and poison-message protection
- *(Planned: Phase 2b)* Dead Letter Queue with a defined wrapper contract
- Correlation-ID propagation across service chains
- OpenTelemetry tracing and Prometheus metrics out of the box
- Structured logging via `slog`
- A `testing/` package for unit-testing handlers without Redis

## Requirements

- Go 1.25+ (generics, `slog`)
- Redis 7.0+ (Redis Streams with `XAUTOCLAIM`)

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

The library generates `event_id`, `timestamp`, and `source`, and injects trace context automatically.

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
- **Ordering is per-stream and only with `Concurrency: 1`** (the default). Reclaimed messages arrive out of order.
- **Publishing is not transactional with your database.** A crash between a DB write and a publish loses the event. Transactional outbox support is on the roadmap, not in the initial release.

## Error handling

> **Planned for Phase 2b — not yet implemented.**

Handlers will classify failures by wrapping the returned error:

```go
return messaging.AsPermanent(err) // → Dead Letter Queue, then ACK
return messaging.AsRetryable(err) // → retried (also the default for unclassified errors)
return messaging.AsDiscard(err)   // → acknowledged without retry
```

Retries will run in three layers: in-process immediate retries, Redis Streams pending-and-reclaim redelivery, and a `MaxDeliveryAttempts` cap that routes poison messages to the DLQ regardless of classification.

## Dead Letter Queue

> **Planned for Phase 2b — not yet implemented.**

Permanent failures and messages exceeding the delivery cap will be published to `<topic>.dlq` using an exported `messaging.DeadLetter` wrapper that preserves the original envelope byte-for-byte alongside failure metadata, so replay tooling can republish without transformation.

## Configuration

A minimal config is three fields; every retry, reclaim, and concurrency knob has a working default.

```go
cfg := messaging.Config{
    Source:   "billing.service",                          // required
    Redis:    messaging.RedisConfig{Address: "redis:6379"}, // Address required
    Consumer: messaging.ConsumerConfig{Group: "billing-service"}, // required for consumers
}
```

Redis auth/TLS, pool sizing, consumer concurrency, claim intervals, delivery caps, retry backoff, and telemetry providers are all configurable. See the [full configuration reference](docs/foi-messaging-go-prd-v1.1.md#17-configuration).

## Observability

Publish and consume are traced with linked OpenTelemetry spans; correlation IDs propagate through the context automatically. Prometheus metrics cover published/received/processed/failed counts, retries, DLQ volume, and processing latency. Structured logs include event, topic, consumer, and error-category fields. Payloads are not logged by default.

## Testing

The `testing/` package lets applications unit-test handlers and publish paths without a Redis instance:

```go
pub := messagingtest.Publisher{}
// ... exercise code that publishes, then assert on pub.Published

err := messagingtest.Deliver(handler, messagingtest.Envelope(payload))
```

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

Only the top-level package and `testing/` are imported by applications. All Watermill and Redis code stays in `internal/`, enforced by a golangci-lint `depguard` rule (CI enforcement is planned for a later phase).

## Roadmap

Planned after v1.0: transactional outbox, a Redis-backed idempotency helper, delayed retry queues, DLQ replay tooling, a schema registry, and additional transports (Kafka, RabbitMQ, and others). Because applications depend only on the library interfaces, these can arrive without application changes.

## Documentation

Full design and rationale live in the [Product Requirements Document](docs/foi-messaging-go-prd-v1.1.md).

## License

_TODO: add license._
