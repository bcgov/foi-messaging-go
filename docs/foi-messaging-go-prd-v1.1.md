# Product Requirements Document (PRD)

# Internal Go Messaging Library

**Project Name:** `foi-messaging-go`

**Status:** Draft

**Version:** 1.1

**Technology Stack**

* Go
* Watermill
* Redis Streams
* go-redis v9
* OpenTelemetry
* Prometheus

---

# 1. Executive Summary

The FOI platform contains multiple services that communicate asynchronously. Today, messaging concerns such as serialization, routing, retries, logging, correlation, and Redis configuration are implemented independently by each service.

This project introduces a reusable internal Go library that standardizes asynchronous communication across the platform.

The library is built on **Watermill** using **Redis Streams** as the transport layer while exposing a transport-agnostic API to application developers.

Application services should only interact with the messaging library. Watermill, Redis Streams, and go-redis remain internal implementation details.

---

# 2. Goals

The library shall provide:

* Standard event envelope
* Strongly typed payloads
* Generic payload support
* Publisher API
* Consumer registration
* Automatic serialization
* Automatic deserialization
* Event routing
* Correlation propagation
* OpenTelemetry integration
* Prometheus metrics
* Structured logging
* Dead Letter Queue support
* Retry support
* Poison-message protection
* Graceful shutdown
* Application test support without a Redis instance
* Consistent developer experience

---

# 3. Non-Goals

The library will not:

* expose Watermill APIs
* expose Redis Streams APIs
* expose go-redis types
* contain business-domain models
* implement workflow orchestration
* implement exactly-once delivery
* implement a transactional outbox (publish-with-DB-write atomicity)
* deduplicate deliveries on behalf of consumers (handlers must be idempotent; see Section 6)
* provide delayed/scheduled redelivery backoff beyond the reclaim idle time
* replace Redis Streams
* support multiple transports in the initial release

---

# 4. High-Level Architecture

```text
                 Application Service
                         │
      Publish() / RegisterHandler()
                         │
                         ▼
             Internal Messaging Library
                         │
     ┌───────────────────┼───────────────────┐
     │                   │                   │
 Envelope          Publisher          Consumer
 Validation        Routing            Middleware
 Logging           Metrics            Tracing
                         │
                         ▼
                    Watermill
                         │
               Redis Streams Adapter
                         │
                         ▼
                   Redis Streams
```

The application depends only on the messaging library.

---

# 5. Event Model

## Event Envelope

Every event uses the same envelope.

```json
{
  "event_id": "018f2e7a-1c6b-7c0a-9f8d-3e4a2b1c5d90",
  "event_type": "document.created",
  "timestamp": "2026-04-23T10:00:00Z",
  "schema_version": "1.0.0",
  "correlation_id": "corr-123",
  "source": "example.service",
  "payload": {}
}
```

The envelope contains only business and workflow metadata.

Transport-specific state such as retry counters, delivery attempts, consumer information, or Redis metadata must not be included in the event contract.

## Transport Metadata

Some cross-cutting data must travel with the message but is not part of the business contract. This data is carried in **Watermill message metadata** (which maps to Redis Streams entry fields), never in the envelope:

| Key            | Purpose                                       | Set by  |
| -------------- | --------------------------------------------- | ------- |
| `traceparent`  | W3C trace context (OpenTelemetry propagation) | Library |
| `tracestate`   | W3C trace state                               | Library |
| `published_at` | Wall-clock publish time for latency metrics   | Library |

Rules:

* The library injects and extracts trace context automatically. Handlers receive a `context.Context` whose span is a child of the publisher's span.
* Applications never read or write transport metadata directly. There is no public API for it.
* Delivery attempt counts are read from Redis Streams (`XPENDING` delivery counter) at consume time; they are observable in logs and metrics but are never serialized into the message.

## Correlation ID Semantics

`correlation_id` is a business-level field and therefore lives in the envelope. Resolution order at publish time:

1. Explicit `messaging.WithCorrelationID(...)` option.
2. Correlation ID present in the inbound `context.Context` (automatically placed there by the consumer when handling an event — this is how correlation propagates across a chain of services with no application code).
3. If neither exists, the library generates a new UUIDv7 and logs at debug level that a new correlation chain was started.

## Go Representation

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

## Event Naming

Event types use two segments:

```text
<entity>.<action>
```

Examples:

```text
document.created
user.updated
payment.completed
```

Where entity names collide across domains, a domain prefix may be added (`billing.invoice.created`), but two segments are the default.

The event contract is uniquely identified by `event_type` + the **major** component of `schema_version` (see Section 11).

---

# 6. Delivery Semantics

The library provides **at-least-once** delivery. This has three consequences that are application obligations, not library features:

**Idempotency is mandatory.** Every handler must tolerate receiving the same event more than once. `event_id` is the deduplication key. The library does not deduplicate on behalf of applications (a Redis-backed idempotency helper is listed as a future enhancement); services with non-idempotent side effects must implement their own dedup, typically a unique constraint on `event_id` in their datastore.

**Ordering is per-stream, and only without concurrency.** Redis Streams preserves insertion order within a stream. The library preserves that order only when a consumer runs with `Concurrency: 1` (the default). With `Concurrency > 1`, events may complete out of order. Reclaimed pending messages (Section 13) are always delivered out of order relative to the live stream. Applications requiring strict ordering must either use single-threaded consumption or design handlers to be order-independent (e.g., version checks, last-write-wins on timestamps).

**Publishing is not transactional with database writes.** If a service writes to its database and then publishes, a crash between the two operations loses the event. The transactional outbox pattern is explicitly out of scope for the initial release (Section 3) and listed as a future enhancement. Services for which this gap is unacceptable should raise it during design review of their integration.

---

# 7. Payload Contracts

The messaging library owns the envelope.

Applications own the payload.

Example:

```go
type DocumentCreatedPayload struct {
    EntityID string `json:"entity_id"`
    Name     string `json:"name"`
}
```

Business contracts are maintained outside the messaging library, in a shared contracts repository. Each contract package declares both the payload type and its `EventDef` (see Section 8), so publishers and consumers share a single source of truth for topic, event type, and version.

---

# 8. Publisher API

Applications publish only business payloads. Event identity is expressed as a typed descriptor rather than positional strings, so that topic, type, and version cannot be transposed:

```go
var DocumentCreated = messaging.EventDef{
    Topic:   "documents",
    Type:    "document.created",
    Version: "1.0.0",
}
```

`EventDef` values are declared once per event, in the contract package that owns the payload type (Section 7), and shared by publishers and consumers.

```go
result, err := publisher.Publish(
    ctx,
    contracts.DocumentCreated,
    payload,
    messaging.WithCorrelationID(correlationID), // optional; see §5
)
```

`Publish` returns:

```go
type PublishResult struct {
    EventID   string    // generated UUIDv7
    Timestamp time.Time // envelope timestamp
}
```

The library automatically generates `event_id` (UUIDv7), `timestamp` (UTC), and `source` (from `Config.Source`), and injects trace context per Section 5.

Publish errors are returned synchronously; the library does not buffer or retry publishes in the initial release. Callers decide whether a failed publish is fatal for their operation.

---

# 9. Consumer API

Consumers are instances, not package-level global state. A service creates one `Consumer` from its `Config`, registers handlers against it, then runs it:

```go
consumer, err := messaging.NewConsumer(cfg)

messaging.RegisterHandler(
    consumer,
    contracts.DocumentCreated, // EventDef: topic + type + version
    documentHandler,           // Handler[DocumentCreatedPayload]
)

// Blocks until ctx is cancelled, then drains in-flight handlers
// (bounded by cfg.Consumer.ShutdownTimeout) before returning.
err = consumer.Run(ctx)
```

`RegisterHandler` is a top-level generic function taking the consumer as its first argument (Go does not permit generic methods). Registration after `Run` has been called returns an error.

Typed handler:

```go
type Handler[T any] interface {
    Handle(context.Context, Envelope[T]) error
}
```

The `EventDef` carries the topic, so registration fully determines which stream the consumer subscribes to — one consumer subscribes to the union of topics across its registered handlers, within a single consumer group (`cfg.Consumer.Group`).

## Raw Handlers

When multiple payload shapes share an event type, or a service needs to consume events without a typed contract:

```go
messaging.RegisterRawHandler(
    consumer,
    messaging.TopicSelector{Topic: "documents"},
    rawHandler, // Handle(ctx, Envelope[json.RawMessage]) error
)
```

A topic may have either typed handlers or one raw handler, not both; overlapping registration returns an error at `Run`.

## Unmatched Events

If an event arrives on a subscribed topic and no registered handler matches its `event_type` + major version, the library ACKs it and increments the `messaging_events_skipped_total` metric with reason `no_handler`. Unmatched events are not errors: topics are shared, and services consume only the subset of event types they care about.

---

# 10. Routing

Routing is a two-level scheme:

**Level 1 — Topic → Stream.** The `EventDef.Topic` names a logical topic, which maps 1:1 to a Redis stream (`foi:{topic}`; the `foi:` prefix is applied by the library and configurable via `Config.StreamPrefix`). Topics group related events that a consumer typically wants together. Topic ownership and naming are governed by the platform team; a topic registry is maintained in the contracts repository.

**Level 2 — Handler dispatch.** Within a consumed stream, the library dispatches each message to a handler by matching:

```text
event_type + major(schema_version)
```

Minor and patch version differences do not affect dispatch (Section 11).

The messaging library remains unaware of business-specific routing rules. Payload-level discrimination (e.g., routing on a field inside the payload) belongs in a raw handler.

---

# 11. Schema Versioning

`schema_version` is semantic (`MAJOR.MINOR.PATCH`) and the library honors semver semantics in routing:

* **Dispatch matches on major version only.** A handler registered for `1.0.0` receives events published as `1.0.0`, `1.1.0`, and `1.4.2`. Producers may add optional fields (minor bump) without coordinating a consumer release.
* **Additive changes are minor.** New optional fields, new enum values consumers are documented to ignore-if-unknown.
* **Breaking changes are major** and are treated as a *new contract*: a new payload type, a new `EventDef`, and typically a migration window in which producers dual-publish `v1` and `v2` while consumers migrate. The library supports registering handlers for both majors of the same `event_type` simultaneously.
* The full version string is always carried in the envelope and included in logs and metrics, so minor-version adoption is observable.

Consumers must therefore deserialize leniently: unknown JSON fields are ignored (this is Go's default `encoding/json` behavior and must not be overridden with `DisallowUnknownFields` for event payloads).

---

# 12. Watermill Integration

Watermill is an internal implementation detail.

The library creates and manages:

* Publisher
* Subscriber
* Router
* Middleware
* Redis Streams adapter

Applications must never instantiate or configure Watermill directly. A CI lint rule enforces that no application code outside the library's `internal/` packages imports Watermill or go-redis (see Section 21).

---

# 13. Retry Strategy

The retry strategy has three layers. Layers 1 and 2 handle transient failures; layer 3 bounds them.

## Layer 1 — Immediate Retry (in-process)

Watermill retry middleware retries the handler in-process with exponential backoff and jitter before NACKing:

* `MaxImmediateRetries` (default **3**)
* `InitialBackoff` (default **100ms**), `MaxBackoff` (default **5s**), multiplier 2, full jitter

## Layer 2 — Persistent Redelivery (Redis Streams pending + reclaim)

If immediate retries are exhausted, the message is NACKed and remains in the stream's Pending Entries List. The library runs a background **reclaim loop** in every consumer instance:

* Every `ClaimInterval` (default **30s**), the consumer issues `XAUTOCLAIM` for messages pending longer than `ClaimMinIdle` (default **60s**), claiming them for reprocessing.
* Reclaim is how messages survive consumer crashes and how NACKed messages are redelivered.

Because Redis Streams has no native delayed delivery, **redelivery backoff is bounded below by `ClaimMinIdle`**: a NACKed message is retried no sooner than ~60s later, and there is no exponential backoff across redeliveries. Delayed retry queues with true scheduling remain a listed future enhancement; the initial release accepts this constraint.

## Layer 3 — Delivery Attempt Cap (poison-message protection)

Every delivery reads the Redis Streams delivery counter for the message. If the counter exceeds `MaxDeliveryAttempts` (default **5**), the message is routed to the DLQ **regardless of error classification**, and then ACKed. This guarantees that a message failing with `Retryable` errors indefinitely — a poison message — cannot loop forever between pending and reclaim.

The effective worst-case processing attempts for a message are therefore:

```text
MaxDeliveryAttempts × (1 + MaxImmediateRetries)
```

with defaults: 5 × 4 = 20 handler invocations over roughly 4–5 minutes.

Retry state (delivery counters, claim timestamps) lives entirely in Redis Streams and is never serialized into the event.

---

# 14. Dead Letter Queue

Permanent failures — a `Permanent` error classification, or exhaustion of `MaxDeliveryAttempts` — are published to the topic's DLQ stream and the original message is ACKed.

Naming convention:

```text
<topic>.dlq
```

Example: stream `foi:documents.dlq`.

## DLQ Contract

The envelope rules of Section 5 forbid failure metadata inside the event. The DLQ therefore uses a **wrapper contract** that carries the original event verbatim alongside failure metadata:

```json
{
  "dead_lettered_at": "2026-04-23T10:05:12Z",
  "reason": "permanent",
  "error": "invoice 42 references unknown account",
  "delivery_attempts": 5,
  "consumer_group": "billing-service",
  "consumer_name": "billing-7f9c4",
  "original_topic": "documents",
  "event": { "...original envelope, byte-for-byte...": {} }
}
```

* `reason` is one of `permanent` | `max_attempts_exceeded` | `deserialization_failed`.
* `event` is the original envelope unmodified, so DLQ replay tooling (future enhancement) can republish it without transformation.
* Messages that cannot be deserialized at all (invalid JSON, missing envelope fields) are dead-lettered with `reason: deserialization_failed` and the raw bytes preserved in `event_raw` (base64) instead of `event`.

The Go type `messaging.DeadLetter` is exported so operational tooling can consume DLQ streams with the same library.

DLQ publish failures are logged at error level and surface in the `messaging_dlq_publish_failures_total` metric; the message is then NACKed rather than ACKed so it is not lost (it will be reclaimed and dead-lettering retried).

---

# 15. Error Classification

Handlers classify failures by **wrapping** errors with library constructors; the library **checks** classification with `errors.As`-compatible predicates. The wrapper and the check are distinct APIs:

```go
// Wrapping (application code, inside handlers)
return messaging.AsPermanent(fmt.Errorf("invoice %s: unknown account", id))
return messaging.AsRetryable(err)   // optional; see default below
return messaging.AsDiscard(err)

// Checking (library-internal, exposed for tests and tooling)
messaging.IsPermanent(err)  // → DLQ, then ACK
messaging.IsRetryable(err)  // → immediate retry, then NACK
messaging.IsDiscard(err)    // → log at warn, increment skip metric, ACK
```

**Default classification:** an unwrapped, unclassified error is treated as **Retryable**. This default is deliberate — a transient dependency failure misclassified as permanent silently loses work to the DLQ, whereas a permanent failure misclassified as retryable is caught by the `MaxDeliveryAttempts` cap (Section 13) and still lands in the DLQ, just later. Handlers should wrap with `AsPermanent` whenever they can prove the failure will not resolve on retry (validation failures, unresolvable references).

Classification wrappers compose with `errors.Is`/`errors.As` chains and preserve the wrapped error for logging.

---

# 16. Observability

The library integrates with OpenTelemetry and Prometheus.

## Tracing

Publish creates a producer span; consume creates a consumer span linked to it via the `traceparent` metadata (Section 5). Handler execution runs inside the consumer span.

## Structured Logging

Structured logs (via `*slog.Logger`) include:

* event ID
* event type
* schema version
* correlation ID
* topic
* consumer group and consumer name
* processing duration
* delivery attempt
* error category

Payload contents are not logged by default (`Telemetry.LogPayloads: false`).

## Metrics

Recommended metrics:

* `messaging_events_published_total`
* `messaging_events_received_total`
* `messaging_events_processed_total`
* `messaging_events_failed_total` (labeled by error category)
* `messaging_events_skipped_total` (labeled by reason: `no_handler`, `discard`)
* `messaging_retries_total`
* `messaging_dlq_total` (labeled by reason)
* `messaging_dlq_publish_failures_total`
* `messaging_processing_duration_seconds` (histogram)

All metrics are labeled with topic, event type, and consumer group where applicable.

---

# 17. Configuration

Applications configure the library through a single configuration object. All fields other than `Source`, `Redis.Address`, and `Consumer.Group` have working defaults; a minimal config is three lines.

```go
cfg := messaging.Config{
    Source:       "billing.service",     // required; envelope `source`
    StreamPrefix: "foi",                 // default "foi"

    Redis: messaging.RedisConfig{
        Address:  "redis:6379",          // required
        Username: os.Getenv("REDIS_USER"),
        Password: os.Getenv("REDIS_PASSWORD"),
        TLS:      &tls.Config{},         // nil = plaintext
        DB:       0,
        PoolSize: 10,                    // default 10 × GOMAXPROCS, capped
    },

    Consumer: messaging.ConsumerConfig{
        Group:               "billing-service",     // required for consumers
        ConsumerName:        os.Getenv("HOSTNAME"), // default: hostname + random suffix
        Concurrency:         1,                     // handlers in flight; default 1 (ordered)
        ClaimInterval:       30 * time.Second,
        ClaimMinIdle:        60 * time.Second,
        MaxDeliveryAttempts: 5,
        ShutdownTimeout:     30 * time.Second,      // drain budget on ctx cancel
    },

    Retry: messaging.RetryConfig{
        MaxImmediateRetries: 3,
        InitialBackoff:      100 * time.Millisecond,
        MaxBackoff:          5 * time.Second,
    },

    Telemetry: messaging.TelemetryConfig{
        TracerProvider: otel.GetTracerProvider(), // default: global provider
        MeterProvider:  otel.GetMeterProvider(),  // default: global provider
        Logger:         slog.Default(),           // *slog.Logger
        LogPayloads:    false,                    // default false; see §16
    },
}
```

`messaging.Config.Validate()` is called by `NewPublisher` / `NewConsumer` and returns descriptive errors for missing required fields or invalid combinations (e.g., `ClaimMinIdle < ClaimInterval` is rejected).

The library initializes the Redis client, Watermill publisher/subscriber, router, middleware chain, and telemetry from this single object. Applications never construct any of these directly.

---

# 18. Package Structure

```text
foi-messaging-go/
├── config.go
├── envelope.go
├── eventdef.go
├── publisher.go
├── consumer.go
├── handler.go
├── validation.go
├── errors.go
├── context.go
├── dlq.go
├── telemetry/
├── internal/
│   ├── watermill/
│   └── redis/
├── testing/
└── examples/
```

Only the public packages are imported by applications.

All Watermill and Redis implementations remain inside `internal/`.

`validation.go` validates envelopes at publish and consume time: required fields present, `event_type` matches the two-or-three-segment naming format, `schema_version` parses as semver, `timestamp` is non-zero.

---

# 19. Testing Strategy

## Unit Tests

* Envelope creation
* Serialization
* Validation
* Routing and handler dispatch (including major-version matching)
* Error classification (wrapping and predicates)
* Correlation propagation
* Config validation

## Integration Tests

Using Testcontainers with Redis:

* Publish
* Consume
* ACK
* NACK
* Immediate retry
* Pending recovery via reclaim
* Delivery attempt cap → DLQ
* Permanent error → DLQ
* DLQ wrapper contract shape
* Graceful shutdown (drain within `ShutdownTimeout`)

## Application Test Support (`testing/` package)

The `testing/` package ships with the library and provides:

* `messagingtest.Publisher` — records published events for assertion; no Redis required
* `messagingtest.Deliver[T](handler, envelope)` — invokes a handler exactly as the router would, including context correlation setup
* Envelope and `EventDef` builders with sensible defaults

---

# 20. Acceptance Criteria

The library is complete when:

* Applications publish events without importing Watermill.
* Applications consume events without importing Watermill.
* A standard event envelope is automatically applied.
* Routing is based on topic (stream) plus `event_type` and major `schema_version` (dispatch).
* Retry middleware is enabled by default.
* The delivery attempt cap routes poison messages to the DLQ by default.
* DLQ support, including the `DeadLetter` wrapper contract, is enabled by default.
* Correlation IDs propagate automatically across a chain of services.
* OpenTelemetry trace context propagates from publisher span to consumer span.
* Prometheus metrics are exposed.
* Structured logging is enabled.
* The `testing/` package allows applications to unit-test handlers and publish paths without a Redis instance.
* Integration tests validate end-to-end communication using Redis Streams.

---

# 21. Rollout, Versioning & Success Metrics

## Rollout

v0.x releases iterate with one pilot service (suggest the service with the simplest current messaging code). v1.0.0 is tagged once the pilot runs in production for two weeks without library-attributed incidents. Remaining services migrate one at a time; migration guides live in `examples/`.

## Library Versioning Policy

The library follows semver on its Go API. Breaking API changes require a major version and a migration guide. The envelope JSON contract is versioned independently via `schema_version` and is expected to remain stable indefinitely.

## Success Metrics

* All targeted services publish/consume through the library by \<date>, with zero direct Watermill or go-redis imports outside the library's `internal/` (enforced by a lint rule in CI).
* Duplicated messaging code removed from each migrated service (measured per-service at migration PR).
* No increase in message-loss or DLQ-rate incidents post-migration versus the 90-day pre-migration baseline.

## Owners

\<Platform team> owns the library and the topic registry; \<named individual> is the approver for envelope-contract and DLQ-contract changes.

---

# 22. Future Enhancements

Potential future capabilities include:

* Kafka transport
* RabbitMQ transport
* Azure Service Bus transport
* Google Pub/Sub transport
* Transactional outbox support
* Redis-backed idempotency/deduplication helper keyed on `event_id`
* Delayed retry queues with true scheduled redelivery
* Scheduled event delivery
* DLQ replay tooling (consuming the `DeadLetter` contract of Section 14)
* Event schema registry
* Event version migration utilities

Because applications depend only on the messaging library interfaces, these enhancements can be introduced without changing application code.

---

# 23. Architecture Decision

This project adopts the following principles:

* **Business events are stable contracts.** The envelope carries business and workflow metadata only; transport state lives in the transport.
* **Transport concerns remain infrastructure details.** Retry counters, delivery attempts, and trace propagation travel in message metadata and Redis Streams state, never in the event contract.
* **Applications depend on abstractions, not messaging implementations.** No Watermill or go-redis types cross the library boundary, enforced in CI.
* **Delivery is at-least-once; handlers are idempotent.** The library is honest about its guarantees and makes the application obligations explicit rather than implying stronger semantics.
* **The library provides opinionated defaults while remaining extensible.** Every retry, reclaim, and concurrency knob has a working default; a minimal configuration is three fields.
* **Watermill is leveraged for routing and middleware rather than reimplemented.**
* **Redis Streams provides durable, at-least-once delivery with consumer groups and pending-message recovery**, with a delivery attempt cap guarding against poison messages.

This architecture promotes consistency, reduces duplicated infrastructure code, and provides a clear foundation for future transport implementations while preserving a simple and stable developer experience.
