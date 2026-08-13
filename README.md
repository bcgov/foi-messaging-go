# foi-messaging-go

[![ci](https://github.com/bcgov/foi-messaging-go/actions/workflows/ci.yml/badge.svg)](https://github.com/bcgov/foi-messaging-go/actions/workflows/ci.yml)

Standardized asynchronous messaging for FOI platform services — a transport-agnostic Go library built on [Watermill](https://watermill.io/) and Redis Streams.

> **Status: released as `v0.1.0`, pre-1.0.** The library is feature-complete against [PRD v1.1](docs/foi-messaging-go-prd-v1.1.md), but the API is not frozen: under `v0.x` breaking changes arrive as minor bumps and the Go toolchain will not auto-upgrade across them. Pin an exact version. `v1.0.0` follows the first FOI service integration.

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
- OpenTelemetry tracing and OTel metrics, exportable to Prometheus
- Structured logging via `slog`
- A `testing/` package for unit-testing handlers and publish paths without Redis

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

### Structured logging

Consumers emit structured `slog` logs when an envelope cannot be decoded, fails validation, or carries an unparseable schema version, a warning whenever an event is dead-lettered or discarded, and a debug log when no registered handler matches an event. Logged fields include `topic`, `event_id`, `event_type` (when a handler is not found), `error`, and `trace_id`/`span_id` so a log line and its trace are navigable from each other. Correlation IDs propagate through handler contexts via `context.Context`.

Payload contents are **not** logged by default. Set `Telemetry.LogPayloads: true` to include them on every consume-path log line for a delivery — not just its error lines, since the payload attaches to that delivery's base logger. They are never placed in span or metric attributes regardless of that setting, because spans and metrics routinely leave the trust boundary that logs stay inside.

### Tracing

`Publish` opens a producer span; `dispatch` opens a consumer span parented to it, so handlers receive a `context.Context` whose span is a child of the publisher's. Trace context travels as `traceparent` transport metadata, injected and extracted by `Telemetry.Propagator`.

That field defaults to `propagation.TraceContext{}` **directly, not to `otel.GetTextMapPropagator()`** — the OTel global is a no-op until the application sets it, and defaulting from it would silently disable cross-service trace continuity with no error anywhere to explain why.

One span per delivery, not per attempt: immediate retries are recorded as span *events*. Span volume therefore scales with redelivery — a persistently failing event produces up to `MaxDeliveryAttempts` spans. Sampling is the application's decision.

### Metrics

The library records through the OpenTelemetry metric API only. It has no Prometheus dependency; hand it a `MeterProvider` and export however you like. `examples/telemetry` is a working Prometheus wiring.

| Prometheus name | Type | Attributes |
| --- | --- | --- |
| `messaging_events_published_total` | counter | topic, event_type |
| `messaging_publish_failures_total` | counter | topic, event_type, stage |
| `messaging_events_received_total` | counter | topic |
| `messaging_events_processed_total` | counter | topic, event_type, group |
| `messaging_events_failed_total` | counter | topic, event_type\*, group, error_category |
| `messaging_events_skipped_total` | counter | topic, group, reason |
| `messaging_retries_total` | counter | topic, event_type, group |
| `messaging_dlq_total` | counter | topic, group, reason |
| `messaging_dlq_publish_failures_total` | counter | topic, group, reason |
| `messaging_processing_duration_seconds` | histogram | topic, event_type, group |
| `messaging_queue_latency_seconds` | histogram | topic |

Every delivery increments exactly one of `processed`, `failed`, or `skipped`, and records `processing_duration` exactly once. `dlq` is orthogonal and fires alongside `failed` on the dead-letter paths reached through `dispatch` — it answers "what are we giving up on", not "what failed".

The one exception is a stream entry Watermill's own marshaller cannot read at all: it never reaches `dispatch`, so it increments `received` and `dlq` and **none** of `processed`/`failed`/`skipped`, and never records `processing_duration`. This is deliberate — there is no envelope to attribute a terminal outcome or a duration to — but it means alerting on `rate(messaging_events_failed_total)` alone will not catch a producer that starts writing corrupt entries; watch `messaging_dlq_total` too.

\* `event_type` is attached only when the event matched a **typed** handler registration, where it comes from a set fixed at registration time. On the no-handler, raw-handler, and deserialization paths it is whatever the wire said — unbounded, and one bad producer away from exploding your metric store — so it is omitted from metrics and recorded on the span instead.

#### These are per-delivery counters

A retryable failure NACKs, and the entry is later reclaimed and redelivered. **One event therefore increments `received` once per delivery**, up to `MaxDeliveryAttempts + 1` times. `received` exceeding `published` is redelivery working as designed, not double-counting.

#### `processing_duration` includes the dead-letter write

On the five dead-letter paths reached through `dispatch`/`runWithRetry` (delivery-cap exceeded, three deserialization failures, and a permanent handler error) the histogram covers the DLQ publish as well as decode and handler time, because that write genuinely occupies the delivery's concurrency slot. A DLQ outage will therefore show up as a p99 `processing_duration_seconds` spike *and* in `messaging_dlq_publish_failures_total`. That correlation is expected; the second metric is the one that tells you which it is. The sixth dead-letter path — an entry the marshaller cannot read at all — never reaches `dispatch`, so it has no `processing_duration` to include a DLQ write time in; see the exception noted above.

#### `queue_latency` depends on clock sync

`published_at` is stamped by the publishing host and read by the consuming host, so `messaging_queue_latency_seconds` measures elapsed time **plus clock skew**. Negative values are clamped to zero. It is only as trustworthy as your fleet's NTP. A missing or unparseable `published_at` skips the observation rather than failing the delivery.

#### The histogram bucket View is required

The OTel Prometheus exporter's default histogram boundaries are millisecond-scaled (`0, 5, 10, ... 10000`). Both of the library's histograms are in **seconds**, so without an explicit View every realistic observation lands in the first bucket and both render as flat lines. The library cannot fix this — your application owns the `MeterProvider` and therefore owns the Views.

Copy the View from [`examples/telemetry`](examples/telemetry/main.go). Omitting it is the most likely way to finish integrating and still be unable to see your own latency.

The exporter also adds `otel_scope_name`/`otel_scope_version` labels to every series and a `target_info` series. Both are normal.

## Testing

### Testing your own service

Import `github.com/bcgov/foi-messaging-go/testing` as `messagingtest`. Nothing in it needs Redis or Docker, and it drives the library's real code rather than a simulation of it — so a malformed `EventDef` or a misclassified error fails your unit test rather than production.

**Recording publishes.** `messagingtest.Publisher` wraps a real publisher with only its transport write redirected, so envelope construction, correlation-ID resolution, and validation are genuine:

```go
pub, _ := messagingtest.NewPublisher()
defer pub.Close()

svc := NewService(pub) // your code, against your own narrow interface
svc.CreateOrder(ctx, order)

published := pub.Published()
payload, _ := messagingtest.PayloadAs[OrderCreated](published[0])
```

**Testing a handler.** `Deliver` reproduces the handler boundary — it installs the context values the router installs, invokes the handler, and returns its error. It does not retry, ack, or dead-letter:

```go
err := messagingtest.Deliver(ctx, handler, env)
```

**Testing what the library would do with it.** `Dispatch` runs the real consume path against your own configured `Consumer`, covering the delivery-attempt cap, error classification, the retry loop, and dead-lettering:

```go
c, _ := messaging.NewConsumer(messagingtest.Config())
messaging.RegisterHandler(c, contracts.OrderCreated, handler)

e, _ := messagingtest.NewEvent(contracts.OrderCreated, payload)
res, _ := messagingtest.Dispatch(ctx, c, e)

res.Outcome              // processed | skipped | dead_lettered | nacked
res.DeadLetters[0].Reason
```

Because `Published()` returns the same `Event` type `Dispatch` accepts, one service's publish is the next service's input — a two-service chain, no Redis:

```go
res, _ := messagingtest.Dispatch(ctx, consumerB, pub.Published()[0])
```

`Dispatch` reports what the runtime *would do* with a delivery; it does not perform one. There is no ack, no pending entry, and no reclaim — for those, use the integration tier against real Redis.

### The library's own suite

The library's own suite has three tiers: `make test` (unit), `make test-examples` (the nested `examples/telemetry` module, which `./...` does not reach), and `make test-integration` (needs Docker). `make test-all` runs the first two.

Integration tests in this repository use [Testcontainers](https://testcontainers.com/) against real Redis.

## Project layout

```text
foi-messaging-go/
├── config.go        envelope.go     eventdef.go
├── publisher.go     consumer.go     handler.go
├── validation.go    errors.go       context.go     dlq.go
├── telemetry.go     registry.go
├── testing/         examples/
└── internal/
    ├── watermill/
    └── redis/
```

Applications import the top-level package, and `testing/` from their tests. All Watermill and Redis code stays in `internal/`, enforced by a golangci-lint `depguard` rule, which CI runs on every pull request.

## Roadmap

Planned after v1.0: transactional outbox, a Redis-backed idempotency helper, delayed retry queues, DLQ replay tooling, a schema registry, and additional transports (Kafka, RabbitMQ, and others). Because applications depend only on the library interfaces, these can arrive without application changes.

## Documentation

Full design and rationale live in the [Product Requirements Document](docs/foi-messaging-go-prd-v1.1.md).

## Releasing

Releases are cut by pushing a tag. A published Go module version is immutable —
once `proxy.golang.org` has served it, it can never be corrected, only
superseded — so the order here matters.

1. Add a `## [X.Y.Z] - YYYY-MM-DD` section to `CHANGELOG.md`, and update the
   link definitions at the bottom of the file.
2. Run `make verify`. This is exactly what CI runs, including the `-race`
   integration tier, which needs Docker.
3. Confirm CI is green on `main`.
4. Tag and push:

   ```bash
   git tag v0.1.0
   git push origin v0.1.0
   ```

`release.yml` then re-runs every gate on the tagged commit and creates the
GitHub Release from the matching changelog section. A tag with no changelog
section fails the release rather than publishing empty notes, and a tag
containing a hyphen (`v0.1.0-rc.1`) is marked as a prerelease.

If the gates fail, no Release is created — but the tag exists. Delete it
immediately (`git push --delete origin vX.Y.Z`) and re-tag; that only works
before anything has fetched the version through the module proxy. After that,
ship the fix as the next patch version.

## License

Apache License 2.0 — see [LICENSE](LICENSE).
