# Phase 2a: Consume Path — Design

**Date:** 2026-08-11
**Status:** Approved
**Author:** brainstorming session (alvesfc + Claude)

## Purpose

Implement the read path described by
[PRD v1.1](../../foi-messaging-go-prd-v1.1.md) against the Phase 1 publish path:
a `Consumer` that subscribes to Redis streams, dispatches each event to a
strongly-typed handler by `event_type` + major `schema_version`, and shuts down
gracefully. Failure handling — error classification, immediate retry, the
delivery-attempt cap, and the DLQ — is Phase 2b.

## Phase split

The PRD's "consumer, routing, retry, DLQ" surface is split in two so each half
is independently reviewable and 2a produces something runnable:

- **Phase 2a (this spec)** — subscriber, handler registry, dispatch, ack/nack,
  graceful shutdown. Any handler error NACKs.
- **Phase 2b** — `errors.go` classification, classification-aware retry
  middleware, delivery-attempt cap, `DeadLetter` contract and DLQ publishing.
  Replaces 2a's blanket NACK. Specced separately once 2a exists.

## Scope

In scope:

- `internal/redis`: `StreamReader` — Redis Streams protocol primitives
- `internal/watermill`: `Subscriber` (implements `message.Subscriber`) and a
  `Router` wrapper
- `handler.go`: `Handler[T]`, `TopicSelector`
- `registry.go`: registration storage and dispatch-closure construction
- `consumer.go`: `Consumer`, `NewConsumer`, `RegisterHandler`,
  `RegisterRawHandler`, `Run`, `Close`
- `ConsumerConfig` defaulting and validation
- Consume-time envelope validation (reusing Phase 1's `validateEnvelope`)
- Correlation ID propagation into handler contexts
- Unit tests for all of the above, plus integration tests against a real Redis

Out of scope:

- **Phase 2b:** `errors.go` classification, retry middleware, delivery-attempt
  cap, `dlq.go` / `DeadLetter`
- **Phase 3:** OpenTelemetry spans, Prometheus metrics. Dispatch logs the events
  that will later carry metrics, but registers no meters.
- **Phase 4:** the `testing/` (`messagingtest`) package, already deferred by the
  Phase 1 spec
- Stream trimming / `MAXLEN`. The PRD is silent and streams grow unbounded. This
  is recorded as a platform gap to raise, not solved here.
- DLQ replay tooling (PRD §22)

## Findings that shaped this design

Both come from reading `watermill-redisstream` v1.4.5 and `watermill` v1.5.2
source directly, and both are load-bearing.

**The redisstream subscriber discards the delivery counter.** Its
`Unmarshaller.Unmarshal(values)` receives only the stream entry's field map —
not the entry ID, not the `XPENDING` delivery count (`subscriber.go:562`). A
handler therefore cannot learn its own delivery attempt, which makes PRD §13
Layer 3 ("every delivery reads the Redis Streams delivery counter") unimplementable
in-band, and would permanently deny Phase 3 the delivery-attempt log and metric
field required by PRD §16.

The library *does* already implement PRD §13 Layer 2: `ClaimInterval` +
`MaxIdleTime` drive an `XPENDING` + `XCLAIM` reclaim loop. We reimplement that
loop only because we must own the read path to keep the counter, not because it
is missing.

**Watermill's Router runs `go h.handleMessage(msg, …)` per message**
(`message/router.go:668`) — unbounded concurrency with no ordering guarantee.
That contradicts PRD §6 and §17, where `Concurrency: 1` is the default and is
documented to preserve per-stream order.

Owning the subscriber resolves both: it recovers the delivery counter at no
extra round-trip cost, and it lets a semaphore bound how many messages can reach
the Router at all.

## 1. Architecture

```text
messaging.Consumer.Run(ctx)
  └─ internal/watermill.Router            (Watermill Router; CloseTimeout = ShutdownTimeout)
       ├─ one NoPublisherHandler per subscribed topic
       │    └─ messaging dispatch: decode → validate → registry lookup → Handler[T]
       └─ internal/watermill.Subscriber   (implements message.Subscriber)
            ├─ semaphore(Concurrency) — acquired before read, released on Ack/Nack
            ├─ stamps _foi_stream_id, _foi_delivery_attempt into msg.Metadata
            └─ internal/redis.StreamReader
                 EnsureGroup / ReadNew / PendingOverIdle / Claim / Ack
                 (returns plain Entry{ID, Fields}; no go-redis type escapes)
```

Watermill's Router is retained, per PRD §12 and §23. It provides handler
lifecycle, the middleware chain that Phase 2b and Phase 3 hang retry and
telemetry off, ack/nack plumbing, and `CloseTimeout`-bounded drain. Its
goroutine-per-message behaviour is neutralised by the subscriber semaphore:
nothing reaches the Router without a slot.

**Boundary discipline** follows Phase 1 unchanged. `internal/redis` imports
go-redis and no Watermill; `internal/watermill` imports Watermill and no
go-redis. Neither type crosses into the root `messaging` package, so the Phase 0
`depguard` rule stays meaningful without carve-outs.

**New file beyond PRD §18's layout:** `registry.go`. Folding registration
storage and dispatch into `consumer.go` would make that file own both lifecycle
and routing. Recorded here as a §18 addendum.

## 2. `internal/redis.StreamReader`

Pure Redis Streams protocol. Returns plain structs so no go-redis type escapes
the package.

```go
type Entry struct {
    ID     string
    Fields map[string]any
}

type PendingEntry struct {
    ID         string
    RetryCount int64
    Idle       time.Duration
}

type StreamReader struct { /* client, group, consumer name */ }

func (r *StreamReader) EnsureGroup(ctx context.Context, stream string) error
func (r *StreamReader) ReadNew(ctx context.Context, stream string, count int64, block time.Duration) ([]Entry, error)
func (r *StreamReader) PendingOverIdle(ctx context.Context, stream string, minIdle time.Duration, count int64) ([]PendingEntry, error)
func (r *StreamReader) Claim(ctx context.Context, stream string, minIdle time.Duration, ids []string) ([]Entry, error)
func (r *StreamReader) Ack(ctx context.Context, stream string, ids ...string) error
```

- `EnsureGroup` issues `XGROUP CREATE <stream> <group> 0 MKSTREAM`, tolerating a
  `BUSYGROUP` error. The `0` start position is the decision recorded in
  §7 below.
- `ReadNew` is `XREADGROUP … STREAMS <stream> >`. Every entry it returns is a
  first delivery, so its delivery attempt is 1 by construction.
- `PendingOverIdle` is `XPENDING … IDLE <minIdle>`, and is the **only** source of
  delivery counts. Because it is already the reclaim discovery step, exact
  attempt counts cost no extra round-trip.
- `Claim` is `XCLAIM … MINIDLE <minIdle>`. `MINIDLE` is what prevents two
  concurrent instances from claiming the same entry. `XCLAIM` increments the
  entry's delivery counter (it is not issued with `JUSTID`), which is why the
  attempt stamped on a reclaimed message is `RetryCount + 1` — see §3.

Batch size for `PendingOverIdle` is an unexported constant of 100, matching
`watermill-redisstream`'s `DefaultClaimBatchSize`. It bounds one sweep, not the
total pending set; a sweep that fills its batch simply continues on the next
tick.

## 3. `internal/watermill.Subscriber`

Implements `message.Subscriber`, so Watermill's `tests.TestPubSub` conformance
suite — the same suite `watermill-redisstream` runs against its own subscriber —
applies to it directly.

```go
type SubscriberOptions struct {
    Reader        *internalredis.StreamReader
    Concurrency   int
    ClaimInterval time.Duration
    ClaimMinIdle  time.Duration
}

func (s *Subscriber) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error)
func (s *Subscriber) Close() error
```

### Read loop

```text
loop:
  acquire semaphore slot          ← before the read, not after
  ReadNew(stream, count=1, block=blockTime)
  decode entry.Fields → message.Message   (see "Entry decoding" below)
  stamp _foi_stream_id = entry.ID, _foi_delivery_attempt = 1
  emit to channel
  (async) on Acked  → StreamReader.Ack(entry.ID), release slot
          on Nacked → release slot only; entry stays in the PEL
```

**Acquiring the slot before the read is deliberate and load-bearing.** A message
read but not yet handed to a handler still has its idle clock running in Redis.
If it sat in a local buffer past `ClaimMinIdle`, another instance could
legitimately reclaim an entry we still hold, producing avoidable duplicate
processing. Never fetching what cannot be immediately emitted removes the
window.

A single read loop yields both required behaviours without a goroutine per
message: at `Concurrency: 1` it blocks on ack/nack before reading again, so
per-stream order is preserved (PRD §6); at `N` it keeps up to N in flight.

### Claim loop

A sibling goroutine on a `ClaimInterval` ticker:

```text
PendingOverIdle(stream, minIdle=ClaimMinIdle, count=batch)
  for each pending entry:
    acquire semaphore slot
    Claim(stream, minIdle=ClaimMinIdle, [entry.ID])
    stamp _foi_delivery_attempt = pending.RetryCount + 1
    emit to channel
```

The `+ 1` is not an off-by-one. `XPENDING` reports the count of deliveries that
have *already* happened; the `XCLAIM` we are about to issue is itself the next
delivery and increments the counter. A message delivered once and NACKed reports
`RetryCount == 1` and is stamped attempt 2, which is what it is. Phase 2b's cap
compares against this stamped value, so the arithmetic has to be right here.

Slot acquisition in this loop respects context cancellation, so a saturated
consumer shutting down does not block on a slot that will never free.

This is PRD §13 Layer 2. Reclaimed messages are delivered out of order relative
to the live stream, which PRD §6 already documents.

### Entry decoding and wire compatibility

The Phase 1 publisher writes entries with
`redisstream.DefaultMarshallerUnmarshaller`, whose field layout is
`_watermill_message_uuid` (the envelope's `event_id`), `metadata` (msgpack), and
`payload`. Replacing the subscriber means we now own the decode side of that
format, and a mismatch would break the publish path silently.

The subscriber therefore calls that same exported
`redisstream.DefaultMarshallerUnmarshaller.Unmarshal(entry.Fields)` rather than
hand-rolling a decoder. Wire compatibility with the Phase 1 publisher is then
guaranteed by construction, the msgpack metadata handling comes for free, and
the dependency stays inside `internal/watermill` where `depguard` already allows
it. Both loops build their `message.Message` this way, then stamp
`_foi_stream_id` and `_foi_delivery_attempt` on the result.

### Blocking read timeout

`blockTime` is an unexported constant of 1s, not configuration. go-redis
blocking commands do not observe context cancellation
([redis/go-redis#2556](https://github.com/redis/go-redis/issues/2556)), so an
unbounded block would leak the reader goroutine at shutdown. 1s caps shutdown
latency while keeping idle polling cheap, and keeps the config surface at
PRD §17.

### Transport metadata

`_foi_stream_id` and `_foi_delivery_attempt` are set at consume time on an
in-process `message.Message` and are discarded when it is acked. They are never
written to Redis and never enter the envelope, so PRD §5's prohibition on
transport state in the event contract is preserved. PRD §5 explicitly anticipates
delivery counts being "read from Redis Streams at consume time".

## 4. Registry and dispatch

```go
// handler.go
type Handler[T any] interface {
    Handle(context.Context, Envelope[T]) error
}

type TopicSelector struct{ Topic string }

// registry.go
type routeKey struct {
    topic     string
    eventType string
    major     int
}

type dispatchFunc func(context.Context, Envelope[json.RawMessage]) error

type registry struct {
    typed  map[routeKey]dispatchFunc
    raw    map[string]dispatchFunc  // topic → raw handler
    topics map[string]struct{}      // subscription set: union of both
}
```

`RegisterHandler[T]` type-erases at registration time, closing over the typed
handler in a `dispatchFunc` that unmarshals `env.Payload` into `T`, rebuilds an
`Envelope[T]` carrying the same header fields, and calls `handler.Handle`.
Payload deserialization therefore happens only after the correct handler has
been found. Plain `encoding/json` is used; `DisallowUnknownFields` must not be
set, per PRD §11.

### Dispatch flow

One Router handler per subscribed topic runs:

```text
1. json.Unmarshal(msg.Payload) → Envelope[json.RawMessage]  ─ fail → invalid-envelope path
2. validateEnvelope(env)                                     ─ fail → invalid-envelope path
3. ctx = contextWithCorrelationID(ctx, env.CorrelationID)
4. raw handler registered for topic? → call it
5. major(env.SchemaVersion); lookup {topic, event_type, major}
     ─ miss → log DEBUG, return nil (ACK), no retry
6. hit → call dispatch closure
```

Decoding once into `Envelope[json.RawMessage]` yields both the routing fields
and the exact argument shape a raw handler takes. Phase 1's `validateEnvelope`
is already generic over `T` and needs no change.

A routing miss is a normal outcome, not an error: topics are shared and services
consume only the event types they care about (PRD §9). Phase 3 adds
`messaging_events_skipped_total{reason="no_handler"}` at this call site —
without it, silent skips are hard to diagnose.

### Version matching contract

Dispatch matches on major version only, so a handler registered at `1.0.0` is
invoked for `1.0.0`, `1.1.0`, and `1.4.2`. This is only sound under an explicit
contract, which the library states normatively:

> Within a major version, payload changes must be backward-compatible: fields
> may be added, existing fields may not change meaning or type and may not be
> removed. Breaking changes require a new major version and a new `EventDef`.
> A handler registered at `1.0.0` will be invoked for every `1.x.y`.

Handlers are responsible for tolerating any additive change within their major.
Two majors of the same `event_type` may be registered simultaneously (PRD §11).

`major()` reads the leading segment of `schema_version`. `validateEnvelope` has
already rejected anything that is not `MAJOR.MINOR.PATCH` before dispatch runs.

### Registration conflicts

Duplicate `{topic, event_type, major}`, and a typed/raw collision on the same
topic, both fail at `RegisterHandler` / `RegisterRawHandler`. The registry has
enough information at that moment; deferring to `Run` as PRD §9 suggests would
only delay a deterministic error. `Run` re-checks so the invariant is asserted
where it matters. Registration after `Run` returns an error.

## 5. Consumer lifecycle

```go
consumer, err := messaging.NewConsumer(cfg)
messaging.RegisterHandler(consumer, contracts.DocumentCreated, h)
err = consumer.Run(ctx)   // blocks until ctx cancelled, then drains
```

`NewConsumer` calls the existing `cfg.Validate()` and then an unexported
`validateConsumer()`. It builds no Redis client, subscriber, or router — the
topic set is unknown until registration completes, and keeping construction
side-effect free means a `Consumer` that is never run holds no resources.

`Run` validates registrations, builds the Redis client, `StreamReader`,
`Subscriber`, and `Router`, adds one `NoPublisherHandler` per distinct topic,
then blocks on `router.Run(ctx)`. `Run` returns an error if called twice or if
no handlers are registered.

`Close` releases the Redis client for a consumer that was constructed but never
run; after `Run` returns, resources are already released. It is idempotent and
safe to `defer` unconditionally, so callers need not know whether `Run` was
reached.

### `ConsumerConfig` defaults and validation

`validateConsumer()` applies PRD §17 defaults — `Concurrency: 1`,
`ClaimInterval: 30s`, `ClaimMinIdle: 60s`, `MaxDeliveryAttempts: 5`,
`ShutdownTimeout: 30s`, `ConsumerName: <hostname>-<5 random chars>` — and
rejects: missing `Group`, `Concurrency < 1`, and `ClaimMinIdle < ClaimInterval`
(explicitly required by PRD §17). Publisher code paths never touch these fields,
which is why the checks live outside `Validate()` rather than inside it.

`MaxDeliveryAttempts` is defaulted here but unused until Phase 2b.

### Shutdown

Context cancellation stops the Router feeding handlers; it waits `CloseTimeout`
(set from `ShutdownTimeout`) for in-flight handlers. The subscriber then stops
both loops, closes its channel, and the Redis client is closed. Any message not
acked within the budget stays pending and is reclaimed by whichever instance
sweeps next. At-least-once delivery is preserved and nothing is lost.

## 6. Outcome matrix

Routing miss, invalid envelope, and handler failure are three distinct outcomes
with deliberately different retry behaviour. This table is the 2a/2b boundary.

| Situation | Phase 2a | Phase 2b |
| --- | --- | --- |
| Handler returns nil | ACK | ACK |
| No handler matches type+major | log DEBUG, ACK, no retry | unchanged (+ skip metric in Phase 3) |
| Envelope undecodable or invalid | log ERROR, **NACK** | DLQ `deserialization_failed`, raw bytes in `event_raw`, then ACK |
| Handler error, unclassified | NACK | immediate retries with jittered backoff, then NACK |
| Handler error, `AsPermanent` | NACK | DLQ `permanent` → ACK, no immediate retry |
| Handler error, `AsDiscard` | NACK | log WARN → ACK |
| attempt > `MaxDeliveryAttempts` | n/a | DLQ `max_attempts_exceeded` → ACK, checked before the handler runs |
| DLQ publish fails | n/a | log ERROR → NACK, so nothing is lost |

The invalid-envelope cell in 2a is knowingly imperfect: a malformed entry
re-loops every `ClaimMinIdle` until 2b ships. This is preferred to ACK-and-drop,
which is unrecoverable data loss. A stuck message is loud, bounded to its own
stream, and drains into the DLQ as soon as 2b deploys.

## 7. Consumer group start position

`EnsureGroup` creates groups at `0` — a newly deployed consumer group replays
whatever history is still on the stream.

This matches `watermill-redisstream`'s own default and is the safer failure mode
for an event platform: a service deployed shortly after its producer does not
silently miss events. The cost is that standing up a new group on a busy
long-lived stream causes a catch-up burst, and handlers must tolerate
reprocessing older events — which PRD §6 already requires of them.

The setting applies only at group creation. Once a group exists, Redis owns its
position.

## 8. Testing

### Unit tests (no Redis)

- registry: major-version matching (`1.0.0` handler receives `1.4.2`), routing
  miss, duplicate registration, typed/raw conflict, registration after `Run`
- dispatch: decode failure, validation failure, correlation ID reaching the
  handler's context, payload deserialized into `T`
- `validateConsumer`: each default applied, each rejection triggered
- subscriber against a fake `StreamReader`: slot acquired before read, order
  preserved at `Concurrency: 1`, slot released on nack with no `XACK` issued

### Integration tests (`-tags=integration`, testcontainers)

1. publish → consume → `XACK`; entry no longer in the PEL
2. NACK leaves the entry pending; the claim loop redelivers after `ClaimMinIdle`
   with `_foi_delivery_attempt == 2`
3. `Concurrency: 1` preserves publish order
4. an unmatched `event_type` is ACKed and the handler is never invoked
5. a group created on a stream with pre-existing entries replays them
   (locks in §7)
6. shutdown drains an in-flight handler within `ShutdownTimeout`
7. Watermill's `tests.TestPubSub` conformance suite against
   `internal/watermill.Subscriber`

PRD §19's remaining integration scenarios — immediate retry, cap → DLQ,
permanent → DLQ, DLQ wrapper shape — belong to Phase 2b.

## 9. Phase 2b preview

Recorded only to show the boundary is credible; 2b gets its own spec written
against the code 2a produces.

- `errors.go`: `AsPermanent` / `AsRetryable` / `AsDiscard` wrappers and
  `IsPermanent` / `IsRetryable` / `IsDiscard` predicates over `errors.As`
- a classification-aware retry middleware — Watermill's own `middleware.Retry`
  has no "should retry" predicate, so `Permanent` and `Discard` errors would be
  retried pointlessly
- the delivery-attempt cap, read from `_foi_delivery_attempt` before dispatch
- `dlq.go`: the exported `DeadLetter` wrapper contract (PRD §14), published to
  `{StreamPrefix}:{topic}.dlq` through an internally constructed publisher
