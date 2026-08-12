# Phase 3: OpenTelemetry Spans and Prometheus Metrics — Design

**Date:** 2026-08-12
**Status:** Approved
**Author:** brainstorming session (alvesfc + Claude)

## Purpose

Make [PRD v1.1](../../foi-messaging-go-prd-v1.1.md) §16 real. `TelemetryConfig`
already carries a `TracerProvider` and a `MeterProvider`; `Validate` already
defaults them; nothing reads either. Phase 3 is where publish and consume start
emitting spans and metrics, and where the `traceparent` / `published_at`
transport metadata PRD §5 mandates actually gets written.

Three findings shaped this design more than the PRD text did:

1. `otel.GetTextMapPropagator()` defaults to a **no-op**. Defaulting the
   propagator from the global — the way `TracerProvider` and `MeterProvider`
   are defaulted — would silently disable trace propagation in any service
   that never called `otel.SetTextMapPropagator`. See §3.
2. The OTel Prometheus exporter's **default histogram buckets are
   millisecond-scaled** (`0, 5, 10, …, 10000`). A duration recorded in seconds
   collapses into the first bucket. The library cannot fix this — the
   application owns the `MeterProvider` and therefore the Views — so it becomes
   a mandatory documented recipe. See §6.
3. `Consumer.dispatch` has seven terminal exit paths. Instrumenting them
   individually is how a metrics layer acquires a permanent "does every path
   record something?" question. See §1.

## Scope

In scope:

- `telemetry.go` (new): instrument construction, the `deliveryOutcome` type,
  and the single deferred recorder both metrics and spans go through
- `config.go`: a new `TelemetryConfig.Propagator` field and its default
- `publisher.go`: producer span, publish metrics, `traceparent` / `tracestate`
  and `published_at` transport metadata
- `consumer.go`: consumer span, the `dispatch` outcome refactor, retry and DLQ
  counters, `trace_id` / `span_id` on consume-path logs
- `TelemetryConfig.LogPayloads`, currently defaulted but read nowhere, becomes
  live
- `examples/telemetry/` (new): a compilable example wiring the OTel Prometheus
  exporter, including the histogram-bucket View
- Deleting the code-free `telemetry/` package

Out of scope:

- **Phase 4:** `testing/` (`messagingtest`)
- Reconciling the existing slog call sites against PRD §16's full field list.
  Phase 3 adds `trace_id` / `span_id` and nothing else to those lines.
- Instrumenting `internal/redis` or `internal/watermill`. Neither imports OTel
  today and neither needs to; see §1.
- Exemplars linking histogram buckets to trace IDs. The OTel Go Prometheus
  exporter supports them, but they need an application-side opt-in we would
  only be guessing at.

---

## 1. Where the instrumentation lives

The package-boundary question answers itself. `config.go` already imports
`go.opentelemetry.io/otel`, `otel/metric`, and `otel/trace`, because
`TelemetryConfig` is public API: applications hand us providers. OTel is
deliberately **not** behind the `internal/` wall that Watermill and go-redis
are behind, and `.golangci.yml` needs no new `depguard` entry.

So instrumentation lives in the **root package**. The internal packages stay
uninstrumented and keep their plain-typed seams. This is not a compromise:
every fact worth recording — event type, schema version, classification,
outcome — is only knowable in the root, and `internal/watermill` importing the
root would be the same import cycle that stopped Phase 2b putting retry in a
middleware.

### The `dispatch` problem

`dispatch` plus `runWithRetry` have seven terminal exits: cap exceeded,
undecodable, invalid envelope, unparseable version, no handler, discard,
permanent, and retries-exhausted. Instrumenting each in place means roughly
twenty new statements threaded through the most subtle code in the repository,
and every exit path added later is a chance to forget one.

Instead, `dispatch` computes a **`deliveryOutcome`** and a single `defer`
records everything from it:

```go
func (c *Consumer) dispatch(ctx context.Context, topic string, payload []byte, metadata map[string]string) (err error) {
    ctx, rec := c.telemetry.beginDelivery(ctx, topic, metadata)
    defer func() { rec.end(err) }()
    ...
}
```

Each exit path sets the outcome and returns; it does not call telemetry.

This shape was chosen over two alternatives:

- **Inline calls at each site** — simplest to write, but puts the correctness
  of the metrics in the hands of whoever next edits the densest function in the
  codebase.
- **A telemetry decorator around `dispatchFunc`** — cleanest separation, except
  the outcomes it must distinguish are invisible in `dispatch`'s `error`
  return. `nil` is returned for success, for discard, for no-handler, and for
  every successful dead-letter. Recovering the distinction would mean inventing
  sentinel errors that exist only to serve metrics.

The deferred recorder also solves something neither alternative does: the
duration histogram and the span need one start timestamp and one end. Holding
both in one value is the only shape where they cannot drift apart.

### Span lifetime and the drain

Message contexts are `context.WithoutCancel`-derived, so they stay live for the
whole `ShutdownTimeout` drain. That is what lets a handler finish after
cancellation — and it means the deferred `rec.end` is the *only* thing
guaranteeing the span is closed when a handler is abandoned at the drain
deadline. That reasoning belongs in a comment at the `defer`, not in this
document alone.

---

## 2. Metrics

Instruments are named with dots and are translated by the exporter. Verified
against `go.opentelemetry.io/otel/exporters/prometheus v0.66.0` with otel
v1.44.0: `messaging.events.published` becomes `messaging_events_published_total`,
and `messaging.processing.duration` with unit `s` becomes
`messaging_processing_duration_seconds`. Annotation units such as `{event}` are
dropped rather than suffixed, so they are safe to set for documentation value.

| Instrument | Kind | Unit | Attributes | Fires when |
| --- | --- | --- | --- | --- |
| `messaging.events.published` | counter | `{event}` | topic, event_type | `Publish` succeeds |
| `messaging.publish.failures` | counter | `{event}` | topic, event_type, stage | `Publish` returns an error |
| `messaging.events.received` | counter | `{event}` | topic | a delivery enters `dispatch`, and in `OnUndecodable` |
| `messaging.events.processed` | counter | `{event}` | topic, event_type, group | handler returned nil |
| `messaging.events.failed` | counter | `{event}` | topic, event_type\*, group, error_category | delivery ended in failure |
| `messaging.events.skipped` | counter | `{event}` | topic, group, reason | `no_handler` or `discard` |
| `messaging.retries` | counter | `{retry}` | topic, event_type, group | each immediate retry |
| `messaging.dlq` | counter | `{event}` | topic, group, reason | each dead letter written |
| `messaging.dlq.publish.failures` | counter | `{event}` | topic, group, reason | DLQ write failed |
| `messaging.processing.duration` | histogram | `s` | topic, event_type, group | every terminal delivery |
| `messaging.queue.latency` | histogram | `s` | topic | `published_at` → dispatch start |

All eleven are created once, in `NewPublisher` / `NewConsumer`, from
`cfg.Telemetry.MeterProvider.Meter("github.com/bcgov/foi-messaging-go")`.

\* `event_type` is omitted on the `deserialization` and `max_attempts`
categories, which never reach a registry lookup. See "`event_type` is attached
only when it came from a registered handler" below. `messaging.retries` can
always attach it, because a retry only exists after a lookup succeeded.

### The terminal invariant

Every delivery increments **exactly one** of `events.processed`,
`events.failed{error_category}`, `events.skipped{reason}`, and records
`processing.duration` **exactly once**.

`messaging.dlq` is orthogonal and fires *alongside* `events.failed` on the
three dead-letter paths. It answers "what are we giving up on", not "what
failed"; conflating them would make a permanent handler error and a
deserialization failure indistinguishable in the one metric operators page on.

`error_category` is one of `permanent`, `retryable`, `deserialization`,
`max_attempts`. The first two come from the PRD §15 classification predicates;
the last two name failures that never reach a handler.

This invariant is testable — see §5 — and should be tested rather than
asserted in a comment.

### These are per-delivery counters

A retryable failure NACKs, and the entry is later reclaimed and redelivered.
One event therefore increments `events.received` once per delivery, up to
`MaxDeliveryAttempts + 1` times.

That is the correct behaviour for a throughput and load metric, and it must be
stated prominently in the README. "Why does `received` exceed `published`?" is
otherwise a permanent source of confusion, and the answer — redelivery — is
exactly what the operator wants to see.

### Three decisions that are not transcription

**`messaging.publish.failures` is an addition to PRD §16.** The PRD lists no
publish-failure metric, presumably because `Publish` returns its error
synchronously and the caller *could* count it. But that leaves publish success
rate unobservable from the library's own dashboards and pushes identical
hand-rolled counters into every FOI service. `stage` is one of `validation`,
`marshal`, `transport`, which separates "this service is emitting garbage" from
"Redis is unreachable".

**`events.received` also fires from the `OnUndecodable` hook.** An entry
Watermill's marshaller cannot read never reaches `dispatch` (Phase 2b §5), so
counting only at dispatch entry would let `messaging_dlq_total` exceed
`messaging_events_received_total` on that path. That reads as a metrics bug and
would hide the real one underneath it. Counting in the hook gives `received`
the meaning "entries taken off the stream", which is the useful one. The hook
knows its stream and therefore its topic; `event_type` is unavailable there,
which the rule below already handles.

**`event_type` is attached only when it came from a registered handler.** On
the consume side `event_type` is read off the wire, so a buggy or hostile
producer can mint unbounded label values — and `skipped{reason="no_handler"}`
is precisely the path where the value is least trustworthy. The attribute is
therefore attached only after a successful registry lookup, where it is drawn
from a set fixed at registration time. On the no-handler, deserialization, and
cap paths it is **omitted from metrics** and logged and span-attributed
instead, where an unbounded value costs nothing.

`topic` and `group` are bounded by configuration and are always safe.

---

## 3. Tracing

### The propagator must not default from the global

`otel.GetTextMapPropagator()` returns a no-op propagator unless the application
has called `otel.SetTextMapPropagator`. Defaulting from it — consistent as that
would look beside the provider defaults — means that in any service missing
that one line, `traceparent` is never written, every consumer span is a
disconnected root, and nothing reports a fault. The failure surfaces during the
first incident the tracing was bought for.

`TelemetryConfig` therefore gains:

```go
// Propagator injects and extracts trace context (PRD §5). It defaults to
// propagation.TraceContext{} directly rather than to
// otel.GetTextMapPropagator(), which is a no-op unless the application has
// set it: a silently non-propagating default would break the cross-service
// trace continuity PRD §5 guarantees, with no error anywhere to explain why.
Propagator propagation.TextMapPropagator
```

defaulted in `Validate()` to `propagation.TraceContext{}`.

`Baggage` is deliberately not composited in by default: PRD §5's transport
metadata table lists `traceparent` and `tracestate` and nothing else, and any
application wanting baggage can pass a composite propagator.

`propagation.MapCarrier` already adapts the `map[string]string` that both the
publish and consume paths carry, so no new seam is required in
`internal/watermill`. Metadata round-trips through
`redisstream.DefaultMarshallerUnmarshaller`, which is what makes this work with
no transport change at all.

### Publish

A `SpanKindProducer` span opens at the top of `Publish` and is ended by defer,
so validation and marshal failures land on it as well as transport ones — the
span and `publish.failures{stage}` tell the same story about the same call.

Span name: `publish {topic}`, using the **logical topic**, not the prefixed
stream. `messaging.destination.name` carries the stream. Traces therefore stay
stable across a `StreamPrefix` change, at the cost of a small deviation from
semconv's "span name should be the destination".

Injection writes `traceparent` (and `tracestate` when present) into the
metadata map already passed to `wm.Publish`, alongside `published_at`.

### Consume

`dispatch` extracts the remote context from metadata and starts a
`SpanKindConsumer` span at entry — the same call that starts the duration
clock, per §1.

The span is **parented** to the extracted remote context, not merely linked to
it. PRD §5 promises handlers "a `context.Context` whose span is a child of the
publisher's span", and a single-message consume path is the case where a real
parent is both accurate and more useful than a link.

One span per delivery, not per handler attempt. Immediate retries become
**span events** carrying attempt number, error, and classification. A span per
attempt would give better per-attempt latency, at the cost of up to four times
the span volume on exactly the failure path where volume is already spiking.

Failure paths call `RecordError` and set status `Error`.

### Attributes

Semconv where one exists:

| Attribute | Value |
| --- | --- |
| `messaging.system` | `redis` |
| `messaging.operation.name` | `publish` / `process` |
| `messaging.destination.name` | the Redis stream (`{prefix}:{topic}`) |
| `messaging.consumer.group.name` | `Consumer.Group` (consume only) |
| `messaging.message.id` | the envelope `event_id` |

FOI-specific facts, which have no semconv equivalent, take a `messaging.foi.*`
prefix: `event_type`, `schema_version`, `correlation_id`, `delivery_attempt`,
`stream_id`.

Unlike the metric attributes, span attributes carry the wire `event_type`
unconditionally. High cardinality is what a trace backend is built for; the
bound in §2 exists for the metric store alone.

### Log correlation

Consume-path slog lines gain `trace_id` and `span_id`, read from the span
already in hand. PRD §16's field list does not name them, but they are what
makes a log line and a trace navigable from each other, and reconciling the
rest of that list is explicitly out of scope.

---

## 4. Transport metadata and `LogPayloads`

### `published_at`

PRD §5 mandates a `published_at` key that nothing currently writes. Phase 3
writes it, in RFC 3339 nanosecond UTC, as an unexported constant in the root
package — PRD §5 states applications never read or write transport metadata and
that there is no public API for it, so it is not exported.

It is the only way to separate "our handlers are slow" from "we are behind on
the stream", which is what `messaging.queue.latency` measures.

Failure handling is deliberately lax in both directions: a missing or
unparseable `published_at` skips the `queue.latency` observation and nothing
else. A message with odd transport metadata is not a message worth failing.

**Clock skew is a documented caveat.** `published_at` is stamped by the
publishing host and read by the consuming host, so `queue.latency` measures
elapsed time plus skew. Negative values are clamped to zero, and the README
says plainly that this histogram is only as trustworthy as the fleet's NTP.

### `LogPayloads`

`TelemetryConfig.LogPayloads` is defaulted but read nowhere. It becomes live
on error-path logs only: when true, the payload bytes are included on the log
lines that report a failure.

Payload bytes are **never** placed in span attributes or metric attributes,
regardless of the setting. A payload on a span is exfiltration with extra
steps, and spans routinely leave the trust boundary that logs stay inside.

---

## 5. Testing

Standard library `testing` only, per the repository convention. Two new
first-party test dependencies: `go.opentelemetry.io/otel/sdk` and
`go.opentelemetry.io/otel/sdk/metric`, for `tracetest.SpanRecorder` and
`metric.NewManualReader`. Neither is testify, which remains forbidden in
first-party test code.

### Unit

- **The terminal invariant.** A table over all seven `dispatch` exit paths,
  each asserting exactly one of `processed` / `failed` / `skipped` fired and
  exactly one `processing.duration` observation was recorded. This is the test
  that keeps §2's invariant true as exit paths are added.
- **Cardinality bound.** A no-handler delivery carrying an arbitrary
  `event_type` must produce no `event_type` metric attribute, and must still
  produce one on the span.
- **Propagator defaulting.** `Validate()` on a zero `TelemetryConfig` yields a
  `TraceContext{}`, not the global no-op. Worth an explicit test precisely
  because the failure mode is silence.
- **`publish.failures` stages.** Each of `validation`, `marshal`, `transport`
  reachable and attributed correctly.
- **`published_at` parsing.** Missing, malformed, and future-dated (skewed)
  values each skip or clamp rather than failing the delivery.
- **`LogPayloads`.** Payload present in error logs when true, absent when
  false, and absent from span attributes in both cases.
- **No-op safety.** A default `Config` — global no-op providers — runs the
  publish and dispatch paths without panicking.

### Integration (`-tags=integration`)

- **End-to-end propagation.** Publish inside a recorded span, consume, and
  assert the consumer span's parent `SpanContext` equals the producer's and is
  marked remote. This is the one assertion that proves the whole chain —
  injection, the Redis round trip through the marshaller, and extraction —
  rather than any single half of it.
- **Retry span events.** A handler failing twice then succeeding produces one
  span with two retry events and a final `Ok` status.
- **Undecodable entry.** `events.received` and `messaging.dlq` both fire from
  the `OnUndecodable` hook, preserving the invariant in §2.

### In `examples/telemetry`

- **Exporter naming.** A test asserting the Prometheus text output contains all
  nine PRD §16 metric names, plus `messaging_publish_failures_total` and
  `messaging_queue_latency_seconds`. This is what stops an instrument rename
  from silently breaking every dashboard.
- **Bucket View.** The same test asserts the recommended View produces
  second-scale bucket boundaries, so §6's recipe is verified rather than
  merely written down.

---

## 6. `examples/telemetry` and the bucket problem

The empty `telemetry/` package is deleted. With metrics flowing through the
OTel API, it has no code to hold, and a package that exports nothing but
documentation is a wart. PRD §18's file listing is a sketch, not a contract.

Its replacement is `examples/telemetry/`, a compilable example wiring
`go.opentelemetry.io/otel/exporters/prometheus` to a `MeterProvider` and
exposing `promhttp`. Compilable rather than prose because of what it has to
carry:

The exporter's default histogram boundaries are `0, 5, 10, 25, …, 10000` —
millisecond-scaled. `messaging.processing.duration` and
`messaging.queue.latency` are in seconds, so every realistic observation lands
in the first bucket and both histograms render as flat lines. The library
cannot fix this, because the application owns the `MeterProvider` and therefore
the Views.

The example therefore includes an explicit View. Note that it must name both
histograms: a single wildcard is a trap here, because the two are not
consistently suffixed (`messaging.processing.duration` versus
`messaging.queue.latency`), and a broader selector such as `messaging.*` would
sweep in the nine counters, for which an explicit-bucket-histogram aggregation
is invalid.

```go
secondsBuckets := sdkmetric.Stream{
    Aggregation: sdkmetric.AggregationExplicitBucketHistogram{
        Boundaries: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
    },
}

sdkmetric.NewMeterProvider(
    sdkmetric.WithReader(exporter),
    sdkmetric.WithView(
        sdkmetric.NewView(sdkmetric.Instrument{Name: "messaging.processing.duration"}, secondsBuckets),
        sdkmetric.NewView(sdkmetric.Instrument{Name: "messaging.queue.latency"}, secondsBuckets),
    ),
)
```

The README must state that omitting this View leaves both histograms useless.
It is the single most likely way a service completes Phase 3 integration and
still cannot see its own latency.

Two further exporter behaviours the example documents: `otel_scope_name` and
`otel_scope_version` labels are added to every series, and a `target_info`
series appears. Both are harmless and both surprise people writing their first
query.

---

## 7. Implementation order

One spec, not a 3a/3b split. Metrics and spans are not independent work:
they share the deferred recorder of §1, and landing them separately means
opening `dispatch`'s control flow twice for the same refactor.

The plan should sequence by path rather than by signal:

1. `config.go` — `Propagator` field and default; `telemetry.go` — instrument
   construction and the `deliveryOutcome` recorder.
2. Publish path — span, metrics, `traceparent` / `published_at` injection.
3. Consume path — the `dispatch` outcome refactor, span, metrics, retry and DLQ
   counters, log correlation.
4. `OnUndecodable` counters, `LogPayloads`.
5. `examples/telemetry`, delete `telemetry/`, README and `doc.go` markers.

Step 3 is the only one that touches subtle existing code, and it is
deliberately a pure refactor of control flow with the telemetry attached — the
seven exit paths keep their current behaviour exactly.

---

## 8. Documentation obligations

Per the repository convention that past reviews repeatedly caught docs claiming
unimplemented behaviour:

- README: drop the two "Planned: Phase 3" markers, add the metric table, the
  per-delivery counter semantics, the bucket View requirement, and the
  `queue.latency` clock-skew caveat.
- `doc.go`: Phase 3 described as landed.
- `CLAUDE.md`: implementation status, and a note that OTel is a root-package
  dependency by design so the `depguard` boundary does not cover it.
- PRD §16 gains `messaging_publish_failures_total` and
  `messaging_queue_latency_seconds`, marked as additions found during Phase 3
  design — following the established practice of carrying findings back into
  the spec of record.

---

## 9. Risks

- **Cardinality via `topic`.** Bounded by registration, but a service
  registering handlers in a loop over dynamic input would defeat the §2 bound.
  No mitigation beyond documentation; the same is already true of the stream
  names themselves.
- **Instrument construction errors.** `Meter.Int64Counter` returns an error.
  Names are compile-time constants, so an error is close to impossible — but if
  it happens, the library logs at warn and substitutes a no-op instrument
  rather than failing `NewPublisher` / `NewConsumer`. Dropping metrics is
  strictly better than refusing to deliver messages.
- **Span volume.** One span per delivery means redelivery multiplies spans by
  up to `MaxDeliveryAttempts`. Expected, and visible as exactly the anomaly it
  represents, but sampling configuration is the application's problem and the
  README should say so.
- **Two new test-only module dependencies.** `otel/sdk` and `otel/sdk/metric`
  become direct requires. Acceptable — they are the canonical test doubles for
  the API the library instruments against, and there is no lighter way to
  assert a histogram observation.
