# Phase 3 Observability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Emit OpenTelemetry spans and eleven OTel metrics from the publish and consume paths, so PRD §16 observability is live rather than configured-but-inert.

**Architecture:** Instrumentation lives in the **root `messaging` package**, because OTel is already a root-package dependency (`TelemetryConfig` is public API) and every fact worth recording — event type, classification, outcome — is only knowable there. `Consumer.dispatch`'s seven exit paths each set a `deliveryOutcome`; one deferred recorder turns that into the span end, the duration histogram, and exactly one terminal counter. Metrics go through the OTel metric API only; Prometheus is an application-side exporter, demonstrated by a compilable example.

**Tech Stack:** Go 1.25, `go.opentelemetry.io/otel` v1.44.0 (`trace`, `metric`, `propagation`, and the `noop` sub-packages), `otel/sdk` + `otel/sdk/metric` as test-only dependencies, `otel/exporters/prometheus` in the example only.

**Design of record:** [`docs/superpowers/specs/2026-08-12-phase-3-observability-design.md`](../specs/2026-08-12-phase-3-observability-design.md). Cited below as "spec §N".

## Global Constraints

- Go 1.25, Redis 7.0+.
- Tests use the **standard library `testing` package only**. No testify in first-party test code, ever, including test-only helper deps.
- Integration tests carry `//go:build integration` and live in the external `_test` package (`package messaging_test`).
- The root `messaging` package must never import Watermill, watermill-redisstream, or go-redis — including their types. `.golangci.yml`'s `depguard` enforces this. OTel is **not** covered by that rule and is allowed in the root by design.
- Comments explain *why*, especially where a subtle bug motivated the code. This codebase's comment density is well above typical Go; match it.
- Commit style: `feat:` / `fix:` / `test:` / `docs:`. No `Co-Authored-By` trailer.
- Metric instrument names use dots (`messaging.events.published`); the Prometheus exporter translates them. Never rename an instrument without updating the naming test in Task 12.
- `event_type` is a metric attribute **only** on a typed registry match. Spans may always carry it.
- Verify with `go test -tags=integration -race -count=1 ./...` before claiming green. `make test` alone proves nothing about the consume path.
- **Task 6 has a mandatory human verification gate before its commit.** It rewrites `dispatch`'s control flow, where a reordered check passes every test in this plan while silently changing when an event is dead-lettered versus nacked. Stop and hand back rather than committing.

---

## File Structure

| File | Responsibility |
| --- | --- |
| `telemetry.go` (new) | Instrument construction with no-op fallback, attribute helpers, `deliveryOutcome`, `deliveryRecorder`. Everything telemetry that is not a call site. |
| `telemetry_test.go` (new) | Unit tests for the above, using `sdkmetric.NewManualReader`. |
| `config.go` | `TelemetryConfig.Propagator` field and its non-global default. |
| `registry.go` | `routeMatch`, so `lookup` reports typed-vs-raw. |
| `publisher.go` | Producer span, publish counters, `traceparent` / `published_at` injection. |
| `consumer.go` | Consumer span, the `dispatch` outcome refactor, retry / DLQ / undecodable counters, log correlation. |
| `examples/telemetry/main.go` (new) | Compilable Prometheus wiring, including the mandatory histogram View. |
| `examples/telemetry/main_test.go` (new) | Asserts all eleven Prometheus names and second-scale buckets. |
| `telemetry/doc.go` | **Deleted.** No code left to hold once metrics go through the OTel API. |

---

### Task 1: The `Propagator` config field

The one field whose default cannot come from the OTel global. `otel.GetTextMapPropagator()` returns a **no-op** unless the application called `otel.SetTextMapPropagator`; defaulting from it would silently disable the cross-service trace continuity PRD §5 guarantees, with no error anywhere to explain why. Spec §3.

**Files:**
- Modify: `config.go:113-123` (`TelemetryConfig`), `config.go:185-193` (defaulting in `Validate`)
- Test: `config_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `TelemetryConfig.Propagator propagation.TextMapPropagator`, defaulted to `propagation.TraceContext{}`. Tasks 5 and 6 read `cfg.Telemetry.Propagator`.

- [ ] **Step 1: Write the failing test**

Add to `config_test.go`:

```go
func TestConfig_Validate_DefaultsPropagatorToTraceContext(t *testing.T) {
	// Defaulted directly rather than from otel.GetTextMapPropagator(),
	// which is a no-op until the application sets it. A no-op default
	// fails silently, so it gets an explicit test.
	cfg := Config{Source: "svc", Redis: RedisConfig{Address: "localhost:6379"}}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() = %v, want nil", err)
	}

	if cfg.Telemetry.Propagator == nil {
		t.Fatal("Telemetry.Propagator = nil, want a default")
	}

	fields := cfg.Telemetry.Propagator.Fields()
	if len(fields) != 1 || fields[0] != "traceparent" {
		t.Fatalf("Propagator.Fields() = %v, want [traceparent]", fields)
	}
}

func TestConfig_Validate_KeepsSuppliedPropagator(t *testing.T) {
	custom := propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})
	cfg := Config{
		Source:    "svc",
		Redis:     RedisConfig{Address: "localhost:6379"},
		Telemetry: TelemetryConfig{Propagator: custom},
	}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() = %v, want nil", err)
	}

	if len(cfg.Telemetry.Propagator.Fields()) != 2 {
		t.Fatalf("Propagator.Fields() = %v, want the supplied composite's two fields",
			cfg.Telemetry.Propagator.Fields())
	}
}
```

The `Fields()` check is what distinguishes default from supplied without depending on concrete types: `TraceContext` reports exactly `["traceparent"]`, and the composite reports two.

Add `"go.opentelemetry.io/otel/propagation"` to `config_test.go`'s imports.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -run 'TestConfig_Validate_(Defaults|Keeps)Propagator|TestConfig_Validate_DefaultsPropagatorToTraceContext' ./...`
Expected: FAIL — `cfg.Telemetry.Propagator undefined (type TelemetryConfig has no field or method Propagator)`

- [ ] **Step 3: Add the field**

In `config.go`, add to `TelemetryConfig` after `MeterProvider`:

```go
	// Propagator injects trace context at publish and extracts it at
	// consume (PRD §5). It defaults to propagation.TraceContext{}
	// directly, NOT to otel.GetTextMapPropagator(), which returns a no-op
	// unless the application has called otel.SetTextMapPropagator. A
	// no-op here would leave traceparent unwritten and every consumer
	// span a disconnected root, with nothing reporting a fault — the
	// failure would surface only during the first incident the tracing
	// was bought for.
	//
	// Baggage is deliberately not composited in: PRD §5's transport
	// metadata table lists traceparent and tracestate and nothing else.
	// Applications wanting baggage pass their own composite here.
	Propagator propagation.TextMapPropagator
```

Update the `TelemetryConfig` doc comment: the sentence "The tracer and meter providers are defaulted but inert: span creation and metric emission arrive in Phase 3." is now false. Replace with "The tracer and meter providers, the propagator, and LogPayloads are all live."

- [ ] **Step 4: Default it in `Validate`**

In `config.go`, alongside the existing provider defaulting:

```go
	if c.Telemetry.Propagator == nil {
		c.Telemetry.Propagator = propagation.TraceContext{}
	}
```

Add `"go.opentelemetry.io/otel/propagation"` to `config.go`'s imports.

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test -run TestConfig ./... -v`
Expected: PASS, including the pre-existing `TestConfig_Validate` cases.

- [ ] **Step 6: Commit**

```bash
git add config.go config_test.go
git commit -m "feat: add TelemetryConfig.Propagator with a non-global default

otel.GetTextMapPropagator() is a no-op until the application sets it, so
defaulting from it would silently disable the trace propagation PRD §5
guarantees. Default to propagation.TraceContext{} directly."
```

---

### Task 2: `routeMatch` — make `lookup` report typed vs raw

Pure refactor, no behaviour change. It exists so Task 6 can tell a bounded `event_type` from an unbounded one: `lookup` returns a topic's raw handler for *every* event on that topic, so "matched a handler" does not imply "matched a known event type". Spec §2.

**Files:**
- Modify: `registry.go:76-84` (`lookup`), `consumer.go:387-399` (the call site)
- Test: `registry_test.go:71,106,128,131,134` (five call sites)

**Interfaces:**
- Consumes: nothing.
- Produces: `type routeMatch int` with `matchNone`, `matchTyped`, `matchRaw`; `func (r *registry) lookup(topic, eventType string, major int) (dispatchFunc, routeMatch)`. Task 6 branches on `matchTyped`.

- [ ] **Step 1: Write the failing test**

Add to `registry_test.go`:

```go
func TestRegistry_LookupReportsHowItMatched(t *testing.T) {
	// The distinction is telemetry's: a typed match means event_type came
	// from a set fixed at registration time and is safe as a metric
	// attribute; a raw match means it is whatever the wire said.
	t.Run("typed", func(t *testing.T) {
		r := newRegistry()
		if err := r.addTyped("documents", "document.created", 1, noopDispatch); err != nil {
			t.Fatalf("addTyped() = %v, want nil", err)
		}

		if _, match := r.lookup("documents", "document.created", 1); match != matchTyped {
			t.Fatalf("lookup() match = %v, want matchTyped", match)
		}
	})

	t.Run("raw takes any event type", func(t *testing.T) {
		r := newRegistry()
		if err := r.addRaw("documents", noopDispatch); err != nil {
			t.Fatalf("addRaw() = %v, want nil", err)
		}

		_, match := r.lookup("documents", "anything.at.all", 7)
		if match != matchRaw {
			t.Fatalf("lookup() match = %v, want matchRaw", match)
		}
	})

	t.Run("no match", func(t *testing.T) {
		r := newRegistry()
		if _, match := r.lookup("documents", "document.created", 1); match != matchNone {
			t.Fatalf("lookup() match = %v, want matchNone", match)
		}
	})
}
```

If `registry_test.go` has no `noopDispatch` helper already, add one:

```go
func noopDispatch(context.Context, Envelope[json.RawMessage]) error { return nil }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -run TestRegistry_LookupReportsHowItMatched ./...`
Expected: FAIL — `undefined: matchTyped`

- [ ] **Step 3: Change `lookup`**

In `registry.go`, replace `lookup` and add the type above it:

```go
// routeMatch says how an event matched a handler.
//
// It exists for telemetry, not for dispatch, which treats both matches
// identically. A typed match means the event type came from a set fixed at
// registration time and is safe to use as a metric attribute; a raw match
// means it is whatever the wire said, and a raw handler takes every event
// on its topic — so a successful lookup alone is not evidence of a bounded
// event type. Attaching one as a label would let any producer mint
// unbounded series.
type routeMatch int

const (
	matchNone routeMatch = iota
	matchTyped
	matchRaw
)

// lookup resolves an event to its handler, reporting how it matched. A raw
// handler, when present, takes every event on its topic.
func (r *registry) lookup(topic, eventType string, major int) (dispatchFunc, routeMatch) {
	if fn, ok := r.raw[topic]; ok {
		return fn, matchRaw
	}
	if fn, ok := r.typed[routeKey{topic: topic, eventType: eventType, major: major}]; ok {
		return fn, matchTyped
	}
	return nil, matchNone
}
```

- [ ] **Step 4: Update the five existing test call sites**

In `registry_test.go`, replace `ok`-style assertions with `match`:

- Line ~71: `fn, ok := r.lookup(...)` → `fn, match := r.lookup(...)`; `if !ok {` → `if match == matchNone {`
- Line ~106: same transformation.
- Lines ~128, ~131, ~134: `if _, ok := r.lookup(...); ok {` → `if _, match := r.lookup(...); match != matchNone {`

- [ ] **Step 5: Update the `consumer.go` call site**

At `consumer.go:388`, replace:

```go
	handler, ok := c.registry.lookup(topic, env.EventType, major)
	c.mu.Unlock()

	if !ok {
```

with:

```go
	handler, match := c.registry.lookup(topic, env.EventType, major)
	c.mu.Unlock()

	if match == matchNone {
```

Task 6 uses `match` again; leave it named.

- [ ] **Step 6: Run the full unit suite**

Run: `go test ./...`
Expected: PASS. This task changes no behaviour, so any failure is a mechanical slip in one of the six call sites.

- [ ] **Step 7: Commit**

```bash
git add registry.go registry_test.go consumer.go
git commit -m "refactor: have registry.lookup report typed vs raw matches

A raw handler takes every event on its topic, so a successful lookup is
not evidence that event_type came from a bounded set. Phase 3 needs that
distinction before it can use event_type as a metric attribute."
```

---

### Task 3: Instruments, with a no-op fallback

All eleven instruments, built once per `Publisher` / `Consumer`. Spec §2, spec §9.

**Files:**
- Create: `telemetry.go`, `telemetry_test.go`
- Modify: `go.mod` (adds `otel/sdk/metric` as a test dependency)

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `const telemetryScope = "github.com/bcgov/foi-messaging-go"`
  - `type instruments struct` with fields `published`, `publishFailures`, `received`, `processed`, `failed`, `skipped`, `retries`, `dlq`, `dlqPublishFailures` (all `metric.Int64Counter`), `processingDuration`, `queueLatency` (both `metric.Float64Histogram`)
  - `func newInstruments(mp metric.MeterProvider, log *slog.Logger) *instruments`
  - Attribute-key constants used by Tasks 5–10.

- [ ] **Step 1: Write the failing test**

Create `telemetry_test.go`:

```go
package messaging

import (
	"context"
	"log/slog"
	"testing"

	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// readMetrics collects everything recorded through reader, keyed by
// instrument name, so tests can assert on one instrument without
// reconstructing the whole ResourceMetrics tree at every call site.
func readMetrics(t *testing.T, reader *metric.ManualReader) map[string]metricdata.Metrics {
	t.Helper()

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect() = %v, want nil", err)
	}

	out := make(map[string]metricdata.Metrics)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			out[m.Name] = m
		}
	}
	return out
}

// newTestMeterProvider returns a provider and the reader collecting from it.
func newTestMeterProvider(t *testing.T) (*metric.MeterProvider, *metric.ManualReader) {
	t.Helper()
	reader := metric.NewManualReader()
	return metric.NewMeterProvider(metric.WithReader(reader)), reader
}

func TestNewInstruments_CreatesEveryInstrument(t *testing.T) {
	mp, reader := newTestMeterProvider(t)
	inst := newInstruments(mp, slog.Default())

	ctx := context.Background()
	inst.published.Add(ctx, 1)
	inst.publishFailures.Add(ctx, 1)
	inst.received.Add(ctx, 1)
	inst.processed.Add(ctx, 1)
	inst.failed.Add(ctx, 1)
	inst.skipped.Add(ctx, 1)
	inst.retries.Add(ctx, 1)
	inst.dlq.Add(ctx, 1)
	inst.dlqPublishFailures.Add(ctx, 1)
	inst.processingDuration.Record(ctx, 0.1)
	inst.queueLatency.Record(ctx, 0.1)

	got := readMetrics(t, reader)

	// The names are the library's contract with every dashboard built on
	// it, so they are asserted literally rather than derived.
	want := []string{
		"messaging.events.published",
		"messaging.publish.failures",
		"messaging.events.received",
		"messaging.events.processed",
		"messaging.events.failed",
		"messaging.events.skipped",
		"messaging.retries",
		"messaging.dlq",
		"messaging.dlq.publish.failures",
		"messaging.processing.duration",
		"messaging.queue.latency",
	}
	for _, name := range want {
		if _, ok := got[name]; !ok {
			t.Errorf("instrument %q was not recorded", name)
		}
	}
	if len(got) != len(want) {
		t.Errorf("recorded %d instruments, want %d", len(got), len(want))
	}
}

func TestNewInstruments_NilProviderFallsBackToNoop(t *testing.T) {
	// A telemetry failure must never stop a service delivering messages,
	// so construction degrades to no-op instruments rather than erroring.
	inst := newInstruments(nil, slog.Default())

	if inst == nil {
		t.Fatal("newInstruments(nil) = nil, want no-op instruments")
	}
	// Recording through the fallback must not panic.
	inst.published.Add(context.Background(), 1)
	inst.processingDuration.Record(context.Background(), 0.1)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -run TestNewInstruments ./...`
Expected: FAIL — `undefined: newInstruments`. It may also fail to build until the SDK dependency is added; that is the next step.

- [ ] **Step 3: Add the test-only SDK dependency**

```bash
go get go.opentelemetry.io/otel/sdk/metric@v1.44.0
go get go.opentelemetry.io/otel/sdk@v1.44.0
```

These are the canonical test doubles for the API being instrumented against. They are not testify and do not violate the standard-library-testing rule.

- [ ] **Step 4: Write `telemetry.go`**

```go
package messaging

import (
	"log/slog"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

// telemetryScope is the instrumentation scope every instrument and tracer
// is created under. It shows up as otel_scope_name on exported Prometheus
// series, so it is stable API in practice.
const telemetryScope = "github.com/bcgov/foi-messaging-go"

// Metric attribute keys. Named constants because the same key is set from
// several call sites across publisher.go and consumer.go, and a typo would
// silently split one series into two rather than failing anything.
const (
	attrTopic         = "topic"
	attrEventType     = "event_type"
	attrGroup         = "group"
	attrReason        = "reason"
	attrErrorCategory = "error_category"
	attrStage         = "stage"
)

// Publish failure stages (spec §2). They separate "this service is
// emitting garbage" from "Redis is unreachable".
const (
	stageValidation = "validation"
	stageMarshal    = "marshal"
	stageTransport  = "transport"
)

// Skip reasons and error categories, fixed by PRD §16 and spec §2.
const (
	reasonNoHandler = "no_handler"
	reasonDiscard   = "discard"

	categoryPermanent       = "permanent"
	categoryRetryable       = "retryable"
	categoryDeserialization = "deserialization"
	categoryMaxAttempts     = "max_attempts"
)

// instruments holds every metric the library records. It is built once per
// Publisher and per Consumer; recording through it is safe from any
// goroutine, as OTel instruments are.
type instruments struct {
	published          metric.Int64Counter
	publishFailures    metric.Int64Counter
	received           metric.Int64Counter
	processed          metric.Int64Counter
	failed             metric.Int64Counter
	skipped            metric.Int64Counter
	retries            metric.Int64Counter
	dlq                metric.Int64Counter
	dlqPublishFailures metric.Int64Counter
	processingDuration metric.Float64Histogram
	queueLatency       metric.Float64Histogram
}

// newInstruments builds every instrument from mp.
//
// Instrument creation can fail, but every name here is a compile-time
// constant, so a failure means something is wrong with the provider rather
// than with this call. Rather than propagate that into NewPublisher /
// NewConsumer — where it would stop a service delivering messages over a
// broken metrics pipeline — the whole set is rebuilt from the no-op
// provider, which cannot fail, and the reason is logged once. Dropping
// metrics is strictly better than dropping messages.
//
// A nil mp takes the same path, which is what makes instruments usable from
// tests that never configure telemetry.
func newInstruments(mp metric.MeterProvider, log *slog.Logger) *instruments {
	if mp != nil {
		if inst, err := buildInstruments(mp); err == nil {
			return inst
		} else if log != nil {
			log.Warn("messaging: metric instrument creation failed; metrics are disabled for this instance",
				"error", err)
		}
	}

	// Cannot fail: the no-op meter returns no errors.
	inst, _ := buildInstruments(noop.NewMeterProvider())
	return inst
}

// buildInstruments creates the full set, returning on the first failure so
// newInstruments never hands back a partially-wired struct with nil
// instruments in it — recording through one of those panics.
func buildInstruments(mp metric.MeterProvider) (*instruments, error) {
	m := mp.Meter(telemetryScope)
	var inst instruments
	var err error

	// The {event} and {retry} units are UCUM annotations. The Prometheus
	// exporter drops them rather than suffixing the metric name, so they
	// are free documentation; "s" by contrast becomes the _seconds suffix
	// that lands these on the PRD §16 spellings.
	if inst.published, err = m.Int64Counter("messaging.events.published",
		metric.WithDescription("Events successfully published."),
		metric.WithUnit("{event}")); err != nil {
		return nil, err
	}
	if inst.publishFailures, err = m.Int64Counter("messaging.publish.failures",
		metric.WithDescription("Publish attempts that returned an error, by stage."),
		metric.WithUnit("{event}")); err != nil {
		return nil, err
	}
	if inst.received, err = m.Int64Counter("messaging.events.received",
		metric.WithDescription("Stream entries taken off a stream for delivery."),
		metric.WithUnit("{event}")); err != nil {
		return nil, err
	}
	if inst.processed, err = m.Int64Counter("messaging.events.processed",
		metric.WithDescription("Deliveries whose handler returned nil."),
		metric.WithUnit("{event}")); err != nil {
		return nil, err
	}
	if inst.failed, err = m.Int64Counter("messaging.events.failed",
		metric.WithDescription("Deliveries that ended in failure, by error category."),
		metric.WithUnit("{event}")); err != nil {
		return nil, err
	}
	if inst.skipped, err = m.Int64Counter("messaging.events.skipped",
		metric.WithDescription("Deliveries acked without processing, by reason."),
		metric.WithUnit("{event}")); err != nil {
		return nil, err
	}
	if inst.retries, err = m.Int64Counter("messaging.retries",
		metric.WithDescription("Immediate in-process handler retries."),
		metric.WithUnit("{retry}")); err != nil {
		return nil, err
	}
	if inst.dlq, err = m.Int64Counter("messaging.dlq",
		metric.WithDescription("Events written to a dead letter queue, by reason."),
		metric.WithUnit("{event}")); err != nil {
		return nil, err
	}
	if inst.dlqPublishFailures, err = m.Int64Counter("messaging.dlq.publish.failures",
		metric.WithDescription("Failed attempts to write a dead letter."),
		metric.WithUnit("{event}")); err != nil {
		return nil, err
	}
	if inst.processingDuration, err = m.Float64Histogram("messaging.processing.duration",
		metric.WithDescription("End-to-end dispatch duration for one delivery, including immediate retries."),
		metric.WithUnit("s")); err != nil {
		return nil, err
	}
	if inst.queueLatency, err = m.Float64Histogram("messaging.queue.latency",
		metric.WithDescription("Elapsed time from publish to the start of dispatch."),
		metric.WithUnit("s")); err != nil {
		return nil, err
	}

	return &inst, nil
}

// consumeAttrs builds the attribute set shared by the consume-path
// instruments.
//
// eventType is attached only when it came from a typed registry match. A
// raw handler takes every event on its topic, and the deserialization and
// cap paths never reach a lookup at all, so in those cases the value is
// whatever the wire said — unbounded, and one bad producer away from
// exploding the metric store. Spans carry it regardless; see spec §2.
func consumeAttrs(topic, group, eventType string, extra ...attribute.KeyValue) []attribute.KeyValue {
	attrs := make([]attribute.KeyValue, 0, 3+len(extra))
	attrs = append(attrs, attribute.String(attrTopic, topic), attribute.String(attrGroup, group))
	if eventType != "" {
		attrs = append(attrs, attribute.String(attrEventType, eventType))
	}
	return append(attrs, extra...)
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test -run TestNewInstruments ./... -v`
Expected: PASS, both cases.

- [ ] **Step 6: Verify the boundary still holds**

Run: `make lint`
Expected: clean. `telemetry.go` imports only OTel, which `depguard` permits in the root.

- [ ] **Step 7: Commit**

```bash
git add telemetry.go telemetry_test.go go.mod go.sum
git commit -m "feat: add the Phase 3 metric instruments

All eleven instruments from spec §2, built once per Publisher/Consumer.
Construction degrades to no-op instruments rather than failing
NewPublisher/NewConsumer: dropping metrics beats dropping messages."
```

---

### Task 4: The `deliveryOutcome` recorder

The single place the consume path turns an outcome into a span end, a duration observation, and exactly one terminal counter. Spec §1.

**Files:**
- Modify: `telemetry.go`, `telemetry_test.go`

**Interfaces:**
- Consumes: `instruments` (Task 3).
- Produces:
  - `type deliveryRecorder struct`
  - `func (r *deliveryRecorder) processed()`
  - `func (r *deliveryRecorder) failed(category string, err error)`
  - `func (r *deliveryRecorder) skipped(reason string)`
  - `func (r *deliveryRecorder) setEventType(eventType string)` — called only on a typed match
  - `func (r *deliveryRecorder) end()`
  - Task 6 constructs it; Tasks 7–10 call the setters.

- [ ] **Step 1: Write the failing test**

Add to `telemetry_test.go`:

```go
func TestDeliveryRecorder_TerminalInvariant(t *testing.T) {
	// Spec §2: every delivery increments exactly one of processed /
	// failed / skipped, and records processing.duration exactly once.
	// This is the test that keeps that true as exit paths are added.
	terminal := []string{
		"messaging.events.processed",
		"messaging.events.failed",
		"messaging.events.skipped",
	}

	tests := []struct {
		name string
		act  func(r *deliveryRecorder)
		want string
	}{
		{"processed", func(r *deliveryRecorder) { r.processed() }, "messaging.events.processed"},
		{"failed", func(r *deliveryRecorder) { r.failed(categoryPermanent, errors.New("boom")) }, "messaging.events.failed"},
		{"skipped", func(r *deliveryRecorder) { r.skipped(reasonNoHandler) }, "messaging.events.skipped"},
		{"unset defaults to failed", func(r *deliveryRecorder) {}, "messaging.events.failed"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mp, reader := newTestMeterProvider(t)
			inst := newInstruments(mp, slog.Default())
			r := newDeliveryRecorder(inst, tracenoop.Span{}, "documents", "billing", slog.Default())

			tt.act(r)
			r.end()

			got := readMetrics(t, reader)

			for _, name := range terminal {
				_, present := got[name]
				if name == tt.want && !present {
					t.Errorf("terminal counter %q was not recorded", name)
				}
				if name != tt.want && present {
					t.Errorf("terminal counter %q was recorded; want only %q", name, tt.want)
				}
			}

			if _, ok := got["messaging.processing.duration"]; !ok {
				t.Error("processing.duration was not recorded")
			}
		})
	}
}

func TestDeliveryRecorder_EndIsIdempotent(t *testing.T) {
	// The recorder is invoked from a defer on a path that also returns
	// early; a double end would double-count every delivery.
	mp, reader := newTestMeterProvider(t)
	inst := newInstruments(mp, slog.Default())
	r := newDeliveryRecorder(inst, tracenoop.Span{}, "documents", "billing", slog.Default())

	r.processed()
	r.end()
	r.end()

	got := readMetrics(t, reader)
	sum, ok := got["messaging.events.processed"].Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("processed data = %T, want Sum[int64]", got["messaging.events.processed"].Data)
	}
	if len(sum.DataPoints) != 1 || sum.DataPoints[0].Value != 1 {
		t.Fatalf("processed = %+v, want a single data point of 1", sum.DataPoints)
	}
}
```

Add imports to `telemetry_test.go`: `"errors"` and `tracenoop "go.opentelemetry.io/otel/trace/noop"`. The alias matters: `telemetry.go` already binds `noop` to the *metric* no-op package, and the two are different modules.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -run TestDeliveryRecorder ./...`
Expected: FAIL — `undefined: newDeliveryRecorder`

- [ ] **Step 3: Implement the recorder**

Add to `telemetry.go` (imports grow by `"context"`, `"time"`, and `"go.opentelemetry.io/otel/trace"`):

```go
// outcomeKind is the terminal disposition of one delivery. Exactly one is
// recorded per delivery — see deliveryRecorder.end.
type outcomeKind int

const (
	outcomeUnset outcomeKind = iota
	outcomeProcessed
	outcomeFailed
	outcomeSkipped
)

// deliveryRecorder accumulates the outcome of one delivery and records all
// of it at once.
//
// dispatch has seven terminal exit paths. Instrumenting each in place means
// twenty-odd statements threaded through the subtlest code in the
// repository, and every exit path added later is a chance to forget one.
// Instead each path states its outcome and a single deferred end() records
// the span status, the duration histogram, and exactly one counter.
//
// It also owns the one thing per-site instrumentation cannot get right: the
// duration histogram and the span need a single start and a single end, and
// holding both here is the only shape where they cannot drift apart.
type deliveryRecorder struct {
	inst  *instruments
	span  trace.Span
	log   *slog.Logger
	topic string
	group string
	start time.Time

	kind      outcomeKind
	category  string
	reason    string
	eventType string
	err       error
	ended     bool
}

func newDeliveryRecorder(inst *instruments, span trace.Span, topic, group string, log *slog.Logger) *deliveryRecorder {
	return &deliveryRecorder{
		inst:  inst,
		span:  span,
		log:   log,
		topic: topic,
		group: group,
		start: time.Now(),
	}
}

// setEventType records the event type for metric attribution. It is called
// only on a typed registry match; see consumeAttrs.
func (r *deliveryRecorder) setEventType(eventType string) { r.eventType = eventType }

func (r *deliveryRecorder) processed() { r.kind = outcomeProcessed }

func (r *deliveryRecorder) failed(category string, err error) {
	r.kind = outcomeFailed
	r.category = category
	r.err = err
}

func (r *deliveryRecorder) skipped(reason string) {
	r.kind = outcomeSkipped
	r.reason = reason
}

// end records the delivery. It is idempotent: it is called from a defer on
// paths that also return early, and a second recording would double-count
// every delivery.
func (r *deliveryRecorder) end() {
	if r.ended {
		return
	}
	r.ended = true

	ctx := context.Background()
	elapsed := time.Since(r.start).Seconds()
	base := consumeAttrs(r.topic, r.group, r.eventType)

	r.inst.processingDuration.Record(ctx, elapsed, metric.WithAttributes(base...))

	switch r.kind {
	case outcomeProcessed:
		r.inst.processed.Add(ctx, 1, metric.WithAttributes(base...))
		r.span.SetStatus(codes.Ok, "")

	case outcomeSkipped:
		r.inst.skipped.Add(ctx, 1, metric.WithAttributes(
			consumeAttrs(r.topic, r.group, "", attribute.String(attrReason, r.reason))...))

	case outcomeFailed:
		r.recordFailure(ctx, base)

	default:
		// Unreachable by design: every dispatch exit path states an
		// outcome. Treated as a failure rather than silently skipped so
		// the "exactly one terminal counter" invariant stays literally
		// true even if a future exit path forgets, and so the omission
		// is visible in the logs rather than as a quiet gap between
		// received and the terminal counters.
		if r.log != nil {
			r.log.Error("messaging: delivery ended with no recorded outcome; this is a bug in the library",
				"topic", r.topic)
		}
		r.category = "unknown"
		r.recordFailure(ctx, base)
	}

	r.span.End()
}

func (r *deliveryRecorder) recordFailure(ctx context.Context, base []attribute.KeyValue) {
	r.inst.failed.Add(ctx, 1, metric.WithAttributes(
		append(base, attribute.String(attrErrorCategory, r.category))...))
	if r.err != nil {
		r.span.RecordError(r.err)
	}
	r.span.SetStatus(codes.Error, r.category)
}
```

Add `"go.opentelemetry.io/otel/codes"` to `telemetry.go`'s imports.

Note on `end()` using `context.Background()`: metric recording only reads a context for exemplars and cancellation, and the delivery's own context may be cancelled at the drain deadline — recording through a cancelled context would silently drop the observation for exactly the deliveries most worth counting. Put that reasoning in a comment at the `ctx :=` line.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test -run TestDeliveryRecorder ./... -v`
Expected: PASS, all five subtests.

- [ ] **Step 5: Commit**

```bash
git add telemetry.go telemetry_test.go
git commit -m "feat: add the delivery outcome recorder

One deferred recorder turns a delivery's outcome into the span end, the
duration histogram, and exactly one terminal counter, so dispatch's seven
exit paths each state an outcome instead of calling telemetry."
```

---

### Task 5: Publish path — span, metrics, and transport metadata

Spec §3 (Publish), §4 (`published_at`).

**Files:**
- Modify: `publisher.go:50-116`
- Test: `publisher_test.go`

**Interfaces:**
- Consumes: `instruments`, `newInstruments` (Task 3); `cfg.Telemetry.Propagator` (Task 1).
- Produces:
  - `Publisher.inst *instruments`, `Publisher.tracer trace.Tracer`
  - `const metadataPublishedAt = "published_at"` — read by Task 10
  - `func publishedAtNow() string` / `func parsePublishedAt(string) (time.Time, bool)` — the latter used by Task 10

- [ ] **Step 1: Write the failing test**

Add to `publisher_test.go`:

```go
func TestPublish_RecordsSpanAndMetadata(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	mp, reader := newTestMeterProvider(t)

	captured := make(map[string]string)
	p := &Publisher{
		cfg: Config{
			Source:       "billing.service",
			StreamPrefix: "foi",
			Telemetry: TelemetryConfig{
				TracerProvider: tp,
				MeterProvider:  mp,
				Propagator:     propagation.TraceContext{},
				Logger:         slog.Default(),
			},
		},
		inst:   newInstruments(mp, slog.Default()),
		tracer: tp.Tracer(telemetryScope),
		wm:     nil,
	}
	p.publishFn = func(ctx context.Context, stream, id string, body []byte, md map[string]string) error {
		for k, v := range md {
			captured[k] = v
		}
		return nil
	}

	def := EventDef{Topic: "documents", Type: "document.created", Version: "1.0.0"}
	if _, err := p.Publish(context.Background(), def, map[string]string{"a": "b"}); err != nil {
		t.Fatalf("Publish() = %v, want nil", err)
	}

	if _, ok := captured["traceparent"]; !ok {
		t.Errorf("metadata = %v, want a traceparent key", captured)
	}
	if _, ok := captured[metadataPublishedAt]; !ok {
		t.Errorf("metadata = %v, want a %s key", captured, metadataPublishedAt)
	}

	spans := recorder.Ended()
	if len(spans) != 1 {
		t.Fatalf("recorded %d spans, want 1", len(spans))
	}
	if got, want := spans[0].Name(), "publish documents"; got != want {
		t.Errorf("span name = %q, want %q", got, want)
	}
	if got := spans[0].SpanKind(); got != trace.SpanKindProducer {
		t.Errorf("span kind = %v, want Producer", got)
	}

	if _, ok := readMetrics(t, reader)["messaging.events.published"]; !ok {
		t.Error("messaging.events.published was not recorded")
	}
}

func TestPublish_RecordsValidationFailureStage(t *testing.T) {
	// An envelope that fails validateEnvelope must be attributed to the
	// validation stage, not lumped in with transport failures — the whole
	// point of the stage attribute is separating "this service is emitting
	// garbage" from "Redis is unreachable".
	mp, reader := newTestMeterProvider(t)
	tp := sdktrace.NewTracerProvider()

	p := &Publisher{
		cfg: Config{
			// An empty Source makes the envelope fail validation.
			Source:       "",
			StreamPrefix: "foi",
			Telemetry: TelemetryConfig{
				TracerProvider: tp,
				MeterProvider:  mp,
				Propagator:     propagation.TraceContext{},
				Logger:         slog.Default(),
			},
		},
		inst:   newInstruments(mp, slog.Default()),
		tracer: tp.Tracer(telemetryScope),
	}
	p.publishFn = func(context.Context, string, string, []byte, map[string]string) error {
		t.Error("transport was reached despite an invalid envelope")
		return nil
	}

	def := EventDef{Topic: "documents", Type: "document.created", Version: "1.0.0"}
	if _, err := p.Publish(context.Background(), def, map[string]string{}); err == nil {
		t.Fatal("Publish() = nil, want a validation error")
	}

	m, ok := readMetrics(t, reader)["messaging.publish.failures"]
	if !ok {
		t.Fatal("messaging.publish.failures was not recorded")
	}
	sum, ok := m.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("publish.failures data = %T, want Sum[int64]", m.Data)
	}
	stage, found := sum.DataPoints[0].Attributes.Value(attribute.Key(attrStage))
	if !found || stage.AsString() != stageValidation {
		t.Errorf("stage = %v, want %q", stage.AsString(), stageValidation)
	}
}

func TestPublish_RecordsMarshalFailureStage(t *testing.T) {
	// validateEnvelope never inspects Payload, so an unmarshalable payload
	// passes validation and fails at json.Marshal — which is exactly the
	// gap the marshal stage exists to name.
	mp, reader := newTestMeterProvider(t)
	tp := sdktrace.NewTracerProvider()

	p := &Publisher{
		cfg: Config{
			Source:       "billing.service",
			StreamPrefix: "foi",
			Telemetry: TelemetryConfig{
				TracerProvider: tp,
				MeterProvider:  mp,
				Propagator:     propagation.TraceContext{},
				Logger:         slog.Default(),
			},
		},
		inst:   newInstruments(mp, slog.Default()),
		tracer: tp.Tracer(telemetryScope),
	}
	p.publishFn = func(context.Context, string, string, []byte, map[string]string) error {
		t.Error("transport was reached despite an unmarshalable payload")
		return nil
	}

	def := EventDef{Topic: "documents", Type: "document.created", Version: "1.0.0"}
	// A channel cannot be JSON-encoded.
	if _, err := p.Publish(context.Background(), def, make(chan int)); err == nil {
		t.Fatal("Publish() = nil, want a marshalling error")
	}

	m, ok := readMetrics(t, reader)["messaging.publish.failures"]
	if !ok {
		t.Fatal("messaging.publish.failures was not recorded")
	}
	sum, ok := m.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("publish.failures data = %T, want Sum[int64]", m.Data)
	}
	stage, found := sum.DataPoints[0].Attributes.Value(attribute.Key(attrStage))
	if !found || stage.AsString() != stageMarshal {
		t.Errorf("stage = %v, want %q", stage.AsString(), stageMarshal)
	}
}

func TestPublish_RecordsFailureStage(t *testing.T) {
	mp, reader := newTestMeterProvider(t)
	tp := sdktrace.NewTracerProvider()

	p := &Publisher{
		cfg: Config{
			Source:       "billing.service",
			StreamPrefix: "foi",
			Telemetry: TelemetryConfig{
				TracerProvider: tp,
				MeterProvider:  mp,
				Propagator:     propagation.TraceContext{},
				Logger:         slog.Default(),
			},
		},
		inst:   newInstruments(mp, slog.Default()),
		tracer: tp.Tracer(telemetryScope),
	}
	p.publishFn = func(context.Context, string, string, []byte, map[string]string) error {
		return errors.New("redis is down")
	}

	def := EventDef{Topic: "documents", Type: "document.created", Version: "1.0.0"}
	if _, err := p.Publish(context.Background(), def, map[string]string{}); err == nil {
		t.Fatal("Publish() = nil, want an error")
	}

	got := readMetrics(t, reader)
	m, ok := got["messaging.publish.failures"]
	if !ok {
		t.Fatal("messaging.publish.failures was not recorded")
	}
	sum, ok := m.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("publish.failures data = %T, want Sum[int64]", m.Data)
	}
	stage, found := sum.DataPoints[0].Attributes.Value(attribute.Key(attrStage))
	if !found || stage.AsString() != stageTransport {
		t.Errorf("stage = %v, want %q", stage.AsString(), stageTransport)
	}

	if _, ok := got["messaging.events.published"]; ok {
		t.Error("messaging.events.published was recorded for a failed publish")
	}
}
```

Imports for `publisher_test.go`: `"errors"`, `"log/slog"`, `"go.opentelemetry.io/otel/attribute"`, `"go.opentelemetry.io/otel/propagation"`, `"go.opentelemetry.io/otel/trace"`, `sdktrace "go.opentelemetry.io/otel/sdk/trace"`, `"go.opentelemetry.io/otel/sdk/trace/tracetest"`, `"go.opentelemetry.io/otel/sdk/metric/metricdata"`.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -run TestPublish_Records ./...`
Expected: FAIL — `unknown field publishFn in struct literal`

- [ ] **Step 3: Add the seam and the fields**

The `publishFn` field exists so the publish path is testable without Redis — the current `Publisher` reaches straight into `*internalwatermill.Publisher`, which needs a live client. In `publisher.go`:

```go
type Publisher struct {
	cfg    Config
	wm     *internalwatermill.Publisher
	inst   *instruments
	tracer trace.Tracer

	// publishFn is the transport write. It defaults to wm.Publish and is
	// replaced in tests: the telemetry around a publish — span status,
	// failure stage attribution — is most interesting on the failure
	// paths, which are the hardest to provoke against a live broker.
	publishFn func(ctx context.Context, stream, id string, body []byte, metadata map[string]string) error
}
```

In `NewPublisher`, after building `wmPublisher`:

```go
	p := &Publisher{
		cfg:    cfg,
		wm:     wmPublisher,
		inst:   newInstruments(cfg.Telemetry.MeterProvider, cfg.Telemetry.Logger),
		tracer: cfg.Telemetry.TracerProvider.Tracer(telemetryScope),
	}
	p.publishFn = p.wm.Publish
	return p, nil
```

- [ ] **Step 4: Instrument `Publish`**

Replace the body of `Publish` (keeping the existing validation of `def.Topic` first, before the span opens — a publish with no topic has no span name to use):

```go
func (p *Publisher) Publish(ctx context.Context, def EventDef, payload any, opts ...PublishOption) (PublishResult, error) {
	if def.Topic == "" {
		return PublishResult{}, fmt.Errorf("event def: topic is required")
	}

	stream := p.cfg.StreamPrefix + ":" + def.Topic

	// The span opens before validation and marshalling, not just around
	// the transport write, so a rejected envelope is as visible in a trace
	// as an unreachable Redis is — and so the span and
	// publish.failures{stage} describe the same call.
	ctx, span := p.tracer.Start(ctx, "publish "+def.Topic,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "redis"),
			attribute.String("messaging.operation.name", "publish"),
			attribute.String("messaging.destination.name", stream),
			attribute.String("messaging.foi.event_type", def.Type),
			attribute.String("messaging.foi.schema_version", def.Version),
		))
	defer span.End()

	attrs := []attribute.KeyValue{
		attribute.String(attrTopic, def.Topic),
		attribute.String(attrEventType, def.Type),
	}

	fail := func(stage string, err error) (PublishResult, error) {
		p.inst.publishFailures.Add(ctx, 1, metric.WithAttributes(
			append(attrs, attribute.String(attrStage, stage))...))
		span.RecordError(err)
		span.SetStatus(codes.Error, stage)
		return PublishResult{}, err
	}

	var options publishOptions
	for _, opt := range opts {
		opt(&options)
	}

	correlationID, err := resolveCorrelationID(ctx, options)
	if err != nil {
		return fail(stageValidation, err)
	}

	env, err := newEnvelope(def, p.cfg.Source, correlationID, payload)
	if err != nil {
		return fail(stageValidation, err)
	}

	if err := validateEnvelope(env); err != nil {
		return fail(stageValidation, err)
	}

	span.SetAttributes(
		attribute.String("messaging.message.id", env.EventID),
		attribute.String("messaging.foi.correlation_id", env.CorrelationID),
	)

	body, err := json.Marshal(env)
	if err != nil {
		return fail(stageMarshal, fmt.Errorf("marshaling envelope: %w", err))
	}

	// Transport metadata (PRD §5): trace context so the consumer span can
	// parent to this one, and published_at so queue latency is measurable
	// separately from handler duration. Neither is ever merged into the
	// envelope.
	metadata := map[string]string{metadataPublishedAt: publishedAtNow()}
	p.cfg.Telemetry.Propagator.Inject(ctx, propagation.MapCarrier(metadata))

	if err := p.publishFn(ctx, stream, env.EventID, body, metadata); err != nil {
		return fail(stageTransport, fmt.Errorf("publishing to stream %q: %w", stream, err))
	}

	p.inst.published.Add(ctx, 1, metric.WithAttributes(attrs...))
	span.SetStatus(codes.Ok, "")

	return PublishResult{EventID: env.EventID, Timestamp: env.Timestamp}, nil
}
```

- [ ] **Step 5: Add the `published_at` helpers**

Add to `telemetry.go`:

```go
// metadataPublishedAt is the transport metadata key carrying wall-clock
// publish time (PRD §5). It is unexported because PRD §5 states there is no
// public API for transport metadata: applications neither read nor write it.
const metadataPublishedAt = "published_at"

func publishedAtNow() string {
	return time.Now().UTC().Format(time.RFC3339Nano)
}

// parsePublishedAt reads the publish timestamp back.
//
// ok is false for a missing or unparseable value, and the caller skips the
// queue-latency observation rather than failing. A message whose transport
// metadata is odd is still a message worth delivering.
func parsePublishedAt(metadata map[string]string) (time.Time, bool) {
	raw, ok := metadata[metadataPublishedAt]
	if !ok {
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `go test -run TestPublish ./... -v`
Expected: PASS, including the pre-existing publisher tests.

- [ ] **Step 7: Run the integration publish test**

Run: `go test -tags=integration -race -count=1 -run TestPublisher ./... -v`
Expected: PASS. This proves `traceparent` and `published_at` survive the real marshaller and Redis round trip.

- [ ] **Step 8: Commit**

```bash
git add publisher.go publisher_test.go telemetry.go
git commit -m "feat: instrument the publish path

Producer span covering validation, marshalling and transport, so a
rejected envelope is as visible as an unreachable Redis; published and
publish.failures{stage} counters; traceparent and published_at injected
as transport metadata per PRD §5."
```

---

### Task 6: Consume path — span and the `dispatch` outcome refactor

The core of the phase. Spec §1, §3 (Consume).

**Files:**
- Modify: `consumer.go:141-283` (`Run`, to build instruments and tracer), `consumer.go:304-403` (`dispatch`)
- Test: `consumer_test.go`

**Interfaces:**
- Consumes: `newInstruments`, `consumeAttrs` (Task 3); `newDeliveryRecorder` and its setters (Task 4); `matchTyped` (Task 2); `parsePublishedAt` (Task 5).
- Produces: `Consumer.inst *instruments`, `Consumer.tracer trace.Tracer`, both set by `NewConsumer`. Tasks 7–11 record through `c.inst` and through the recorder `dispatch` creates.

- [ ] **Step 1: Write the failing test**

Add to `consumer_test.go`:

```go
func TestDispatch_TerminalInvariantAcrossExitPaths(t *testing.T) {
	// Spec §2's invariant, asserted against the real dispatch rather than
	// the recorder in isolation: every exit path must record exactly one
	// terminal counter and exactly one duration observation.
	terminal := []string{
		"messaging.events.processed",
		"messaging.events.failed",
		"messaging.events.skipped",
	}

	validEnvelope := func(eventType string) []byte {
		env := Envelope[json.RawMessage]{
			EventID:       "018f2e7a-1c6b-7c0a-9f8d-3e4a2b1c5d90",
			EventType:     eventType,
			Timestamp:     time.Now().UTC(),
			SchemaVersion: "1.0.0",
			CorrelationID: "corr-1",
			Source:        "test",
			Payload:       json.RawMessage(`{}`),
		}
		b, err := json.Marshal(env)
		if err != nil {
			t.Fatalf("marshalling test envelope: %v", err)
		}
		return b
	}

	tests := []struct {
		name     string
		payload  []byte
		metadata map[string]string
		handler  func(context.Context, Envelope[json.RawMessage]) error
		register bool
		want     string
	}{
		{
			name:     "processed",
			payload:  validEnvelope("document.created"),
			handler:  func(context.Context, Envelope[json.RawMessage]) error { return nil },
			register: true,
			want:     "messaging.events.processed",
		},
		{
			name:     "no handler",
			payload:  validEnvelope("document.created"),
			register: false,
			want:     "messaging.events.skipped",
		},
		{
			name:     "discard",
			payload:  validEnvelope("document.created"),
			handler:  func(context.Context, Envelope[json.RawMessage]) error { return AsDiscard(errors.New("nope")) },
			register: true,
			want:     "messaging.events.skipped",
		},
		{
			name:     "permanent",
			payload:  validEnvelope("document.created"),
			handler:  func(context.Context, Envelope[json.RawMessage]) error { return AsPermanent(errors.New("bad")) },
			register: true,
			want:     "messaging.events.failed",
		},
		{
			name:    "undecodable",
			payload: []byte("{not json"),
			want:    "messaging.events.failed",
		},
		{
			name:     "cap exceeded",
			payload:  validEnvelope("document.created"),
			metadata: map[string]string{internalwatermill.MetadataDeliveryAttempt: "99"},
			want:     "messaging.events.failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mp, reader := newTestMeterProvider(t)
			c := newTestConsumerWithTelemetry(t, mp)
			c.dlq = &recordingSink{}

			if tt.register {
				h := tt.handler
				if err := c.registry.addTyped("documents", "document.created", 1,
					func(ctx context.Context, env Envelope[json.RawMessage]) error { return h(ctx, env) }); err != nil {
					t.Fatalf("addTyped() = %v, want nil", err)
				}
			}

			_ = c.dispatch(context.Background(), "documents", tt.payload, tt.metadata)

			got := readMetrics(t, reader)
			for _, name := range terminal {
				_, present := got[name]
				if name == tt.want && !present {
					t.Errorf("terminal counter %q was not recorded", name)
				}
				if name != tt.want && present {
					t.Errorf("terminal counter %q was recorded; want only %q", name, tt.want)
				}
			}
			if _, ok := got["messaging.processing.duration"]; !ok {
				t.Error("processing.duration was not recorded")
			}
			if _, ok := got["messaging.events.received"]; !ok {
				t.Error("messaging.events.received was not recorded")
			}
		})
	}
}

func TestDispatch_EventTypeAttributeIsBounded(t *testing.T) {
	// A raw handler takes every event on its topic, so the wire event_type
	// must not become a metric attribute — that is the unbounded case the
	// rule exists for. The span carries it regardless.
	mp, reader := newTestMeterProvider(t)
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))

	c := newTestConsumerWithTelemetry(t, mp)
	c.tracer = tp.Tracer(telemetryScope)
	c.dlq = &recordingSink{}

	if err := c.registry.addRaw("documents", func(context.Context, Envelope[json.RawMessage]) error { return nil }); err != nil {
		t.Fatalf("addRaw() = %v, want nil", err)
	}

	env := Envelope[json.RawMessage]{
		EventID: "018f2e7a-1c6b-7c0a-9f8d-3e4a2b1c5d90", EventType: "attacker.controlled.value",
		Timestamp: time.Now().UTC(), SchemaVersion: "1.0.0", CorrelationID: "c", Source: "s",
		Payload: json.RawMessage(`{}`),
	}
	body, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshalling envelope: %v", err)
	}

	if err := c.dispatch(context.Background(), "documents", body, nil); err != nil {
		t.Fatalf("dispatch() = %v, want nil", err)
	}

	m := readMetrics(t, reader)["messaging.events.processed"]
	sum, ok := m.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("processed data = %T, want Sum[int64]", m.Data)
	}
	if _, found := sum.DataPoints[0].Attributes.Value(attribute.Key(attrEventType)); found {
		t.Error("event_type was attached as a metric attribute on a raw-handler match")
	}

	spans := recorder.Ended()
	if len(spans) != 1 {
		t.Fatalf("recorded %d spans, want 1", len(spans))
	}
	var sawEventType bool
	for _, a := range spans[0].Attributes() {
		if a.Key == "messaging.foi.event_type" && a.Value.AsString() == "attacker.controlled.value" {
			sawEventType = true
		}
	}
	if !sawEventType {
		t.Error("span did not carry the wire event_type")
	}
}
```

Add a helper to `consumer_test.go`:

```go
// newTestConsumerWithTelemetry builds a Consumer wired to mp, reusing the
// existing testConsumerConfig() so these tests stay in step with the rest
// of the suite's defaults. dispatch is reachable without Run, which is
// what makes the failure paths testable at all.
func newTestConsumerWithTelemetry(t *testing.T, mp metric.MeterProvider) *Consumer {
	t.Helper()

	cfg := testConsumerConfig()
	cfg.Telemetry.MeterProvider = mp

	c, err := NewConsumer(cfg)
	if err != nil {
		t.Fatalf("NewConsumer() = %v, want nil", err)
	}
	return c
}
```

`testConsumerConfig()` is defined at `consumer_test.go:15` and already supplies `Source`, `Redis.Address`, and `Consumer.Group`. Add `"go.opentelemetry.io/otel/metric"` to `consumer_test.go`'s imports for the parameter type.

`recordingSink` already exists at `consumer_test.go:664` — Phase 2b's DLQ stub, with an `err` field for the failure case and an `only(t)` helper for asserting a single dead letter. Use it; do not add another sink type.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -run 'TestDispatch_(TerminalInvariant|EventTypeAttribute)' ./...`
Expected: FAIL — `c.tracer undefined` and no metrics recorded.

- [ ] **Step 3: Add the fields to `Consumer` and `NewConsumer`**

In `consumer.go`, add to the `Consumer` struct:

```go
	inst   *instruments
	tracer trace.Tracer
```

and in `NewConsumer`, after the two validation calls:

```go
	return &Consumer{
		cfg:      cfg,
		registry: newRegistry(),
		inst:     newInstruments(cfg.Telemetry.MeterProvider, cfg.Telemetry.Logger),
		tracer:   cfg.Telemetry.TracerProvider.Tracer(telemetryScope),
	}, nil
```

- [ ] **Step 4: Refactor `dispatch`**

Replace `dispatch` with the version below. Every existing behaviour is preserved exactly — same order, same returns, same log lines — with an outcome stated on each path.

```go
func (c *Consumer) dispatch(ctx context.Context, topic string, payload []byte, metadata map[string]string) error {
	// Extract before the span opens so the consumer span parents to the
	// producer's (PRD §5). A message with no traceparent simply starts a
	// new trace here.
	ctx = c.cfg.Telemetry.Propagator.Extract(ctx, propagation.MapCarrier(metadata))

	stream := c.streamName(topic)
	ctx, span := c.tracer.Start(ctx, "process "+topic,
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("messaging.system", "redis"),
			attribute.String("messaging.operation.name", "process"),
			attribute.String("messaging.destination.name", stream),
			attribute.String("messaging.consumer.group.name", c.cfg.Consumer.Group),
			attribute.String("messaging.foi.stream_id", metadata[internalwatermill.MetadataStreamID]),
		))

	rec := newDeliveryRecorder(c.inst, span, topic, c.cfg.Consumer.Group, c.cfg.Telemetry.Logger)
	// The only thing guaranteeing the span is closed when a handler is
	// abandoned at the drain deadline: message contexts are
	// context.WithoutCancel-derived and stay live for the whole
	// ShutdownTimeout, so nothing else will end it.
	defer rec.end()

	c.inst.received.Add(ctx, 1, metric.WithAttributes(attribute.String(attrTopic, topic)))

	log := c.cfg.Telemetry.Logger.With(
		"stream_id", metadata[internalwatermill.MetadataStreamID],
		"delivery_attempt", metadata[internalwatermill.MetadataDeliveryAttempt],
	)

	attempt := deliveryAttempt(metadata)
	span.SetAttributes(attribute.Int64("messaging.foi.delivery_attempt", attempt))

	if attempt > int64(c.cfg.Consumer.MaxDeliveryAttempts) {
		log.Warn("messaging: delivery attempt cap exceeded",
			"topic", topic, "max_delivery_attempts", c.cfg.Consumer.MaxDeliveryAttempts)

		err := fmt.Errorf("delivery attempt %d exceeded MaxDeliveryAttempts %d",
			attempt, c.cfg.Consumer.MaxDeliveryAttempts)
		rec.failed(categoryMaxAttempts, err)

		dl := c.newDeadLetter(topic, ReasonMaxAttemptsExceeded, err, attempt)
		dl.Event, dl.EventRaw = deadLetterBody(payload)
		return c.deadLetter(ctx, topic, dl)
	}

	var env Envelope[json.RawMessage]
	if err := json.Unmarshal(payload, &env); err != nil {
		log.Error("messaging: undecodable event envelope", "topic", topic, "error", err)
		rec.failed(categoryDeserialization, err)

		dl := c.newDeadLetter(topic, ReasonDeserializationFailed,
			fmt.Errorf("unmarshalling envelope on topic %q: %w", topic, err), attempt)
		dl.EventRaw = payload
		return c.deadLetter(ctx, topic, dl)
	}

	span.SetAttributes(
		attribute.String("messaging.message.id", env.EventID),
		attribute.String("messaging.foi.event_type", env.EventType),
		attribute.String("messaging.foi.schema_version", env.SchemaVersion),
		attribute.String("messaging.foi.correlation_id", env.CorrelationID),
	)

	if err := validateEnvelope(env); err != nil {
		log.Error("messaging: invalid event envelope",
			"topic", topic, "event_id", env.EventID, "error", err)
		rec.failed(categoryDeserialization, err)

		dl := c.newDeadLetter(topic, ReasonDeserializationFailed,
			fmt.Errorf("validating envelope on topic %q: %w", topic, err), attempt)
		dl.EventRaw = payload
		return c.deadLetter(ctx, topic, dl)
	}

	major, err := majorVersion(env.SchemaVersion)
	if err != nil {
		log.Error("messaging: unparseable schema version",
			"topic", topic, "event_id", env.EventID, "error", err)
		rec.failed(categoryDeserialization, err)

		dl := c.newDeadLetter(topic, ReasonDeserializationFailed,
			fmt.Errorf("parsing schema version on topic %q: %w", topic, err), attempt)
		dl.EventRaw = payload
		return c.deadLetter(ctx, topic, dl)
	}

	c.mu.Lock()
	handler, match := c.registry.lookup(topic, env.EventType, major)
	c.mu.Unlock()

	if match == matchNone {
		log.Debug("messaging: no handler for event",
			"topic", topic, "event_type", env.EventType,
			"schema_version", env.SchemaVersion, "event_id", env.EventID)
		rec.skipped(reasonNoHandler)
		return nil
	}

	// Only a typed match proves the event type came from a bounded set
	// fixed at registration; a raw handler takes whatever the wire said.
	if match == matchTyped {
		rec.setEventType(env.EventType)
	}

	ctx = contextWithCorrelationID(ctx, env.CorrelationID)
	return c.runWithRetry(ctx, topic, payload, attempt, handler, env, rec)
}
```

Add to `consumer.go`'s imports: `"go.opentelemetry.io/otel/attribute"`, `"go.opentelemetry.io/otel/metric"`, `"go.opentelemetry.io/otel/propagation"`, `"go.opentelemetry.io/otel/trace"`.

- [ ] **Step 5: Thread the recorder into `runWithRetry`**

Change the signature at `consumer.go:545` to take `rec *deliveryRecorder` as its final parameter, and set outcomes on its three terminal branches. Leave the retry counter and span events to Task 7.

```go
		case err == nil:
			rec.processed()
			return nil

		case IsDiscard(err):
			log.Warn("messaging: handler discarded event", "error", err)
			rec.skipped(reasonDiscard)
			return nil

		case IsPermanent(err):
			log.Error("messaging: handler returned a permanent error", "error", err)
			rec.failed(categoryPermanent, err)
			dl := c.newDeadLetter(topic, ReasonPermanent, err, attempt)
			dl.Event, dl.EventRaw = deadLetterBody(payload)
			return c.deadLetter(ctx, topic, dl)

		case i >= c.cfg.Retry.MaxImmediateRetries:
			log.Error("messaging: handler returned error, immediate retries exhausted",
				"immediate_attempts", i+1, "error", err)
			rec.failed(categoryRetryable, err)
			return err
```

And the mid-backoff abandonment path near the end of the loop:

```go
		if !sleepWithJitter(ctx, backoffUpperBound(c.cfg.Retry, i)) {
			// Abandoned mid-backoff. Nack so the entry survives.
			rec.failed(categoryRetryable, err)
			return err
		}
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `go test -run TestDispatch ./... -v`
Expected: PASS, all seven subtests plus the cardinality test.

- [ ] **Step 7: Run the whole unit suite**

Run: `go test -race ./...`
Expected: PASS. `dispatch`'s existing tests assert behaviour this task deliberately did not change; a failure means the refactor altered a return or a log line.

- [ ] **Step 8: Run the integration suite**

Run: `go test -tags=integration -race -count=1 ./...`
Expected: PASS. Phase 2b's cap, DLQ, and retry tests are the ones that would catch a reordered check; they only run under this tag.

- [ ] **STOP — Step 9: Manual verification gate**

**Do not commit. Stop here and hand back to the human.**

This is the only task that rewrites the control flow of the most subtle function in the repository, and the failure mode is silent: a reordered check still passes every telemetry test in this plan while changing when an event is dead-lettered versus nacked. Tests alone are not sufficient evidence here.

Produce this for review and wait for an explicit go-ahead:

```bash
git diff consumer.go
```

State, in the handoff, the answers to each of these — do not merely assert "behaviour is unchanged":

1. **Check order.** The delivery-attempt cap still fires *before* `json.Unmarshal`. Quote the two lines in order from the new code. An over-cap event must not spend handler invocations, or its concurrency slot, proving what its counter already said.
2. **Dead-letter vs nack.** All three deserialization failures (undecodable, invalid envelope, unparseable version) still `return c.deadLetter(...)` rather than returning a bare error. Nacking them instead would burn five reclaim cycles to reach a verdict available on the first look.
3. **Return values.** Every exit path returns what it returned before: `nil` for no-handler and discard, the handler's error for retries-exhausted, `deadLetter`'s result for the four DLQ paths.
4. **Log lines.** Every existing `log.Warn` / `log.Error` / `log.Debug` call survives with its original message string and attributes. Phase 2b's integration tests assert on some of these.
5. **The lock.** The `c.mu.Lock()` / `Unlock()` around `registry.lookup` still spans only the lookup, and no telemetry call was added inside it.
6. **Test edits.** Confirm you changed **no** existing test assertion. If a Phase 2b test failed and you altered it, say so explicitly — that is a signal the refactor changed behaviour, not that the test was wrong.

If the human approves, commit:

```bash
git add consumer.go consumer_test.go
git commit -m "feat: instrument the consume path

Consumer span parented to the producer's via extracted trace context, and
a deferred outcome recorder so dispatch's seven exit paths each state an
outcome. event_type becomes a metric attribute only on a typed match: a
raw handler takes every event on its topic."
```

---

### Task 7: Retry counter and retry span events

Spec §3 (one span per delivery, retries as span events).

**Files:**
- Modify: `consumer.go` (`runWithRetry`)
- Test: `consumer_test.go`

**Interfaces:**
- Consumes: `deliveryRecorder` (Task 4), `c.inst.retries` (Task 3).
- Produces: `func (r *deliveryRecorder) retry(attempt int, err error)`.

- [ ] **Step 1: Write the failing test**

Add to `consumer_test.go`:

```go
func TestRunWithRetry_RecordsRetriesAsCounterAndSpanEvents(t *testing.T) {
	mp, reader := newTestMeterProvider(t)
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))

	c := newTestConsumerWithTelemetry(t, mp)
	c.tracer = tp.Tracer(telemetryScope)
	c.dlq = &recordingSink{}
	// Keep the backoff out of the test's runtime.
	c.cfg.Retry.InitialBackoff = time.Nanosecond
	c.cfg.Retry.MaxBackoff = time.Nanosecond

	var calls int
	if err := c.registry.addTyped("documents", "document.created", 1,
		func(context.Context, Envelope[json.RawMessage]) error {
			calls++
			if calls < 3 {
				return errors.New("transient")
			}
			return nil
		}); err != nil {
		t.Fatalf("addTyped() = %v, want nil", err)
	}

	env := Envelope[json.RawMessage]{
		EventID: "018f2e7a-1c6b-7c0a-9f8d-3e4a2b1c5d90", EventType: "document.created",
		Timestamp: time.Now().UTC(), SchemaVersion: "1.0.0", CorrelationID: "c", Source: "s",
		Payload: json.RawMessage(`{}`),
	}
	body, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshalling envelope: %v", err)
	}

	if err := c.dispatch(context.Background(), "documents", body, nil); err != nil {
		t.Fatalf("dispatch() = %v, want nil", err)
	}

	m, ok := readMetrics(t, reader)["messaging.retries"]
	if !ok {
		t.Fatal("messaging.retries was not recorded")
	}
	sum, ok := m.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("retries data = %T, want Sum[int64]", m.Data)
	}
	if got := sum.DataPoints[0].Value; got != 2 {
		t.Errorf("retries = %d, want 2", got)
	}

	spans := recorder.Ended()
	if len(spans) != 1 {
		t.Fatalf("recorded %d spans, want 1 span per delivery regardless of retries", len(spans))
	}
	var retryEvents int
	for _, e := range spans[0].Events() {
		if e.Name == "retry" {
			retryEvents++
		}
	}
	if retryEvents != 2 {
		t.Errorf("retry span events = %d, want 2", retryEvents)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -run TestRunWithRetry_RecordsRetries ./...`
Expected: FAIL — `messaging.retries was not recorded`

- [ ] **Step 3: Add `retry` to the recorder**

In `telemetry.go`:

```go
// retry records one immediate retry: a counter increment and a span event.
//
// A span event rather than a child span, deliberately. One span per
// delivery keeps trace volume proportional to messages; a span per attempt
// would multiply spans by up to 1+MaxImmediateRetries on exactly the
// failure path where volume is already spiking, to answer a question the
// event's timestamp already answers.
func (r *deliveryRecorder) retry(attempt int, err error) {
	r.inst.retries.Add(context.Background(), 1, metric.WithAttributes(
		consumeAttrs(r.topic, r.group, r.eventType)...))
	r.span.AddEvent("retry", trace.WithAttributes(
		attribute.Int("messaging.foi.immediate_attempt", attempt),
		attribute.String("error", err.Error()),
		attribute.Bool("error.permanent", IsPermanent(err)),
	))
}
```

- [ ] **Step 4: Call it from `runWithRetry`**

In the retry branch, just before the backoff sleep:

```go
		log.Debug("messaging: retrying handler", "immediate_attempt", i+1, "error", err)
		rec.retry(i+1, err)
		if !sleepWithJitter(ctx, backoffUpperBound(c.cfg.Retry, i)) {
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test -run TestRunWithRetry ./... -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add consumer.go telemetry.go consumer_test.go
git commit -m "feat: count immediate retries and record them as span events

A span event rather than a child span per attempt: one span per delivery
keeps trace volume proportional to messages instead of multiplying it on
the failure path."
```

---

### Task 8: DLQ and undecodable-entry counters

Spec §2 (`dlq`, `dlq.publish.failures`, and why `received` fires from the hook).

**Files:**
- Modify: `consumer.go` (`deadLetter`, and the `OnUndecodable` hook inside `Run`)
- Test: `consumer_test.go`

**Interfaces:**
- Consumes: `c.inst` (Task 3).
- Produces: nothing new; later tasks do not depend on this one.

- [ ] **Step 1: Write the failing test**

Add to `consumer_test.go`:

```go
func TestDeadLetter_RecordsCounters(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		mp, reader := newTestMeterProvider(t)
		c := newTestConsumerWithTelemetry(t, mp)
		c.dlq = &recordingSink{}

		dl := c.newDeadLetter("documents", ReasonPermanent, errors.New("bad"), 1)
		if err := c.deadLetter(context.Background(), "documents", dl); err != nil {
			t.Fatalf("deadLetter() = %v, want nil", err)
		}

		got := readMetrics(t, reader)
		if _, ok := got["messaging.dlq"]; !ok {
			t.Error("messaging.dlq was not recorded")
		}
		if _, ok := got["messaging.dlq.publish.failures"]; ok {
			t.Error("messaging.dlq.publish.failures was recorded for a successful write")
		}
	})

	t.Run("publish failure", func(t *testing.T) {
		mp, reader := newTestMeterProvider(t)
		c := newTestConsumerWithTelemetry(t, mp)
		c.dlq = &recordingSink{err: errors.New("redis down")}

		dl := c.newDeadLetter("documents", ReasonPermanent, errors.New("bad"), 1)
		if err := c.deadLetter(context.Background(), "documents", dl); err == nil {
			t.Fatal("deadLetter() = nil, want an error so the entry is nacked")
		}

		got := readMetrics(t, reader)
		if _, ok := got["messaging.dlq.publish.failures"]; !ok {
			t.Error("messaging.dlq.publish.failures was not recorded")
		}
		if _, ok := got["messaging.dlq"]; ok {
			t.Error("messaging.dlq was recorded for a write that failed")
		}
	})
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -run TestDeadLetter_RecordsCounters ./...`
Expected: FAIL — `messaging.dlq was not recorded`

- [ ] **Step 3: Instrument `deadLetter`**

In `consumer.go`'s `deadLetter`, add the counter to each branch:

```go
	attrs := consumeAttrs(topic, c.cfg.Consumer.Group, "", attribute.String(attrReason, dl.Reason))

	if err := sink.publish(ctx, stream, body); err != nil {
		c.inst.dlqPublishFailures.Add(ctx, 1, metric.WithAttributes(attrs...))
		c.cfg.Telemetry.Logger.Error("messaging: dead letter publish failed",
			"topic", topic, "dlq_stream", stream, "reason", dl.Reason,
			"delivery_attempts", dl.DeliveryAttempts, "error", err)
		return fmt.Errorf("publishing dead letter to %q: %w", stream, err)
	}

	c.inst.dlq.Add(ctx, 1, metric.WithAttributes(attrs...))
```

`event_type` is deliberately empty here: the DLQ paths include deserialization failures where no bounded event type exists, and a metric whose attribute set changes shape by reason is harder to query than one that never carries it.

- [ ] **Step 4: Extract the undecodable hook into a method**

The hook is currently an inline closure inside `Run` (`consumer.go:202-236`), so it is reachable only with a live Redis and a deliberately corrupted stream entry. Extracting it makes the counter testable and shortens `Run`, which is already long.

In `consumer.go`, add a method carrying the closure's body verbatim:

```go
// handleUndecodable records and dead-letters a stream entry that Watermill's
// marshaller could not read at all.
//
// It is a method rather than the inline closure it began as so its metrics
// and its DLQ routing are reachable from a unit test: provoking a
// marshaller-level failure against a live broker means writing a
// deliberately corrupt entry, which is a lot of setup for a path that is
// pure error handling.
//
// runCtx is Run's context, used only as the parent to detach from.
func (c *Consumer) handleUndecodable(runCtx context.Context, topicByStream map[string]string, stream, entryID string, fields map[string]any) error {
	topic, ok := topicByStream[stream]
	if !ok {
		return fmt.Errorf("no topic registered for stream %q", stream)
	}

	// Counted here and nowhere else: an entry the marshaller cannot read
	// never reaches dispatch, so without this messaging_dlq_total would
	// exceed messaging_events_received_total on this path — an invariant
	// violation that reads as a metrics bug and hides the real one
	// underneath it.
	c.inst.received.Add(runCtx, 1, metric.WithAttributes(attribute.String(attrTopic, topic)))

	// The original bytes are unreachable — the marshaller failed before
	// producing a payload — so the raw Redis fields are what gets
	// preserved. PRD §14 did not anticipate a marshaller-level failure;
	// this is the nearest thing to "the raw bytes" that exists here.
	raw, err := json.Marshal(fields)
	if err != nil {
		return fmt.Errorf("marshalling fields of undecodable entry %q: %w", entryID, err)
	}

	dl := c.newDeadLetter(topic, ReasonDeserializationFailed,
		fmt.Errorf("stream entry %q could not be unmarshalled", entryID), 1)
	dl.EventRaw = raw

	// Detached from Run's ctx, and timeout-bounded, for the same reason
	// the subscriber's ack path is: an entry reaching this hook during the
	// drain must still be recorded, and Run's ctx is already cancelled by
	// then. Without this the DLQ write fails with context.Canceled at
	// exactly the moment there is a backlog to clear.
	dlqCtx, cancelDLQ := context.WithTimeout(context.WithoutCancel(runCtx), dlqPublishTimeout)
	defer cancelDLQ()
	return c.deadLetter(dlqCtx, topic, dl)
}
```

Then reduce the `OnUndecodable` option in `Run` to:

```go
		OnUndecodable: func(stream, entryID string, fields map[string]any) error {
			return c.handleUndecodable(ctx, topicByStream, stream, entryID, fields)
		},
```

The `dlq` counter needs no addition: this path already routes through `c.deadLetter`, which Step 3 instrumented.

- [ ] **Step 5: Test the extracted hook**

Add to `consumer_test.go`:

```go
func TestHandleUndecodable_CountsReceivedAndDeadLetters(t *testing.T) {
	// The invariant this protects: an entry the marshaller cannot read
	// never reaches dispatch, so if it were not counted here,
	// messaging_dlq_total would exceed messaging_events_received_total.
	mp, reader := newTestMeterProvider(t)
	c := newTestConsumerWithTelemetry(t, mp)
	sink := &recordingSink{}
	c.dlq = sink

	topicByStream := map[string]string{"foi:documents": "documents"}
	fields := map[string]any{"garbage": "value"}

	if err := c.handleUndecodable(context.Background(), topicByStream, "foi:documents", "1-0", fields); err != nil {
		t.Fatalf("handleUndecodable() = %v, want nil", err)
	}

	got := readMetrics(t, reader)
	if _, ok := got["messaging.events.received"]; !ok {
		t.Error("messaging.events.received was not recorded for an undecodable entry")
	}
	if _, ok := got["messaging.dlq"]; !ok {
		t.Error("messaging.dlq was not recorded for an undecodable entry")
	}

	dl := sink.only(t)
	if dl.Reason != ReasonDeserializationFailed {
		t.Errorf("dead letter reason = %q, want %q", dl.Reason, ReasonDeserializationFailed)
	}
}

func TestHandleUndecodable_UnknownStreamIsAnError(t *testing.T) {
	mp, _ := newTestMeterProvider(t)
	c := newTestConsumerWithTelemetry(t, mp)
	c.dlq = &recordingSink{}

	err := c.handleUndecodable(context.Background(), map[string]string{}, "foi:unknown", "1-0", nil)
	if err == nil {
		t.Fatal("handleUndecodable() = nil, want an error so the entry stays pending")
	}
}
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `go test -run "TestDeadLetter|TestDispatch|TestHandleUndecodable" ./... -v`
Expected: PASS.

- [ ] **Step 7: Run the integration suite for the undecodable path**

Run: `go test -tags=integration -race -count=1 -run TestConsumer ./... -v`
Expected: PASS, including Phase 2b's undecodable-entry test.

- [ ] **Step 8: Commit**

```bash
git add consumer.go consumer_test.go
git commit -m "feat: count dead letters, DLQ write failures, and undecodable entries

received is counted from the OnUndecodable hook as well as from dispatch:
an entry the marshaller cannot read never reaches dispatch, so counting
only there would let dlq_total exceed received_total on that path."
```

---

### Task 9: Queue latency from `published_at`

Spec §2, §4.

**Files:**
- Modify: `consumer.go` (`dispatch`)
- Test: `consumer_test.go`

**Interfaces:**
- Consumes: `parsePublishedAt` (Task 5), `c.inst.queueLatency` (Task 3).
- Produces: nothing new.

- [ ] **Step 1: Write the failing test**

Add to `consumer_test.go`:

```go
func TestDispatch_QueueLatency(t *testing.T) {
	valid := func(t *testing.T) []byte {
		t.Helper()
		env := Envelope[json.RawMessage]{
			EventID: "018f2e7a-1c6b-7c0a-9f8d-3e4a2b1c5d90", EventType: "document.created",
			Timestamp: time.Now().UTC(), SchemaVersion: "1.0.0", CorrelationID: "c", Source: "s",
			Payload: json.RawMessage(`{}`),
		}
		b, err := json.Marshal(env)
		if err != nil {
			t.Fatalf("marshalling envelope: %v", err)
		}
		return b
	}

	tests := []struct {
		name       string
		publishedAt string
		wantRecord bool
	}{
		{"recorded", time.Now().UTC().Add(-2 * time.Second).Format(time.RFC3339Nano), true},
		{"missing", "", false},
		{"unparseable", "not-a-timestamp", false},
		// Clock skew: a publisher whose clock runs ahead yields a
		// negative elapsed time. Clamped to zero rather than skipped —
		// the delivery did happen, and a negative bucket is nonsense.
		{"skewed future", time.Now().UTC().Add(time.Hour).Format(time.RFC3339Nano), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mp, reader := newTestMeterProvider(t)
			c := newTestConsumerWithTelemetry(t, mp)
			c.dlq = &recordingSink{}
			if err := c.registry.addTyped("documents", "document.created", 1,
				func(context.Context, Envelope[json.RawMessage]) error { return nil }); err != nil {
				t.Fatalf("addTyped() = %v, want nil", err)
			}

			metadata := map[string]string{}
			if tt.publishedAt != "" {
				metadata[metadataPublishedAt] = tt.publishedAt
			}

			if err := c.dispatch(context.Background(), "documents", valid(t), metadata); err != nil {
				t.Fatalf("dispatch() = %v, want nil", err)
			}

			m, ok := readMetrics(t, reader)["messaging.queue.latency"]
			if ok != tt.wantRecord {
				t.Fatalf("queue.latency recorded = %v, want %v", ok, tt.wantRecord)
			}
			if !tt.wantRecord {
				return
			}

			hist, ok := m.Data.(metricdata.Histogram[float64])
			if !ok {
				t.Fatalf("queue.latency data = %T, want Histogram[float64]", m.Data)
			}
			if got := hist.DataPoints[0].Sum; got < 0 {
				t.Errorf("queue.latency sum = %v, want >= 0 (negatives clamped)", got)
			}
		})
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -run TestDispatch_QueueLatency ./...`
Expected: FAIL — `queue.latency recorded = false, want true`

- [ ] **Step 3: Record it in `dispatch`**

In `dispatch`, immediately after the `received` counter:

```go
	// Publish-to-dispatch latency, which is the only way to tell "our
	// handlers are slow" from "we are behind on the stream". A missing or
	// unparseable published_at skips the observation and nothing else —
	// odd transport metadata is not a reason to fail a message.
	if publishedAt, ok := parsePublishedAt(metadata); ok {
		// published_at is stamped by the publishing host and read by
		// this one, so this measures elapsed time plus clock skew. A
		// publisher running ahead yields a negative value, which is
		// clamped rather than recorded: the delivery did happen.
		latency := time.Since(publishedAt).Seconds()
		if latency < 0 {
			latency = 0
		}
		c.inst.queueLatency.Record(ctx, latency, metric.WithAttributes(
			attribute.String(attrTopic, topic)))
	}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test -run TestDispatch_QueueLatency ./... -v`
Expected: PASS, all four subtests.

- [ ] **Step 5: Commit**

```bash
git add consumer.go consumer_test.go
git commit -m "feat: record publish-to-dispatch queue latency

Separates 'our handlers are slow' from 'we are behind on the stream'.
Clock skew is clamped rather than recorded negative, and a missing or
unparseable published_at skips the observation rather than failing the
delivery."
```

---

### Task 10: `LogPayloads` and log/trace correlation

Spec §3 (log correlation), §4 (`LogPayloads`).

**Files:**
- Modify: `consumer.go` (`dispatch`)
- Test: `consumer_test.go`

**Interfaces:**
- Consumes: the span from Task 6.
- Produces: nothing new.

- [ ] **Step 1: Write the failing test**

Add to `consumer_test.go`. If the file already has a log-capturing helper from earlier phases, reuse it; otherwise:

```go
// captureLogs returns a logger writing JSON into buf, so tests can assert
// on emitted attributes.
func captureLogs(buf *bytes.Buffer) *slog.Logger {
	return slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func TestDispatch_LogPayloadsAndTraceCorrelation(t *testing.T) {
	tests := []struct {
		name        string
		logPayloads bool
		wantPayload bool
	}{
		{"payloads off", false, false},
		{"payloads on", true, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			mp, _ := newTestMeterProvider(t)
			tp := sdktrace.NewTracerProvider()

			c := newTestConsumerWithTelemetry(t, mp)
			c.cfg.Telemetry.Logger = captureLogs(&buf)
			c.cfg.Telemetry.LogPayloads = tt.logPayloads
			c.tracer = tp.Tracer(telemetryScope)
			c.dlq = &recordingSink{}

			// An undecodable payload takes an error path, which is
			// where payloads are logged if at all.
			_ = c.dispatch(context.Background(), "documents", []byte(`{"secret":"hunter2"`), nil)

			out := buf.String()
			if got := strings.Contains(out, "hunter2"); got != tt.wantPayload {
				t.Errorf("log contains payload = %v, want %v\nlog: %s", got, tt.wantPayload, out)
			}
			if !strings.Contains(out, "trace_id") {
				t.Errorf("log is missing trace_id\nlog: %s", out)
			}
			if !strings.Contains(out, "span_id") {
				t.Errorf("log is missing span_id\nlog: %s", out)
			}
		})
	}
}
```

Add `"bytes"` and `"strings"` to `consumer_test.go`'s imports.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -run TestDispatch_LogPayloadsAndTraceCorrelation ./...`
Expected: FAIL — log is missing `trace_id`.

- [ ] **Step 3: Add correlation and payload gating**

In `dispatch`, replace the `log := ...` assignment with:

```go
	sc := span.SpanContext()
	log := c.cfg.Telemetry.Logger.With(
		"stream_id", metadata[internalwatermill.MetadataStreamID],
		"delivery_attempt", metadata[internalwatermill.MetadataDeliveryAttempt],
		// What makes a log line and a trace navigable from each other.
		// PRD §16's field list does not name these; reconciling the rest
		// of that list is out of scope for Phase 3.
		"trace_id", sc.TraceID().String(),
		"span_id", sc.SpanID().String(),
	)

	// PRD §16: payload contents are not logged by default. When enabled
	// they appear on error paths only — never in span attributes or metric
	// attributes, whatever this is set to, because spans routinely leave
	// the trust boundary logs stay inside.
	if c.cfg.Telemetry.LogPayloads {
		log = log.With("payload", string(payload))
	}
```

`runWithRetry` currently builds its own logger from `c.cfg.Telemetry.Logger` (`consumer.go:553`), which would miss both the trace fields and the payload. Pass the decorated logger down instead. Its full signature after this task — Task 6 already added `rec` — is:

```go
func (c *Consumer) runWithRetry(
	ctx context.Context,
	topic string,
	payload []byte,
	attempt int64,
	handler dispatchFunc,
	env Envelope[json.RawMessage],
	rec *deliveryRecorder,
	log *slog.Logger,
) error {
```

and its first statement becomes an extension of the passed logger rather than a fresh one:

```go
	log = log.With(
		"topic", topic, "event_type", env.EventType,
		"schema_version", env.SchemaVersion, "event_id", env.EventID,
		"delivery_attempt", attempt,
	)
```

Update the call site at the end of `dispatch` to `return c.runWithRetry(ctx, topic, payload, attempt, handler, env, rec, log)`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test -run TestDispatch_LogPayloads ./... -v`
Expected: PASS, both subtests.

- [ ] **Step 5: Run the whole unit suite**

Run: `go test -race ./...`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add consumer.go consumer_test.go
git commit -m "feat: honour LogPayloads and add trace_id/span_id to consume logs

LogPayloads was defaulted but read nowhere. Payloads now appear on error
paths when enabled, and never in span or metric attributes regardless."
```

---

### Task 11: End-to-end propagation (integration)

The one assertion that proves the whole chain — injection, the Redis round trip through the marshaller, and extraction — rather than either half. Spec §5.

**Files:**
- Test: `consumer_integration_test.go`

**Interfaces:**
- Consumes: everything from Tasks 1, 5, 6.
- Produces: nothing.

- [ ] **Step 1: Write the failing test**

Add to `consumer_integration_test.go` (external `package messaging_test`, `//go:build integration`):

```go
func TestConsumer_PropagatesTraceContextFromPublisher(t *testing.T) {
	// consumeFixture (consumer_integration_test.go:29) starts Redis,
	// registers its termination with t.Cleanup, and returns a config
	// pointed at it.
	cfg := consumeFixture(t)

	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	cfg.Telemetry.TracerProvider = tp

	consumer, err := messaging.NewConsumer(cfg)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}

	handler := newCollectingHandler(0)
	if err := messaging.RegisterHandler(consumer, documentCreated, handler); err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runErr := make(chan error, 1)
	go func() { runErr <- consumer.Run(ctx) }()

	publisher, err := messaging.NewPublisher(cfg)
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	defer publisher.Close()

	// Published inside a span of our own so the producer span has a
	// known, non-root trace to compare against.
	pubCtx, root := tp.Tracer("test").Start(context.Background(), "root")
	if _, err := publisher.Publish(pubCtx, documentCreated,
		documentCreatedPayload{DocumentID: "doc-1"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	root.End()

	select {
	case <-handler.notify:
	case <-time.After(30 * time.Second):
		t.Fatal("handler was not invoked within 30s")
	}

	cancel()
	if err := <-runErr; err != nil {
		t.Fatalf("Run: %v", err)
	}

	var producer, consumerSpan sdktrace.ReadOnlySpan
	for _, s := range recorder.Ended() {
		switch s.Name() {
		case "publish documents":
			producer = s
		case "process documents":
			consumerSpan = s
		}
	}
	if producer == nil {
		t.Fatal("no producer span was recorded")
	}
	if consumerSpan == nil {
		t.Fatal("no consumer span was recorded")
	}

	if got, want := consumerSpan.Parent().TraceID(), producer.SpanContext().TraceID(); got != want {
		t.Errorf("consumer span trace id = %s, want the producer's %s", got, want)
	}
	if got, want := consumerSpan.Parent().SpanID(), producer.SpanContext().SpanID(); got != want {
		t.Errorf("consumer span parent = %s, want the producer span %s", got, want)
	}
	if !consumerSpan.Parent().IsRemote() {
		t.Error("consumer span parent is not marked remote; trace context did not travel through Redis")
	}
}
```

This reuses three things the file already defines: `consumeFixture(t)` for Redis setup, the `documentCreated` `EventDef`, and `newCollectingHandler(0)` (whose `notify` channel signals delivery). Check `documentCreatedPayload`'s actual field names before writing the publish call. Add imports: `sdktrace "go.opentelemetry.io/otel/sdk/trace"` and `"go.opentelemetry.io/otel/sdk/trace/tracetest"`.

- [ ] **Step 2: Run test to verify it fails or passes**

Run: `go test -tags=integration -race -count=1 -run TestConsumer_PropagatesTraceContext ./... -v`
Expected: PASS if Tasks 1, 5, and 6 are correct. If it fails on `IsRemote()`, the propagator is not being applied on one of the two sides; if it fails on trace id, the metadata is not surviving the marshaller round trip.

This is a verification task rather than a red-green one: the behaviour was built in earlier tasks, and this test is what proves the pieces meet.

- [ ] **Step 3: Commit**

```bash
git add consumer_integration_test.go
git commit -m "test: prove trace context survives the Redis round trip

Publishes inside a known root span and asserts the consumer span's parent
is the producer span and is marked remote — the one assertion covering
injection, the marshaller round trip, and extraction together."
```

---

### Task 12: `examples/telemetry` and deleting the `telemetry/` package

Spec §6. The example is compilable and tested because it carries the bucket View, without which both histograms are useless.

**Files:**
- Create: `examples/telemetry/go.mod`, `examples/telemetry/main.go`, `examples/telemetry/main_test.go`
- Delete: `telemetry/doc.go`
- The root `go.mod` is **not** modified.

**Interfaces:**
- Consumes: the instrument names from Task 3.
- Produces: nothing importable; this is documentation that compiles.

- [ ] **Step 1: Make the example its own module**

The example needs `go.opentelemetry.io/otel/exporters/prometheus`, which pulls in `github.com/prometheus/client_golang` and its transitive tree. Adding that to the root `go.mod` would put a Prometheus dependency in the graph of every service importing this library — for a library whose entire premise is a narrow, enforced dependency boundary, and immediately after choosing the OTel-API-only approach specifically to avoid a `client_golang` dependency.

A nested module keeps it out entirely:

```bash
mkdir -p examples/telemetry
cd examples/telemetry
go mod init github.com/bcgov/foi-messaging-go/examples/telemetry
go mod edit -replace github.com/bcgov/foi-messaging-go=../..
go get github.com/bcgov/foi-messaging-go
go get go.opentelemetry.io/otel/exporters/prometheus@v0.66.0
cd ../..
```

The `replace` points at the parent so the example always compiles against the working tree rather than a published version.

The trade-off: `go build ./...` and `go test ./...` from the root do **not** descend into a nested module, so this example needs its own invocation. Add it to the `Makefile` so it is not silently skipped:

```makefile
test-examples:
	cd examples/telemetry && go test ./...
```

and reference `test-examples` from whatever aggregate target the Makefile already uses, so a rename that breaks the naming test is caught.

- [ ] **Step 2: Write the failing test**

Create `examples/telemetry/main_test.go`:

```go
package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// TestPrometheusNames pins the exported metric names. They are the
// library's contract with every dashboard and alert built on it, so an
// instrument rename must break a test rather than a Grafana panel.
func TestPrometheusNames(t *testing.T) {
	reg := prometheus.NewRegistry()
	mp, err := NewMeterProvider(reg)
	if err != nil {
		t.Fatalf("NewMeterProvider() = %v, want nil", err)
	}

	if err := recordOneOfEach(context.Background(), mp); err != nil {
		t.Fatalf("recordOneOfEach() = %v, want nil", err)
	}

	srv := httptest.NewServer(promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("GET %s = %v, want nil", srv.URL, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading body = %v, want nil", err)
	}
	out := string(body)

	want := []string{
		"messaging_events_published_total",
		"messaging_publish_failures_total",
		"messaging_events_received_total",
		"messaging_events_processed_total",
		"messaging_events_failed_total",
		"messaging_events_skipped_total",
		"messaging_retries_total",
		"messaging_dlq_total",
		"messaging_dlq_publish_failures_total",
		"messaging_processing_duration_seconds",
		"messaging_queue_latency_seconds",
	}
	for _, name := range want {
		if !strings.Contains(out, name) {
			t.Errorf("exporter output is missing %q", name)
		}
	}
}

// TestSecondScaleBuckets guards the View. Without it the exporter's
// default boundaries are millisecond-scaled (0, 5, 10, ... 10000) and
// every realistic duration in seconds lands in the first bucket, making
// both histograms render as flat lines.
func TestSecondScaleBuckets(t *testing.T) {
	reg := prometheus.NewRegistry()
	mp, err := NewMeterProvider(reg)
	if err != nil {
		t.Fatalf("NewMeterProvider() = %v, want nil", err)
	}
	if err := recordOneOfEach(context.Background(), mp); err != nil {
		t.Fatalf("recordOneOfEach() = %v, want nil", err)
	}

	srv := httptest.NewServer(promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("GET %s = %v, want nil", srv.URL, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading body = %v, want nil", err)
	}
	out := string(body)

	for _, histogram := range []string{"messaging_processing_duration_seconds", "messaging_queue_latency_seconds"} {
		if !strings.Contains(out, histogram+`_bucket{`) {
			t.Fatalf("%s has no buckets", histogram)
		}
		// A second-scale boundary that the default millisecond
		// boundaries do not contain.
		if !strings.Contains(out, `le="0.005"`) {
			t.Errorf("%s is using default millisecond buckets; the View was not applied", histogram)
		}
	}
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cd examples/telemetry && go test ./... ; cd ../..`
Expected: FAIL — `undefined: NewMeterProvider`

- [ ] **Step 4: Write the example**

Create `examples/telemetry/main.go`:

```go
// Command telemetry shows how to expose foi-messaging-go's metrics to
// Prometheus.
//
// The library records through the OpenTelemetry metric API only; it has no
// Prometheus dependency and exposes no HTTP handler. Applications choose
// their own pipeline, and this is the recipe for the Prometheus one.
//
// The View below is not optional. Without it the exporter's default
// histogram boundaries apply, and those are millisecond-scaled
// (0, 5, 10, ... 10000). Both of the library's histograms are in seconds,
// so every realistic observation lands in the first bucket and the
// histograms render as flat lines. This is the single most likely way to
// finish integrating and still be unable to see your own latency.
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/attribute"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// scopeName must match the library's instrumentation scope.
const scopeName = "github.com/bcgov/foi-messaging-go"

// NewMeterProvider builds a MeterProvider that exports to reg.
//
// Pass the result as messaging.Config.Telemetry.MeterProvider.
func NewMeterProvider(reg prometheus.Registerer) (*sdkmetric.MeterProvider, error) {
	exporter, err := otelprom.New(otelprom.WithRegisterer(reg))
	if err != nil {
		return nil, fmt.Errorf("creating prometheus exporter: %w", err)
	}

	secondsBuckets := sdkmetric.Stream{
		Aggregation: sdkmetric.AggregationExplicitBucketHistogram{
			Boundaries: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		},
	}

	// Both histograms are named explicitly. A single wildcard is a trap:
	// the two are not consistently suffixed
	// (messaging.processing.duration versus messaging.queue.latency), and
	// a broader selector such as "messaging.*" would sweep in the nine
	// counters, for which this aggregation is invalid.
	return sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(exporter),
		sdkmetric.WithView(
			sdkmetric.NewView(sdkmetric.Instrument{Name: "messaging.processing.duration"}, secondsBuckets),
			sdkmetric.NewView(sdkmetric.Instrument{Name: "messaging.queue.latency"}, secondsBuckets),
		),
	), nil
}

// recordOneOfEach touches every instrument the library defines, so the
// exporter has a series for each. It exists for the tests in this package,
// which pin the exported Prometheus names; a real application records
// nothing itself and simply hands the provider to messaging.Config.
func recordOneOfEach(ctx context.Context, mp *sdkmetric.MeterProvider) error {
	m := mp.Meter(scopeName)
	attrs := metric.WithAttributes(attribute.String("topic", "documents"))

	counters := []struct {
		name string
		unit string
	}{
		{"messaging.events.published", "{event}"},
		{"messaging.publish.failures", "{event}"},
		{"messaging.events.received", "{event}"},
		{"messaging.events.processed", "{event}"},
		{"messaging.events.failed", "{event}"},
		{"messaging.events.skipped", "{event}"},
		{"messaging.retries", "{retry}"},
		{"messaging.dlq", "{event}"},
		{"messaging.dlq.publish.failures", "{event}"},
	}
	for _, c := range counters {
		counter, err := m.Int64Counter(c.name, metric.WithUnit(c.unit))
		if err != nil {
			return fmt.Errorf("creating %s: %w", c.name, err)
		}
		counter.Add(ctx, 1, attrs)
	}

	for _, name := range []string{"messaging.processing.duration", "messaging.queue.latency"} {
		h, err := m.Float64Histogram(name, metric.WithUnit("s"))
		if err != nil {
			return fmt.Errorf("creating %s: %w", name, err)
		}
		h.Record(ctx, 0.42, attrs)
	}

	return nil
}

func main() {
	reg := prometheus.NewRegistry()

	mp, err := NewMeterProvider(reg)
	if err != nil {
		log.Fatalf("building meter provider: %v", err)
	}
	defer func() {
		if err := mp.Shutdown(context.Background()); err != nil {
			log.Printf("shutting down meter provider: %v", err)
		}
	}()

	// cfg := messaging.Config{
	//     Source: "billing.service",
	//     Redis:  messaging.RedisConfig{Address: "redis:6379"},
	//     Telemetry: messaging.TelemetryConfig{MeterProvider: mp},
	// }

	http.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	log.Println("serving metrics on :2112/metrics")
	if err := http.ListenAndServe(":2112", nil); err != nil {
		log.Fatalf("serving metrics: %v", err)
	}
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd examples/telemetry && go test ./... -v ; cd ../..`
Expected: PASS, both tests.

- [ ] **Step 6: Delete the `telemetry/` package**

```bash
git rm telemetry/doc.go
```

Confirm nothing imports it:

```bash
grep -rn "foi-messaging-go/telemetry" --include=*.go .
```

Expected: no matches.

- [ ] **Step 7: Verify the build and lint**

Run: `make build && make lint`
Expected: clean.

- [ ] **Step 8: Commit**

```bash
git add examples/telemetry Makefile
git rm -r --cached telemetry 2>/dev/null || true
git commit -m "docs: add a compilable Prometheus wiring example

Replaces the code-free telemetry/ package. Compilable rather than prose
because it carries the histogram View: the exporter's default buckets are
millisecond-scaled, and without the View both of the library's
second-scale histograms render as flat lines."
```

---

### Task 13: Documentation

Spec §8. The repository convention is that past review rounds repeatedly caught the docs claiming unimplemented behaviour; this task is what keeps that from recurring.

**Files:**
- Modify: `README.md:23,25,180,184`, `doc.go`, `CLAUDE.md`, `docs/foi-messaging-go-prd-v1.1.md` (§16)

**Interfaces:**
- Consumes: everything.
- Produces: nothing.

- [ ] **Step 1: Update the README**

- Line 23: drop `*(Planned: Phase 3)*` from the OpenTelemetry/Prometheus bullet.
- Line 180: replace the "Planned for Phase 3" blockquote with a real section containing:
  - the eleven-metric table from spec §2
  - **the per-delivery caveat**, stated prominently: one event increments `received` once per delivery, up to `MaxDeliveryAttempts + 1` times, so `received` exceeding `published` is redelivery working as designed
  - **the bucket View requirement**, with a pointer to `examples/telemetry`, stated as required rather than suggested
  - the `queue_latency` clock-skew caveat: it measures elapsed time plus skew between publishing and consuming hosts
  - a note that `otel_scope_*` labels and a `target_info` series are added by the exporter
  - a note that span volume scales with redelivery, so sampling is the application's decision
- Leave line 25 and 184 (Phase 4 / `testing/`) untouched.

- [ ] **Step 2: Update `doc.go`**

Describe Phase 3 as landed: spans on both paths, eleven metrics, trace context propagation via `Telemetry.Propagator`. Keep the Phase 4 marker.

- [ ] **Step 3: Update `CLAUDE.md`**

- "Implementation status": move Phase 3 into the done list; leave Phase 4.
- Remove the "`TelemetryConfig.TracerProvider`/`MeterProvider` are defaulted but inert; only `Logger` is live" sentence — now false.
- Remove `telemetry/` from the "currently hold only `doc.go`" sentence, leaving `testing/`.
- Add to the Architecture section: OTel is a root-package dependency by design and is deliberately **not** covered by the `depguard` boundary, because `TelemetryConfig` is public API. A future contributor will otherwise try to hide it behind `internal/`.
- Add a line to the consume-path description: `dispatch` states a `deliveryOutcome` on each exit path and one deferred recorder emits the span end, the duration histogram, and exactly one terminal counter.

- [ ] **Step 4: Amend PRD §16**

Add `messaging_publish_failures_total` and `messaging_queue_latency_seconds` to the recommended metrics list, each marked as an addition found during Phase 3 design, following the established practice of carrying findings back into the spec of record. Note against the metric list that all counters are per-delivery.

- [ ] **Step 5: Verify every doc claim**

Run: `go test -tags=integration -race -count=1 ./...`
Expected: PASS. Then re-read each edited doc paragraph against the code it describes. A claim you cannot point at a test for does not go in.

- [ ] **Step 6: Commit**

```bash
git add README.md doc.go CLAUDE.md docs/foi-messaging-go-prd-v1.1.md
git commit -m "docs: mark Phase 3 landed

Metric table, the per-delivery counter caveat, the required histogram
View, and the queue-latency clock-skew caveat. PRD §16 gains the two
metrics Phase 3 design added."
```

---

### Task 14: Full verification

- [ ] **Step 1: Lint**

Run: `make lint`
Expected: clean. In particular `depguard` must still pass: the root package gained OTel imports, which are permitted, but no Watermill or go-redis import may have crept in with them.

- [ ] **Step 2: Unit tests with race detection**

Run: `go test -race -count=1 ./...`
Expected: PASS.

- [ ] **Step 3: Integration tests**

Run: `go test -tags=integration -race -count=1 ./...`
Expected: PASS. This is the only tier that exercises the consume path; `make test` alone proves very little.

- [ ] **Step 3b: The example module**

Run: `cd examples/telemetry && go test ./... ; cd ../..`
Expected: PASS. A nested module is invisible to `./...` from the root, so this is easy to forget — and it is the test pinning the exported Prometheus metric names.

- [ ] **Step 4: Confirm the no-op path costs nothing catastrophic**

Run: `go test -run TestNewInstruments_NilProviderFallsBackToNoop -race ./...`
Expected: PASS. A service that configures no telemetry must still publish and consume.

- [ ] **Step 5: Merge the phase branch**

Follow the repository convention: a merge commit for the phase branch. Use `superpowers:finishing-a-development-branch`.

---

## Notes for the implementer

**The refactor in Task 6 is the risky one.** Every existing behaviour in `dispatch` is preserved exactly — same check order, same return values, same log messages. The delivery-attempt cap must stay ahead of decoding (an over-cap event must not spend four handler invocations and its concurrency slot proving what its counter already said), and the three deserialization failures must keep dead-lettering rather than nacking. If a Phase 2b test fails after Task 6, the refactor changed behaviour; do not adjust the test to match.

**`end()` records through `context.Background()`.** That is deliberate: the delivery's own context may be cancelled at the drain deadline, and recording through a cancelled context would silently drop the observation for exactly the deliveries most worth counting.

**Do not add `outcome` as a histogram attribute.** It multiplies bucket series by four to answer a question the counters already answer.

**If you need a new metric attribute,** check it against spec §2's cardinality rule first. Anything read off the wire is unbounded until proven otherwise, and `matchTyped` is the only proof the consume path has.
