# Phase 4 — `messagingtest` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship the application-facing `testing/` (`messagingtest`) package so services can unit-test their publish paths and handlers — including error classification and dead-lettering — without Redis or Docker.

**Architecture:** `messagingtest` drives the library's *real* code rather than reimplementing it. A new `internal/testseam` package holds three function variables the root registers at `init`; `testing/` imports it. The fake publisher wraps a genuine `*messaging.Publisher` with only its `publishFn` transport write redirected, and `Dispatch` calls the genuine `Consumer.dispatch` with a `*testseam.Probe` on the context that collects the verdict `deliveryRecorder` already computes. The root package's exported API is unchanged.

**Tech Stack:** Go 1.25, standard-library `testing` only, `github.com/google/uuid` (already a direct dependency).

**Spec:** [`docs/superpowers/specs/2026-08-13-phase-4-messagingtest-design.md`](../specs/2026-08-13-phase-4-messagingtest-design.md)

## Global Constraints

- Go 1.25, Redis 7.0+.
- **Standard-library `testing` package only** — no testify anywhere in first-party test code, including test-only dependencies.
- `messagingtest` itself must **not import `testing`** in non-test files (§7 of the spec): it registers test flags into any binary that links it.
- The root `messaging` package must never import Watermill or go-redis. `depguard` (`.golangci.yml`) denies `github.com/ThreeDotsLabs/watermill`, `github.com/ThreeDotsLabs/watermill-redisstream`, and `github.com/redis/go-redis` outside `**/internal/**`. Our own `internal/watermill` package is *not* denied and may be imported from `testing/`.
- Integration tests carry `//go:build integration` and live in the external `_test` package.
- Comments explain *why*, especially where a subtle bug motivated the code. This codebase's comment density is well above typical Go; match it in the packages you touch.
- Commit style: `feat:` / `fix:` / `test:` / `docs:`. **No `Co-Authored-By` trailer.**
- Work happens on the existing `phase-4-messagingtest` branch.
- The root `messaging` package's exported API surface must be **unchanged** at the end of this phase. `internal/testseam` is invisible to applications.

## Four deliberate deviations from the spec's §5 signatures

Each is strictly simpler or more honest than what §5 sketched. Flagged here so a reviewer sees them as decisions rather than drift.

1. **`NewPublisher` returns `(*Publisher, error)`**, not `*Publisher`. The root's own `NewPublisher`/`NewConsumer` both return errors; consistency wins over saving a line.
2. **`Config()` takes no options.** It returns a `messaging.Config` — a plain struct the caller can mutate directly. An options API over it would be pure ceremony.
3. **Option names are disambiguated by boundary**: `WithPublisherSource` (publisher) vs `WithSource` (event), because both would otherwise collide in one package.
4. **`FailWith` fails at the transport stage** — the recorder returns the error — rather than short-circuiting `Publish`. Higher fidelity: envelope construction and validation still run, and the failure surfaces exactly as a Redis outage would.

## File Structure

| File | Responsibility |
| --- | --- |
| `internal/testseam/testseam.go` | **Create.** Probe type, context carrier, three hook variables. Imports nothing of ours. |
| `internal/testseam/testseam_test.go` | **Create.** Context round-trip. |
| `testhooks.go` (root) | **Create.** The `init` that registers the three hooks. |
| `telemetry.go` | **Modify.** `deliveryRecorder` gains a probe field; `end()` copies the verdict into it. |
| `consumer.go` | **Modify.** `dispatch` looks the probe up once; `deadLetter` uses `probe.Sink`; `runWithRetry` honours `NoBackoff`. |
| `testhooks_test.go` (root) | **Create.** Hooks registered; probe populated on each terminal path. |
| `testing/event.go` | **Create.** `Event`, `PayloadAs`, `NewEvent`, `EventOption`s, `Config`. |
| `testing/publisher.go` | **Create.** The recording `Publisher`. |
| `testing/deliver.go` | **Create.** `Deliver`. |
| `testing/dispatch.go` | **Create.** `Dispatch`, `Result`, `Outcome`, `DispatchOption`s. |
| `testing/doc.go` | **Modify.** Replace the Phase 0 stub. |
| `testing/*_test.go`, `testing/example_test.go` | **Create.** Package tests and runnable doc examples. |
| `messagingtest_integration_test.go` (root) | **Create.** The Redis cross-check. |

---

### Task 1: `internal/testseam` — the seam package

**Files:**
- Create: `internal/testseam/testseam.go`
- Test: `internal/testseam/testseam_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `testseam.Probe` struct; `testseam.NewContext(ctx, *Probe) context.Context`; `testseam.FromContext(ctx) *Probe`; kind constants `KindProcessed`/`KindSkipped`/`KindFailed`; hook variables `WithCorrelationID`, `NewRecordingPublisher`, `Dispatch`.

- [ ] **Step 1: Write the failing test**

Create `internal/testseam/testseam_test.go`:

```go
package testseam_test

import (
	"context"
	"testing"

	"github.com/bcgov/foi-messaging-go/internal/testseam"
)

func TestFromContext_RoundTrip(t *testing.T) {
	p := &testseam.Probe{Kind: testseam.KindProcessed}

	got := testseam.FromContext(testseam.NewContext(context.Background(), p))

	if got != p {
		t.Fatalf("got %p, want %p", got, p)
	}
}

// The production case: nothing outside this module's tests places a probe,
// so every delivery in a real service takes this path.
func TestFromContext_AbsentIsNil(t *testing.T) {
	if got := testseam.FromContext(context.Background()); got != nil {
		t.Fatalf("got %v, want nil", got)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/testseam/ -v`
Expected: FAIL — the package `internal/testseam` does not exist.

- [ ] **Step 3: Write minimal implementation**

Create `internal/testseam/testseam.go`:

```go
// Package testseam carries the hooks the root messaging package registers
// at init so this module's testing/ package can drive the real publish and
// consume paths without a Redis instance.
//
// It exists because three things testing/ needs are unexported in the root
// package, and no exported path reaches any of them: the correlation-ID
// context setter, Consumer.dispatch, and a dead-letter sink — Consumer.dlq
// is nil until Run, and deadLetter refuses to run without one.
//
// Moving dispatch into internal/ instead was not an option: it needs
// Envelope[T], IsPermanent, DeadLetter, and the registry, so an internal
// package holding it would import the root and cycle. That is the same
// constraint that already places OpenTelemetry outside the internal/
// boundary.
//
// This package imports nothing of ours, so it cannot participate in a
// cycle. It lives under internal/, so no application can see it, and the
// root package's exported API is unchanged by its existence.
package testseam

import "context"

// Delivery kinds, mirroring the root package's outcomeKind. Declared here
// so both sides of the seam compare against the same strings rather than
// against literals of their own.
const (
	KindProcessed = "processed"
	KindSkipped   = "skipped"
	KindFailed    = "failed"
)

// Probe collects what a single delivery did.
//
// It travels on the delivery's context rather than living on the Consumer.
// That way messagingtest.Dispatch needs no lock, is safe to call
// concurrently on one Consumer, leaves the application's Consumer
// unmutated once it returns, and cannot leak state between deliveries.
type Probe struct {
	// Kind, Category, Reason, and Err are copied from the root package's
	// deliveryRecorder at the end of the delivery — the same state that
	// produces the metrics, which is why a probe cannot report an outcome
	// production would not.
	Kind     string
	Category string
	Reason   string
	Err      error

	// NoBackoff collapses the immediate-retry loop's jittered sleeps to
	// nothing. The retry *count* is unaffected; only the waiting is.
	NoBackoff bool

	// Sink stands in for Consumer.dlq, which is nil until Run. It receives
	// the marshalled DeadLetter. Returning an error from it exercises the
	// DLQ-write-failure path, which nacks rather than acking the event
	// into nothing.
	Sink func(ctx context.Context, stream string, body []byte) error
}

type probeKey struct{}

// NewContext returns ctx carrying p.
func NewContext(ctx context.Context, p *Probe) context.Context {
	return context.WithValue(ctx, probeKey{}, p)
}

// FromContext returns the probe on ctx, or nil when there is none — which
// is every delivery in a real service.
func FromContext(ctx context.Context) *Probe {
	p, _ := ctx.Value(probeKey{}).(*Probe)
	return p
}

// The hooks. Each is a whole path rather than a fragment, so testing/
// reuses real behaviour instead of reimplementing it. All three are
// registered by the root package's init (testhooks.go); testing/ imports
// the root, so registration always precedes any use.
var (
	// WithCorrelationID is messaging.contextWithCorrelationID. It is what
	// makes a correlation ID chain from a consumed event into a follow-on
	// publish, because resolveCorrelationID reads that context value.
	WithCorrelationID func(ctx context.Context, id string) context.Context

	// NewRecordingPublisher returns a *messaging.Publisher whose transport
	// write is record instead of Redis. The concrete type is returned as
	// any because this package cannot name it; messagingtest, which
	// imports the root, asserts it back.
	NewRecordingPublisher func(source, streamPrefix string,
		record func(ctx context.Context, stream, id string, body []byte,
			metadata map[string]string) error) (any, error)

	// Dispatch runs Consumer.dispatch against consumer — which must be a
	// *messaging.Consumer — with p attached to the context.
	Dispatch func(ctx context.Context, consumer any, topic string,
		body []byte, metadata map[string]string, p *Probe) error
)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/testseam/ -v`
Expected: PASS — both tests.

- [ ] **Step 5: Commit**

```bash
git add internal/testseam/
git commit -m "feat: add internal/testseam, the seam testing/ drives the real paths through"
```

---

### Task 2: Probe wiring in the root package

Everything the seam needs from the root, plus the `init` that registers it. This is one task rather than four because none of the four pieces is independently testable — a probe that is populated but never reachable proves nothing.

**Files:**
- Create: `testhooks.go`
- Modify: `telemetry.go` (`deliveryRecorder` struct ~line 242, `newDeliveryRecorder` ~line 258, `end()` ~line 309)
- Modify: `consumer.go` (`dispatch` ~line 389, `deadLetter` ~line 634, `runWithRetry` ~line 762)
- Test: `testhooks_test.go`

**Interfaces:**
- Consumes: `testseam.Probe`, `testseam.NewContext`, `testseam.FromContext`, `testseam.KindProcessed`/`KindSkipped`/`KindFailed`, and the three hook variables from Task 1.
- Produces: all three hooks registered and non-nil. `testseam.Dispatch(ctx, consumer, topic, body, metadata, probe) error` returns dispatch's own error (nil means the runtime would ack). After it returns, `probe.Kind`/`Category`/`Reason`/`Err` describe the delivery and `probe.Sink` has received one marshalled `messaging.DeadLetter` per dead letter.

- [ ] **Step 1: Write the failing test**

Create `testhooks_test.go`. It is an internal test (`package messaging`) because it drives the unexported `dispatch` through the seam and needs `testConsumerConfig` from `consumer_test.go`:

```go
package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/bcgov/foi-messaging-go/internal/testseam"
	internalwatermill "github.com/bcgov/foi-messaging-go/internal/watermill"
)

func TestSeamHooksRegistered(t *testing.T) {
	// A nil hook is unreachable today: testing/ imports this package, so
	// this init always runs first. Asserted anyway so a refactor that
	// drops a registration fails loudly here, rather than nil-panicking
	// inside some application's test suite.
	if testseam.WithCorrelationID == nil {
		t.Error("testseam.WithCorrelationID is nil")
	}
	if testseam.NewRecordingPublisher == nil {
		t.Error("testseam.NewRecordingPublisher is nil")
	}
	if testseam.Dispatch == nil {
		t.Error("testseam.Dispatch is nil")
	}
}

// seamProbe returns a probe whose Sink collects dead letters, plus the
// slice it collects into.
func seamProbe(sinkErr error) (*testseam.Probe, *[]DeadLetter) {
	var letters []DeadLetter
	p := &testseam.Probe{NoBackoff: true}
	p.Sink = func(_ context.Context, _ string, body []byte) error {
		if sinkErr != nil {
			return sinkErr
		}
		var dl DeadLetter
		if err := json.Unmarshal(body, &dl); err != nil {
			return err
		}
		letters = append(letters, dl)
		return nil
	}
	return p, &letters
}

func seamEnvelopeBody(t *testing.T) []byte {
	t.Helper()

	env := Envelope[json.RawMessage]{
		EventID:       "01900000-0000-7000-8000-000000000001",
		EventType:     "test.event",
		Timestamp:     time.Now().UTC(),
		SchemaVersion: "1.0.0",
		CorrelationID: "01900000-0000-7000-8000-000000000002",
		Source:        "test.service",
		Payload:       json.RawMessage(`{"value":"v"}`),
	}
	body, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshalling envelope: %v", err)
	}
	return body
}

type seamHandler struct{ err error }

func (h seamHandler) Handle(context.Context, Envelope[testPayload]) error { return h.err }

func TestSeamDispatch_Processed(t *testing.T) {
	c, err := NewConsumer(testConsumerConfig())
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	def := EventDef{Topic: "things", Type: "test.event", Version: "1.0.0"}
	if err := RegisterHandler(c, def, seamHandler{}); err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}

	p, letters := seamProbe(nil)
	derr := testseam.Dispatch(context.Background(), c, "things",
		seamEnvelopeBody(t), nil, p)

	if derr != nil {
		t.Fatalf("dispatch: %v", derr)
	}
	if p.Kind != testseam.KindProcessed {
		t.Fatalf("got kind %q, want %q", p.Kind, testseam.KindProcessed)
	}
	if len(*letters) != 0 {
		t.Fatalf("got %d dead letters, want 0", len(*letters))
	}
}

func TestSeamDispatch_NoHandlerSkips(t *testing.T) {
	c, err := NewConsumer(testConsumerConfig())
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	def := EventDef{Topic: "things", Type: "other.event", Version: "1.0.0"}
	if err := RegisterHandler(c, def, seamHandler{}); err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}

	p, _ := seamProbe(nil)
	if err := testseam.Dispatch(context.Background(), c, "things",
		seamEnvelopeBody(t), nil, p); err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	if p.Kind != testseam.KindSkipped {
		t.Fatalf("got kind %q, want %q", p.Kind, testseam.KindSkipped)
	}
	if p.Reason != reasonNoHandler {
		t.Fatalf("got reason %q, want %q", p.Reason, reasonNoHandler)
	}
}

func TestSeamDispatch_PermanentDeadLetters(t *testing.T) {
	c, err := NewConsumer(testConsumerConfig())
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	def := EventDef{Topic: "things", Type: "test.event", Version: "1.0.0"}
	want := errors.New("boom")
	if err := RegisterHandler(c, def, seamHandler{err: AsPermanent(want)}); err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}

	p, letters := seamProbe(nil)
	// nil, not an error: a dead-lettered event is acked.
	if err := testseam.Dispatch(context.Background(), c, "things",
		seamEnvelopeBody(t), nil, p); err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	if p.Kind != testseam.KindFailed {
		t.Fatalf("got kind %q, want %q", p.Kind, testseam.KindFailed)
	}
	if p.Category != categoryPermanent {
		t.Fatalf("got category %q, want %q", p.Category, categoryPermanent)
	}
	if len(*letters) != 1 {
		t.Fatalf("got %d dead letters, want 1", len(*letters))
	}
	if (*letters)[0].Reason != ReasonPermanent {
		t.Fatalf("got reason %q, want %q", (*letters)[0].Reason, ReasonPermanent)
	}
}

// The cap fires before decode, so this also proves the probe is reachable
// on dispatch's earliest exit path.
func TestSeamDispatch_CapDeadLetters(t *testing.T) {
	c, err := NewConsumer(testConsumerConfig())
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	def := EventDef{Topic: "things", Type: "test.event", Version: "1.0.0"}
	if err := RegisterHandler(c, def, seamHandler{}); err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}

	p, letters := seamProbe(nil)
	metadata := map[string]string{internalwatermill.MetadataDeliveryAttempt: "99"}
	if err := testseam.Dispatch(context.Background(), c, "things",
		seamEnvelopeBody(t), metadata, p); err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	if p.Category != categoryMaxAttempts {
		t.Fatalf("got category %q, want %q", p.Category, categoryMaxAttempts)
	}
	if len(*letters) != 1 || (*letters)[0].Reason != ReasonMaxAttemptsExceeded {
		t.Fatalf("got %+v, want one max_attempts_exceeded dead letter", *letters)
	}
}

// A DLQ write that fails must nack, not ack the event into nothing.
func TestSeamDispatch_SinkFailureNacks(t *testing.T) {
	c, err := NewConsumer(testConsumerConfig())
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	def := EventDef{Topic: "things", Type: "test.event", Version: "1.0.0"}
	if err := RegisterHandler(c, def, seamHandler{err: AsPermanent(errors.New("boom"))}); err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}

	p, _ := seamProbe(errors.New("dlq unavailable"))
	if err := testseam.Dispatch(context.Background(), c, "things",
		seamEnvelopeBody(t), nil, p); err == nil {
		t.Fatal("expected an error so the entry stays pending")
	}
}

// NoBackoff must not change how many times the handler runs.
func TestSeamDispatch_NoBackoffKeepsRetryCount(t *testing.T) {
	cfg := testConsumerConfig()
	cfg.Retry.MaxImmediateRetries = 2

	c, err := NewConsumer(cfg)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}

	var calls int
	def := EventDef{Topic: "things", Type: "test.event", Version: "1.0.0"}
	h := countingHandler{count: &calls, err: errors.New("transient")}
	if err := RegisterHandler(c, def, h); err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}

	p, _ := seamProbe(nil)
	start := time.Now()
	if err := testseam.Dispatch(context.Background(), c, "things",
		seamEnvelopeBody(t), nil, p); err == nil {
		t.Fatal("expected a nack after retries were exhausted")
	}

	if calls != 3 {
		t.Fatalf("got %d handler calls, want 3 (1 + 2 retries)", calls)
	}
	// The library default backoff would spend hundreds of milliseconds
	// here; NoBackoff is what keeps application test suites fast.
	if elapsed := time.Since(start); elapsed > 50*time.Millisecond {
		t.Fatalf("took %v; NoBackoff did not collapse the sleeps", elapsed)
	}
}

type countingHandler struct {
	count *int
	err   error
}

func (h countingHandler) Handle(context.Context, Envelope[testPayload]) error {
	*h.count++
	return h.err
}

func TestSeamNewRecordingPublisher_RecordsWithoutRedis(t *testing.T) {
	var got []byte
	v, err := testseam.NewRecordingPublisher("test.service", "foi",
		func(_ context.Context, _, _ string, body []byte, _ map[string]string) error {
			got = body
			return nil
		})
	if err != nil {
		t.Fatalf("NewRecordingPublisher: %v", err)
	}

	p, ok := v.(*Publisher)
	if !ok {
		t.Fatalf("got %T, want *messaging.Publisher", v)
	}
	def := EventDef{Topic: "things", Type: "test.event", Version: "1.0.0"}
	if _, err := p.Publish(context.Background(), def, testPayload{Value: "v"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	var env Envelope[json.RawMessage]
	if err := json.Unmarshal(got, &env); err != nil {
		t.Fatalf("decoding recorded body: %v", err)
	}
	if env.EventType != "test.event" {
		t.Fatalf("got event type %q, want test.event", env.EventType)
	}
	// Proof the real publish path ran: these are filled in by
	// newEnvelope and resolveCorrelationID, not by the recorder.
	if env.EventID == "" || env.CorrelationID == "" || env.Timestamp.IsZero() {
		t.Fatalf("recorded envelope was not built by the real path: %+v", env)
	}
}

func TestSeamWithCorrelationID(t *testing.T) {
	ctx := testseam.WithCorrelationID(context.Background(), "cid-1")

	got, ok := correlationIDFromContext(ctx)
	if !ok || got != "cid-1" {
		t.Fatalf("got (%q, %v), want (\"cid-1\", true)", got, ok)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -run 'TestSeam' ./ -v`
Expected: FAIL — `testhooks.go` does not exist, so every hook is nil and `TestSeamHooksRegistered` reports three nils while the rest panic.

- [ ] **Step 3a: Give `deliveryRecorder` a probe**

In `telemetry.go`, add the field to the struct (after `ended bool`):

```go
	// probe is non-nil only when this delivery came from messagingtest.
	// It is threaded in from dispatch rather than read from a context
	// here, so the whole delivery costs one lookup rather than two.
	probe *testseam.Probe
```

Change `newDeliveryRecorder` to take it:

```go
func newDeliveryRecorder(inst *instruments, span trace.Span, topic, group string, log *slog.Logger, probe *testseam.Probe) *deliveryRecorder {
	return &deliveryRecorder{
		inst:  inst,
		span:  span,
		log:   log,
		topic: topic,
		group: group,
		start: time.Now(),
		probe: probe,
	}
}
```

Add the import `"github.com/bcgov/foi-messaging-go/internal/testseam"`.

In `end()`, immediately after the `r.ended = true` line, copy the verdict out:

```go
	// The verdict messagingtest reads. Copied here rather than derived
	// anywhere else because this is the one place that already holds it —
	// the same state the metrics below are built from, which is what makes
	// a probe incapable of reporting an outcome production would not have.
	if r.probe != nil {
		r.probe.Kind = kindName(r.kind)
		r.probe.Category = r.category
		r.probe.Reason = r.reason
		r.probe.Err = r.err
	}
```

Note the ordering constraint: this must sit **after** `r.ended = true` (so the idempotence guard still wins) and **before** the `switch r.kind`, whose `default` branch overwrites `r.category` with `categoryUnknown` on the handler-panic path.

Add `kindName` beneath `end()`:

```go
// kindName maps an outcomeKind to the string testseam compares against.
// The default is deliberately KindFailed rather than an empty string: an
// unstated outcome is the handler-panic path, which end() itself records
// as a failure.
func kindName(k outcomeKind) string {
	switch k {
	case outcomeProcessed:
		return testseam.KindProcessed
	case outcomeSkipped:
		return testseam.KindSkipped
	default:
		return testseam.KindFailed
	}
}
```

- [ ] **Step 3b: Look the probe up in `dispatch` and pass it down**

In `consumer.go`, in `dispatch`, replace the `newDeliveryRecorder` call:

```go
	// One of the two ctx.Value lookups a delivery pays for messagingtest
	// (the other is in deadLetter). Both return nil in production. That
	// cost buys Dispatch its freedom from locking and from mutating the
	// application's Consumer — see internal/testseam. Do not "optimise"
	// this into a field on Consumer without reading that comment.
	probe := testseam.FromContext(ctx)
	rec := newDeliveryRecorder(c.inst, span, topic, c.cfg.Consumer.Group, c.cfg.Telemetry.Logger, probe)
```

Add the `testseam` import to `consumer.go`.

- [ ] **Step 3c: Let the probe stand in for the DLQ sink**

In `consumer.go`, in `deadLetter`, after the existing `c.mu` block that reads `sink`:

```go
	// A probe replaces the sink outright. messagingtest drives this
	// function on a Consumer that was never run, where c.dlq is nil and
	// the guard below would refuse every dead letter.
	if p := testseam.FromContext(ctx); p != nil && p.Sink != nil {
		sink = probeSink(p.Sink)
	}
```

Add the adapter at the bottom of `consumer.go`, next to `redisDeadLetterSink`:

```go
// probeSink adapts a testseam.Probe's sink function to deadLetterSink, so
// the probe path and the Redis path go through the identical code in
// deadLetter — including its marshalling, its metrics, and the ack/nack
// contract on its return value.
type probeSink func(ctx context.Context, stream string, body []byte) error

func (f probeSink) publish(ctx context.Context, stream string, body []byte) error {
	return f(ctx, stream, body)
}
```

- [ ] **Step 3d: Honour `NoBackoff`**

In `consumer.go`, in `runWithRetry`, replace the `sleepWithJitter` call:

```go
		upper := backoffUpperBound(c.cfg.Retry, i)
		// messagingtest collapses the wait, never the count: an
		// application asserting "this error is retried three times" must
		// get the same three invocations it would in production.
		if rec.probe != nil && rec.probe.NoBackoff {
			upper = 0
		}
		if !sleepWithJitter(ctx, upper) {
```

`sleepWithJitter` already treats `upper <= 0` as "do not sleep" and returns `ctx.Err() == nil`, so no change is needed there.

- [ ] **Step 3e: Register the hooks**

Create `testhooks.go`:

```go
package messaging

import (
	"context"
	"fmt"

	"github.com/bcgov/foi-messaging-go/internal/testseam"
)

// init registers the hooks this module's testing/ (messagingtest) package
// needs to drive the real publish and consume paths without Redis.
//
// See internal/testseam for why a seam is required at all and why it is
// not an exported API instead. Nothing here widens the library's public
// surface: internal/ is invisible to applications.
func init() {
	testseam.WithCorrelationID = contextWithCorrelationID

	testseam.NewRecordingPublisher = func(source, streamPrefix string,
		record func(ctx context.Context, stream, id string, body []byte,
			metadata map[string]string) error) (any, error) {
		cfg := Config{
			Source:       source,
			StreamPrefix: streamPrefix,
			// Never dialed. NewPublisher performs no connection at
			// construction — redisstream.NewPublisher is struct
			// construction plus config validation, and go-redis connects
			// lazily — and publishFn below replaces the only call that
			// would ever touch the network.
			Redis: RedisConfig{Address: "127.0.0.1:6379"},
		}

		p, err := NewPublisher(cfg)
		if err != nil {
			return nil, fmt.Errorf("building recording publisher: %w", err)
		}
		p.publishFn = record
		return p, nil
	}

	testseam.Dispatch = func(ctx context.Context, consumer any, topic string,
		body []byte, metadata map[string]string, p *testseam.Probe) error {
		c, ok := consumer.(*Consumer)
		if !ok {
			return fmt.Errorf("messaging: testseam.Dispatch wants a *messaging.Consumer, got %T", consumer)
		}
		return c.dispatch(testseam.NewContext(ctx, p), topic, body, metadata)
	}
}
```

- [ ] **Step 4: Run the tests**

Run: `go test -run 'TestSeam' ./ -v`
Expected: PASS — all nine tests.

Then the full unit tier, because `newDeliveryRecorder`'s signature changed and existing callers in `telemetry_test.go` / `consumer_test.go` may need a trailing `nil`:

Run: `go test ./...`
Expected: PASS. Fix any call site by passing `nil` as the new final argument.

Run: `go test -race -count=1 ./...`
Expected: PASS. The probe is written by `end()` and read by the caller after `Dispatch` returns, so there is no race; this confirms it.

Run: `make lint`
Expected: clean. `depguard` is unaffected — `internal/testseam` imports only `context`.

- [ ] **Step 5: Commit**

```bash
git add testhooks.go testhooks_test.go telemetry.go consumer.go
git commit -m "feat: wire the delivery probe through dispatch, the recorder, and the DLQ"
```

---

### Task 3: `Event`, `PayloadAs`, `NewEvent`, `Config`

**Files:**
- Create: `testing/event.go`
- Test: `testing/event_test.go`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: `messagingtest.Event{Topic string; Envelope messaging.Envelope[json.RawMessage]; Payload any}`; `PayloadAs[T any](Event) (T, error)`; `NewEvent[T any](messaging.EventDef, T, ...EventOption) (Event, error)`; `EventOption` with `WithEventID`, `WithCorrelationID`, `WithSource`, `WithTimestamp`; `Config() messaging.Config`; unexported `defaultSource`, `defaultStreamPrefix`.

- [ ] **Step 1: Write the failing test**

Create `testing/event_test.go`:

```go
package messagingtest_test

import (
	"testing"
	"time"

	messaging "github.com/bcgov/foi-messaging-go"
	messagingtest "github.com/bcgov/foi-messaging-go/testing"
)

type orderCreated struct {
	OrderID string `json:"order_id"`
}

var orderCreatedDef = messaging.EventDef{
	Topic:   "orders",
	Type:    "order.created",
	Version: "1.0.0",
}

func TestNewEvent_Defaults(t *testing.T) {
	e, err := messagingtest.NewEvent(orderCreatedDef, orderCreated{OrderID: "o-1"})
	if err != nil {
		t.Fatalf("NewEvent: %v", err)
	}

	if e.Topic != "orders" {
		t.Fatalf("got topic %q, want orders", e.Topic)
	}
	if e.Envelope.EventType != "order.created" {
		t.Fatalf("got event type %q, want order.created", e.Envelope.EventType)
	}
	if e.Envelope.SchemaVersion != "1.0.0" {
		t.Fatalf("got schema version %q, want 1.0.0", e.Envelope.SchemaVersion)
	}
	// The defaults must produce a *valid* envelope, so the options exist
	// to make one invalid on purpose rather than by accident.
	if e.Envelope.EventID == "" || e.Envelope.CorrelationID == "" {
		t.Fatalf("got empty ids: %+v", e.Envelope)
	}
	if e.Envelope.Timestamp.IsZero() {
		t.Fatal("got a zero timestamp")
	}
	if e.Envelope.Source == "" {
		t.Fatal("got an empty source")
	}
}

func TestNewEvent_Options(t *testing.T) {
	ts := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)

	e, err := messagingtest.NewEvent(orderCreatedDef, orderCreated{OrderID: "o-1"},
		messagingtest.WithEventID("evt-1"),
		messagingtest.WithCorrelationID("cid-1"),
		messagingtest.WithSource("other.service"),
		messagingtest.WithTimestamp(ts))
	if err != nil {
		t.Fatalf("NewEvent: %v", err)
	}

	if e.Envelope.EventID != "evt-1" {
		t.Fatalf("got event id %q, want evt-1", e.Envelope.EventID)
	}
	if e.Envelope.CorrelationID != "cid-1" {
		t.Fatalf("got correlation id %q, want cid-1", e.Envelope.CorrelationID)
	}
	if e.Envelope.Source != "other.service" {
		t.Fatalf("got source %q, want other.service", e.Envelope.Source)
	}
	if !e.Envelope.Timestamp.Equal(ts) {
		t.Fatalf("got timestamp %v, want %v", e.Envelope.Timestamp, ts)
	}
}

// Options must be able to produce a deliberately invalid envelope — that
// is how an application tests its own dead-letter handling.
func TestNewEvent_OptionsCanInvalidate(t *testing.T) {
	e, err := messagingtest.NewEvent(orderCreatedDef, orderCreated{},
		messagingtest.WithEventID(""))
	if err != nil {
		t.Fatalf("NewEvent: %v", err)
	}

	if e.Envelope.EventID != "" {
		t.Fatalf("got event id %q, want it cleared", e.Envelope.EventID)
	}
}

func TestPayloadAs(t *testing.T) {
	e, err := messagingtest.NewEvent(orderCreatedDef, orderCreated{OrderID: "o-1"})
	if err != nil {
		t.Fatalf("NewEvent: %v", err)
	}

	got, err := messagingtest.PayloadAs[orderCreated](e)
	if err != nil {
		t.Fatalf("PayloadAs: %v", err)
	}
	if got.OrderID != "o-1" {
		t.Fatalf("got order id %q, want o-1", got.OrderID)
	}
}

func TestPayloadAs_WrongType(t *testing.T) {
	e, err := messagingtest.NewEvent(orderCreatedDef, orderCreated{OrderID: "o-1"})
	if err != nil {
		t.Fatalf("NewEvent: %v", err)
	}

	if _, err := messagingtest.PayloadAs[int](e); err == nil {
		t.Fatal("expected an error decoding an object into an int")
	}
}

// A payload the real publisher could not marshal must fail here too,
// rather than being recorded as if it had been published.
func TestNewEvent_UnmarshalablePayload(t *testing.T) {
	if _, err := messagingtest.NewEvent(orderCreatedDef, make(chan int)); err == nil {
		t.Fatal("expected an error marshalling a channel")
	}
}

func TestConfig_IsValidForAConsumer(t *testing.T) {
	// Validate requires Redis.Address even for a Consumer that never
	// connects, which is the whole reason Config exists.
	if _, err := messaging.NewConsumer(messagingtest.Config()); err != nil {
		t.Fatalf("NewConsumer with messagingtest.Config(): %v", err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./testing/ -v`
Expected: FAIL — `NewEvent`, `PayloadAs`, `Config`, and the options are undefined.

- [ ] **Step 3: Write minimal implementation**

Create `testing/event.go`:

```go
package messagingtest

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	messaging "github.com/bcgov/foi-messaging-go"
)

const (
	// defaultSource is what messagingtest stamps on envelopes and
	// publishes when the caller does not choose one. It satisfies the
	// envelope's event-source requirement without pretending to be a real
	// service name.
	defaultSource = "messagingtest"

	// defaultStreamPrefix mirrors the library's own default (PRD §8). It
	// is only ever used to strip the prefix back off a recorded stream
	// name, since nothing here writes to Redis.
	defaultStreamPrefix = "foi"
)

// Event is a message as messagingtest models it: the logical topic, the
// envelope as it would appear on the wire, and the payload value the
// caller passed.
//
// Both payload representations are kept deliberately. Envelope.Payload is
// marshalled JSON, so a payload holding a channel or a NaN, or a struct
// with wrong json tags, fails in the test rather than in production.
// Payload is the caller's own value, so the common assertion needs no
// decode step.
//
// Event is the currency of this package: Publisher.Published returns
// []Event, NewEvent produces one, and Dispatch consumes one. That is what
// makes a two-service chain testable without Redis.
type Event struct {
	Topic    string
	Envelope messaging.Envelope[json.RawMessage]

	// Payload is the value as passed. It is nil for an Event decoded from
	// bytes rather than built from a value.
	Payload any
}

// PayloadAs decodes e's marshalled payload into T.
//
// A free function rather than a method because Go does not permit generic
// methods — the same reason messaging.RegisterHandler is one.
func PayloadAs[T any](e Event) (T, error) {
	var payload T
	if len(e.Envelope.Payload) == 0 {
		return payload, nil
	}
	if err := json.Unmarshal(e.Envelope.Payload, &payload); err != nil {
		return payload, fmt.Errorf("messagingtest: decoding payload of event type %q: %w",
			e.Envelope.EventType, err)
	}
	return payload, nil
}

type eventOptions struct {
	eventID       string
	correlationID string
	source        string
	timestamp     time.Time
}

// EventOption customizes an Event built by NewEvent.
type EventOption func(*eventOptions)

// WithEventID sets the envelope's event_id. Pass "" to clear it, which is
// how a test provokes the dead-letter path for an invalid envelope.
func WithEventID(id string) EventOption {
	return func(o *eventOptions) { o.eventID = id }
}

// WithCorrelationID sets the envelope's correlation_id.
//
// Note the deliberate name collision: messaging.WithCorrelationID is a
// PublishOption for the publish boundary, this one is an EventOption for
// the consume boundary.
func WithCorrelationID(id string) EventOption {
	return func(o *eventOptions) { o.correlationID = id }
}

// WithSource sets the envelope's source.
func WithSource(source string) EventOption {
	return func(o *eventOptions) { o.source = source }
}

// WithTimestamp sets the envelope's timestamp. Pass the zero time to
// provoke the dead-letter path for an invalid envelope.
func WithTimestamp(t time.Time) EventOption {
	return func(o *eventOptions) { o.timestamp = t }
}

// NewEvent builds an Event for def carrying payload.
//
// The defaults produce an envelope that passes the library's validation:
// UUIDv7 event and correlation ids, the current time, and defaultSource.
// Options are applied over the defaults, so any of them can be cleared
// deliberately.
func NewEvent[T any](def messaging.EventDef, payload T, opts ...EventOption) (Event, error) {
	eventID, err := uuid.NewV7()
	if err != nil {
		return Event{}, fmt.Errorf("messagingtest: generating event id: %w", err)
	}
	correlationID, err := uuid.NewV7()
	if err != nil {
		return Event{}, fmt.Errorf("messagingtest: generating correlation id: %w", err)
	}

	o := eventOptions{
		eventID:       eventID.String(),
		correlationID: correlationID.String(),
		source:        defaultSource,
		timestamp:     time.Now().UTC(),
	}
	for _, opt := range opts {
		opt(&o)
	}

	raw, err := json.Marshal(payload)
	if err != nil {
		return Event{}, fmt.Errorf("messagingtest: marshalling payload for event type %q: %w",
			def.Type, err)
	}

	return Event{
		Topic: def.Topic,
		Envelope: messaging.Envelope[json.RawMessage]{
			EventID:       o.eventID,
			EventType:     def.Type,
			Timestamp:     o.timestamp,
			SchemaVersion: def.Version,
			CorrelationID: o.correlationID,
			Source:        o.source,
			Payload:       raw,
		},
		Payload: payload,
	}, nil
}

// Config returns a messaging.Config that passes validation without dialing
// anything, for building a Consumer to hand to Dispatch.
//
// It exists because Config.Validate requires Redis.Address even for a
// Consumer that never connects, which would otherwise make every
// application test invent a fake address. The returned value is a plain
// struct: adjust any field directly rather than reaching for options.
func Config() messaging.Config {
	return messaging.Config{
		Source:       defaultSource,
		StreamPrefix: defaultStreamPrefix,
		Redis:        messaging.RedisConfig{Address: "127.0.0.1:6379"},
		Consumer:     messaging.ConsumerConfig{Group: defaultSource},
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./testing/ -v`
Expected: PASS — all seven tests.

- [ ] **Step 5: Commit**

```bash
git add testing/event.go testing/event_test.go
git commit -m "feat: add messagingtest.Event, NewEvent, PayloadAs, and Config"
```

---

### Task 4: The recording `Publisher`

**Files:**
- Create: `testing/publisher.go`
- Test: `testing/publisher_test.go`

**Interfaces:**
- Consumes: `testseam.NewRecordingPublisher` (Task 2); `Event`, `defaultSource`, `defaultStreamPrefix` (Task 3).
- Produces: `NewPublisher(...PublisherOption) (*Publisher, error)`; `WithPublisherSource(string) PublisherOption`; methods `Publish`, `Published() []Event`, `Reset()`, `FailWith(error)`, `Close() error`.

- [ ] **Step 1: Write the failing test**

Create `testing/publisher_test.go`:

```go
package messagingtest_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	messaging "github.com/bcgov/foi-messaging-go"
	messagingtest "github.com/bcgov/foi-messaging-go/testing"
)

func newTestPublisher(t *testing.T) *messagingtest.Publisher {
	t.Helper()

	p, err := messagingtest.NewPublisher()
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}

func TestPublisher_RecordsPublish(t *testing.T) {
	p := newTestPublisher(t)

	res, err := p.Publish(context.Background(), orderCreatedDef, orderCreated{OrderID: "o-1"})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	pubs := p.Published()
	if len(pubs) != 1 {
		t.Fatalf("got %d publishes, want 1", len(pubs))
	}
	if pubs[0].Topic != "orders" {
		t.Fatalf("got topic %q, want orders (the prefix must be stripped)", pubs[0].Topic)
	}
	if pubs[0].Envelope.EventType != "order.created" {
		t.Fatalf("got event type %q, want order.created", pubs[0].Envelope.EventType)
	}
	// The real publish path built this, not the recorder.
	if pubs[0].Envelope.EventID != res.EventID {
		t.Fatalf("recorded event id %q != returned %q", pubs[0].Envelope.EventID, res.EventID)
	}
	if pubs[0].Envelope.Source != "messagingtest" {
		t.Fatalf("got source %q, want messagingtest", pubs[0].Envelope.Source)
	}

	payload, err := messagingtest.PayloadAs[orderCreated](pubs[0])
	if err != nil {
		t.Fatalf("PayloadAs: %v", err)
	}
	if payload.OrderID != "o-1" {
		t.Fatalf("got order id %q, want o-1", payload.OrderID)
	}
	if got, ok := pubs[0].Payload.(orderCreated); !ok || got.OrderID != "o-1" {
		t.Fatalf("got original payload %#v, want orderCreated{o-1}", pubs[0].Payload)
	}
}

// The whole point of delegating to a real *messaging.Publisher: a malformed
// EventDef must fail the unit test, not the first production publish.
func TestPublisher_RejectsInvalidEventDef(t *testing.T) {
	p := newTestPublisher(t)

	bad := messaging.EventDef{Topic: "orders", Type: "order.created", Version: "v1"}
	if _, err := p.Publish(context.Background(), bad, orderCreated{}); err == nil {
		t.Fatal("expected an error for a non-semver schema version")
	}
	if len(p.Published()) != 0 {
		t.Fatal("a rejected publish must not be recorded")
	}
}

// Correlation resolution is the real one: explicit option beats context
// beats a freshly generated id.
func TestPublisher_CorrelationIDPrecedence(t *testing.T) {
	p := newTestPublisher(t)

	_, err := p.Publish(context.Background(), orderCreatedDef, orderCreated{},
		messaging.WithCorrelationID("cid-explicit"))
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	if got := p.Published()[0].Envelope.CorrelationID; got != "cid-explicit" {
		t.Fatalf("got correlation id %q, want cid-explicit", got)
	}
}

func TestPublisher_FailWith(t *testing.T) {
	p := newTestPublisher(t)
	want := errors.New("redis is down")
	p.FailWith(want)

	_, err := p.Publish(context.Background(), orderCreatedDef, orderCreated{})
	if !errors.Is(err, want) {
		t.Fatalf("got %v, want it to wrap %v", err, want)
	}
	if len(p.Published()) != 0 {
		t.Fatal("a failed publish must not be recorded")
	}
	// It failed at the transport stage, so the envelope was still built
	// and validated on the way there.
	if !strings.Contains(err.Error(), "publishing to stream") {
		t.Fatalf("got %q, want a transport-stage error", err)
	}
}

func TestPublisher_Reset(t *testing.T) {
	p := newTestPublisher(t)

	if _, err := p.Publish(context.Background(), orderCreatedDef, orderCreated{}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	p.Reset()

	if got := len(p.Published()); got != 0 {
		t.Fatalf("got %d publishes after Reset, want 0", got)
	}
}

// Published must hand back a copy: a caller appending to the returned
// slice must not corrupt the recorder.
func TestPublisher_PublishedIsACopy(t *testing.T) {
	p := newTestPublisher(t)

	if _, err := p.Publish(context.Background(), orderCreatedDef, orderCreated{}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	pubs := p.Published()
	pubs[0].Topic = "mutated"

	if got := p.Published()[0].Topic; got != "orders" {
		t.Fatalf("got topic %q, want orders; Published returned a shared backing array", got)
	}
}

// Run with -race. The original payload rides the context precisely so
// concurrent publishes cannot cross their payloads.
func TestPublisher_ConcurrentPublish(t *testing.T) {
	p := newTestPublisher(t)

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = p.Publish(context.Background(), orderCreatedDef, orderCreated{OrderID: "o"})
		}()
	}
	wg.Wait()

	if got := len(p.Published()); got != 50 {
		t.Fatalf("got %d publishes, want 50", got)
	}
	for i, e := range p.Published() {
		if _, ok := e.Payload.(orderCreated); !ok {
			t.Fatalf("publish %d recorded payload %#v, want orderCreated", i, e.Payload)
		}
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./testing/ -run TestPublisher -v`
Expected: FAIL — `messagingtest.NewPublisher` is undefined.

- [ ] **Step 3: Write minimal implementation**

Create `testing/publisher.go`:

```go
package messagingtest

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	messaging "github.com/bcgov/foi-messaging-go"
	"github.com/bcgov/foi-messaging-go/internal/testseam"
)

// publisher is the method set an application substitutes on.
//
// The library exports no publisher interface — applications declare their
// own narrow one at the point of consumption, which is the Go idiom. This
// assertion is what keeps the fake honest in the meantime: a change to
// messaging.Publisher.Publish's signature breaks the library's own build
// here, rather than breaking every application's tests silently.
type publisher interface {
	Publish(context.Context, messaging.EventDef, any,
		...messaging.PublishOption) (messaging.PublishResult, error)
}

var (
	_ publisher = (*messaging.Publisher)(nil)
	_ publisher = (*Publisher)(nil)
)

// Publisher records published events for assertion and never contacts
// Redis.
//
// It wraps a real *messaging.Publisher with only its transport write
// redirected, so Publish runs genuine library code: correlation-ID
// resolution, envelope construction, validation, marshalling, and the
// producer span and metrics. That is what makes a malformed EventDef fail
// here rather than on the first production publish.
type Publisher struct {
	real   *messaging.Publisher
	prefix string

	mu      sync.Mutex
	events  []Event
	failErr error
}

type publisherOptions struct{ source string }

// PublisherOption customizes a Publisher.
type PublisherOption func(*publisherOptions)

// WithPublisherSource sets the source stamped on published envelopes.
// Named for its boundary because WithSource is already this package's
// EventOption.
func WithPublisherSource(source string) PublisherOption {
	return func(o *publisherOptions) { o.source = source }
}

// NewPublisher returns a Publisher that records instead of publishing.
//
// It returns an error only for a library-internal failure — there is no
// input a caller can supply that makes it fail — but returns one anyway
// for consistency with messaging.NewPublisher and messaging.NewConsumer.
func NewPublisher(opts ...PublisherOption) (*Publisher, error) {
	o := publisherOptions{source: defaultSource}
	for _, opt := range opts {
		opt(&o)
	}

	p := &Publisher{prefix: defaultStreamPrefix}

	v, err := testseam.NewRecordingPublisher(o.source, defaultStreamPrefix, p.record)
	if err != nil {
		return nil, fmt.Errorf("messagingtest: building recording publisher: %w", err)
	}
	real, ok := v.(*messaging.Publisher)
	if !ok {
		return nil, fmt.Errorf("messagingtest: seam returned %T, want *messaging.Publisher", v)
	}
	p.real = real

	return p, nil
}

type payloadKey struct{}

// Publish delegates to the real publisher, whose transport write lands in
// record.
func (p *Publisher) Publish(ctx context.Context, def messaging.EventDef, payload any,
	opts ...messaging.PublishOption) (messaging.PublishResult, error) {
	// The original payload value rides the context, because record sees
	// only marshalled bytes. On the context rather than in a field so
	// concurrent Publish calls cannot cross their payloads — the
	// alternative is holding the lock across the whole delegated call.
	ctx = context.WithValue(ctx, payloadKey{}, payload)
	return p.real.Publish(ctx, def, payload, opts...)
}

// record is the transport write the real publisher calls in place of a
// Redis XADD.
func (p *Publisher) record(ctx context.Context, stream, _ string, body []byte,
	_ map[string]string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Failing here rather than short-circuiting Publish is deliberate: the
	// envelope is still built and validated on the way to this point, so a
	// simulated outage behaves exactly like a real one — a transport-stage
	// failure, not a validation-stage one.
	if p.failErr != nil {
		return p.failErr
	}

	var env messaging.Envelope[json.RawMessage]
	if err := json.Unmarshal(body, &env); err != nil {
		return fmt.Errorf("messagingtest: decoding recorded envelope: %w", err)
	}

	p.events = append(p.events, Event{
		Topic:    strings.TrimPrefix(stream, p.prefix+":"),
		Envelope: env,
		Payload:  ctx.Value(payloadKey{}),
	})
	return nil
}

// Published returns the events recorded so far, oldest first.
//
// The result is a copy: a caller that appends to it, or edits an element,
// must not be able to corrupt the recorder.
func (p *Publisher) Published() []Event {
	p.mu.Lock()
	defer p.mu.Unlock()

	out := make([]Event, len(p.events))
	copy(out, p.events)
	return out
}

// Reset discards everything recorded so far, for a test that reuses one
// Publisher across subtests.
func (p *Publisher) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = nil
}

// FailWith makes every subsequent Publish fail with err, so an application
// can test its own publish-failure path. Pass nil to stop failing.
func (p *Publisher) FailWith(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.failErr = err
}

// Close releases the underlying publisher. Safe to call on a Publisher
// that never connected to anything, which is all of them.
func (p *Publisher) Close() error {
	return p.real.Close()
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./testing/ -run TestPublisher -v`
Expected: PASS — all seven tests.

Run: `go test -race -count=1 ./testing/`
Expected: PASS — `TestPublisher_ConcurrentPublish` is the one that matters here.

- [ ] **Step 5: Commit**

```bash
git add testing/publisher.go testing/publisher_test.go
git commit -m "feat: add the recording messagingtest.Publisher over the real publish path"
```

---

### Task 5: `Deliver`

**Files:**
- Create: `testing/deliver.go`
- Test: `testing/deliver_test.go`

**Interfaces:**
- Consumes: `testseam.WithCorrelationID` (Task 2).
- Produces: `Deliver[T any](context.Context, messaging.Handler[T], messaging.Envelope[T]) error`.

- [ ] **Step 1: Write the failing test**

Create `testing/deliver_test.go`:

```go
package messagingtest_test

import (
	"context"
	"errors"
	"testing"

	messaging "github.com/bcgov/foi-messaging-go"
	messagingtest "github.com/bcgov/foi-messaging-go/testing"
)

type recordingHandler struct {
	gotEnvelope messaging.Envelope[orderCreated]
	err         error
}

func (h *recordingHandler) Handle(_ context.Context, env messaging.Envelope[orderCreated]) error {
	h.gotEnvelope = env
	return h.err
}

func TestDeliver_InvokesHandler(t *testing.T) {
	h := &recordingHandler{}
	env := messaging.Envelope[orderCreated]{
		EventID:   "evt-1",
		EventType: "order.created",
		Payload:   orderCreated{OrderID: "o-1"},
	}

	if err := messagingtest.Deliver(context.Background(), h, env); err != nil {
		t.Fatalf("Deliver: %v", err)
	}

	if h.gotEnvelope.Payload.OrderID != "o-1" {
		t.Fatalf("got order id %q, want o-1", h.gotEnvelope.Payload.OrderID)
	}
}

func TestDeliver_ReturnsHandlerErrorVerbatim(t *testing.T) {
	want := errors.New("boom")
	h := &recordingHandler{err: messaging.AsPermanent(want)}

	err := messagingtest.Deliver(context.Background(), h,
		messaging.Envelope[orderCreated]{})

	if !errors.Is(err, want) {
		t.Fatalf("got %v, want it to wrap %v", err, want)
	}
	// Verbatim means the classification survives: Deliver must not
	// interpret, retry, or re-wrap.
	if !messaging.IsPermanent(err) {
		t.Fatal("classification was lost on the way back")
	}
}

// The load-bearing behaviour: a handler that publishes downstream must
// inherit the consumed event's correlation ID, which only happens if
// Deliver installs it on the context the way the router does.
func TestDeliver_CorrelationIDChainsIntoAFollowOnPublish(t *testing.T) {
	pub := newTestPublisher(t)
	h := &publishingHandler{pub: pub}

	env := messaging.Envelope[orderCreated]{
		EventID:       "evt-1",
		EventType:     "order.created",
		CorrelationID: "cid-upstream",
	}
	if err := messagingtest.Deliver(context.Background(), h, env); err != nil {
		t.Fatalf("Deliver: %v", err)
	}

	pubs := pub.Published()
	if len(pubs) != 1 {
		t.Fatalf("got %d publishes, want 1", len(pubs))
	}
	if got := pubs[0].Envelope.CorrelationID; got != "cid-upstream" {
		t.Fatalf("got correlation id %q, want cid-upstream", got)
	}
}

type publishingHandler struct{ pub *messagingtest.Publisher }

func (h *publishingHandler) Handle(ctx context.Context, _ messaging.Envelope[orderCreated]) error {
	_, err := h.pub.Publish(ctx, orderShippedDef, orderShipped{OrderID: "o-1"})
	return err
}

type orderShipped struct {
	OrderID string `json:"order_id"`
}

var orderShippedDef = messaging.EventDef{
	Topic:   "orders",
	Type:    "order.shipped",
	Version: "1.0.0",
}

// An envelope with no correlation ID must not install an empty one, which
// would defeat the publisher's own resolution and stamp "" downstream.
func TestDeliver_NoCorrelationIDLeavesResolutionToThePublisher(t *testing.T) {
	pub := newTestPublisher(t)
	h := &publishingHandler{pub: pub}

	if err := messagingtest.Deliver(context.Background(), h,
		messaging.Envelope[orderCreated]{EventID: "evt-1"}); err != nil {
		t.Fatalf("Deliver: %v", err)
	}

	if got := pub.Published()[0].Envelope.CorrelationID; got == "" {
		t.Fatal("got an empty correlation id; the publisher should have generated one")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./testing/ -run TestDeliver -v`
Expected: FAIL — `messagingtest.Deliver` is undefined.

- [ ] **Step 3: Write minimal implementation**

Create `testing/deliver.go`:

```go
package messagingtest

import (
	"context"

	messaging "github.com/bcgov/foi-messaging-go"
	"github.com/bcgov/foi-messaging-go/internal/testseam"
)

// Deliver invokes h with env exactly as the router would, and returns the
// handler's error verbatim.
//
// Deliver reproduces the handler boundary, not the transport or router
// lifecycle. It does not retry, ack, nack, reclaim, dead-letter, or
// interpret retry exhaustion — for those, use Dispatch, which runs the
// library's real dispatch path.
//
// The correlation-ID installation is the load-bearing part. That context
// value is what the publisher reads when resolving the correlation ID for
// a follow-on publish, so it is what makes a correlation ID chain from a
// consumed event into the next one. Without it, a handler that consumes
// and then publishes would appear to work while asserting nothing about
// the chain.
func Deliver[T any](ctx context.Context, h messaging.Handler[T], env messaging.Envelope[T]) error {
	// Empty is left alone rather than installed: an empty context value
	// would defeat the publisher's own resolution, which falls through to
	// generating a fresh id precisely when there is nothing to inherit.
	if env.CorrelationID != "" {
		ctx = testseam.WithCorrelationID(ctx, env.CorrelationID)
	}
	return h.Handle(ctx, env)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./testing/ -run TestDeliver -v`
Expected: PASS — all five tests.

- [ ] **Step 5: Commit**

```bash
git add testing/deliver.go testing/deliver_test.go
git commit -m "feat: add messagingtest.Deliver for the handler boundary"
```

---

### Task 6: `Dispatch`, `Result`, `Outcome`

**Files:**
- Create: `testing/dispatch.go`
- Test: `testing/dispatch_test.go`

**Interfaces:**
- Consumes: `testseam.Dispatch`, `testseam.Probe`, `testseam.KindFailed`/`KindSkipped` (Tasks 1–2); `Event` (Task 3); `internalwatermill.MetadataDeliveryAttempt`.
- Produces: `Dispatch(context.Context, *messaging.Consumer, Event, ...DispatchOption) (Result, error)`; `Result`; `Outcome` with `OutcomeProcessed`/`OutcomeSkipped`/`OutcomeDeadLettered`/`OutcomeNacked` and a `String()`; `DispatchOption` with `WithDeliveryAttempt`, `WithMetadata`, `WithRealBackoff`, `WithFailingDLQ`.

- [ ] **Step 1: Write the failing test**

Create `testing/dispatch_test.go`:

```go
package messagingtest_test

import (
	"context"
	"errors"
	"testing"
	"time"

	messaging "github.com/bcgov/foi-messaging-go"
	messagingtest "github.com/bcgov/foi-messaging-go/testing"
)

type stubHandler struct {
	calls int
	err   error
}

func (h *stubHandler) Handle(context.Context, messaging.Envelope[orderCreated]) error {
	h.calls++
	return h.err
}

// newDispatchConsumer builds a Consumer with h registered for def, exactly
// as an application would.
func newDispatchConsumer(t *testing.T, def messaging.EventDef, h messaging.Handler[orderCreated]) *messaging.Consumer {
	t.Helper()

	c, err := messaging.NewConsumer(messagingtest.Config())
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	if err := messaging.RegisterHandler(c, def, h); err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}
	return c
}

func newDispatchEvent(t *testing.T) messagingtest.Event {
	t.Helper()

	e, err := messagingtest.NewEvent(orderCreatedDef, orderCreated{OrderID: "o-1"})
	if err != nil {
		t.Fatalf("NewEvent: %v", err)
	}
	return e
}

func TestDispatch_Processed(t *testing.T) {
	h := &stubHandler{}
	c := newDispatchConsumer(t, orderCreatedDef, h)

	res, err := messagingtest.Dispatch(context.Background(), c, newDispatchEvent(t))
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}

	if res.Outcome != messagingtest.OutcomeProcessed {
		t.Fatalf("got %v, want OutcomeProcessed", res.Outcome)
	}
	if h.calls != 1 {
		t.Fatalf("got %d handler calls, want 1", h.calls)
	}
}

func TestDispatch_NoHandlerSkips(t *testing.T) {
	other := messaging.EventDef{Topic: "orders", Type: "order.cancelled", Version: "1.0.0"}
	c := newDispatchConsumer(t, other, &stubHandler{})

	res, err := messagingtest.Dispatch(context.Background(), c, newDispatchEvent(t))
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}

	if res.Outcome != messagingtest.OutcomeSkipped {
		t.Fatalf("got %v, want OutcomeSkipped", res.Outcome)
	}
	if res.Reason != "no_handler" {
		t.Fatalf("got reason %q, want no_handler", res.Reason)
	}
}

func TestDispatch_DiscardSkips(t *testing.T) {
	h := &stubHandler{err: messaging.AsDiscard(errors.New("not for us"))}
	c := newDispatchConsumer(t, orderCreatedDef, h)

	res, err := messagingtest.Dispatch(context.Background(), c, newDispatchEvent(t))
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}

	if res.Outcome != messagingtest.OutcomeSkipped {
		t.Fatalf("got %v, want OutcomeSkipped", res.Outcome)
	}
	if res.Reason != "discard" {
		t.Fatalf("got reason %q, want discard", res.Reason)
	}
	if h.calls != 1 {
		t.Fatalf("got %d handler calls, want 1; a discard must not retry", h.calls)
	}
}

func TestDispatch_PermanentDeadLetters(t *testing.T) {
	h := &stubHandler{err: messaging.AsPermanent(errors.New("bad reference"))}
	c := newDispatchConsumer(t, orderCreatedDef, h)

	res, err := messagingtest.Dispatch(context.Background(), c, newDispatchEvent(t))
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}

	if res.Outcome != messagingtest.OutcomeDeadLettered {
		t.Fatalf("got %v, want OutcomeDeadLettered", res.Outcome)
	}
	if len(res.DeadLetters) != 1 {
		t.Fatalf("got %d dead letters, want 1", len(res.DeadLetters))
	}
	if res.DeadLetters[0].Reason != messaging.ReasonPermanent {
		t.Fatalf("got reason %q, want %q", res.DeadLetters[0].Reason, messaging.ReasonPermanent)
	}
	if h.calls != 1 {
		t.Fatalf("got %d handler calls, want 1; a permanent error must not retry", h.calls)
	}
}

func TestDispatch_RetryableExhaustsThenNacks(t *testing.T) {
	h := &stubHandler{err: errors.New("transient")}
	c := newDispatchConsumer(t, orderCreatedDef, h)

	res, err := messagingtest.Dispatch(context.Background(), c, newDispatchEvent(t))
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}

	if res.Outcome != messagingtest.OutcomeNacked {
		t.Fatalf("got %v, want OutcomeNacked", res.Outcome)
	}
	// 1 initial attempt + the library's default 3 immediate retries.
	if h.calls != 4 {
		t.Fatalf("got %d handler calls, want 4", h.calls)
	}
	if res.Err == nil {
		t.Fatal("want the delivery's error on the result")
	}
}

func TestDispatch_DeliveryAttemptCap(t *testing.T) {
	h := &stubHandler{}
	c := newDispatchConsumer(t, orderCreatedDef, h)

	res, err := messagingtest.Dispatch(context.Background(), c, newDispatchEvent(t),
		messagingtest.WithDeliveryAttempt(6))
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}

	if res.Outcome != messagingtest.OutcomeDeadLettered {
		t.Fatalf("got %v, want OutcomeDeadLettered", res.Outcome)
	}
	if res.DeadLetters[0].Reason != messaging.ReasonMaxAttemptsExceeded {
		t.Fatalf("got reason %q, want %q", res.DeadLetters[0].Reason,
			messaging.ReasonMaxAttemptsExceeded)
	}
	// The cap fires before decode, so the handler never runs — that
	// ordering is what keeps a poison message from burning four handler
	// invocations and its concurrency slot.
	if h.calls != 0 {
		t.Fatalf("got %d handler calls, want 0", h.calls)
	}
}

func TestDispatch_InvalidEnvelopeDeadLetters(t *testing.T) {
	h := &stubHandler{}
	c := newDispatchConsumer(t, orderCreatedDef, h)

	e, err := messagingtest.NewEvent(orderCreatedDef, orderCreated{},
		messagingtest.WithEventID(""))
	if err != nil {
		t.Fatalf("NewEvent: %v", err)
	}

	res, err := messagingtest.Dispatch(context.Background(), c, e)
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}

	if res.Outcome != messagingtest.OutcomeDeadLettered {
		t.Fatalf("got %v, want OutcomeDeadLettered", res.Outcome)
	}
	if res.DeadLetters[0].Reason != messaging.ReasonDeserializationFailed {
		t.Fatalf("got reason %q, want %q", res.DeadLetters[0].Reason,
			messaging.ReasonDeserializationFailed)
	}
}

// Minor and patch versions do not participate in routing: a 1.0.0
// registration must receive a 1.4.2 event.
func TestDispatch_MajorVersionMatching(t *testing.T) {
	h := &stubHandler{}
	c := newDispatchConsumer(t, orderCreatedDef, h)

	newer := messaging.EventDef{Topic: "orders", Type: "order.created", Version: "1.4.2"}
	e, err := messagingtest.NewEvent(newer, orderCreated{OrderID: "o-1"})
	if err != nil {
		t.Fatalf("NewEvent: %v", err)
	}

	res, err := messagingtest.Dispatch(context.Background(), c, e)
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}

	if res.Outcome != messagingtest.OutcomeProcessed {
		t.Fatalf("got %v, want OutcomeProcessed", res.Outcome)
	}
}

func TestDispatch_MajorVersionMismatchSkips(t *testing.T) {
	h := &stubHandler{}
	c := newDispatchConsumer(t, orderCreatedDef, h)

	nextMajor := messaging.EventDef{Topic: "orders", Type: "order.created", Version: "2.0.0"}
	e, err := messagingtest.NewEvent(nextMajor, orderCreated{})
	if err != nil {
		t.Fatalf("NewEvent: %v", err)
	}

	res, err := messagingtest.Dispatch(context.Background(), c, e)
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}

	if res.Outcome != messagingtest.OutcomeSkipped {
		t.Fatalf("got %v, want OutcomeSkipped", res.Outcome)
	}
}

func TestDispatch_FailingDLQNacks(t *testing.T) {
	h := &stubHandler{err: messaging.AsPermanent(errors.New("bad"))}
	c := newDispatchConsumer(t, orderCreatedDef, h)

	res, err := messagingtest.Dispatch(context.Background(), c, newDispatchEvent(t),
		messagingtest.WithFailingDLQ(errors.New("dlq unavailable")))
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}

	// The event must stay pending rather than be acked into nothing.
	if res.Outcome != messagingtest.OutcomeNacked {
		t.Fatalf("got %v, want OutcomeNacked", res.Outcome)
	}
}

// Chaining: a recorded publish is a valid Dispatch input.
func TestDispatch_AcceptsARecordedPublish(t *testing.T) {
	pub := newTestPublisher(t)
	if _, err := pub.Publish(context.Background(), orderCreatedDef,
		orderCreated{OrderID: "o-1"}, messaging.WithCorrelationID("cid-1")); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	h := &stubHandler{}
	c := newDispatchConsumer(t, orderCreatedDef, h)

	res, err := messagingtest.Dispatch(context.Background(), c, pub.Published()[0])
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}

	if res.Outcome != messagingtest.OutcomeProcessed {
		t.Fatalf("got %v, want OutcomeProcessed", res.Outcome)
	}
}

// The default collapses the sleeps; WithRealBackoff must put them back,
// because a test asserting real timing behaviour needs the real loop.
func TestDispatch_RealBackoffSleeps(t *testing.T) {
	h := &stubHandler{err: errors.New("transient")}
	c := newDispatchConsumer(t, orderCreatedDef, h)

	start := time.Now()
	res, err := messagingtest.Dispatch(context.Background(), c, newDispatchEvent(t),
		messagingtest.WithRealBackoff())
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	elapsed := time.Since(start)

	if res.Outcome != messagingtest.OutcomeNacked {
		t.Fatalf("got %v, want OutcomeNacked", res.Outcome)
	}
	// Backoff is full jitter — a sleep in [0, upper) — so the only safe
	// lower bound is "more than the collapsed path could possibly take".
	// The default collapsed run finishes in microseconds.
	if elapsed < time.Millisecond {
		t.Fatalf("took %v; WithRealBackoff did not restore the sleeps", elapsed)
	}
}

func TestDispatch_NilConsumer(t *testing.T) {
	if _, err := messagingtest.Dispatch(context.Background(), nil, newDispatchEvent(t)); err == nil {
		t.Fatal("expected a harness error for a nil consumer")
	}
}

func TestOutcome_String(t *testing.T) {
	for _, tc := range []struct {
		outcome messagingtest.Outcome
		want    string
	}{
		{messagingtest.OutcomeProcessed, "processed"},
		{messagingtest.OutcomeSkipped, "skipped"},
		{messagingtest.OutcomeDeadLettered, "dead_lettered"},
		{messagingtest.OutcomeNacked, "nacked"},
	} {
		if got := tc.outcome.String(); got != tc.want {
			t.Errorf("got %q, want %q", got, tc.want)
		}
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./testing/ -run 'TestDispatch|TestOutcome' -v`
Expected: FAIL — `messagingtest.Dispatch` is undefined.

- [ ] **Step 3: Write minimal implementation**

Create `testing/dispatch.go`:

```go
package messagingtest

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"

	messaging "github.com/bcgov/foi-messaging-go"
	"github.com/bcgov/foi-messaging-go/internal/testseam"
	internalwatermill "github.com/bcgov/foi-messaging-go/internal/watermill"
)

// Outcome is the terminal verdict of a delivery.
type Outcome int

const (
	// OutcomeProcessed: the handler returned nil. Acked.
	OutcomeProcessed Outcome = iota
	// OutcomeSkipped: no handler matched, or the handler classified its
	// error with AsDiscard. Acked; see Result.Reason.
	OutcomeSkipped
	// OutcomeDeadLettered: written to the topic's DLQ and acked. See
	// Result.DeadLetters for the reason and the preserved event.
	OutcomeDeadLettered
	// OutcomeNacked: the entry stays pending for redelivery — a retryable
	// failure that exhausted its immediate retries, or a dead letter the
	// DLQ would not accept.
	OutcomeNacked
)

func (o Outcome) String() string {
	switch o {
	case OutcomeProcessed:
		return "processed"
	case OutcomeSkipped:
		return "skipped"
	case OutcomeDeadLettered:
		return "dead_lettered"
	case OutcomeNacked:
		return "nacked"
	default:
		return "unknown"
	}
}

// Result describes what the library would do with a delivery.
type Result struct {
	Outcome Outcome

	// Reason is the *skip* reason: "no_handler" or "discard". A dead
	// letter's reason lives on DeadLetters[0].Reason, alongside the rest
	// of the wrapper operational tooling actually sees — one place for it
	// rather than two that can disagree.
	Reason string

	// Category is the failure category when the delivery failed:
	// "permanent", "retryable", "deserialization", or "max_attempts".
	Category string

	// Err is the delivery's own error. It is not a harness failure —
	// Dispatch reports those through its second return value.
	Err error

	// DeadLetters holds every DeadLetter the delivery produced, decoded.
	DeadLetters []messaging.DeadLetter
}

type dispatchOptions struct {
	attempt     int64
	metadata    map[string]string
	realBackoff bool
	dlqErr      error
}

// DispatchOption customizes a Dispatch call.
type DispatchOption func(*dispatchOptions)

// WithDeliveryAttempt sets the delivery attempt number the consume path
// sees, which is how a test drives the delivery-attempt cap. Defaults to 1.
func WithDeliveryAttempt(n int64) DispatchOption {
	return func(o *dispatchOptions) { o.attempt = n }
}

// WithMetadata adds transport metadata to the delivery — a traceparent, a
// published_at. Keys the library sets itself take precedence.
func WithMetadata(md map[string]string) DispatchOption {
	return func(o *dispatchOptions) { o.metadata = md }
}

// WithRealBackoff restores the library's real jittered retry sleeps.
//
// By default Dispatch collapses them, because at the library defaults a
// single retryable failure would spend hundreds of milliseconds sleeping
// in a unit test. The retry *count* is honoured either way.
func WithRealBackoff() DispatchOption {
	return func(o *dispatchOptions) { o.realBackoff = true }
}

// WithFailingDLQ makes the dead-letter write fail with err, so a test can
// assert that an unwritable DLQ nacks rather than acking the event into
// nothing.
func WithFailingDLQ(err error) DispatchOption {
	return func(o *dispatchOptions) { o.dlqErr = err }
}

// Dispatch runs the library's real consume path against c and reports the
// terminal verdict, with no Redis involved.
//
// The subject is the application's own Consumer, built exactly as in
// production — messaging.NewConsumer plus its RegisterHandler calls — so
// the real registry lookup runs and the application's own wiring is under
// test too: that a 1.0.0 handler receives a 1.4.2 event, that an
// unregistered event type is skipped rather than erroring.
//
// Dispatch reports what the runtime *would do* with the delivery. It does
// not perform one: there is no ack, no nack, no pending entry, no reclaim.
//
// The returned error is a harness failure — a nil consumer, an
// unmarshalable event. A delivery that failed is not one of those; it is
// reported in Result.
func Dispatch(ctx context.Context, c *messaging.Consumer, e Event,
	opts ...DispatchOption) (Result, error) {
	if c == nil {
		return Result{}, fmt.Errorf("messagingtest: Dispatch needs a consumer, got nil")
	}

	o := dispatchOptions{attempt: 1}
	for _, opt := range opts {
		opt(&o)
	}

	body, err := json.Marshal(e.Envelope)
	if err != nil {
		return Result{}, fmt.Errorf("messagingtest: marshalling event: %w", err)
	}

	metadata := make(map[string]string, len(o.metadata)+1)
	for k, v := range o.metadata {
		metadata[k] = v
	}
	metadata[internalwatermill.MetadataDeliveryAttempt] = strconv.FormatInt(o.attempt, 10)

	// Guarded because a handler may dead-letter from more than one
	// goroutine in principle, and because -race should stay quiet on the
	// package's own tests.
	var (
		mu      sync.Mutex
		letters []messaging.DeadLetter
	)

	probe := &testseam.Probe{
		NoBackoff: !o.realBackoff,
		Sink: func(_ context.Context, _ string, body []byte) error {
			if o.dlqErr != nil {
				return o.dlqErr
			}
			var dl messaging.DeadLetter
			if err := json.Unmarshal(body, &dl); err != nil {
				return fmt.Errorf("messagingtest: decoding dead letter: %w", err)
			}
			mu.Lock()
			defer mu.Unlock()
			letters = append(letters, dl)
			return nil
		},
	}

	deliveryErr := testseam.Dispatch(ctx, c, e.Topic, body, metadata, probe)

	res := Result{
		Reason:      probe.Reason,
		Category:    probe.Category,
		Err:         deliveryErr,
		DeadLetters: letters,
	}
	// A dead-lettered delivery acks, so dispatch returns nil and the cause
	// is only on the probe.
	if res.Err == nil {
		res.Err = probe.Err
	}

	switch {
	case len(letters) > 0:
		res.Outcome = OutcomeDeadLettered
	case probe.Kind == testseam.KindFailed:
		res.Outcome = OutcomeNacked
	case probe.Kind == testseam.KindSkipped:
		res.Outcome = OutcomeSkipped
	default:
		res.Outcome = OutcomeProcessed
	}

	return res, nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./testing/ -run 'TestDispatch|TestOutcome' -v`
Expected: PASS — all fourteen tests.

Run: `go test -race -count=1 ./...`
Expected: PASS.

Run: `make lint`
Expected: clean. Note `testing/dispatch.go` imports our own `internal/watermill` for the metadata key — that is the single source of truth for it, and `depguard` denies only the three third-party modules, not our internal packages.

- [ ] **Step 5: Commit**

```bash
git add testing/dispatch.go testing/dispatch_test.go
git commit -m "feat: add messagingtest.Dispatch over the library's real consume path"
```

---

### Task 7: Doc examples and the integration cross-check

**Files:**
- Create: `testing/example_test.go`
- Create: `messagingtest_integration_test.go`
- Modify: `testing/doc.go`

**Interfaces:**
- Consumes: everything from Tasks 3–6.
- Produces: no new API.

- [ ] **Step 1: Write the failing test**

Create `messagingtest_integration_test.go` in the repository root:

This file reuses `consumeFixture` from `consumer_integration_test.go` (same
`messaging_test` package) rather than starting its own Redis, and reuses
`testsupport.ReadStreamEntries` — both already exist. **No new helper is
needed in `internal/testsupport`.**

```go
//go:build integration

package messaging_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	messaging "github.com/bcgov/foi-messaging-go"
	"github.com/bcgov/foi-messaging-go/internal/testsupport"
	messagingtest "github.com/bcgov/foi-messaging-go/testing"
)

type crosscheckPayload struct {
	Value string `json:"value"`
}

var crosscheckDef = messaging.EventDef{
	Topic:   "crosscheck",
	Type:    "crosscheck.event",
	Version: "1.0.0",
}

type crosscheckHandler struct{}

func (crosscheckHandler) Handle(context.Context, messaging.Envelope[crosscheckPayload]) error {
	return messaging.AsPermanent(errors.New("permanently broken"))
}

// The probe must reflect reality, not only itself. This runs one
// permanent-error scenario through real Redis and through Dispatch, and
// asserts the two produce the same DLQ reason.
func TestMessagingTest_MatchesRealRedisDLQReason(t *testing.T) {
	cfg := consumeFixture(t)
	ctx := context.Background()

	// ── the real path ────────────────────────────────────────────────
	consumer, err := messaging.NewConsumer(cfg)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	if err := messaging.RegisterHandler(consumer, crosscheckDef, crosscheckHandler{}); err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- consumer.Run(runCtx) }()

	publisher, err := messaging.NewPublisher(cfg)
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	t.Cleanup(func() { _ = publisher.Close() })

	if _, err := publisher.Publish(ctx, crosscheckDef, crosscheckPayload{Value: "v"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	realReason := awaitDLQReason(t, cfg.Redis.Address, "foi:crosscheck.dlq")

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}

	// ── the messagingtest path ───────────────────────────────────────
	fake, err := messaging.NewConsumer(messagingtest.Config())
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	t.Cleanup(func() { _ = fake.Close() })
	if err := messaging.RegisterHandler(fake, crosscheckDef, crosscheckHandler{}); err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}

	e, err := messagingtest.NewEvent(crosscheckDef, crosscheckPayload{Value: "v"})
	if err != nil {
		t.Fatalf("NewEvent: %v", err)
	}
	res, err := messagingtest.Dispatch(ctx, fake, e)
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}

	if len(res.DeadLetters) != 1 {
		t.Fatalf("got %d dead letters from Dispatch, want 1", len(res.DeadLetters))
	}
	if res.DeadLetters[0].Reason != realReason {
		t.Fatalf("Dispatch reported %q, real Redis reported %q",
			res.DeadLetters[0].Reason, realReason)
	}
	if res.DeadLetters[0].OriginalTopic != "crosscheck" {
		t.Fatalf("got original topic %q, want crosscheck",
			res.DeadLetters[0].OriginalTopic)
	}
}

// awaitDLQReason polls stream until a dead letter appears and returns its
// reason. The DLQ write and the ack are asynchronous to the publish, so
// polling is what the existing integration tests do here too.
func awaitDLQReason(t *testing.T, addr, stream string) string {
	t.Helper()
	ctx := context.Background()

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		entries, err := testsupport.ReadStreamEntries(ctx, addr, stream)
		if err != nil {
			t.Fatalf("ReadStreamEntries: %v", err)
		}
		if len(entries) > 0 {
			var dl messaging.DeadLetter
			if err := json.Unmarshal([]byte(entries[0].Fields["payload"]), &dl); err != nil {
				t.Fatalf("unmarshalling dead letter: %v", err)
			}
			return dl.Reason
		}
		time.Sleep(100 * time.Millisecond)
	}

	t.Fatalf("no dead letter appeared on %s within 30s", stream)
	return ""
}
```

Create `testing/example_test.go`:

```go
package messagingtest_test

import (
	"context"
	"errors"
	"fmt"

	messaging "github.com/bcgov/foi-messaging-go"
	messagingtest "github.com/bcgov/foi-messaging-go/testing"
)

func ExampleNewPublisher() {
	pub, err := messagingtest.NewPublisher()
	if err != nil {
		panic(err)
	}
	defer func() { _ = pub.Close() }()

	def := messaging.EventDef{Topic: "orders", Type: "order.created", Version: "1.0.0"}
	if _, err := pub.Publish(context.Background(), def,
		struct {
			OrderID string `json:"order_id"`
		}{OrderID: "o-1"}); err != nil {
		panic(err)
	}

	published := pub.Published()
	fmt.Println(len(published), published[0].Topic, published[0].Envelope.EventType)
	// Output: 1 orders order.created
}

type exampleHandler struct{}

func (exampleHandler) Handle(_ context.Context, env messaging.Envelope[exampleOrder]) error {
	if env.Payload.OrderID == "" {
		return messaging.AsPermanent(errors.New("order id is required"))
	}
	return nil
}

type exampleOrder struct {
	OrderID string `json:"order_id"`
}

func ExampleDeliver() {
	env := messaging.Envelope[exampleOrder]{
		EventID:       "evt-1",
		EventType:     "order.created",
		CorrelationID: "cid-1",
		Payload:       exampleOrder{OrderID: "o-1"},
	}

	fmt.Println(messagingtest.Deliver(context.Background(), exampleHandler{}, env))
	// Output: <nil>
}

func ExampleDispatch() {
	c, err := messaging.NewConsumer(messagingtest.Config())
	if err != nil {
		panic(err)
	}
	defer func() { _ = c.Close() }()

	def := messaging.EventDef{Topic: "orders", Type: "order.created", Version: "1.0.0"}
	if err := messaging.RegisterHandler(c, def, exampleHandler{}); err != nil {
		panic(err)
	}

	// An order with no id: the handler classifies that as permanent, so
	// the library dead-letters it rather than retrying.
	e, err := messagingtest.NewEvent(def, exampleOrder{})
	if err != nil {
		panic(err)
	}

	res, err := messagingtest.Dispatch(context.Background(), c, e)
	if err != nil {
		panic(err)
	}

	fmt.Println(res.Outcome, res.DeadLetters[0].Reason)
	// Output: dead_lettered permanent
}

// A two-service chain, with no Redis: service A's recorded publish is
// service B's input, correlation ID included.
func ExampleDispatch_chain() {
	pub, err := messagingtest.NewPublisher()
	if err != nil {
		panic(err)
	}
	defer func() { _ = pub.Close() }()

	def := messaging.EventDef{Topic: "orders", Type: "order.created", Version: "1.0.0"}
	if _, err := pub.Publish(context.Background(), def, exampleOrder{OrderID: "o-1"},
		messaging.WithCorrelationID("cid-1")); err != nil {
		panic(err)
	}

	c, err := messaging.NewConsumer(messagingtest.Config())
	if err != nil {
		panic(err)
	}
	defer func() { _ = c.Close() }()
	if err := messaging.RegisterHandler(c, def, exampleHandler{}); err != nil {
		panic(err)
	}

	res, err := messagingtest.Dispatch(context.Background(), c, pub.Published()[0])
	if err != nil {
		panic(err)
	}

	fmt.Println(res.Outcome, pub.Published()[0].Envelope.CorrelationID)
	// Output: processed cid-1
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./testing/ -run Example -v`
Expected: FAIL — the examples reference nothing new, so this should actually PASS once Tasks 3–6 are in. If any example fails, its `// Output:` comment is wrong; fix the comment to match reality rather than bending the code.

Run: `go test -tags=integration -run TestMessagingTest_MatchesRealRedisDLQReason ./ -v`
Expected: PASS. This test uses only APIs that already exist, so it should
pass as soon as Tasks 1–6 are in. If it fails, that is a genuine defect in
the probe wiring — the real path and `Dispatch` disagree — not a missing
helper. Debug it before continuing.

- [ ] **Step 3: Rewrite `testing/doc.go`**

```go
// Package messagingtest (imported from the testing/ directory) lets
// applications that consume this library unit-test their publish paths and
// handlers without a running Redis instance.
//
// It drives the library's real code rather than reimplementing it.
// Publisher wraps a genuine messaging.Publisher with only its transport
// write redirected, so envelope construction, correlation-ID resolution,
// and validation are the real ones; Dispatch runs the library's real
// consume path, so the delivery-attempt cap, error classification, the
// immediate-retry loop, and dead-lettering behave exactly as they do in
// production.
//
// Three boundaries, in increasing order of what they cover:
//
//	messagingtest.Deliver    // unit-test a handler
//	messagingtest.Dispatch   // test messaging and router behaviour
//	// real Redis + real messaging stack — see the integration test tier
//
// Deliver reproduces the handler boundary only: it installs the context
// values the router installs, invokes the handler, and returns its error.
// It does not retry, ack, nack, reclaim, or dead-letter. Dispatch covers
// all of that, reporting what the runtime would do with a delivery without
// performing one.
//
// What is deliberately absent: there is no in-memory Redis, no
// Consumer.Run substitute, and no assertion helpers — this package exposes
// accessors and leaves the assertions to the standard library's testing
// package, which it does not import.
//
// See docs/foi-messaging-go-prd-v1.1.md §19.
package messagingtest
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./testing/ -v`
Expected: PASS, including all four examples.

Run: `go test -tags=integration -run TestMessagingTest_MatchesRealRedisDLQReason ./ -v`
Expected: PASS — both paths report `permanent`.

- [ ] **Step 5: Commit**

```bash
git add testing/example_test.go testing/doc.go messagingtest_integration_test.go
git commit -m "test: add messagingtest doc examples and the real-Redis DLQ cross-check"
```

---

### Task 8: Documentation sweep

The phase is not done until the docs stop claiming Phase 4 is unimplemented. Past review rounds on this repository repeatedly caught exactly that.

**Files:**
- Modify: `doc.go` (line ~23)
- Modify: `README.md` (lines ~25, ~238, ~252, ~258)
- Modify: `CLAUDE.md` ("Implementation status", "Architecture")
- Modify: `docs/foi-messaging-go-prd-v1.1.md` (§19)

**Interfaces:**
- Consumes: the finished package from Tasks 1–7.
- Produces: no code.

- [ ] **Step 1: Verify the claims you are about to make**

Run: `make test && make test-examples && make lint`
Expected: all green.

Run: `go test -tags=integration -race -count=1 ./...`
Expected: PASS. This is what the repository treats as "green"; do not write "Phase 4 is done" without it.

Run: `go doc -all ./testing | head -60`
Use the real output to write the README section — do not describe an API from memory.

- [ ] **Step 2: Update `doc.go`**

Replace:

```go
// The application-facing testing package (Phase 4) is not yet implemented.
```

with:

```go
// The application-facing testing package is implemented: import
// github.com/bcgov/foi-messaging-go/testing to record publishes, invoke a
// handler at the router's boundary, and assert what the consume path would
// do with a delivery — all without Redis.
```

- [ ] **Step 3: Update `README.md`**

Line ~25 — drop the marker from the bullet:

```markdown
- A `testing/` package for unit-testing handlers and publish paths without Redis
```

Line ~238 — replace the `> **Planned for Phase 4** — ...` blockquote with this section, placed above the existing "The library's own suite has three tiers" paragraph:

````markdown
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
````

Line ~252 — the tree needs no change; `testing/` is already listed unannotated.

Line ~258 — replace the sentence:

```markdown
Applications import the top-level package, and `testing/` from their tests. All Watermill and Redis code stays in `internal/`, enforced by a golangci-lint `depguard` rule (CI enforcement is planned for a later phase).
```

- [ ] **Step 4: Update `CLAUDE.md`**

- "Implementation status": move Phase 4 out of "Not yet implemented" and state that all phases are complete.
- "Architecture": add a paragraph on `internal/testseam` — that it exists because `dispatch`, `contextWithCorrelationID`, and the DLQ sink are unexported and `dispatch` cannot move to `internal/` without an import cycle; that the probe travels on the context so `Dispatch` needs no lock and does not mutate the application's Consumer; and that the two `ctx.Value` lookups per delivery are the accepted price.
- "Commands": note that `go test ./...` now covers `testing/` too.

- [ ] **Step 5: Update PRD §19**

Under "Application Test Support", record the two cuts and the shipped shape:

- `messagingtest.Publisher` records published events; no `EventDef` builder ships, because a test that invents its own `EventDef` stops testing the application's real contract.
- `Deliver[T]` is the handler boundary; `Dispatch` is the router boundary and reports a terminal `Outcome`.
- There is no raw-bytes entry point, so literally-malformed JSON is covered by the integration tier rather than by `messagingtest`.

- [ ] **Step 6: Commit**

```bash
git add doc.go README.md CLAUDE.md docs/foi-messaging-go-prd-v1.1.md
git commit -m "docs: mark Phase 4 landed and document the testseam boundary"
```

---

## Finishing the branch

After Task 8, use the `superpowers:finishing-a-development-branch` skill. The repository's convention is a merge commit for the phase branch, and review findings get carried back into the spec at `docs/superpowers/specs/2026-08-13-phase-4-messagingtest-design.md`.

Final verification before merging:

```bash
make test-all
go test -tags=integration -race -count=1 ./...
make lint
```
