# Phase 2b: Retry, Delivery Cap, and DLQ — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace Phase 2a's blanket NACK with error classification, in-process immediate retry, a delivery-attempt cap, and DLQ routing, so a failing event reaches a bounded, observable end state instead of looping forever.

**Architecture:** Retry, the cap, and DLQ routing all live in the root `messaging` package inside `Consumer.dispatch` — not as Watermill middleware. The classification predicates live in the root package, and the root already imports `internal/watermill`, so a middleware there would be an import cycle. `internal/watermill` gains exactly one new thing: an `OnUndecodable` callback for entries Watermill's marshaller cannot read at all, which never reach `dispatch`.

**Tech Stack:** Go 1.25, Redis 7.0+, Watermill v1.5.2, watermill-redisstream v1.4.5, go-redis v9. Standard library `testing` only. Testcontainers for integration tests.

**Design of record:** [`docs/superpowers/specs/2026-08-11-phase-2b-retry-cap-dlq-design.md`](../specs/2026-08-11-phase-2b-retry-cap-dlq-design.md). PRD sections cited throughout are [`docs/foi-messaging-go-prd-v1.1.md`](../../foi-messaging-go-prd-v1.1.md).

## Global Constraints

- **No testify.** Standard library `testing` only, including test-only dependencies.
- **The root `messaging` package must never import Watermill or go-redis**, including types. `depguard` in `.golangci.yml` enforces this. `internal/redis` never imports Watermill; `internal/watermill` never imports go-redis; neither imports the root package.
- **Integration tests carry `//go:build integration`** and live in the external `_test` package (`package messaging_test`, `package watermill_test`).
- **Unit tests for unexported behaviour live in-package** (`package messaging`), as `consumer_test.go` already does.
- **Comment density is well above typical Go.** Comments explain *why*, especially where a subtle bug motivated the code. Match the surrounding files.
- **Commit style:** `feat:` / `fix:` / `test:` / `docs:`. No `Co-Authored-By` trailer.
- **JSON decoding stays lenient** — never `DisallowUnknownFields`.
- **Verification before claiming green:** `go test -tags=integration -race -count=1 ./...` plus `golangci-lint run ./...`.

## File Structure

| File | Responsibility | Status |
|---|---|---|
| `errors.go` | `AsPermanent`/`AsRetryable`/`AsDiscard`, `IsPermanent`/`IsRetryable`/`IsDiscard` | Replace stub comment |
| `errors_test.go` | Classification round-trips, `errors.Is`/`As` composition | Create |
| `dlq.go` | `DeadLetter`, reason constants, `deadLetterBody` | Replace stub comment |
| `dlq_test.go` | Wire shape of the `DeadLetter` document | Create |
| `config.go` | `backoffUpperBound`, `worstCaseBackoff`, negative-retry validation, the `ClaimMinIdle` rule, doc restatements | Modify |
| `config_test.go` | Backoff arithmetic and the new validation rule | Modify |
| `consumer.go` | `deadLetterSink`, `newDeadLetter`, `deadLetter`, `deliveryAttempt`, `runWithRetry`, `sleepWithJitter`, cap + DLQ ordering in `dispatch`, `Run` wiring | Modify |
| `consumer_test.go` | Cap, DLQ routing, retry loop; four existing tests rewritten | Modify |
| `internal/watermill/subscriber.go` | `OnUndecodable` option and its use in `emit` | Modify |
| `internal/watermill/subscriber_test.go` | The hook's ack/leave-pending behaviour | Modify |
| `consumer_integration_test.go` | Per-topic isolation, poison backlog, DLQ contents; one existing test retuned | Modify |
| `README.md`, `doc.go`, `CLAUDE.md` | Phase markers | Modify |

---

### Task 1: Error classification

**Files:**
- Modify: `errors.go` (replace the 5-line stub comment entirely)
- Test: `errors_test.go` (create)

**Interfaces:**
- Consumes: nothing.
- Produces: `AsPermanent(error) error`, `AsRetryable(error) error`, `AsDiscard(error) error`, `IsPermanent(error) bool`, `IsRetryable(error) bool`, `IsDiscard(error) bool`. Tasks 5–7 use the `Is*` predicates.

- [ ] **Step 1: Write the failing tests**

Create `errors_test.go`:

```go
package messaging

import (
	"errors"
	"fmt"
	"testing"
)

var errSentinel = errors.New("sentinel")

func TestClassification_PredicatesMatchConstructors(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		permanent bool
		retryable bool
		discard   bool
	}{
		{"permanent", AsPermanent(errSentinel), true, false, false},
		{"retryable", AsRetryable(errSentinel), false, true, false},
		{"discard", AsDiscard(errSentinel), false, false, true},
		// PRD §15: an unwrapped error is retryable by default. Misclassifying
		// a transient failure as permanent silently loses work to the DLQ;
		// the reverse is caught by the delivery-attempt cap.
		{"unclassified", errSentinel, false, true, false},
		{"nil", nil, false, false, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsPermanent(tc.err); got != tc.permanent {
				t.Errorf("IsPermanent = %v, want %v", got, tc.permanent)
			}
			if got := IsRetryable(tc.err); got != tc.retryable {
				t.Errorf("IsRetryable = %v, want %v", got, tc.retryable)
			}
			if got := IsDiscard(tc.err); got != tc.discard {
				t.Errorf("IsDiscard = %v, want %v", got, tc.discard)
			}
		})
	}
}

func TestClassification_PreservesTheWrappedError(t *testing.T) {
	err := AsPermanent(fmt.Errorf("loading account: %w", errSentinel))

	if !errors.Is(err, errSentinel) {
		t.Error("errors.Is must see through the classification wrapper")
	}
	if got, want := err.Error(), "loading account: sentinel"; got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}
}

func TestClassification_SurvivesOuterWrapping(t *testing.T) {
	// Handlers classify, then a caller adds context with %w. The
	// classification must still be visible or every wrapped permanent
	// failure would silently become retryable and burn the whole cap.
	err := fmt.Errorf("processing document 42: %w", AsPermanent(errSentinel))

	if !IsPermanent(err) {
		t.Error("IsPermanent must find a classification nested under %w wrapping")
	}
	if IsRetryable(err) {
		t.Error("a nested permanent error must not report as retryable")
	}
}

func TestClassification_OutermostWins(t *testing.T) {
	// Re-classifying is legal: an outer layer that knows more overrides an
	// inner verdict. Documented so the behaviour is not accidental.
	err := AsDiscard(AsPermanent(errSentinel))

	if !IsDiscard(err) {
		t.Error("the outermost classification must win")
	}
	if IsPermanent(err) {
		t.Error("the overridden inner classification must not leak through")
	}
}

func TestClassification_NilStaysNil(t *testing.T) {
	// Wrapping "no error" as a failure would turn a successful handler
	// return into a dead letter.
	if AsPermanent(nil) != nil {
		t.Error("AsPermanent(nil) must be nil")
	}
	if AsRetryable(nil) != nil {
		t.Error("AsRetryable(nil) must be nil")
	}
	if AsDiscard(nil) != nil {
		t.Error("AsDiscard(nil) must be nil")
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test -run TestClassification ./...`
Expected: FAIL — `undefined: AsPermanent`, `undefined: IsPermanent`, and so on.

- [ ] **Step 3: Write the implementation**

Replace the entire contents of `errors.go`:

```go
package messaging

import "errors"

// classification is the failure category a handler error carries.
// Applications attach one by wrapping with AsPermanent, AsRetryable, or
// AsDiscard; the consume path reads it back with the Is* predicates.
// See PRD §15.
type classification int

const (
	classPermanent classification = iota + 1
	classRetryable
	classDiscard
)

// classifiedError carries a classification alongside the error it wraps.
//
// Unwrap is what lets errors.Is and errors.As chain straight through it. A
// handler that wraps its own sentinel must stay matchable by its caller, so
// classification has to be additive rather than a replacement.
type classifiedError struct {
	class classification
	err   error
}

func (e *classifiedError) Error() string { return e.err.Error() }
func (e *classifiedError) Unwrap() error { return e.err }

// AsPermanent marks err as a failure that will not resolve on retry — a
// validation failure, an unresolvable reference. The consume path routes it
// straight to the DLQ and acks, skipping both immediate retry and the
// delivery-attempt cap.
//
// A nil err returns nil: wrapping "no error" would turn a successful
// handler return into a dead letter.
func AsPermanent(err error) error { return classify(classPermanent, err) }

// AsRetryable marks err as transient. It is optional — an unclassified
// error is already retryable (PRD §15) — and exists so handlers can say so
// deliberately rather than by omission.
func AsRetryable(err error) error { return classify(classRetryable, err) }

// AsDiscard marks err as a failure the application wants dropped: the event
// is logged at warn and acked, with no retry and no DLQ entry.
func AsDiscard(err error) error { return classify(classDiscard, err) }

func classify(c classification, err error) error {
	if err == nil {
		return nil
	}
	return &classifiedError{class: c, err: err}
}

// classOf returns the outermost classification in err's chain.
//
// Outermost wins: errors.As walks from the outside in, so an outer layer
// that re-classifies overrides an inner verdict. That is the useful
// direction — the outer layer has strictly more context — and fixing it in
// one place keeps all three predicates consistent.
func classOf(err error) (classification, bool) {
	var ce *classifiedError
	if errors.As(err, &ce) {
		return ce.class, true
	}
	return 0, false
}

// IsPermanent reports whether err is classified permanent.
func IsPermanent(err error) bool {
	c, ok := classOf(err)
	return ok && c == classPermanent
}

// IsDiscard reports whether err is classified discard.
func IsDiscard(err error) bool {
	c, ok := classOf(err)
	return ok && c == classDiscard
}

// IsRetryable reports whether err should be retried, which an unclassified
// error is by default (PRD §15).
//
// The retry loop does not call this — retrying is its fallthrough — but
// operational tooling and tests do, and the default has to be stated
// somewhere executable rather than only in a comment.
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}
	c, ok := classOf(err)
	return !ok || c == classRetryable
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test -race -run TestClassification ./...`
Expected: PASS (5 tests, all subtests).

- [ ] **Step 5: Lint**

Run: `golangci-lint run ./...`
Expected: no findings.

- [ ] **Step 6: Commit**

```bash
git add errors.go errors_test.go
git commit -m "feat: add handler error classification

AsPermanent/AsRetryable/AsDiscard and their predicates, per PRD §15.
Unclassified errors are retryable by default; the outermost
classification wins so an outer layer can override an inner verdict."
```

---

### Task 2: The DeadLetter contract

**Files:**
- Modify: `dlq.go` (replace the 6-line stub comment entirely)
- Test: `dlq_test.go` (create)

**Interfaces:**
- Consumes: nothing.
- Produces: the `DeadLetter` struct; `ReasonPermanent`, `ReasonMaxAttemptsExceeded`, `ReasonDeserializationFailed` constants; `deadLetterBody([]byte) (json.RawMessage, []byte)`. Tasks 4–8 build `DeadLetter` values.

- [ ] **Step 1: Write the failing tests**

Create `dlq_test.go`:

```go
package messaging

import (
	"encoding/json"
	"testing"
	"time"
)

func TestDeadLetter_WireShapeMatchesTheContract(t *testing.T) {
	dl := DeadLetter{
		DeadLetteredAt:   time.Date(2026, 4, 23, 10, 5, 12, 0, time.UTC),
		Reason:           ReasonPermanent,
		Error:            "invoice 42 references unknown account",
		DeliveryAttempts: 5,
		ConsumerGroup:    "billing-service",
		ConsumerName:     "billing-7f9c4",
		OriginalTopic:    "documents",
		Event:            json.RawMessage(`{"event_id":"abc"}`),
	}

	body, err := json.Marshal(dl)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	// Field names are the operational contract PRD §14 publishes; replay
	// tooling reads them, so a rename is a breaking change.
	var got map[string]json.RawMessage
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	for _, key := range []string{
		"dead_lettered_at", "reason", "error", "delivery_attempts",
		"consumer_group", "consumer_name", "original_topic", "event",
	} {
		if _, ok := got[key]; !ok {
			t.Errorf("missing field %q in %s", key, body)
		}
	}
	if _, ok := got["event_raw"]; ok {
		t.Error("event_raw must be omitted when event is set")
	}
}

func TestDeadLetter_EventPreservedByteForByte(t *testing.T) {
	// PRD §14: replay tooling republishes event without transformation, so
	// key order and spacing must survive the round trip.
	original := `{"event_id":"abc","payload":{"b":1,"a":2}}`
	dl := DeadLetter{Reason: ReasonPermanent, Event: json.RawMessage(original)}

	body, err := json.Marshal(dl)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	var got DeadLetter
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if string(got.Event) != original {
		t.Errorf("Event = %s, want %s", got.Event, original)
	}
}

func TestDeadLetterBody_RoutesByValidity(t *testing.T) {
	tests := []struct {
		name     string
		payload  string
		wantRaw  bool
	}{
		{"valid json goes to event", `{"event_id":"abc"}`, false},
		{"invalid json goes to event_raw", `not json at all`, true},
		{"truncated json goes to event_raw", `{"event_id":`, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			event, raw := deadLetterBody([]byte(tc.payload))
			if tc.wantRaw {
				if event != nil {
					t.Errorf("Event = %s, want nil", event)
				}
				if string(raw) != tc.payload {
					t.Errorf("EventRaw = %s, want %s", raw, tc.payload)
				}
				return
			}
			if raw != nil {
				t.Errorf("EventRaw = %s, want nil", raw)
			}
			if string(event) != tc.payload {
				t.Errorf("Event = %s, want %s", event, tc.payload)
			}
		})
	}
}

func TestDeadLetter_UnparseableBytesSurviveAsBase64(t *testing.T) {
	// The whole point of event_raw: a DeadLetter carrying unparseable input
	// must itself still be valid JSON, or the DLQ entry cannot be read back
	// by the tooling the DLQ exists for.
	dl := DeadLetter{Reason: ReasonDeserializationFailed, EventRaw: []byte(`not json`)}

	body, err := json.Marshal(dl)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if !json.Valid(body) {
		t.Fatalf("dead letter document is not valid JSON: %s", body)
	}

	var got DeadLetter
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if string(got.EventRaw) != "not json" {
		t.Errorf("EventRaw = %q, want %q", got.EventRaw, "not json")
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test -run 'TestDeadLetter' ./...`
Expected: FAIL — `undefined: DeadLetter`, `undefined: ReasonPermanent`, `undefined: deadLetterBody`.

- [ ] **Step 3: Write the implementation**

Replace the entire contents of `dlq.go`:

```go
package messaging

import (
	"encoding/json"
	"time"
)

// DLQ reason values, fixed by PRD §14. Exported so operational tooling
// consuming DLQ streams can compare against them rather than against string
// literals of its own.
const (
	// ReasonPermanent is a handler error classified with AsPermanent.
	ReasonPermanent = "permanent"
	// ReasonMaxAttemptsExceeded is the delivery-attempt cap firing,
	// regardless of how the failures were classified.
	ReasonMaxAttemptsExceeded = "max_attempts_exceeded"
	// ReasonDeserializationFailed covers every event that could not be read
	// well enough to dispatch: invalid JSON, a envelope failing validation,
	// an unparseable schema version, or a stream entry Watermill's own
	// marshaller rejected.
	ReasonDeserializationFailed = "deserialization_failed"
)

// DeadLetter is the wrapper written to a topic's DLQ stream.
//
// PRD §5 forbids failure metadata inside the event, so the original event
// travels verbatim alongside the metadata rather than being modified to
// carry it. That is what lets replay tooling republish the event without
// transformation.
//
// Exactly one of Event and EventRaw is set. Event holds the original bytes
// when they were valid JSON; EventRaw holds them — base64-encoded by
// encoding/json — when they were not, which is the only case where
// preserving unparseable input is what matters.
type DeadLetter struct {
	DeadLetteredAt   time.Time       `json:"dead_lettered_at"`
	Reason           string          `json:"reason"`
	Error            string          `json:"error"`
	DeliveryAttempts int64           `json:"delivery_attempts"`
	ConsumerGroup    string          `json:"consumer_group"`
	ConsumerName     string          `json:"consumer_name"`
	OriginalTopic    string          `json:"original_topic"`
	Event            json.RawMessage `json:"event,omitempty"`
	EventRaw         []byte          `json:"event_raw,omitempty"`
}

// deadLetterBody assigns payload to Event when it is valid JSON and to
// EventRaw otherwise.
//
// The validity check is load-bearing, not defensive: json.RawMessage is
// spliced into the document unquoted, so unparseable bytes placed there
// would make the DeadLetter itself invalid JSON — unreadable by the tooling
// it exists for, and undetectable until someone tried to replay it.
func deadLetterBody(payload []byte) (json.RawMessage, []byte) {
	if json.Valid(payload) {
		return json.RawMessage(payload), nil
	}
	return nil, payload
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test -race -run 'TestDeadLetter' ./...`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add dlq.go dlq_test.go
git commit -m "feat: add the DeadLetter wrapper contract

PRD §14's DLQ document: the original event verbatim in event, or the
raw bytes in event_raw when they could not be parsed. Splicing
unparseable bytes into event would make the wrapper itself invalid
JSON, so deadLetterBody routes on json.Valid."
```

---

### Task 3: Backoff arithmetic and config validation

**Files:**
- Modify: `config.go:29-76` (doc comments), `config.go:115-149` (`Validate`), `config.go:157-215` (`validateConsumer`)
- Test: `config_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `backoffUpperBound(RetryConfig, int) time.Duration` and `worstCaseBackoff(RetryConfig) time.Duration`. Task 7's retry loop calls `backoffUpperBound`.

**Why both functions:** `worstCaseBackoff` must agree with what the retry loop actually sleeps, or the validation rule guards a number nothing produces. Defining the per-attempt bound once and summing it is what keeps them from drifting.

- [ ] **Step 1: Write the failing tests**

Add to `config_test.go`:

```go
func TestBackoffUpperBound_DoublesThenCaps(t *testing.T) {
	r := RetryConfig{
		MaxImmediateRetries: 6,
		InitialBackoff:      100 * time.Millisecond,
		MaxBackoff:          500 * time.Millisecond,
	}

	want := []time.Duration{
		100 * time.Millisecond,
		200 * time.Millisecond,
		400 * time.Millisecond,
		500 * time.Millisecond, // capped
		500 * time.Millisecond,
		500 * time.Millisecond,
	}
	for i, w := range want {
		if got := backoffUpperBound(r, i); got != w {
			t.Errorf("backoffUpperBound(%d) = %v, want %v", i, got, w)
		}
	}
}

func TestWorstCaseBackoff_SumsTheDefaults(t *testing.T) {
	// The library defaults: 100ms + 200ms + 400ms across three retries.
	r := RetryConfig{
		MaxImmediateRetries: 3,
		InitialBackoff:      100 * time.Millisecond,
		MaxBackoff:          5 * time.Second,
	}
	if got, want := worstCaseBackoff(r), 700*time.Millisecond; got != want {
		t.Errorf("worstCaseBackoff = %v, want %v", got, want)
	}
}

func TestWorstCaseBackoff_ZeroRetriesIsZero(t *testing.T) {
	r := RetryConfig{MaxImmediateRetries: 0, InitialBackoff: time.Second, MaxBackoff: time.Minute}
	if got := worstCaseBackoff(r); got != 0 {
		t.Errorf("worstCaseBackoff = %v, want 0", got)
	}
}

func TestValidateConsumer_RejectsBackoffReachingClaimMinIdle(t *testing.T) {
	// 10 retries capped at 30s is ~150s of backoff against a 60s
	// ClaimMinIdle: the entry is reclaimed and processed a second time by
	// this same process before the first delivery has finished sleeping.
	cfg := Config{
		Source: "test.service",
		Redis:  RedisConfig{Address: "localhost:6379"},
		Consumer: ConsumerConfig{
			Group:        "test-group",
			ClaimMinIdle: 60 * time.Second,
		},
		Retry: RetryConfig{
			MaxImmediateRetries: 10,
			InitialBackoff:      time.Second,
			MaxBackoff:          30 * time.Second,
		},
	}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	err := cfg.validateConsumer()
	if err == nil {
		t.Fatal("expected worst-case backoff exceeding ClaimMinIdle to be rejected")
	}
	if !strings.Contains(err.Error(), "ClaimMinIdle") {
		t.Errorf("error should name ClaimMinIdle, got %q", err)
	}
}

func TestValidateConsumer_AcceptsDefaultBackoffAgainstDefaultClaimMinIdle(t *testing.T) {
	cfg := Config{
		Source:   "test.service",
		Redis:    RedisConfig{Address: "localhost:6379"},
		Consumer: ConsumerConfig{Group: "test-group"},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if err := cfg.validateConsumer(); err != nil {
		t.Fatalf("the library's own defaults must validate: %v", err)
	}
}

func TestValidate_RejectsNegativeRetryFields(t *testing.T) {
	// Zero means "use the default" for every field in this config, so a
	// negative value would otherwise sail past defaulting into the backoff
	// arithmetic.
	tests := []struct {
		name  string
		retry RetryConfig
		field string
	}{
		{"retries", RetryConfig{MaxImmediateRetries: -1}, "MaxImmediateRetries"},
		{"initial", RetryConfig{InitialBackoff: -time.Second}, "InitialBackoff"},
		{"max", RetryConfig{MaxBackoff: -time.Second}, "MaxBackoff"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{
				Source: "test.service",
				Redis:  RedisConfig{Address: "localhost:6379"},
				Retry:  tc.retry,
			}
			err := cfg.Validate()
			if err == nil {
				t.Fatalf("expected a negative %s to be rejected", tc.field)
			}
			if !strings.Contains(err.Error(), tc.field) {
				t.Errorf("error should name %s, got %q", tc.field, err)
			}
		})
	}
}
```

If `config_test.go` does not already import `strings` and `time`, add them.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test -run 'TestBackoff|TestWorstCase|TestValidateConsumer_Rejects|TestValidateConsumer_Accepts|TestValidate_RejectsNegativeRetry' ./...`
Expected: FAIL — `undefined: backoffUpperBound`, `undefined: worstCaseBackoff`, and the two validation tests failing with "expected ... to be rejected".

- [ ] **Step 3: Add the backoff arithmetic**

Append to `config.go`:

```go
// backoffUpperBound returns the upper bound of retry i's jittered sleep:
// InitialBackoff doubled per retry, capped at MaxBackoff (PRD §13 Layer 1).
// i is zero-based, so retry 0 is the sleep before the second attempt.
//
// The retry loop jitters within this bound rather than sleeping it exactly;
// worstCaseBackoff sums it. Both callers go through here so the number
// config validation guards is the number the loop can actually spend.
func backoffUpperBound(r RetryConfig, i int) time.Duration {
	d := r.InitialBackoff
	for range i {
		if d >= r.MaxBackoff {
			return r.MaxBackoff
		}
		d *= 2
	}
	if d > r.MaxBackoff {
		return r.MaxBackoff
	}
	return d
}

// worstCaseBackoff is the total time the retry loop can spend sleeping
// across all immediate retries, taking every jittered sleep at its bound.
//
// It exists because it is the only part of the ClaimMinIdle invariant that
// is computable at construction: handler duration, the other term, is not
// knowable until the handler runs.
func worstCaseBackoff(r RetryConfig) time.Duration {
	var total time.Duration
	for i := range r.MaxImmediateRetries {
		total += backoffUpperBound(r, i)
	}
	return total
}
```

- [ ] **Step 4: Add the negative-field checks to `Validate`**

In `config.go`, insert immediately before the `if c.Retry.MaxImmediateRetries == 0 {` defaulting block (currently `config.go:129`):

```go
	// Zero means "use the default" for all three, so a negative value would
	// otherwise skip defaulting entirely and feed the backoff arithmetic a
	// nonsense bound.
	if c.Retry.MaxImmediateRetries < 0 {
		return fmt.Errorf("config: Retry.MaxImmediateRetries must not be negative, got %d", c.Retry.MaxImmediateRetries)
	}
	if c.Retry.InitialBackoff < 0 {
		return fmt.Errorf("config: Retry.InitialBackoff must not be negative, got %v", c.Retry.InitialBackoff)
	}
	if c.Retry.MaxBackoff < 0 {
		return fmt.Errorf("config: Retry.MaxBackoff must not be negative, got %v", c.Retry.MaxBackoff)
	}
```

- [ ] **Step 5: Add the ClaimMinIdle rule to `validateConsumer`**

In `config.go`, insert after the existing `ClaimMinIdle < ClaimInterval` check and before the `ConsumerName` defaulting block (currently between `config.go:204` and `config.go:206`):

```go
	// Layer 1 retry sleeps inside the handler goroutine, which holds the
	// message's per-topic concurrency slot for the whole loop. Backoff
	// therefore counts against ClaimMinIdle exactly as handler runtime
	// does. A config whose backoff alone reaches ClaimMinIdle guarantees
	// the entry is reclaimed — and processed a second time by this very
	// process — before the first delivery has finished sleeping, with zero
	// handler runtime needed to trigger it.
	if wc := worstCaseBackoff(c.Retry); wc >= c.Consumer.ClaimMinIdle {
		return fmt.Errorf(
			"config: worst-case retry backoff (%v) must be < Consumer.ClaimMinIdle (%v); "+
				"lower Retry.MaxImmediateRetries or Retry.MaxBackoff, or raise Consumer.ClaimMinIdle",
			wc, c.Consumer.ClaimMinIdle,
		)
	}
```

- [ ] **Step 6: Restate the doc comments Phase 2b redefines**

In `config.go`, replace the `ClaimMinIdle` doc comment (currently `config.go:54-60`) with:

```go
	// ClaimMinIdle is how long an entry must sit unacknowledged before
	// another consumer may reclaim it. It must be >= ClaimInterval, and it
	// must also exceed the longest one delivery can occupy its concurrency
	// slot — which, with immediate retry, is
	//
	//	(1 + Retry.MaxImmediateRetries) × handler duration + worst-case backoff
	//
	// not one handler run. A 20s handler at the defaults occupies its slot
	// for up to 80.7s, so the 60s default is already too short for it: the
	// entry is reclaimed and processed concurrently by this same process
	// while the first delivery is still retrying.
	//
	// The backoff term alone is checked at construction; the handler term
	// cannot be. Defaults to 60s.
	ClaimMinIdle time.Duration
```

Replace the `MaxDeliveryAttempts` doc comment (currently `config.go:62-63`) with:

```go
	// MaxDeliveryAttempts bounds redeliveries. On the delivery whose
	// attempt exceeds it, the event is dead-lettered and acked before it is
	// even decoded — regardless of how its failures were classified
	// (PRD §13 Layer 3). Attempts 1..MaxDeliveryAttempts dispatch;
	// attempt MaxDeliveryAttempts+1 is dead-lettered. Defaults to 5.
	MaxDeliveryAttempts int
```

Replace the `ShutdownTimeout` doc comment (currently `config.go:65-67`) with:

```go
	// ShutdownTimeout bounds the drain of in-flight handlers after the Run
	// context is cancelled.
	//
	// Immediate retry extends the drain: a message that fails on the last
	// attempt before shutdown can still spend
	// Retry.MaxImmediateRetries × handler duration + worst-case backoff
	// finishing, across Concurrency × (number of subscribed topics)
	// messages. Retries are deliberately not interrupted by shutdown —
	// message contexts stay live for the whole drain — so budget for it
	// here. Defaults to 30s.
	ShutdownTimeout time.Duration
```

Replace the `RetryConfig` doc comment (currently `config.go:70-76`) with:

```go
// RetryConfig configures the in-process immediate-retry layer (PRD §13
// Layer 1): the retries a handler gets within one delivery, before the
// message is nacked and left for the reclaim loop.
//
// Zero values mean "use the default" — 3 retries, 100ms initial, 5s max —
// so there is no way to express "no retries" by zeroing MaxImmediateRetries.
// Set InitialBackoff and MaxBackoff to a nanosecond in tests that need the
// loop to run without waiting.
//
// Retries sleep inside the handler's concurrency slot, so these values
// interact with Consumer.ClaimMinIdle; see its documentation.
type RetryConfig struct {
	// MaxImmediateRetries is the number of retries after the first
	// attempt, so a message gets 1+MaxImmediateRetries invocations per
	// delivery. Defaults to 3.
	MaxImmediateRetries int

	// InitialBackoff is the upper bound of the first retry's jittered
	// sleep, doubling per retry. Defaults to 100ms.
	InitialBackoff time.Duration

	// MaxBackoff caps that doubling. Defaults to 5s.
	MaxBackoff time.Duration
}
```

- [ ] **Step 7: Run the tests to verify they pass**

Run: `go test -race ./...`
Expected: PASS. The whole unit suite must still pass — this task changes validation, and `config_test.go` has existing coverage of both functions.

- [ ] **Step 8: Lint and commit**

```bash
golangci-lint run ./...
git add config.go config_test.go
git commit -m "feat: validate retry backoff against ClaimMinIdle

Layer 1 retry sleeps inside the message's concurrency slot, so backoff
counts against ClaimMinIdle exactly as handler runtime does. The
backoff term is computable at construction, so reject configs where it
alone guarantees self-reclaim. Restates the ClaimMinIdle,
MaxDeliveryAttempts, and ShutdownTimeout docs in retry's terms."
```

---

### Task 4: The dead letter sink

**Files:**
- Modify: `consumer.go:20-28` (struct), `consumer.go:150-185` (`Run` wiring), and append `newDeadLetter` / `deadLetter`
- Test: `consumer_test.go`

**Interfaces:**
- Consumes: `DeadLetter`, `deadLetterBody`, the `Reason*` constants (Task 2).
- Produces:
  - `type deadLetterSink interface { publish(ctx context.Context, stream string, body []byte) error }`
  - `func (c *Consumer) newDeadLetter(topic, reason string, cause error, attempt int64) DeadLetter`
  - `func (c *Consumer) deadLetter(ctx context.Context, topic string, dl DeadLetter) error`
  - `Consumer.dlq deadLetterSink` field
  - Tasks 5–8 call `newDeadLetter` then `deadLetter`.

**Why an interface:** `dispatch` is unit-tested in-package with no Redis. The sink is the seam that lets those tests assert on the DeadLetter that *would* be published.

- [ ] **Step 1: Write the failing tests**

Add to `consumer_test.go`:

```go
// recordingSink captures dead letters instead of publishing them, so
// dispatch's DLQ routing can be asserted without Redis.
type recordingSink struct {
	mu      sync.Mutex
	streams []string
	bodies  [][]byte
	err     error
}

func (s *recordingSink) publish(_ context.Context, stream string, body []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	s.streams = append(s.streams, stream)
	s.bodies = append(s.bodies, body)
	return nil
}

func (s *recordingSink) only(t *testing.T) DeadLetter {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.bodies) != 1 {
		t.Fatalf("expected exactly 1 dead letter, got %d", len(s.bodies))
	}
	var dl DeadLetter
	if err := json.Unmarshal(s.bodies[0], &dl); err != nil {
		t.Fatalf("unmarshalling dead letter: %v", err)
	}
	return dl
}

func (s *recordingSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.bodies)
}

func TestDeadLetter_WritesToTheTopicsDLQStream(t *testing.T) {
	consumer, err := NewConsumer(testConsumerConfig())
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	sink := &recordingSink{}
	consumer.dlq = sink

	dl := consumer.newDeadLetter("documents", ReasonPermanent, errors.New("boom"), 3)
	dl.Event = json.RawMessage(`{"event_id":"abc"}`)
	if err := consumer.deadLetter(context.Background(), "documents", dl); err != nil {
		t.Fatalf("deadLetter: %v", err)
	}

	if got, want := sink.streams[0], "foi:documents.dlq"; got != want {
		t.Errorf("stream = %q, want %q", got, want)
	}
	got := sink.only(t)
	if got.Reason != ReasonPermanent {
		t.Errorf("Reason = %q, want %q", got.Reason, ReasonPermanent)
	}
	if got.Error != "boom" {
		t.Errorf("Error = %q, want %q", got.Error, "boom")
	}
	if got.DeliveryAttempts != 3 {
		t.Errorf("DeliveryAttempts = %d, want 3", got.DeliveryAttempts)
	}
	if got.OriginalTopic != "documents" {
		t.Errorf("OriginalTopic = %q, want %q", got.OriginalTopic, "documents")
	}
	if got.ConsumerGroup != "test-group" {
		t.Errorf("ConsumerGroup = %q, want %q", got.ConsumerGroup, "test-group")
	}
	if got.ConsumerName == "" {
		t.Error("ConsumerName must be set so an operator can identify the instance")
	}
	if got.DeadLetteredAt.IsZero() {
		t.Error("DeadLetteredAt must be set")
	}
}

func TestDeadLetter_PublishFailureReturnsAnError(t *testing.T) {
	// The caller nacks on a non-nil return. Acking here would drop the
	// event with nothing anywhere holding it (PRD §14).
	consumer, err := NewConsumer(testConsumerConfig())
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	consumer.dlq = &recordingSink{err: errors.New("redis down")}

	dl := consumer.newDeadLetter("documents", ReasonPermanent, errors.New("boom"), 1)
	if err := consumer.deadLetter(context.Background(), "documents", dl); err == nil {
		t.Error("expected a DLQ publish failure to be reported so the message nacks")
	}
}

func TestDeadLetter_WithoutASinkReturnsAnError(t *testing.T) {
	// Only reachable from a Consumer that was constructed but never run.
	// Erroring nacks, which is the safe direction.
	consumer, err := NewConsumer(testConsumerConfig())
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}

	dl := consumer.newDeadLetter("documents", ReasonPermanent, errors.New("boom"), 1)
	if err := consumer.deadLetter(context.Background(), "documents", dl); err == nil {
		t.Error("expected an error when no dead letter sink is configured")
	}
}
```

Add `"sync"` to `consumer_test.go`'s imports if absent.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test -run 'TestDeadLetter_' ./...`
Expected: FAIL — `consumer.dlq undefined`, `consumer.newDeadLetter undefined`.

- [ ] **Step 3: Add the sink type and the Consumer field**

In `consumer.go`, add to the imports: `"time"`, `"github.com/google/uuid"`.

Add the field to the `Consumer` struct (`consumer.go:20-28`), inside the mutex-guarded group:

```go
type Consumer struct {
	cfg Config

	mu       sync.Mutex
	registry *registry
	running  bool
	// dlq is nil until Run builds it. Guarded by mu for the same reason
	// reader is: Run writes it while callers may be reading.
	dlq deadLetterSink

	reader *internalredis.StreamReader
}
```

Append to `consumer.go`:

```go
// deadLetterSink publishes DeadLetter documents to a DLQ stream.
//
// It is an interface so dispatch's DLQ routing is unit-testable without
// Redis: the failure paths it guards are exactly the ones hardest to
// provoke against a live broker.
type deadLetterSink interface {
	publish(ctx context.Context, stream string, body []byte) error
}

// redisDeadLetterSink writes dead letters through the Redis client Run
// already holds for the reader.
type redisDeadLetterSink struct {
	pub *internalwatermill.Publisher
}

func (s redisDeadLetterSink) publish(ctx context.Context, stream string, body []byte) error {
	// A dead letter is a new stream entry with no meaningful predecessor,
	// so it gets a fresh id rather than reusing the original event's —
	// which may not even be readable, on the deserialization paths.
	id, err := uuid.NewV7()
	if err != nil {
		return fmt.Errorf("generating dead letter id: %w", err)
	}
	return s.pub.Publish(ctx, stream, id.String(), body, nil)
}

// newDeadLetter fills in the fields every dead letter carries. The caller
// sets Event or EventRaw, because only the caller knows whether the bytes
// it holds are a parseable envelope.
func (c *Consumer) newDeadLetter(topic, reason string, cause error, attempt int64) DeadLetter {
	return DeadLetter{
		DeadLetteredAt:   time.Now().UTC(),
		Reason:           reason,
		Error:            cause.Error(),
		DeliveryAttempts: attempt,
		ConsumerGroup:    c.cfg.Consumer.Group,
		ConsumerName:     c.cfg.Consumer.ConsumerName,
		OriginalTopic:    topic,
	}
}

// deadLetter publishes dl to topic's DLQ stream.
//
// Returning nil means the caller may ack: the event is durably recorded
// somewhere else. Returning an error means it must nack — the entry stays
// pending and the next reclaim sweep retries the DLQ write. While the DLQ
// is unwritable this loops, which is the correct trade: the alternative
// acks the event into nothing (PRD §14).
func (c *Consumer) deadLetter(ctx context.Context, topic string, dl DeadLetter) error {
	c.mu.Lock()
	sink := c.dlq
	c.mu.Unlock()

	stream := c.streamName(topic) + ".dlq"

	if sink == nil {
		// Only reachable from a Consumer constructed but never run.
		// Erroring nacks, which keeps the event rather than dropping it.
		return fmt.Errorf("dead-lettering to %q: no dead letter sink configured", stream)
	}

	body, err := json.Marshal(dl)
	if err != nil {
		return fmt.Errorf("marshalling dead letter for %q: %w", stream, err)
	}

	if err := sink.publish(ctx, stream, body); err != nil {
		c.cfg.Telemetry.Logger.Error("messaging: dead letter publish failed",
			"topic", topic, "dlq_stream", stream, "reason", dl.Reason,
			"delivery_attempts", dl.DeliveryAttempts, "error", err)
		return fmt.Errorf("publishing dead letter to %q: %w", stream, err)
	}

	// Warn, not Info: a dead letter is an event no handler will ever
	// process, and it needs to be visible without turning on debug logging.
	c.cfg.Telemetry.Logger.Warn("messaging: event dead-lettered",
		"topic", topic, "dlq_stream", stream, "reason", dl.Reason,
		"delivery_attempts", dl.DeliveryAttempts, "error", dl.Error)
	return nil
}
```

- [ ] **Step 4: Wire the real sink into `Run`**

In `consumer.go`, replace the block that sets `c.reader` (currently `consumer.go:153-155`) with:

```go
	// The DLQ publisher shares Run's client rather than opening a second
	// connection pool. It is deliberately never Closed here:
	// internalwatermill.Publisher.Close closes the client it was built
	// over, and closeReader already owns that client — a second Close
	// returns ErrClosed from go-redis's pool and would surface as a
	// spurious teardown failure.
	dlqPublisher, err := internalwatermill.NewPublisher(client)
	if err != nil {
		_ = reader.Close()
		return fmt.Errorf("creating dead letter publisher: %w", err)
	}

	c.mu.Lock()
	c.reader = reader
	c.dlq = redisDeadLetterSink{pub: dlqPublisher}
	c.mu.Unlock()
```

Note the existing `subscriber, err :=` further down must become `subscriber, err =` if `err` is now already declared; adjust to whatever compiles.

In `closeReader`, clear the sink alongside the reader so a Consumer that has finished running holds no stale publisher:

```go
func (c *Consumer) closeReader(reader *internalredis.StreamReader) error {
	err := reader.Close()
	c.mu.Lock()
	c.reader = nil
	c.dlq = nil
	c.mu.Unlock()
	return err
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `go test -race ./...`
Expected: PASS.

- [ ] **Step 6: Verify the real sink against Redis**

Run: `go test -tags=integration -race -count=1 -run TestConsumer ./...`
Expected: PASS — no behaviour has changed yet, so this is a regression check that `Run`'s new wiring did not break startup or teardown.

- [ ] **Step 7: Lint and commit**

```bash
golangci-lint run ./...
git add consumer.go consumer_test.go
git commit -m "feat: add the dead letter sink

Run builds a DLQ publisher over the client it already holds for the
reader, so a running consumer opens one connection pool rather than
two. The publisher is never Closed here — closeReader owns the shared
client, and closing both returns ErrClosed from go-redis's pool."
```

---

### Task 5: The delivery-attempt cap

**Files:**
- Modify: `consumer.go` (`dispatch`, currently `consumer.go:241-293`)
- Test: `consumer_test.go`

**Interfaces:**
- Consumes: `newDeadLetter`, `deadLetter` (Task 4); `ReasonMaxAttemptsExceeded`, `deadLetterBody` (Task 2).
- Produces: `deliveryAttempt(map[string]string) int64`. Tasks 6–7 use the `attempt` value it returns.

- [ ] **Step 1: Write the failing tests**

Add to `consumer_test.go`:

```go
func TestDeliveryAttempt_DefaultsToTheFirstDelivery(t *testing.T) {
	// A missing or malformed counter must not dead-letter an event. The cap
	// exists to bound redelivery, not to punish odd transport metadata.
	tests := []struct {
		name     string
		metadata map[string]string
		want     int64
	}{
		{"nil metadata", nil, 1},
		{"absent key", map[string]string{}, 1},
		{"unparseable", map[string]string{"_foi_delivery_attempt": "many"}, 1},
		{"zero", map[string]string{"_foi_delivery_attempt": "0"}, 1},
		{"negative", map[string]string{"_foi_delivery_attempt": "-4"}, 1},
		{"valid", map[string]string{"_foi_delivery_attempt": "4"}, 4},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := deliveryAttempt(tc.metadata); got != tc.want {
				t.Errorf("deliveryAttempt = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestDispatch_CapBoundary(t *testing.T) {
	// PRD §13: attempts 1..MaxDeliveryAttempts dispatch, and the next one
	// is dead-lettered. That is what makes the stated worst case of
	// MaxDeliveryAttempts × (1+MaxImmediateRetries) = 20 invocations right.
	tests := []struct {
		name        string
		attempt     string
		wantHandler bool
		wantDLQ     int
	}{
		{"at the cap still dispatches", "5", true, 0},
		{"over the cap dead-letters", "6", false, 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			consumer, err := NewConsumer(testConsumerConfig())
			if err != nil {
				t.Fatalf("NewConsumer: %v", err)
			}
			sink := &recordingSink{}
			consumer.dlq = sink

			var called bool
			def := EventDef{Topic: "documents", Type: "document.created", Version: "1.0.0"}
			h := handlerFunc(func(context.Context, Envelope[testPayload]) error {
				called = true
				return nil
			})
			if err := RegisterHandler(consumer, def, h); err != nil {
				t.Fatalf("RegisterHandler: %v", err)
			}

			metadata := map[string]string{"_foi_delivery_attempt": tc.attempt}
			if err := consumer.dispatch(context.Background(), "documents", validEnvelopeJSON(), metadata); err != nil {
				t.Fatalf("dispatch: %v", err)
			}

			if called != tc.wantHandler {
				t.Errorf("handler called = %v, want %v", called, tc.wantHandler)
			}
			if got := sink.count(); got != tc.wantDLQ {
				t.Errorf("dead letters = %d, want %d", got, tc.wantDLQ)
			}
		})
	}
}

func TestDispatch_CapDeadLettersBeforeDecoding(t *testing.T) {
	// The cap is checked before json.Unmarshal, so an over-cap event that
	// is ALSO undecodable still reports max_attempts_exceeded — and, more
	// importantly, never reaches the retry loop, where it would occupy its
	// topic's concurrency slot for four handler invocations.
	consumer, err := NewConsumer(testConsumerConfig())
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	sink := &recordingSink{}
	consumer.dlq = sink

	def := EventDef{Topic: "documents", Type: "document.created", Version: "1.0.0"}
	if err := RegisterHandler(consumer, def, noopHandler{}); err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}

	metadata := map[string]string{"_foi_delivery_attempt": "9"}
	if err := consumer.dispatch(context.Background(), "documents", []byte(`not json`), metadata); err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	got := sink.only(t)
	if got.Reason != ReasonMaxAttemptsExceeded {
		t.Errorf("Reason = %q, want %q — the cap must be checked before decoding", got.Reason, ReasonMaxAttemptsExceeded)
	}
	if got.DeliveryAttempts != 9 {
		t.Errorf("DeliveryAttempts = %d, want 9", got.DeliveryAttempts)
	}
}
```

Add this helper to `consumer_test.go` — later tasks reuse it:

```go
// validEnvelopeJSON is an envelope that passes validation and routes to the
// documents/document.created/1.x.x handler.
func validEnvelopeJSON() []byte {
	return []byte(`{
		"event_id":"01234567-89ab-7def-8000-000000000000",
		"event_type":"document.created",
		"timestamp":"2026-04-23T10:00:00Z",
		"schema_version":"1.0.0",
		"correlation_id":"corr-1",
		"source":"other.service",
		"payload":{}
	}`)
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test -run 'TestDeliveryAttempt|TestDispatch_Cap' ./...`
Expected: FAIL — `undefined: deliveryAttempt`, and the cap tests failing because the handler is called at attempt 6.

- [ ] **Step 3: Write the implementation**

Append to `consumer.go`:

```go
// deliveryAttempt reads the attempt counter the subscriber stamped on the
// message.
//
// A missing, unparseable, or nonsensical value is treated as the first
// delivery. The cap exists to bound redelivery of events that keep failing,
// not to dead-letter an event whose transport metadata was odd — and every
// caller of dispatch outside the router (tests, future tooling) passes nil.
func deliveryAttempt(metadata map[string]string) int64 {
	n, err := strconv.ParseInt(metadata[internalwatermill.MetadataDeliveryAttempt], 10, 64)
	if err != nil || n < 1 {
		return 1
	}
	return n
}
```

Add `"strconv"` to `consumer.go`'s imports.

In `dispatch`, insert immediately after the `log := ...` assignment and before `var env Envelope[json.RawMessage]`:

```go
	attempt := deliveryAttempt(metadata)
	if attempt > int64(c.cfg.Consumer.MaxDeliveryAttempts) {
		// Checked before decoding, and before any classification is
		// consulted — PRD §13 Layer 3 caps regardless of classification.
		//
		// The ordering is load-bearing for throughput, not just tidiness.
		// A capped event occupies its topic's concurrency slot for one
		// metadata read and one DLQ publish; run through the retry loop
		// instead it would hold that slot for (1+MaxImmediateRetries)
		// handler invocations. At the default Concurrency of 1 a reclaim
		// sweep of accumulated poison entries is what starves the read
		// loop, so this bound is what keeps live traffic moving.
		log.Warn("messaging: delivery attempt cap exceeded",
			"topic", topic, "max_delivery_attempts", c.cfg.Consumer.MaxDeliveryAttempts)

		dl := c.newDeadLetter(topic, ReasonMaxAttemptsExceeded,
			fmt.Errorf("delivery attempt %d exceeded MaxDeliveryAttempts %d",
				attempt, c.cfg.Consumer.MaxDeliveryAttempts),
			attempt)
		dl.Event, dl.EventRaw = deadLetterBody(payload)
		return c.deadLetter(ctx, topic, dl)
	}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test -race ./...`
Expected: PASS.

- [ ] **Step 5: Lint and commit**

```bash
golangci-lint run ./...
git add consumer.go consumer_test.go
git commit -m "feat: enforce the delivery-attempt cap

Checked before decoding and before classification, per PRD §13 Layer 3.
The ordering bounds slot occupancy: a capped event costs one metadata
read and one DLQ publish rather than four handler invocations, which is
what keeps a poison-entry reclaim sweep from starving the read loop at
Concurrency 1."
```

---

### Task 6: Dead-letter unusable envelopes

**Files:**
- Modify: `consumer.go` (`dispatch`'s three decode/validate/version failure paths)
- Test: `consumer_test.go` — **three existing tests are rewritten**

**Interfaces:**
- Consumes: `newDeadLetter`, `deadLetter` (Task 4); `attempt` from `deliveryAttempt` (Task 5); `ReasonDeserializationFailed`, `deadLetterBody` (Task 2).
- Produces: nothing new.

**Behaviour change:** these three paths NACK today. After this task they dead-letter and ack. Malformed JSON does not become valid on redelivery, so routing them through the cap would cost five reclaim cycles and roughly five minutes — holding a concurrency slot on each pass — to reach a conclusion available on the first look.

- [ ] **Step 1: Rewrite the three existing tests**

In `consumer_test.go`, replace `TestDispatch_ReturnsErrorOnUndecodableEnvelope` (currently `consumer_test.go:284-299`), `TestDispatch_ReturnsErrorOnInvalidEnvelope` (`consumer_test.go:301-323`), and `TestDispatch_ValidatesEnvelopeBeforeParsingMajorVersion` (`consumer_test.go:325-355`) with:

```go
func TestDispatch_DeadLettersUndecodableEnvelope(t *testing.T) {
	consumer, err := NewConsumer(testConsumerConfig())
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	sink := &recordingSink{}
	consumer.dlq = sink

	def := EventDef{Topic: "documents", Type: "document.created", Version: "1.0.0"}
	if err := RegisterHandler(consumer, def, noopHandler{}); err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}

	// Acked, not nacked: malformed JSON does not become valid on
	// redelivery, so burning the cap on it only delays the same verdict.
	if err := consumer.dispatch(context.Background(), "documents", []byte(`not json`), nil); err != nil {
		t.Fatalf("dispatch must ack after dead-lettering, got %v", err)
	}

	got := sink.only(t)
	if got.Reason != ReasonDeserializationFailed {
		t.Errorf("Reason = %q, want %q", got.Reason, ReasonDeserializationFailed)
	}
	if string(got.EventRaw) != "not json" {
		t.Errorf("EventRaw = %q, want the original bytes preserved", got.EventRaw)
	}
	if got.Event != nil {
		t.Error("unparseable bytes must not be spliced into event")
	}
}

func TestDispatch_DeadLettersInvalidEnvelope(t *testing.T) {
	consumer, err := NewConsumer(testConsumerConfig())
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	sink := &recordingSink{}
	consumer.dlq = sink

	def := EventDef{Topic: "documents", Type: "document.created", Version: "1.0.0"}
	if err := RegisterHandler(consumer, def, noopHandler{}); err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}

	// Valid JSON, but source and correlation_id are missing.
	body := []byte(`{
		"event_id":"01234567-89ab-7def-8000-000000000000",
		"event_type":"document.created",
		"timestamp":"2026-04-23T10:00:00Z",
		"schema_version":"1.0.0",
		"payload":{}
	}`)

	if err := consumer.dispatch(context.Background(), "documents", body, nil); err != nil {
		t.Fatalf("dispatch must ack after dead-lettering, got %v", err)
	}

	got := sink.only(t)
	if got.Reason != ReasonDeserializationFailed {
		t.Errorf("Reason = %q, want %q", got.Reason, ReasonDeserializationFailed)
	}
	// PRD §14 puts an envelope failing validation in event_raw too: it did
	// not deserialize into a usable event, whatever its syntax.
	if got.Event != nil {
		t.Error("an envelope failing validation belongs in event_raw, not event")
	}
	if len(got.EventRaw) == 0 {
		t.Error("the original bytes must be preserved")
	}
}

func TestDispatch_ValidatesEnvelopeBeforeParsingMajorVersion(t *testing.T) {
	consumer, err := NewConsumer(testConsumerConfig())
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	sink := &recordingSink{}
	consumer.dlq = sink

	def := EventDef{Topic: "documents", Type: "document.created", Version: "1.0.0"}
	if err := RegisterHandler(consumer, def, noopHandler{}); err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}

	// dispatch's ordering (json.Unmarshal -> validateEnvelope -> majorVersion
	// -> registry lookup) is load-bearing: validateEnvelope's
	// schemaVersionPattern (^\d+\.\d+\.\d+$) rejects a signed major before
	// majorVersion's strconv.Atoi ever sees it. If dispatch ever called
	// majorVersion first, "-1" would parse cleanly as major -1 and this
	// event would be routed (or silently acked) instead of dead-lettered.
	body := []byte(`{
		"event_id":"01234567-89ab-7def-8000-000000000000",
		"event_type":"document.created",
		"timestamp":"2026-04-23T10:00:00Z",
		"schema_version":"-1.0.0",
		"correlation_id":"corr-1",
		"source":"other.service",
		"payload":{}
	}`)

	if err := consumer.dispatch(context.Background(), "documents", body, nil); err != nil {
		t.Fatalf("dispatch must ack after dead-lettering, got %v", err)
	}

	got := sink.only(t)
	// The error text is how the ordering is observable now that both paths
	// end in the same reason: validateEnvelope names schema_version,
	// majorVersion's failure would name parsing instead.
	if !strings.Contains(got.Error, "validating envelope") {
		t.Errorf("Error = %q, want a validateEnvelope failure: it must reject the signed major before majorVersion runs", got.Error)
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test -run 'TestDispatch_DeadLetters|TestDispatch_ValidatesEnvelope' ./...`
Expected: FAIL — `dispatch must ack after dead-lettering, got ...`, because all three paths still return errors.

- [ ] **Step 3: Write the implementation**

In `dispatch`, replace the three failure returns. Each becomes the same four-line shape; the comment goes on the first one only:

```go
	var env Envelope[json.RawMessage]
	if err := json.Unmarshal(payload, &env); err != nil {
		log.Error("messaging: undecodable event envelope",
			"topic", topic, "error", err)
		// Dead-lettered rather than nacked. These three failures are
		// definitionally permanent — malformed JSON does not become valid
		// on redelivery, and a missing event_id does not appear — so
		// routing them through the cap would spend five reclaim cycles,
		// each holding a concurrency slot, to reach a verdict that was
		// available on the first look.
		dl := c.newDeadLetter(topic, ReasonDeserializationFailed,
			fmt.Errorf("unmarshalling envelope on topic %q: %w", topic, err), attempt)
		dl.EventRaw = payload
		return c.deadLetter(ctx, topic, dl)
	}

	if err := validateEnvelope(env); err != nil {
		log.Error("messaging: invalid event envelope",
			"topic", topic, "event_id", env.EventID, "error", err)
		dl := c.newDeadLetter(topic, ReasonDeserializationFailed,
			fmt.Errorf("validating envelope on topic %q: %w", topic, err), attempt)
		dl.EventRaw = payload
		return c.deadLetter(ctx, topic, dl)
	}

	major, err := majorVersion(env.SchemaVersion)
	if err != nil {
		log.Error("messaging: unparseable schema version",
			"topic", topic, "event_id", env.EventID, "error", err)
		dl := c.newDeadLetter(topic, ReasonDeserializationFailed,
			fmt.Errorf("parsing schema version on topic %q: %w", topic, err), attempt)
		dl.EventRaw = payload
		return c.deadLetter(ctx, topic, dl)
	}
```

`EventRaw` is set directly rather than through `deadLetterBody`: PRD §14 puts everything that failed to deserialize into a usable event in `event_raw`, including a syntactically valid envelope that failed validation.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test -race ./...`
Expected: PASS.

- [ ] **Step 5: Lint and commit**

```bash
golangci-lint run ./...
git add consumer.go consumer_test.go
git commit -m "feat: dead-letter envelopes that cannot be dispatched

Undecodable JSON, envelopes failing validation, and unparseable schema
versions now DLQ and ack instead of nacking. All three are permanent by
definition, so redelivering them only delays the same verdict while
holding a concurrency slot on every pass."
```

---

### Task 7: The immediate-retry loop

**Files:**
- Modify: `consumer.go` (`dispatch`'s handler call; append `runWithRetry`, `sleepWithJitter`)
- Test: `consumer_test.go` — **one existing test is rewritten**

**Interfaces:**
- Consumes: `IsDiscard`, `IsPermanent` (Task 1); `backoffUpperBound` (Task 3); `newDeadLetter`, `deadLetter` (Task 4); `attempt` (Task 5).
- Produces: `func (c *Consumer) runWithRetry(ctx context.Context, topic string, payload []byte, attempt int64, handler dispatchFunc, env Envelope[json.RawMessage]) error`.

- [ ] **Step 1: Write the failing tests**

In `consumer_test.go`, add a config helper with instant backoff — every retry test needs it:

```go
// fastRetryConfig keeps the retry loop's structure but removes the waiting.
// Zero means "use the default" for RetryConfig, so retries cannot be
// switched off; they can only be made instant.
func fastRetryConfig(retries int) Config {
	cfg := testConsumerConfig()
	cfg.Retry = RetryConfig{
		MaxImmediateRetries: retries,
		InitialBackoff:      time.Nanosecond,
		MaxBackoff:          time.Nanosecond,
	}
	return cfg
}
```

Replace `TestDispatch_PropagatesHandlerError` (currently `consumer_test.go:357-384`) and add the rest:

```go
func TestDispatch_RetriesThenNacksARetryableError(t *testing.T) {
	consumer, err := NewConsumer(fastRetryConfig(3))
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	sink := &recordingSink{}
	consumer.dlq = sink

	var calls int
	def := EventDef{Topic: "documents", Type: "document.created", Version: "1.0.0"}
	h := handlerFunc(func(context.Context, Envelope[testPayload]) error {
		calls++
		return errBoom
	})
	if err := RegisterHandler(consumer, def, h); err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}

	// Exhausting immediate retries nacks: the entry stays pending and the
	// reclaim loop redelivers it with its counter advanced toward the cap.
	if err := consumer.dispatch(context.Background(), "documents", validEnvelopeJSON(), nil); err == nil {
		t.Error("expected exhausted retries to nack")
	}
	if calls != 4 {
		t.Errorf("handler called %d times, want 4 (1 attempt + 3 retries)", calls)
	}
	if sink.count() != 0 {
		t.Error("exhausted retries must nack, not dead-letter: the cap decides that, not the retry loop")
	}
}

func TestDispatch_StopsRetryingOnceTheHandlerSucceeds(t *testing.T) {
	consumer, err := NewConsumer(fastRetryConfig(3))
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	consumer.dlq = &recordingSink{}

	var calls int
	def := EventDef{Topic: "documents", Type: "document.created", Version: "1.0.0"}
	h := handlerFunc(func(context.Context, Envelope[testPayload]) error {
		calls++
		if calls < 3 {
			return errBoom
		}
		return nil
	})
	if err := RegisterHandler(consumer, def, h); err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}

	if err := consumer.dispatch(context.Background(), "documents", validEnvelopeJSON(), nil); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if calls != 3 {
		t.Errorf("handler called %d times, want 3", calls)
	}
}

func TestDispatch_PermanentErrorDeadLettersWithoutRetrying(t *testing.T) {
	consumer, err := NewConsumer(fastRetryConfig(3))
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	sink := &recordingSink{}
	consumer.dlq = sink

	var calls int
	def := EventDef{Topic: "documents", Type: "document.created", Version: "1.0.0"}
	h := handlerFunc(func(context.Context, Envelope[testPayload]) error {
		calls++
		return AsPermanent(errBoom)
	})
	if err := RegisterHandler(consumer, def, h); err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}

	if err := consumer.dispatch(context.Background(), "documents", validEnvelopeJSON(), nil); err != nil {
		t.Fatalf("a dead-lettered event must ack, got %v", err)
	}
	if calls != 1 {
		t.Errorf("handler called %d times, want 1: a permanent error must not be retried", calls)
	}
	got := sink.only(t)
	if got.Reason != ReasonPermanent {
		t.Errorf("Reason = %q, want %q", got.Reason, ReasonPermanent)
	}
	if string(got.Event) == "" {
		t.Error("a decodable event belongs in event, so replay tooling can republish it")
	}
}

func TestDispatch_DiscardErrorAcksWithoutDLQOrRetry(t *testing.T) {
	consumer, err := NewConsumer(fastRetryConfig(3))
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	sink := &recordingSink{}
	consumer.dlq = sink

	var calls int
	def := EventDef{Topic: "documents", Type: "document.created", Version: "1.0.0"}
	h := handlerFunc(func(context.Context, Envelope[testPayload]) error {
		calls++
		return AsDiscard(errBoom)
	})
	if err := RegisterHandler(consumer, def, h); err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}

	if err := consumer.dispatch(context.Background(), "documents", validEnvelopeJSON(), nil); err != nil {
		t.Fatalf("a discarded event must ack, got %v", err)
	}
	if calls != 1 {
		t.Errorf("handler called %d times, want 1", calls)
	}
	if sink.count() != 0 {
		t.Error("a discarded event must produce no dead letter")
	}
}

func TestDispatch_ReclassifiesOnEveryAttempt(t *testing.T) {
	// A handler may fail transiently and then discover the failure is
	// permanent. Deciding classification once, on the first error, would
	// keep retrying an error the handler has already given up on.
	consumer, err := NewConsumer(fastRetryConfig(3))
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	sink := &recordingSink{}
	consumer.dlq = sink

	var calls int
	def := EventDef{Topic: "documents", Type: "document.created", Version: "1.0.0"}
	h := handlerFunc(func(context.Context, Envelope[testPayload]) error {
		calls++
		if calls == 1 {
			return errBoom // unclassified: retryable
		}
		return AsPermanent(errBoom)
	})
	if err := RegisterHandler(consumer, def, h); err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}

	if err := consumer.dispatch(context.Background(), "documents", validEnvelopeJSON(), nil); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if calls != 2 {
		t.Errorf("handler called %d times, want 2: the second attempt's permanent verdict must stop the loop", calls)
	}
	if got := sink.only(t); got.Reason != ReasonPermanent {
		t.Errorf("Reason = %q, want %q", got.Reason, ReasonPermanent)
	}
}

func TestDispatch_CancelledContextAbandonsRetry(t *testing.T) {
	// ctx here is the message context, which stays live through the whole
	// drain by design. Its cancellation means the subscriber closed, so
	// abandoning the retry to a nack is right — the entry is still pending.
	cfg := testConsumerConfig()
	cfg.Retry = RetryConfig{
		MaxImmediateRetries: 3,
		InitialBackoff:      10 * time.Second,
		MaxBackoff:          10 * time.Second,
	}
	consumer, err := NewConsumer(cfg)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	consumer.dlq = &recordingSink{}

	ctx, cancel := context.WithCancel(context.Background())
	var calls int
	def := EventDef{Topic: "documents", Type: "document.created", Version: "1.0.0"}
	h := handlerFunc(func(context.Context, Envelope[testPayload]) error {
		calls++
		cancel()
		return errBoom
	})
	if err := RegisterHandler(consumer, def, h); err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}

	start := time.Now()
	if err := consumer.dispatch(ctx, "documents", validEnvelopeJSON(), nil); err == nil {
		t.Error("expected an abandoned retry to nack")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("dispatch took %v: a cancelled context must abort the backoff, not sleep through it", elapsed)
	}
	if calls != 1 {
		t.Errorf("handler called %d times, want 1", calls)
	}
}
```

Add `var errBoom = errors.New("boom")` near the top of `consumer_test.go` if the file does not already define an equivalent, and `"time"` to its imports.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test -run 'TestDispatch_Retries|TestDispatch_Stops|TestDispatch_Permanent|TestDispatch_Discard|TestDispatch_Reclassifies|TestDispatch_Cancelled' ./...`
Expected: FAIL — the handler is called once, not four times, and no dead letters are produced.

- [ ] **Step 3: Write the implementation**

Append to `consumer.go`:

```go
// runWithRetry is PRD §13 Layer 1: immediate in-process retry with full
// jitter, ending in an ack, a dead letter, or a nack.
//
// Every attempt runs inside the message's per-topic concurrency slot, which
// is held for the whole loop. At the default Concurrency of 1 that means a
// retrying message blocks its topic's read loop until the loop finishes.
// That is deliberate rather than overlooked: releasing the slot across the
// sleep would let a later message overtake the retrying one, and per-topic
// ordering at Concurrency 1 is a documented guarantee (PRD §6). The bound
// being per topic is what keeps the stall from reaching other topics.
func (c *Consumer) runWithRetry(
	ctx context.Context,
	topic string,
	payload []byte,
	attempt int64,
	handler dispatchFunc,
	env Envelope[json.RawMessage],
) error {
	log := c.cfg.Telemetry.Logger.With(
		"topic", topic, "event_type", env.EventType,
		"schema_version", env.SchemaVersion, "event_id", env.EventID,
		"delivery_attempt", attempt,
	)

	for i := 0; ; i++ {
		err := handler(ctx, env)

		// Classification is re-read on every attempt rather than decided
		// once from the first error: a handler may fail transiently and
		// then discover the failure is permanent, and the latest verdict is
		// the one that should apply.
		switch {
		case err == nil:
			return nil

		case IsDiscard(err):
			log.Warn("messaging: handler discarded event", "error", err)
			return nil

		case IsPermanent(err):
			log.Error("messaging: handler returned a permanent error", "error", err)
			dl := c.newDeadLetter(topic, ReasonPermanent, err, attempt)
			dl.Event, dl.EventRaw = deadLetterBody(payload)
			return c.deadLetter(ctx, topic, dl)

		case i >= c.cfg.Retry.MaxImmediateRetries:
			// Nack, not a dead letter. The cap decides when to give up on
			// an event; this loop only decides when to stop trying within
			// one delivery. The entry stays pending and the reclaim loop
			// redelivers it with its counter advanced.
			log.Error("messaging: handler returned error, immediate retries exhausted",
				"immediate_attempts", i+1, "error", err)
			return err
		}

		log.Debug("messaging: retrying handler", "immediate_attempt", i+1, "error", err)
		if !sleepWithJitter(ctx, backoffUpperBound(c.cfg.Retry, i)) {
			// Abandoned mid-backoff. Nack so the entry survives.
			return err
		}
	}
}

// sleepWithJitter sleeps a random duration in [0, upper) — PRD §13's full
// jitter — reporting false if ctx was cancelled first.
//
// A cancellation here is not the shutdown drain: message contexts are
// derived from context.WithoutCancel and stay live for the whole
// ShutdownTimeout, so ctx is Done only once the subscriber has closed.
// Retries are therefore never interrupted by a graceful shutdown, which is
// why ShutdownTimeout has to be budgeted with the retry multiplier in mind.
func sleepWithJitter(ctx context.Context, upper time.Duration) bool {
	if upper <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(time.Duration(rand.Int64N(int64(upper))))
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
```

Add `"math/rand/v2"` to `consumer.go`'s imports.

Replace `dispatch`'s handler call (currently `consumer.go:281-292`) with:

```go
	ctx = contextWithCorrelationID(ctx, env.CorrelationID)
	return c.runWithRetry(ctx, topic, payload, attempt, handler, env)
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test -race ./...`
Expected: PASS.

- [ ] **Step 5: Lint and commit**

```bash
golangci-lint run ./...
git add consumer.go consumer_test.go
git commit -m "feat: retry handlers in-process with full jitter

PRD §13 Layer 1, as a plain loop in the root package rather than
watermill middleware: the classification predicates live here, and the
root already imports internal/watermill, so a middleware there would be
an import cycle. Classification is re-read every attempt."
```

---

### Task 8: Dead-letter undecodable stream entries

**Files:**
- Modify: `internal/watermill/subscriber.go:52-69` (options), `:74-85` (struct), `:103-126` (constructor), `:279-294` (`emit`)
- Modify: `consumer.go` (`Run` wiring)
- Test: `internal/watermill/subscriber_test.go`

**Interfaces:**
- Consumes: `newDeadLetter`, `deadLetter` (Task 4).
- Produces: `SubscriberOptions.OnUndecodable func(stream, entryID string, fields map[string]any) error`.

**Why this one touches `internal/watermill`:** these entries fail in Watermill's own marshaller, before `emit` produces a message, so they never reach `dispatch` — neither the cap nor the DLQ can see them. Today they re-loop every `ClaimMinIdle` forever. It is the one poison case the cap cannot bound.

- [ ] **Step 1: Write the failing tests**

Add to `internal/watermill/subscriber_test.go`:

```go
func TestSubscriber_UndecodableEntryReachesTheHookAndIsAcked(t *testing.T) {
	// An entry whose fields the redisstream marshaller cannot read. It
	// never becomes a message, so nothing downstream can dead-letter it.
	bad := internalredis.Entry{ID: "1-1", Fields: map[string]any{"garbage": "value"}}
	reader := newFakeReader(bad)

	type call struct {
		stream  string
		entryID string
	}
	calls := make(chan call, 1)

	sub, err := NewSubscriber(SubscriberOptions{
		Reader:    reader,
		BlockTime: 10 * time.Millisecond,
		Logger:    slog.New(slog.DiscardHandler),
		OnUndecodable: func(stream, entryID string, fields map[string]any) error {
			calls <- call{stream: stream, entryID: entryID}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewSubscriber: %v", err)
	}
	t.Cleanup(func() { _ = sub.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if _, err := sub.Subscribe(ctx, "foi:documents"); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	select {
	case got := <-calls:
		if got.stream != "foi:documents" {
			t.Errorf("stream = %q, want %q", got.stream, "foi:documents")
		}
		if got.entryID != "1-1" {
			t.Errorf("entryID = %q, want %q", got.entryID, "1-1")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("OnUndecodable was never called")
	}

	// Acked only after the hook succeeded: the entry is now recorded
	// somewhere else, so leaving it pending would redeliver it forever.
	waitForAck(t, reader, "1-1")
}

func TestSubscriber_UndecodableEntryStaysPendingWhenTheHookFails(t *testing.T) {
	bad := internalredis.Entry{ID: "1-1", Fields: map[string]any{"garbage": "value"}}
	reader := newFakeReader(bad)

	called := make(chan struct{}, 1)
	sub, err := NewSubscriber(SubscriberOptions{
		Reader:    reader,
		BlockTime: 10 * time.Millisecond,
		Logger:    slog.New(slog.DiscardHandler),
		OnUndecodable: func(string, string, map[string]any) error {
			select {
			case called <- struct{}{}:
			default:
			}
			return errors.New("dlq unavailable")
		},
	})
	if err != nil {
		t.Fatalf("NewSubscriber: %v", err)
	}
	t.Cleanup(func() { _ = sub.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if _, err := sub.Subscribe(ctx, "foi:documents"); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	select {
	case <-called:
	case <-time.After(2 * time.Second):
		t.Fatal("OnUndecodable was never called")
	}

	// Give the read loop room to have acked if it were going to.
	time.Sleep(200 * time.Millisecond)
	if reader.ackedIDs() != nil {
		t.Error("a failed dead-letter must leave the entry pending, not ack it")
	}
}

func TestSubscriber_UndecodableEntryWithoutAHookKeepsTheOldBehaviour(t *testing.T) {
	// Nil-safe: unset, the subscriber logs and leaves the entry pending,
	// which is what internal/watermill's own tests rely on.
	bad := internalredis.Entry{ID: "1-1", Fields: map[string]any{"garbage": "value"}}
	reader := newFakeReader(bad)

	sub, err := NewSubscriber(SubscriberOptions{
		Reader:    reader,
		BlockTime: 10 * time.Millisecond,
		Logger:    slog.New(slog.DiscardHandler),
	})
	if err != nil {
		t.Fatalf("NewSubscriber: %v", err)
	}
	t.Cleanup(func() { _ = sub.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if _, err := sub.Subscribe(ctx, "foi:documents"); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	time.Sleep(200 * time.Millisecond)
	if reader.ackedIDs() != nil {
		t.Error("without a hook the entry must be left pending")
	}
}
```

These need two helpers on `fakeReader`. Add them if `subscriber_test.go` does not already have equivalents:

```go
// ackedIDs returns the entry ids acked so far.
func (f *fakeReader) ackedIDs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.acked...)
}

// waitForAck blocks until id has been acked, or fails the test.
func waitForAck(t *testing.T, f *fakeReader, id string) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		for _, got := range f.ackedIDs() {
			if got == id {
				return
			}
		}
		select {
		case <-deadline:
			t.Fatalf("entry %q was never acked", id)
		case <-time.After(10 * time.Millisecond):
		}
	}
}
```

Confirm the `Fields` map above actually fails `redisstream.DefaultMarshallerUnmarshaller{}.Unmarshal`; if it does not, use a map missing the marshaller's expected payload/metadata keys until the existing `decodeEntry` returns an error for it.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test -race -run TestSubscriber_Undecodable ./internal/watermill/`
Expected: FAIL — `unknown field OnUndecodable in struct literal`.

- [ ] **Step 3: Add the option**

In `internal/watermill/subscriber.go`, add to `SubscriberOptions`:

```go
	// OnUndecodable is called for a stream entry that cannot be
	// unmarshalled at all. Returning nil acks the entry; returning an error
	// leaves it pending for the next reclaim sweep.
	//
	// The hook exists because this failure happens before a message is
	// produced, so the entry never reaches the consumer's dispatch — the
	// delivery-attempt cap and the DLQ both live there and neither can see
	// it. Left nil the subscriber logs and leaves the entry pending, which
	// re-loops every ClaimMinIdle forever.
	//
	// It takes only plain types, so the caller can dead-letter without any
	// watermill value crossing back over the package boundary.
	OnUndecodable func(stream, entryID string, fields map[string]any) error
```

Add the matching field to the `Subscriber` struct:

```go
	onUndecodable func(stream, entryID string, fields map[string]any) error
```

And to `NewSubscriber`'s returned struct literal:

```go
		onUndecodable: opts.OnUndecodable,
```

- [ ] **Step 4: Use it in `emit`**

In `emit`, replace the decode-failure block (currently `internal/watermill/subscriber.go:282-294`) with:

```go
	msg, err := decodeEntry(e, attempt)
	if err != nil {
		// Spec §6 requires an ERROR for an undecodable envelope, and only
		// the JSON layer's version of that failure was being logged.
		s.logger.Error("messaging: undecodable stream entry",
			"stream", sc.stream, "entry_id", e.ID, "error", err)

		if s.onUndecodable != nil {
			if hookErr := s.onUndecodable(sc.stream, e.ID, e.Fields); hookErr != nil {
				// The hook owns recording the entry elsewhere. If it
				// failed, leave the entry pending rather than acking an
				// event nothing is holding — the same rule the dispatch
				// path's DLQ writes follow.
				s.logger.Error("messaging: dead-lettering undecodable entry failed",
					"stream", sc.stream, "entry_id", e.ID, "error", hookErr)
			} else {
				sc.ack(ctx, e.ID)
			}
		}

		sc.release()
		return !s.stopped(ctx)
	}
```

- [ ] **Step 5: Run the subscriber tests**

Run: `go test -race ./internal/watermill/`
Expected: PASS.

- [ ] **Step 6: Wire the hook in `Run`**

In `consumer.go`'s `Run`, build a stream-to-topic map before the subscriber is constructed. The hook receives a Redis stream name; `deadLetter` needs the logical topic:

```go
	// The hook is handed a Redis stream name, but dead-lettering is
	// expressed in logical topics, so the mapping Run already computes for
	// AddHandler is inverted once here rather than parsed back out of the
	// stream name.
	topicByStream := make(map[string]string, len(topics))
	for _, topic := range topics {
		topicByStream[c.streamName(topic)] = topic
	}
```

Add to the `SubscriberOptions` literal:

```go
		OnUndecodable: func(stream, entryID string, fields map[string]any) error {
			topic, ok := topicByStream[stream]
			if !ok {
				return fmt.Errorf("no topic registered for stream %q", stream)
			}

			// The original bytes are unreachable — the marshaller failed
			// before producing a payload — so the raw Redis fields are what
			// gets preserved. PRD §14 did not anticipate a
			// marshaller-level failure; this is the nearest thing to
			// "the raw bytes" that exists at this point.
			raw, err := json.Marshal(fields)
			if err != nil {
				return fmt.Errorf("marshalling fields of undecodable entry %q: %w", entryID, err)
			}

			dl := c.newDeadLetter(topic, ReasonDeserializationFailed,
				fmt.Errorf("stream entry %q could not be unmarshalled", entryID), 1)
			dl.EventRaw = raw

			// Detached from Run's ctx, and timeout-bounded, for the same
			// reason the subscriber's ack path is: an entry reaching this
			// hook during the drain must still be recorded, and Run's ctx
			// is already cancelled by then. Without this the DLQ write
			// fails with context.Canceled at exactly the moment there is a
			// backlog to clear.
			dlqCtx, cancelDLQ := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancelDLQ()
			return c.deadLetter(dlqCtx, topic, dl)
		},
```

The same reasoning does *not* apply to the DLQ writes inside `dispatch`: those run on the message context, which is already `context.WithoutCancel`-derived and stays live for the whole drain (2a spec §10).

- [ ] **Step 7: Run everything**

Run: `go test -race ./... && go test -tags=integration -race -count=1 ./...`
Expected: PASS.

- [ ] **Step 8: Lint and commit**

```bash
golangci-lint run ./...
git add internal/watermill/subscriber.go internal/watermill/subscriber_test.go consumer.go
git commit -m "feat: dead-letter stream entries that cannot be unmarshalled

These fail in watermill's marshaller before a message exists, so they
never reach dispatch and neither the cap nor the DLQ could see them —
the one poison case the cap cannot bound. A plain-typed OnUndecodable
hook routes them out without any watermill value crossing back."
```

---

### Task 9: Integration tests

**Files:**
- Modify: `consumer_integration_test.go`

**Interfaces:**
- Consumes: everything from Tasks 1–8.
- Produces: nothing.

**One existing test must be retuned.** `TestConsumer_RedeliversNackedEventViaReclaim` (`consumer_integration_test.go:237`) uses `newCollectingHandler(2)`. With three immediate retries the third attempt now succeeds in-process, so the test never nacks and never exercises reclaim — it would pass while testing nothing. Its `failFirst` must exceed `1+MaxImmediateRetries`.

- [ ] **Step 1: Add retry settings to the shared fixture**

In `consumeFixture` (`consumer_integration_test.go:25-49`), add to the returned `Config`:

```go
		Retry: messaging.RetryConfig{
			MaxImmediateRetries: 3,
			// Nanosecond backoff keeps the retry loop's structure without
			// its waiting: these tests assert on attempt counts and
			// outcomes, never on timing.
			InitialBackoff: time.Nanosecond,
			MaxBackoff:     time.Nanosecond,
		},
```

- [ ] **Step 2: Retune the reclaim test**

In `TestConsumer_RedeliversNackedEventViaReclaim`, change `newCollectingHandler(2)` to:

```go
	// Must exceed 1+MaxImmediateRetries, or the first delivery's own
	// retries succeed and the reclaim path this test exists for never runs.
	handler := newCollectingHandler(4)
```

- [ ] **Step 3: Run it to confirm it still passes**

Run: `go test -tags=integration -race -count=1 -run TestConsumer_RedeliversNackedEventViaReclaim ./ -v`
Expected: PASS, with the handler reporting 5 attempts.

- [ ] **Step 4: Write the per-topic isolation test**

This is the regression test for the semantic the whole design turns on. Append to `consumer_integration_test.go`:

```go
// TestConsumer_RetryStormOnOneTopicDoesNotStallAnother is the regression
// test for per-topic concurrency. Concurrency bounds in-flight handlers per
// subscribed topic, so one topic exhausting its single slot on retries must
// not stop another topic from consuming.
//
// Under a Subscriber-wide semaphore — which is what this codebase had
// before — topic A's retrying handler holds the only slot and topic B stops
// entirely. This test is what would catch someone "simplifying" the
// per-subscription semaphore back to a shared one.
func TestConsumer_RetryStormOnOneTopicDoesNotStallAnother(t *testing.T) {
	cfg := consumeFixture(t)
	cfg.Consumer.Concurrency = 1
	// Real backoff, so topic A genuinely occupies its slot for a while.
	cfg.Retry = messaging.RetryConfig{
		MaxImmediateRetries: 3,
		InitialBackoff:      200 * time.Millisecond,
		MaxBackoff:          200 * time.Millisecond,
	}

	stalling := messaging.EventDef{Topic: "stalling", Type: "document.created", Version: "1.0.0"}
	flowing := messaging.EventDef{Topic: "flowing", Type: "document.created", Version: "1.0.0"}

	consumer, err := messaging.NewConsumer(cfg)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	t.Cleanup(func() { _ = consumer.Close() })

	// Always fails: every delivery burns all four attempts and the full
	// backoff, holding the stalling topic's only slot throughout.
	stallingHandler := newCollectingHandler(1 << 30)
	if err := messaging.RegisterHandler(consumer, stalling, stallingHandler); err != nil {
		t.Fatalf("RegisterHandler(stalling): %v", err)
	}
	flowingHandler := newCollectingHandler(0)
	if err := messaging.RegisterHandler(consumer, flowing, flowingHandler); err != nil {
		t.Fatalf("RegisterHandler(flowing): %v", err)
	}

	publisher, err := messaging.NewPublisher(cfg)
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	t.Cleanup(func() { _ = publisher.Close() })

	ctx := context.Background()
	const flowingCount = 5
	for i := 0; i < flowingCount; i++ {
		if _, err := publisher.Publish(ctx, stalling, documentCreatedPayload{}); err != nil {
			t.Fatalf("Publish(stalling): %v", err)
		}
		if _, err := publisher.Publish(ctx, flowing, documentCreatedPayload{}); err != nil {
			t.Fatalf("Publish(flowing): %v", err)
		}
	}

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- consumer.Run(runCtx) }()

	// The flowing topic must drain on its own slot while the stalling topic
	// is still working through its first message's retries.
	deadline := time.After(15 * time.Second)
	for len(flowingHandler.snapshot()) < flowingCount {
		select {
		case <-flowingHandler.notify:
		case <-deadline:
			cancel()
			<-done
			t.Fatalf("flowing topic received %d/%d events: one topic's retries are stalling another",
				len(flowingHandler.snapshot()), flowingCount)
		}
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
}
```

- [ ] **Step 5: Run it**

Run: `go test -tags=integration -race -count=1 -run TestConsumer_RetryStormOnOneTopicDoesNotStallAnother ./ -v`
Expected: PASS.

- [ ] **Step 6: Write the DLQ contents test**

Append to `consumer_integration_test.go`:

```go
func TestConsumer_PermanentErrorReachesTheDLQStream(t *testing.T) {
	cfg := consumeFixture(t)

	consumer, err := messaging.NewConsumer(cfg)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	t.Cleanup(func() { _ = consumer.Close() })

	handled := make(chan struct{}, 1)
	h := permanentlyFailingHandler{done: handled}
	if err := messaging.RegisterHandler(consumer, documentCreated, h); err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}

	publisher, err := messaging.NewPublisher(cfg)
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	t.Cleanup(func() { _ = publisher.Close() })

	ctx := context.Background()
	published, err := publisher.Publish(ctx, documentCreated, documentCreatedPayload{})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- consumer.Run(runCtx) }()

	select {
	case <-handled:
	case <-time.After(10 * time.Second):
		cancel()
		<-done
		t.Fatal("handler was never called")
	}

	// Let the DLQ publish and the ack settle before tearing down.
	time.Sleep(500 * time.Millisecond)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}

	entries, err := testsupport.ReadStreamEntries(ctx, cfg.Redis.Address, "foi:documents.dlq")
	if err != nil {
		t.Fatalf("ReadStreamEntries: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("DLQ has %d entries, want 1", len(entries))
	}

	var dl messaging.DeadLetter
	if err := json.Unmarshal([]byte(entries[0].Fields["payload"]), &dl); err != nil {
		t.Fatalf("unmarshalling dead letter: %v", err)
	}
	if dl.Reason != messaging.ReasonPermanent {
		t.Errorf("Reason = %q, want %q", dl.Reason, messaging.ReasonPermanent)
	}
	if dl.OriginalTopic != "documents" {
		t.Errorf("OriginalTopic = %q, want %q", dl.OriginalTopic, "documents")
	}
	if dl.ConsumerGroup != cfg.Consumer.Group {
		t.Errorf("ConsumerGroup = %q, want %q", dl.ConsumerGroup, cfg.Consumer.Group)
	}

	// The original envelope must survive byte-for-byte so replay tooling
	// can republish it without transformation (PRD §14).
	var env messaging.Envelope[documentCreatedPayload]
	if err := json.Unmarshal(dl.Event, &env); err != nil {
		t.Fatalf("dead-lettered event is not a readable envelope: %v", err)
	}
	if env.EventID != published.EventID {
		t.Errorf("EventID = %q, want %q", env.EventID, published.EventID)
	}

	// And the original entry must be acked: a dead-lettered event is
	// durably recorded elsewhere, so leaving it pending would redeliver it.
	pending, err := testsupport.PendingCount(ctx, cfg.Redis.Address, "foi:documents", cfg.Consumer.Group)
	if err != nil {
		t.Fatalf("PendingCount: %v", err)
	}
	if pending != 0 {
		t.Errorf("pending = %d, want 0: a dead-lettered event must be acked", pending)
	}
}

// permanentlyFailingHandler classifies its failure as permanent, so it is
// dead-lettered on the first delivery without retry.
type permanentlyFailingHandler struct {
	done chan struct{}
}

func (h permanentlyFailingHandler) Handle(context.Context, messaging.Envelope[documentCreatedPayload]) error {
	select {
	case h.done <- struct{}{}:
	default:
	}
	return messaging.AsPermanent(errDeliberate)
}
```

Add `"encoding/json"` to the file's imports.

Confirm the DLQ payload really lands in the `payload` field of the Redis entry — that is `redisstream.DefaultMarshallerUnmarshaller`'s key. If `ReadStreamEntries` shows a different key, use whichever key the marshaller writes.

- [ ] **Step 7: Write the poison-backlog test**

This is the test 2a spec §10 explicitly asked for. Append:

```go
// TestConsumer_PoisonBacklogDrainsAndLiveTrafficResumes is 2a spec §10's
// requested test. A reclaim sweep claims up to 100 entries and each occupies
// a concurrency slot for a full claim/decode/release cycle — with retry,
// for four handler invocations. At Concurrency 1 an accumulated poison
// backlog therefore starves the read loop, and it is the delivery-attempt
// cap that bounds it: once each entry is dead-lettered and acked, the
// backlog is gone for good and live traffic moves again.
func TestConsumer_PoisonBacklogDrainsAndLiveTrafficResumes(t *testing.T) {
	cfg := consumeFixture(t)
	cfg.Consumer.Concurrency = 1
	cfg.Consumer.MaxDeliveryAttempts = 2
	cfg.Consumer.ClaimInterval = 500 * time.Millisecond
	cfg.Consumer.ClaimMinIdle = 500 * time.Millisecond

	consumer, err := messaging.NewConsumer(cfg)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	t.Cleanup(func() { _ = consumer.Close() })

	// Fails every delivery: unclassified, so retryable, so it is the cap
	// rather than classification that ends it.
	handler := newCollectingHandler(1 << 30)
	if err := messaging.RegisterHandler(consumer, documentCreated, handler); err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}

	publisher, err := messaging.NewPublisher(cfg)
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	t.Cleanup(func() { _ = publisher.Close() })

	ctx := context.Background()
	const poisonCount = 20
	for i := 0; i < poisonCount; i++ {
		if _, err := publisher.Publish(ctx, documentCreated, documentCreatedPayload{}); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- consumer.Run(runCtx) }()

	// Every poison entry must end up in the DLQ and be acked, leaving the
	// pending list empty — the state in which live traffic can flow again.
	deadline := time.After(60 * time.Second)
	for {
		pending, err := testsupport.PendingCount(ctx, cfg.Redis.Address, "foi:documents", cfg.Consumer.Group)
		if err != nil {
			t.Fatalf("PendingCount: %v", err)
		}
		if pending == 0 {
			break
		}
		select {
		case <-deadline:
			cancel()
			<-done
			t.Fatalf("%d entries still pending: the cap is not draining the poison backlog", pending)
		case <-time.After(200 * time.Millisecond):
		}
	}

	entries, err := testsupport.ReadStreamEntries(ctx, cfg.Redis.Address, "foi:documents.dlq")
	if err != nil {
		t.Fatalf("ReadStreamEntries: %v", err)
	}
	if len(entries) != poisonCount {
		t.Errorf("DLQ has %d entries, want %d", len(entries), poisonCount)
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
}
```

- [ ] **Step 8: Run the full integration suite**

Run: `go test -tags=integration -race -count=1 ./... -v`
Expected: PASS, including every pre-existing test.

- [ ] **Step 9: Commit**

```bash
git add consumer_integration_test.go
git commit -m "test: cover retry, the cap, and the DLQ against real Redis

Includes the per-topic isolation test 2a's review implied and the
poison-backlog test it asked for. Retunes the reclaim test: with three
immediate retries, failing twice no longer reaches the reclaim path it
exists to exercise."
```

---

### Task 10: Documentation

**Files:**
- Modify: `README.md:135`, `README.md:147-149`, `README.md:158`, `README.md:172`
- Modify: `doc.go`
- Modify: `CLAUDE.md` ("Implementation status", "Consume path")

**Interfaces:**
- Consumes: everything.
- Produces: nothing.

Past review rounds repeatedly caught the docs claiming unimplemented behaviour. This task exists so the reverse — docs still marked "planned" for shipped behaviour — does not replace it.

- [ ] **Step 1: Update README**

Remove the `(Phase 2b)` markers from the classification examples (`README.md:147-149`) and rewrite each line's outcome to match what Task 7 built:

```markdown
return messaging.AsPermanent(err) // → Dead Letter Queue, then ACK
return messaging.AsRetryable(err) // → retried in-process, then NACKed
return messaging.AsDiscard(err)   // → acknowledged without retry or DLQ
```

Rewrite the DLQ paragraph (`README.md:158`) in the present tense, and note the `event`/`event_raw` split.

Rewrite the configuration paragraph (`README.md:172`), which currently says `MaxDeliveryAttempts` and `RetryConfig` "are accepted and defaulted but are not yet read by any code path". They are now read. Replace that clause with a note that worst-case retry backoff is validated against `ClaimMinIdle`.

Extend the ordering bullet (`README.md:135`) with the retry consequence:

```markdown
- **Ordering is per-stream and only with `Concurrency: 1`** (the default). Reclaimed messages arrive out of order. `Concurrency` bounds in-flight handlers *per subscribed topic*, so a consumer registered on three topics at `Concurrency: 3` can be running nine handlers. Immediate retries run inside the message's slot, so a retrying message holds its topic's slot for the whole retry window — which is what preserves ordering at `Concurrency: 1`.
```

- [ ] **Step 2: Update `doc.go`**

Remove Phase 2b from its "Planned" list and describe the failure path in the present tense: classification, immediate retry, the cap, and the DLQ. Leave the Phase 3 and Phase 4 markers exactly as they are.

- [ ] **Step 3: Update `CLAUDE.md`**

In "Implementation status", move Phase 2b from "Not yet implemented" to the done list, and delete the "**Consequence today: any handler error NACKs and the reclaim loop redelivers forever.**" line — it is no longer true.

In the "Consume path" section, add the dispatch pipeline's order and the reason for it, since it is exactly the kind of non-obvious invariant that section exists to record:

```markdown
`Consumer.dispatch` is the whole failure path: the delivery-attempt cap fires
before decoding (an over-cap event must not spend four handler invocations, and
its concurrency slot, proving it), then undecodable/invalid/unversioned
envelopes are dead-lettered rather than nacked (all three are permanent by
definition), then `runWithRetry` runs the handler with full-jitter backoff.
Retry is a plain loop in the root package, **not** Watermill middleware: the
classification predicates live in the root, and the root imports
`internal/watermill`, so a middleware there would be an import cycle.
Retries sleep inside the message's per-topic concurrency slot, which is why
`ClaimMinIdle` must exceed `(1+MaxImmediateRetries) × handler + backoff`.
```

- [ ] **Step 4: Verify the docs against the code**

Re-read each changed paragraph against the implementation. Every claim must be one Task 1–9 actually delivered.

- [ ] **Step 5: Full verification**

```bash
go build ./...
golangci-lint run ./...
go test -race -count=1 ./...
go test -tags=integration -race -count=1 ./...
```
Expected: all four clean. Do not claim the phase is done on anything less.

- [ ] **Step 6: Commit**

```bash
git add README.md doc.go CLAUDE.md
git commit -m "docs: mark Phase 2b landed

Classification, retry, the cap, and the DLQ ship in this phase; the
docs described them as planned. Records the dispatch pipeline's
ordering and why retry is not watermill middleware."
```

---

## Verification Checklist

Before the phase branch is merged:

- [ ] `go build ./...` clean
- [ ] `golangci-lint run ./...` clean, with no new `//nolint` directives
- [ ] `go test -race -count=1 ./...` passes
- [ ] `go test -tags=integration -race -count=1 ./...` passes
- [ ] `errors.go` and `dlq.go` no longer contain "Not yet implemented"
- [ ] No `messaging` root-package file imports Watermill or go-redis
- [ ] `README.md`, `doc.go`, and `CLAUDE.md` carry no "Phase 2b" planned markers
- [ ] Findings from review are carried back into
      `docs/superpowers/specs/2026-08-11-phase-2b-retry-cap-dlq-design.md`
