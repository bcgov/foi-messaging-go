# Phase 2a: Consume Path Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement a `Consumer` that subscribes to Redis streams, dispatches each event to a strongly-typed handler by `event_type` + major `schema_version`, and shuts down gracefully — with no error classification, retry, or DLQ (those are Phase 2b).

**Architecture:** We own the read path. `internal/redis.StreamReader` issues the raw Redis Streams commands and returns plain structs. `internal/watermill.Subscriber` implements Watermill's `message.Subscriber` on top of it, bounding in-flight messages with a semaphore and stamping the delivery attempt into message metadata. Watermill's `Router` is retained for handler lifecycle and drain, wrapped so the root package never sees a Watermill type. The root `messaging` package owns registration, dispatch, and lifecycle.

**Tech Stack:** Go 1.25, `github.com/ThreeDotsLabs/watermill` (Router, `message.Subscriber`, `pubsub/tests` conformance suite), `github.com/ThreeDotsLabs/watermill-redisstream` (entry marshaller only), `github.com/redis/go-redis/v9`, `github.com/testcontainers/testcontainers-go/modules/redis`.

**Spec:** [`docs/superpowers/specs/2026-08-11-phase-2a-consume-path-design.md`](../specs/2026-08-11-phase-2a-consume-path-design.md)

## Global Constraints

- Module path: `github.com/bcgov/foi-messaging-go`
- No error classification (`errors.go`), retry middleware, delivery-attempt cap, or DLQ (`dlq.go`) in this phase — those are Phase 2b. `dlq.go` and `errors.go` keep their Phase 0 stub comments.
- No OpenTelemetry spans or Prometheus metrics this phase (Phase 3). Dispatch logs via `cfg.Telemetry.Logger` but registers no meters.
- No `testing/` (`messagingtest`) package this phase (Phase 4).
- `depguard` must continue to block `github.com/ThreeDotsLabs/watermill`, `github.com/ThreeDotsLabs/watermill-redisstream`, and `github.com/redis/go-redis` outside `internal/`. **The root `messaging` package must never import any of them, including `message.Message`.**
- `internal/redis` imports go-redis and never Watermill. `internal/watermill` imports Watermill and never go-redis.
- Consumer groups are created at ID `0` (`XGROUP CREATE … 0 MKSTREAM`), tolerating `BUSYGROUP`.
- A reclaimed message's stamped attempt is `RetryCount + 1`, because the `XCLAIM` being issued is itself the next delivery.
- The Watermill "topic" passed to `Subscribe` is the **full stream name** (`{StreamPrefix}:{EventDef.Topic}`), not the logical topic.
- `encoding/json` decoding must never set `DisallowUnknownFields` (PRD §11).
- Tests use the standard library `testing` package only — no testify in first-party test code (matching Phases 0 and 1). Integration tests carry `//go:build integration` and live in `package messaging_test`.

---

## Task 1: `internal/redis.StreamReader`

**Files:**
- Create: `internal/redis/stream.go`
- Test: `internal/redis/stream_integration_test.go`

**Interfaces:**
- Consumes: `internal/redis.NewClient` (Phase 1), `internal/testsupport.StartRedis` (Phase 0)
- Produces:
  - `type Entry struct { ID string; Fields map[string]any }`
  - `type PendingEntry struct { ID string; RetryCount int64; Idle time.Duration }`
  - `type StreamReader struct { … }`
  - `func NewStreamReader(client *goredis.Client, group, consumer string) *StreamReader`
  - `func (r *StreamReader) EnsureGroup(ctx context.Context, stream string) error`
  - `func (r *StreamReader) ReadNew(ctx context.Context, stream string, count int64, block time.Duration) ([]Entry, error)`
  - `func (r *StreamReader) PendingOverIdle(ctx context.Context, stream string, minIdle time.Duration, count int64) ([]PendingEntry, error)`
  - `func (r *StreamReader) Claim(ctx context.Context, stream string, minIdle time.Duration, ids []string) ([]Entry, error)`
  - `func (r *StreamReader) Ack(ctx context.Context, stream string, ids ...string) error`
  - `func (r *StreamReader) Close() error`

  Task 2 consumes all of these through an interface.

- [ ] **Step 1: Write the failing integration test**

Create `internal/redis/stream_integration_test.go`:

```go
//go:build integration

package redis_test

import (
	"context"
	"testing"
	"time"

	internalredis "github.com/bcgov/foi-messaging-go/internal/redis"
	"github.com/bcgov/foi-messaging-go/internal/testsupport"
	goredis "github.com/redis/go-redis/v9"
)

const (
	testGroup    = "test-group"
	testConsumer = "test-consumer"
)

// newReader starts a Redis container and returns a StreamReader bound to it.
func newReader(t *testing.T) (*internalredis.StreamReader, *goredis.Client) {
	t.Helper()
	ctx := context.Background()

	addr, terminate, err := testsupport.StartRedis(ctx)
	if err != nil {
		t.Fatalf("StartRedis: %v", err)
	}
	t.Cleanup(func() {
		if err := terminate(ctx); err != nil {
			t.Errorf("terminate: %v", err)
		}
	})

	client := internalredis.NewClient(internalredis.ClientOptions{Address: addr})
	reader := internalredis.NewStreamReader(client, testGroup, testConsumer)
	t.Cleanup(func() {
		if err := reader.Close(); err != nil {
			t.Errorf("reader.Close: %v", err)
		}
	})
	return reader, client
}

func TestStreamReader_EnsureGroup_IsIdempotent(t *testing.T) {
	ctx := context.Background()
	reader, _ := newReader(t)

	if err := reader.EnsureGroup(ctx, "s1"); err != nil {
		t.Fatalf("first EnsureGroup: %v", err)
	}
	if err := reader.EnsureGroup(ctx, "s1"); err != nil {
		t.Fatalf("second EnsureGroup should tolerate BUSYGROUP, got: %v", err)
	}
}

func TestStreamReader_EnsureGroup_ReadsFromOldest(t *testing.T) {
	ctx := context.Background()
	reader, client := newReader(t)

	// Publish BEFORE the group exists. Creating at "0" must still see it.
	if err := client.XAdd(ctx, &goredis.XAddArgs{
		Stream: "s2",
		Values: map[string]any{"payload": "before-group"},
	}).Err(); err != nil {
		t.Fatalf("XAdd: %v", err)
	}

	if err := reader.EnsureGroup(ctx, "s2"); err != nil {
		t.Fatalf("EnsureGroup: %v", err)
	}

	entries, err := reader.ReadNew(ctx, "s2", 10, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("ReadNew: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("len(entries) = %d, want 1 (group created at 0 must replay history)", len(entries))
	}
	if got := entries[0].Fields["payload"]; got != "before-group" {
		t.Errorf("payload = %v, want %q", got, "before-group")
	}
}

func TestStreamReader_ReadNew_ReturnsEmptyWhenNothingPending(t *testing.T) {
	ctx := context.Background()
	reader, _ := newReader(t)

	if err := reader.EnsureGroup(ctx, "s3"); err != nil {
		t.Fatalf("EnsureGroup: %v", err)
	}

	entries, err := reader.ReadNew(ctx, "s3", 10, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("ReadNew on empty stream must not error, got: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("len(entries) = %d, want 0", len(entries))
	}
}

func TestStreamReader_PendingOverIdle_ReportsRetryCount(t *testing.T) {
	ctx := context.Background()
	reader, client := newReader(t)

	if err := reader.EnsureGroup(ctx, "s4"); err != nil {
		t.Fatalf("EnsureGroup: %v", err)
	}
	if err := client.XAdd(ctx, &goredis.XAddArgs{
		Stream: "s4",
		Values: map[string]any{"payload": "p"},
	}).Err(); err != nil {
		t.Fatalf("XAdd: %v", err)
	}

	// One delivery, never acked.
	entries, err := reader.ReadNew(ctx, "s4", 10, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("ReadNew: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("len(entries) = %d, want 1", len(entries))
	}

	time.Sleep(150 * time.Millisecond)

	pending, err := reader.PendingOverIdle(ctx, "s4", 100*time.Millisecond, 10)
	if err != nil {
		t.Fatalf("PendingOverIdle: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("len(pending) = %d, want 1", len(pending))
	}
	if pending[0].ID != entries[0].ID {
		t.Errorf("pending ID = %q, want %q", pending[0].ID, entries[0].ID)
	}
	if pending[0].RetryCount != 1 {
		t.Errorf("RetryCount = %d, want 1 after a single delivery", pending[0].RetryCount)
	}
}

func TestStreamReader_Claim_IncrementsDeliveryCount(t *testing.T) {
	ctx := context.Background()
	reader, client := newReader(t)

	if err := reader.EnsureGroup(ctx, "s5"); err != nil {
		t.Fatalf("EnsureGroup: %v", err)
	}
	if err := client.XAdd(ctx, &goredis.XAddArgs{
		Stream: "s5",
		Values: map[string]any{"payload": "p"},
	}).Err(); err != nil {
		t.Fatalf("XAdd: %v", err)
	}
	if _, err := reader.ReadNew(ctx, "s5", 10, 100*time.Millisecond); err != nil {
		t.Fatalf("ReadNew: %v", err)
	}

	time.Sleep(150 * time.Millisecond)

	pending, err := reader.PendingOverIdle(ctx, "s5", 100*time.Millisecond, 10)
	if err != nil {
		t.Fatalf("PendingOverIdle: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("len(pending) = %d, want 1", len(pending))
	}

	claimed, err := reader.Claim(ctx, "s5", 100*time.Millisecond, []string{pending[0].ID})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("len(claimed) = %d, want 1", len(claimed))
	}

	time.Sleep(150 * time.Millisecond)

	after, err := reader.PendingOverIdle(ctx, "s5", 100*time.Millisecond, 10)
	if err != nil {
		t.Fatalf("PendingOverIdle after claim: %v", err)
	}
	if len(after) != 1 {
		t.Fatalf("len(after) = %d, want 1", len(after))
	}
	// This is why the subscriber stamps RetryCount+1: XCLAIM is itself a delivery.
	if after[0].RetryCount != 2 {
		t.Errorf("RetryCount after claim = %d, want 2", after[0].RetryCount)
	}
}

func TestStreamReader_Ack_RemovesFromPending(t *testing.T) {
	ctx := context.Background()
	reader, client := newReader(t)

	if err := reader.EnsureGroup(ctx, "s6"); err != nil {
		t.Fatalf("EnsureGroup: %v", err)
	}
	if err := client.XAdd(ctx, &goredis.XAddArgs{
		Stream: "s6",
		Values: map[string]any{"payload": "p"},
	}).Err(); err != nil {
		t.Fatalf("XAdd: %v", err)
	}
	entries, err := reader.ReadNew(ctx, "s6", 10, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("ReadNew: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("len(entries) = %d, want 1", len(entries))
	}

	if err := reader.Ack(ctx, "s6", entries[0].ID); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	pending, err := reader.PendingOverIdle(ctx, "s6", 0, 10)
	if err != nil {
		t.Fatalf("PendingOverIdle: %v", err)
	}
	if len(pending) != 0 {
		t.Errorf("len(pending) = %d, want 0 after Ack", len(pending))
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test -tags=integration ./internal/redis/... -v`
Expected: FAIL — `undefined: internalredis.NewStreamReader`, `undefined: internalredis.StreamReader`.

- [ ] **Step 3: Create `internal/redis/stream.go`**

```go
package redis

import (
	"context"
	"fmt"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// Entry is a single Redis Streams entry, reduced to plain types so no
// go-redis value escapes this package.
type Entry struct {
	ID     string
	Fields map[string]any
}

// PendingEntry describes one entry in a consumer group's Pending Entries
// List. RetryCount is the number of deliveries that have already happened;
// a claim or read issued now is the next one.
type PendingEntry struct {
	ID         string
	RetryCount int64
	Idle       time.Duration
}

// StreamReader issues the Redis Streams commands the consumer needs,
// bound to a single consumer group and consumer name.
type StreamReader struct {
	client   *goredis.Client
	group    string
	consumer string
}

// NewStreamReader builds a StreamReader. It takes ownership of client and
// closes it when Close is called.
func NewStreamReader(client *goredis.Client, group, consumer string) *StreamReader {
	return &StreamReader{client: client, group: group, consumer: consumer}
}

// EnsureGroup creates the consumer group on stream at ID "0", creating the
// stream if absent. An already-existing group is not an error, so every
// consumer instance can call this unconditionally on startup.
//
// Starting at "0" means a newly created group replays whatever history is
// still on the stream, so a service deployed after its producer does not
// silently miss events.
func (r *StreamReader) EnsureGroup(ctx context.Context, stream string) error {
	err := r.client.XGroupCreateMkStream(ctx, stream, r.group, "0").Err()
	if err == nil || strings.Contains(err.Error(), "BUSYGROUP") {
		return nil
	}
	return fmt.Errorf("creating consumer group %q on stream %q: %w", r.group, stream, err)
}

// ReadNew issues XREADGROUP for never-delivered entries. Every entry it
// returns is a first delivery. An empty result is not an error.
func (r *StreamReader) ReadNew(ctx context.Context, stream string, count int64, block time.Duration) ([]Entry, error) {
	streams, err := r.client.XReadGroup(ctx, &goredis.XReadGroupArgs{
		Group:    r.group,
		Consumer: r.consumer,
		Streams:  []string{stream, ">"},
		Count:    count,
		Block:    block,
	}).Result()
	if err == goredis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("reading group %q on stream %q: %w", r.group, stream, err)
	}

	var entries []Entry
	for _, s := range streams {
		for _, m := range s.Messages {
			entries = append(entries, Entry{ID: m.ID, Fields: m.Values})
		}
	}
	return entries, nil
}

// PendingOverIdle lists entries pending longer than minIdle. It is the only
// source of delivery counts, and is already the reclaim discovery step, so
// exact attempt counts cost no extra round-trip.
func (r *StreamReader) PendingOverIdle(ctx context.Context, stream string, minIdle time.Duration, count int64) ([]PendingEntry, error) {
	pending, err := r.client.XPendingExt(ctx, &goredis.XPendingExtArgs{
		Stream: stream,
		Group:  r.group,
		Idle:   minIdle,
		Start:  "-",
		End:    "+",
		Count:  count,
	}).Result()
	if err == goredis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("listing pending entries on stream %q: %w", stream, err)
	}

	entries := make([]PendingEntry, 0, len(pending))
	for _, p := range pending {
		entries = append(entries, PendingEntry{
			ID:         p.ID,
			RetryCount: p.RetryCount,
			Idle:       p.Idle,
		})
	}
	return entries, nil
}

// Claim takes ownership of pending entries via XCLAIM. minIdle is passed as
// MINIDLE so two instances cannot claim the same entry concurrently.
//
// XCLAIM increments each entry's delivery counter — it is deliberately not
// issued with JUSTID — so a claimed entry's true attempt number is the
// RetryCount reported by PendingOverIdle plus one.
func (r *StreamReader) Claim(ctx context.Context, stream string, minIdle time.Duration, ids []string) ([]Entry, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	messages, err := r.client.XClaim(ctx, &goredis.XClaimArgs{
		Stream:   stream,
		Group:    r.group,
		Consumer: r.consumer,
		MinIdle:  minIdle,
		Messages: ids,
	}).Result()
	if err == goredis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("claiming entries on stream %q: %w", stream, err)
	}

	entries := make([]Entry, 0, len(messages))
	for _, m := range messages {
		entries = append(entries, Entry{ID: m.ID, Fields: m.Values})
	}
	return entries, nil
}

// Ack acknowledges entries, removing them from the group's pending list.
func (r *StreamReader) Ack(ctx context.Context, stream string, ids ...string) error {
	if len(ids) == 0 {
		return nil
	}
	if err := r.client.XAck(ctx, stream, r.group, ids...).Err(); err != nil {
		return fmt.Errorf("acking entries on stream %q: %w", stream, err)
	}
	return nil
}

// Close releases the underlying Redis client.
func (r *StreamReader) Close() error {
	return r.client.Close()
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test -tags=integration ./internal/redis/... -v`
Expected: PASS — all six tests.

- [ ] **Step 5: Run the linter and the unit suite**

Run: `make lint && make test`
Expected: no findings, all existing tests still pass.

- [ ] **Step 6: Commit**

```bash
git add internal/redis/stream.go internal/redis/stream_integration_test.go
git commit -m "feat: add internal/redis StreamReader for Redis Streams consumption"
```

---

## Task 2: `internal/watermill.Subscriber` — read loop and ack handling

**Files:**
- Create: `internal/watermill/subscriber.go`
- Test: `internal/watermill/subscriber_test.go`

**Interfaces:**
- Consumes: `internal/redis.Entry`, `internal/redis.PendingEntry` (Task 1)
- Produces:
  - `const MetadataStreamID = "_foi_stream_id"`
  - `const MetadataDeliveryAttempt = "_foi_delivery_attempt"`
  - `type StreamReader interface { … }` (the five methods from Task 1)
  - `type SubscriberOptions struct { Reader StreamReader; Concurrency int; ClaimInterval, ClaimMinIdle, BlockTime time.Duration }`
  - `func NewSubscriber(opts SubscriberOptions) (*Subscriber, error)`
  - `func (s *Subscriber) Subscribe(ctx context.Context, stream string) (<-chan *message.Message, error)`
  - `func (s *Subscriber) Close() error`

  Task 3 adds the claim loop to this same type. Task 6 passes the subscriber to the Router wrapper.

Note: `BlockTime` is an internal knob defaulted to 1s. It exists so the Task 3 conformance suite can lower it; it is deliberately absent from the public `messaging.Config`.

- [ ] **Step 1: Write the failing test**

Create `internal/watermill/subscriber_test.go`:

```go
package watermill

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	internalredis "github.com/bcgov/foi-messaging-go/internal/redis"
)

// fakeReader is a StreamReader that serves canned entries and records calls,
// so the subscriber's slot and ack behaviour can be tested without Redis.
type fakeReader struct {
	mu sync.Mutex

	queued  []internalredis.Entry
	pending []internalredis.PendingEntry
	claimed map[string]internalredis.Entry

	acked    []string
	reads    int
	readGate chan struct{} // if non-nil, ReadNew blocks on it after draining
}

func newFakeReader(entries ...internalredis.Entry) *fakeReader {
	return &fakeReader{queued: entries, claimed: map[string]internalredis.Entry{}}
}

func (f *fakeReader) EnsureGroup(context.Context, string) error { return nil }

func (f *fakeReader) ReadNew(ctx context.Context, _ string, count int64, block time.Duration) ([]internalredis.Entry, error) {
	f.mu.Lock()
	f.reads++
	if len(f.queued) > 0 {
		n := int(count)
		if n > len(f.queued) {
			n = len(f.queued)
		}
		out := f.queued[:n]
		f.queued = f.queued[n:]
		f.mu.Unlock()
		return out, nil
	}
	f.mu.Unlock()

	// Nothing left: emulate a blocking read that times out.
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(block):
		return nil, nil
	}
}

func (f *fakeReader) PendingOverIdle(context.Context, string, time.Duration, int64) ([]internalredis.PendingEntry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := f.pending
	f.pending = nil
	return out, nil
}

func (f *fakeReader) Claim(_ context.Context, _ string, _ time.Duration, ids []string) ([]internalredis.Entry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []internalredis.Entry
	for _, id := range ids {
		if e, ok := f.claimed[id]; ok {
			out = append(out, e)
		}
	}
	return out, nil
}

func (f *fakeReader) Ack(_ context.Context, _ string, ids ...string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.acked = append(f.acked, ids...)
	return nil
}

func (f *fakeReader) ackedIDs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.acked...)
}

func (f *fakeReader) readCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.reads
}

// entry builds a stream entry in the wire format the Phase 1 publisher writes.
func entry(id, uuid, payload string) internalredis.Entry {
	return internalredis.Entry{
		ID: id,
		Fields: map[string]any{
			"_watermill_message_uuid": uuid,
			"payload":                 payload,
		},
	}
}

func TestSubscriber_DeliversEntryWithMetadata(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reader := newFakeReader(entry("1-0", "event-a", `{"hello":"world"}`))
	sub, err := NewSubscriber(SubscriberOptions{
		Reader:      reader,
		Concurrency: 1,
		BlockTime:   10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewSubscriber: %v", err)
	}
	defer func() {
		if err := sub.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()

	out, err := sub.Subscribe(ctx, "stream")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	select {
	case msg := <-out:
		if msg.UUID != "event-a" {
			t.Errorf("UUID = %q, want %q", msg.UUID, "event-a")
		}
		if string(msg.Payload) != `{"hello":"world"}` {
			t.Errorf("Payload = %q, want %q", msg.Payload, `{"hello":"world"}`)
		}
		if got := msg.Metadata.Get(MetadataStreamID); got != "1-0" {
			t.Errorf("%s = %q, want %q", MetadataStreamID, got, "1-0")
		}
		if got := msg.Metadata.Get(MetadataDeliveryAttempt); got != "1" {
			t.Errorf("%s = %q, want %q for a fresh read", MetadataDeliveryAttempt, got, "1")
		}
		msg.Ack()
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for a message")
	}
}

func TestSubscriber_AcksEntryOnMessageAck(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reader := newFakeReader(entry("1-0", "event-a", "{}"))
	sub, err := NewSubscriber(SubscriberOptions{
		Reader:      reader,
		Concurrency: 1,
		BlockTime:   10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewSubscriber: %v", err)
	}
	defer func() { _ = sub.Close() }()

	out, err := sub.Subscribe(ctx, "stream")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	msg := <-out
	msg.Ack()

	deadline := time.After(2 * time.Second)
	for {
		if ids := reader.ackedIDs(); len(ids) == 1 && ids[0] == "1-0" {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("acked = %v, want [1-0]", reader.ackedIDs())
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestSubscriber_DoesNotAckOnNack(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reader := newFakeReader(entry("1-0", "event-a", "{}"))
	sub, err := NewSubscriber(SubscriberOptions{
		Reader:      reader,
		Concurrency: 1,
		BlockTime:   10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewSubscriber: %v", err)
	}
	defer func() { _ = sub.Close() }()

	out, err := sub.Subscribe(ctx, "stream")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	msg := <-out
	msg.Nack()

	time.Sleep(200 * time.Millisecond)
	if ids := reader.ackedIDs(); len(ids) != 0 {
		t.Errorf("acked = %v, want none — a nacked entry must stay pending", ids)
	}
}

func TestSubscriber_AcquiresSlotBeforeReading(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reader := newFakeReader(
		entry("1-0", "event-a", "{}"),
		entry("2-0", "event-b", "{}"),
	)
	sub, err := NewSubscriber(SubscriberOptions{
		Reader:      reader,
		Concurrency: 1,
		BlockTime:   10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewSubscriber: %v", err)
	}
	defer func() { _ = sub.Close() }()

	out, err := sub.Subscribe(ctx, "stream")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	first := <-out
	// Hold the only slot. The loop must not read again while it is held,
	// so an unread entry must never be fetched into a local buffer where
	// its Redis idle clock would run down.
	countWhileHeld := reader.readCount()
	time.Sleep(200 * time.Millisecond)
	if got := reader.readCount(); got != countWhileHeld {
		t.Errorf("reads went %d → %d while the slot was held; must not read without a free slot", countWhileHeld, got)
	}

	first.Ack()

	select {
	case second := <-out:
		if second.UUID != "event-b" {
			t.Errorf("UUID = %q, want %q", second.UUID, "event-b")
		}
		second.Ack()
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the second message after the slot freed")
	}
}

func TestSubscriber_ClosesOutputChannelOnClose(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sub, err := NewSubscriber(SubscriberOptions{
		Reader:      newFakeReader(),
		Concurrency: 1,
		BlockTime:   10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewSubscriber: %v", err)
	}

	out, err := sub.Subscribe(ctx, "stream")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	if err := sub.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := sub.Close(); err != nil {
		t.Fatalf("second Close must be idempotent, got: %v", err)
	}

	select {
	case _, open := <-out:
		if open {
			t.Error("expected the output channel to be closed")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the output channel to close")
	}
}

func TestNewSubscriber_RejectsMissingReader(t *testing.T) {
	_, err := NewSubscriber(SubscriberOptions{Concurrency: 1})
	if err == nil {
		t.Fatal("expected an error when Reader is nil")
	}
	if !errors.Is(err, ErrNoReader) {
		t.Errorf("err = %v, want ErrNoReader", err)
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./internal/watermill/... -v`
Expected: FAIL — `undefined: NewSubscriber`, `undefined: MetadataStreamID`, `undefined: ErrNoReader`.

- [ ] **Step 3: Create `internal/watermill/subscriber.go`**

The claim loop is added in Task 3; this step wires the read loop only.

```go
package watermill

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/ThreeDotsLabs/watermill-redisstream/pkg/redisstream"
	"github.com/ThreeDotsLabs/watermill/message"

	internalredis "github.com/bcgov/foi-messaging-go/internal/redis"
)

// Metadata keys stamped on messages at consume time. They live on the
// in-process message only, are never written to Redis, and never enter the
// event envelope.
const (
	MetadataStreamID        = "_foi_stream_id"
	MetadataDeliveryAttempt = "_foi_delivery_attempt"
)

// ErrNoReader is returned by NewSubscriber when no StreamReader is supplied.
var ErrNoReader = errors.New("subscriber: Reader is required")

const (
	defaultBlockTime = time.Second
	// claimBatchSize bounds one reclaim sweep, not the total pending set.
	claimBatchSize = int64(100)
	// readErrorBackoff throttles a failing read loop so a broken connection
	// does not spin.
	readErrorBackoff = 500 * time.Millisecond
)

// StreamReader is the Redis Streams surface the Subscriber needs. It is
// satisfied by *internal/redis.StreamReader.
type StreamReader interface {
	EnsureGroup(ctx context.Context, stream string) error
	ReadNew(ctx context.Context, stream string, count int64, block time.Duration) ([]internalredis.Entry, error)
	PendingOverIdle(ctx context.Context, stream string, minIdle time.Duration, count int64) ([]internalredis.PendingEntry, error)
	Claim(ctx context.Context, stream string, minIdle time.Duration, ids []string) ([]internalredis.Entry, error)
	Ack(ctx context.Context, stream string, ids ...string) error
}

// SubscriberOptions configures a Subscriber. BlockTime is an internal knob
// (defaulted to 1s) so tests can shorten the blocking read; it deliberately
// has no equivalent in the public messaging.Config.
type SubscriberOptions struct {
	Reader        StreamReader
	Concurrency   int
	ClaimInterval time.Duration
	ClaimMinIdle  time.Duration
	BlockTime     time.Duration
}

// Subscriber implements watermill's message.Subscriber over Redis Streams,
// bounding in-flight messages so the router's goroutine-per-message
// behaviour cannot exceed the configured concurrency.
type Subscriber struct {
	reader        StreamReader
	claimInterval time.Duration
	claimMinIdle  time.Duration
	blockTime     time.Duration

	sem       chan struct{}
	closing   chan struct{}
	wg        sync.WaitGroup
	closeOnce sync.Once
}

// NewSubscriber builds a Subscriber. Concurrency below 1 is treated as 1.
func NewSubscriber(opts SubscriberOptions) (*Subscriber, error) {
	if opts.Reader == nil {
		return nil, ErrNoReader
	}
	if opts.Concurrency < 1 {
		opts.Concurrency = 1
	}
	if opts.BlockTime <= 0 {
		opts.BlockTime = defaultBlockTime
	}

	return &Subscriber{
		reader:        opts.Reader,
		claimInterval: opts.ClaimInterval,
		claimMinIdle:  opts.ClaimMinIdle,
		blockTime:     opts.BlockTime,
		sem:           make(chan struct{}, opts.Concurrency),
		closing:       make(chan struct{}),
	}, nil
}

// Subscribe consumes stream — the full Redis stream name, not the logical
// topic. The returned channel is closed when the subscriber is closed or ctx
// is cancelled.
func (s *Subscriber) Subscribe(ctx context.Context, stream string) (<-chan *message.Message, error) {
	if err := s.reader.EnsureGroup(ctx, stream); err != nil {
		return nil, fmt.Errorf("ensuring consumer group on %q: %w", stream, err)
	}

	out := make(chan *message.Message)

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer close(out)

		var loops sync.WaitGroup
		loops.Add(1)
		go func() {
			defer loops.Done()
			s.readLoop(ctx, stream, out)
		}()
		loops.Wait()
	}()

	return out, nil
}

// readLoop fetches never-delivered entries one slot at a time.
func (s *Subscriber) readLoop(ctx context.Context, stream string, out chan<- *message.Message) {
	for {
		if !s.acquire(ctx) {
			return
		}

		entries, err := s.reader.ReadNew(ctx, stream, 1, s.blockTime)
		if err != nil || len(entries) == 0 {
			s.release()
			if s.stopped(ctx) {
				return
			}
			if err != nil {
				s.pause(ctx, readErrorBackoff)
			}
			continue
		}

		// One slot was acquired, so exactly one entry is emitted; any
		// surplus would have nowhere to run.
		if !s.emit(ctx, stream, out, entries[0], 1) {
			return
		}
	}
}

// emit decodes an entry, hands it to out, and arranges for its ack or nack
// to be honoured. The caller must already hold a semaphore slot; emit takes
// responsibility for releasing it.
func (s *Subscriber) emit(ctx context.Context, stream string, out chan<- *message.Message, e internalredis.Entry, attempt int64) bool {
	msg, err := decodeEntry(e, attempt)
	if err != nil {
		// An entry we cannot even decode is left pending rather than
		// dropped; Phase 2b routes it to the DLQ.
		s.release()
		return !s.stopped(ctx)
	}
	msg.SetContext(ctx)

	select {
	case out <- msg:
	case <-s.closing:
		s.release()
		return false
	case <-ctx.Done():
		s.release()
		return false
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer s.release()

		select {
		case <-msg.Acked():
			// Ack with a context detached from the subscription so a
			// shutdown in progress still records completed work.
			ackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			_ = s.reader.Ack(ackCtx, stream, e.ID)
		case <-msg.Nacked():
			// Leave the entry pending; the claim loop redelivers it.
		case <-s.closing:
		}
	}()

	return true
}

// decodeEntry converts a stream entry into a watermill message using the
// same marshaller the Phase 1 publisher writes with, so the wire format
// cannot drift between the two halves of the library.
func decodeEntry(e internalredis.Entry, attempt int64) (*message.Message, error) {
	msg, err := redisstream.DefaultMarshallerUnmarshaller{}.Unmarshal(e.Fields)
	if err != nil {
		return nil, fmt.Errorf("unmarshalling entry %q: %w", e.ID, err)
	}
	msg.Metadata.Set(MetadataStreamID, e.ID)
	msg.Metadata.Set(MetadataDeliveryAttempt, strconv.FormatInt(attempt, 10))
	return msg, nil
}

// acquire takes a concurrency slot, reporting false if the subscriber is
// shutting down instead.
func (s *Subscriber) acquire(ctx context.Context) bool {
	select {
	case s.sem <- struct{}{}:
		return true
	case <-s.closing:
		return false
	case <-ctx.Done():
		return false
	}
}

func (s *Subscriber) release() {
	<-s.sem
}

// stopped reports whether the subscriber should stop looping.
func (s *Subscriber) stopped(ctx context.Context) bool {
	select {
	case <-s.closing:
		return true
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

// pause waits for d unless the subscriber is shutting down.
func (s *Subscriber) pause(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
	case <-s.closing:
	case <-ctx.Done():
	}
}

// Close stops all loops and waits for in-flight messages to settle. It is
// idempotent.
func (s *Subscriber) Close() error {
	s.closeOnce.Do(func() { close(s.closing) })
	s.wg.Wait()
	return nil
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./internal/watermill/... -race -v`
Expected: PASS — all six tests, no race warnings.

- [ ] **Step 5: Run the linter and the full unit suite**

Run: `make lint && make test`
Expected: no findings, all existing tests still pass.

- [ ] **Step 6: Commit**

```bash
git add internal/watermill/subscriber.go internal/watermill/subscriber_test.go
git commit -m "feat: add internal/watermill Subscriber read loop with bounded concurrency"
```

---

## Task 3: Subscriber claim loop and Pub/Sub conformance

**Files:**
- Modify: `internal/watermill/subscriber.go` (add `claimLoop`, start it from `Subscribe`)
- Modify: `internal/watermill/subscriber_test.go` (append the claim-loop tests)
- Create: `internal/watermill/subscriber_integration_test.go`

**Interfaces:**
- Consumes: everything from Task 2
- Produces: no new exported API. `Subscribe` now starts two loops instead of one.

- [ ] **Step 1: Write the failing claim-loop test**

Append to `internal/watermill/subscriber_test.go`:

```go
func TestSubscriber_ClaimLoopRedeliversWithIncrementedAttempt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reader := newFakeReader()
	// One entry already delivered once and left pending.
	reader.pending = []internalredis.PendingEntry{
		{ID: "1-0", RetryCount: 1, Idle: time.Second},
	}
	reader.claimed["1-0"] = entry("1-0", "event-a", "{}")

	sub, err := NewSubscriber(SubscriberOptions{
		Reader:        reader,
		Concurrency:   1,
		ClaimInterval: 20 * time.Millisecond,
		ClaimMinIdle:  10 * time.Millisecond,
		BlockTime:     10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewSubscriber: %v", err)
	}
	defer func() { _ = sub.Close() }()

	out, err := sub.Subscribe(ctx, "stream")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	select {
	case msg := <-out:
		if msg.UUID != "event-a" {
			t.Errorf("UUID = %q, want %q", msg.UUID, "event-a")
		}
		// XPENDING reported 1 prior delivery; the XCLAIM just issued is
		// the second, so the stamped attempt must be 2.
		if got := msg.Metadata.Get(MetadataDeliveryAttempt); got != "2" {
			t.Errorf("%s = %q, want %q (RetryCount+1)", MetadataDeliveryAttempt, got, "2")
		}
		if got := msg.Metadata.Get(MetadataStreamID); got != "1-0" {
			t.Errorf("%s = %q, want %q", MetadataStreamID, got, "1-0")
		}
		msg.Ack()
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for a reclaimed message")
	}
}

func TestSubscriber_ClaimLoopSkippedWithoutClaimInterval(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reader := newFakeReader()
	reader.pending = []internalredis.PendingEntry{
		{ID: "1-0", RetryCount: 1, Idle: time.Second},
	}
	reader.claimed["1-0"] = entry("1-0", "event-a", "{}")

	sub, err := NewSubscriber(SubscriberOptions{
		Reader:      reader,
		Concurrency: 1,
		BlockTime:   10 * time.Millisecond,
		// ClaimInterval left zero: reclaim disabled.
	})
	if err != nil {
		t.Fatalf("NewSubscriber: %v", err)
	}
	defer func() { _ = sub.Close() }()

	out, err := sub.Subscribe(ctx, "stream")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	select {
	case msg := <-out:
		t.Fatalf("unexpected reclaim with ClaimInterval unset: %q", msg.UUID)
	case <-time.After(300 * time.Millisecond):
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./internal/watermill/... -run TestSubscriber_ClaimLoop -v`
Expected: FAIL — `TestSubscriber_ClaimLoopRedeliversWithIncrementedAttempt` times out, because nothing reclaims pending entries yet.

- [ ] **Step 3: Add the claim loop**

In `internal/watermill/subscriber.go`, replace the goroutine body inside `Subscribe` with one that starts both loops:

```go
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer close(out)

		var loops sync.WaitGroup

		loops.Add(1)
		go func() {
			defer loops.Done()
			s.readLoop(ctx, stream, out)
		}()

		if s.claimInterval > 0 {
			loops.Add(1)
			go func() {
				defer loops.Done()
				s.claimLoop(ctx, stream, out)
			}()
		}

		loops.Wait()
	}()
```

Then append `claimLoop` to the same file:

```go
// claimLoop implements PRD §13 Layer 2. Every ClaimInterval it looks for
// entries pending longer than ClaimMinIdle and reclaims them, which is how
// nacked messages are redelivered and how messages survive a crashed
// consumer. Reclaimed messages arrive out of order relative to the live
// stream, which is inherent to reclaim.
func (s *Subscriber) claimLoop(ctx context.Context, stream string, out chan<- *message.Message) {
	ticker := time.NewTicker(s.claimInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.closing:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		pending, err := s.reader.PendingOverIdle(ctx, stream, s.claimMinIdle, claimBatchSize)
		if err != nil {
			s.pause(ctx, readErrorBackoff)
			continue
		}

		for _, p := range pending {
			if !s.acquire(ctx) {
				return
			}

			entries, err := s.reader.Claim(ctx, stream, s.claimMinIdle, []string{p.ID})
			if err != nil || len(entries) == 0 {
				// Lost the race to another instance, or the entry is gone.
				s.release()
				if s.stopped(ctx) {
					return
				}
				continue
			}

			// XPENDING reports deliveries that already happened; the XCLAIM
			// just issued is the next one.
			if !s.emit(ctx, stream, out, entries[0], p.RetryCount+1) {
				return
			}
		}
	}
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./internal/watermill/... -race -v`
Expected: PASS — all eight tests, no race warnings.

- [ ] **Step 5: Write the conformance-suite integration test**

Create `internal/watermill/subscriber_integration_test.go`:

```go
//go:build integration

package watermill_test

import (
	"context"
	"testing"
	"time"

	wm "github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-redisstream/pkg/redisstream"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/tests"

	internalredis "github.com/bcgov/foi-messaging-go/internal/redis"
	"github.com/bcgov/foi-messaging-go/internal/testsupport"
	internalwatermill "github.com/bcgov/foi-messaging-go/internal/watermill"
)

// redisAddr starts one Redis container shared by every case in the suite.
func redisAddr(t *testing.T) string {
	t.Helper()
	// Not t.Context(): it is cancelled before cleanups run, so terminating
	// the container with it would always fail.
	ctx := context.Background()

	addr, terminate, err := testsupport.StartRedis(ctx)
	if err != nil {
		t.Fatalf("StartRedis: %v", err)
	}
	t.Cleanup(func() {
		if err := terminate(ctx); err != nil {
			t.Errorf("terminate: %v", err)
		}
	})
	return addr
}

// TestPubSubConformance runs watermill's universal Pub/Sub suite — the same
// suite watermill-redisstream runs against its own subscriber — over our
// subscriber, paired with the upstream redisstream publisher so wire
// compatibility is exercised too.
func TestPubSubConformance(t *testing.T) {
	addr := redisAddr(t)

	newPubSub := func(t *testing.T, group string) (message.Publisher, message.Subscriber) {
		t.Helper()

		pubClient := internalredis.NewClient(internalredis.ClientOptions{Address: addr})
		pub, err := redisstream.NewPublisher(
			redisstream.PublisherConfig{
				Client:     pubClient,
				Marshaller: redisstream.DefaultMarshallerUnmarshaller{},
			},
			wm.NewStdLogger(false, false),
		)
		if err != nil {
			t.Fatalf("NewPublisher: %v", err)
		}

		subClient := internalredis.NewClient(internalredis.ClientOptions{Address: addr})
		reader := internalredis.NewStreamReader(subClient, group, wm.NewShortUUID())
		sub, err := internalwatermill.NewSubscriber(internalwatermill.SubscriberOptions{
			Reader:        reader,
			Concurrency:   1,
			ClaimInterval: 3 * time.Second,
			ClaimMinIdle:  5 * time.Second,
			BlockTime:     10 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("NewSubscriber: %v", err)
		}

		return pub, sub
	}

	features := tests.Features{
		ConsumerGroups:                      true,
		ExactlyOnceDelivery:                 false,
		GuaranteedOrder:                     false,
		GuaranteedOrderWithSingleSubscriber: true,
		Persistent:                          true,
		RequireSingleInstance:               false,
		NewSubscriberReceivesOldMessages:    true,
	}

	tests.TestPubSub(
		t,
		features,
		func(t *testing.T) (message.Publisher, message.Subscriber) {
			return newPubSub(t, wm.NewShortUUID())
		},
		newPubSub,
	)
}
```

- [ ] **Step 6: Run the conformance suite**

Run: `go test -tags=integration ./internal/watermill/... -run TestPubSubConformance -v -timeout 20m`
Expected: PASS.

If `TestResendOnError` or `TestNoAck` fails, the cause is almost certainly redelivery timing rather than a logic error: reclaim cannot redeliver sooner than `ClaimMinIdle`. Confirm by checking whether the message eventually arrives, and if so raise `defaultTimeout` pressure by lowering `ClaimMinIdle`/`ClaimInterval` in `newPubSub` — do **not** change subscriber logic to make the suite pass.

- [ ] **Step 7: Tidy modules, lint, and run everything**

Run: `make tidy && make lint && make test`
Expected: `go.mod` may gain `github.com/stretchr/testify` as an indirect requirement (pulled in by `watermill/pubsub/tests`); no linter findings; unit tests pass.

- [ ] **Step 8: Commit**

```bash
git add internal/watermill/subscriber.go internal/watermill/subscriber_test.go internal/watermill/subscriber_integration_test.go go.mod go.sum
git commit -m "feat: add subscriber reclaim loop and watermill Pub/Sub conformance test"
```

---

## Task 4: `internal/watermill.Router`

**Files:**
- Create: `internal/watermill/router.go`
- Test: `internal/watermill/router_test.go`

**Interfaces:**
- Consumes: `*Subscriber` (Tasks 2–3)
- Produces:
  - `type MessageHandler func(ctx context.Context, payload []byte, metadata map[string]string) error`
  - `type Router struct { … }`
  - `func NewRouter(closeTimeout time.Duration) (*Router, error)`
  - `func (r *Router) AddHandler(name, stream string, sub *Subscriber, h MessageHandler)`
  - `func (r *Router) Run(ctx context.Context) error`
  - `func (r *Router) Close() error`

  Task 7 wires `Consumer.Run` to all of these. `MessageHandler` is the seam that keeps `*message.Message` out of the root package.

- [ ] **Step 1: Write the failing test**

Create `internal/watermill/router_test.go`:

```go
package watermill

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRouter_DeliversPayloadAndMetadataToHandler(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reader := newFakeReader(entry("1-0", "event-a", `{"k":"v"}`))
	sub, err := NewSubscriber(SubscriberOptions{
		Reader:      reader,
		Concurrency: 1,
		BlockTime:   10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewSubscriber: %v", err)
	}

	router, err := NewRouter(time.Second)
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}

	type received struct {
		payload  string
		attempt  string
		streamID string
	}
	got := make(chan received, 1)

	router.AddHandler("h", "stream", sub, func(_ context.Context, payload []byte, metadata map[string]string) error {
		got <- received{
			payload:  string(payload),
			attempt:  metadata[MetadataDeliveryAttempt],
			streamID: metadata[MetadataStreamID],
		}
		return nil
	})

	go func() {
		if err := router.Run(ctx); err != nil {
			t.Errorf("router.Run: %v", err)
		}
	}()

	select {
	case r := <-got:
		if r.payload != `{"k":"v"}` {
			t.Errorf("payload = %q, want %q", r.payload, `{"k":"v"}`)
		}
		if r.attempt != "1" {
			t.Errorf("attempt = %q, want %q", r.attempt, "1")
		}
		if r.streamID != "1-0" {
			t.Errorf("streamID = %q, want %q", r.streamID, "1-0")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the handler to run")
	}

	cancel()
	if err := router.Close(); err != nil {
		t.Errorf("router.Close: %v", err)
	}
	if err := sub.Close(); err != nil {
		t.Errorf("sub.Close: %v", err)
	}
}

func TestRouter_HandlerErrorLeavesEntryUnacked(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reader := newFakeReader(entry("1-0", "event-a", "{}"))
	sub, err := NewSubscriber(SubscriberOptions{
		Reader:      reader,
		Concurrency: 1,
		BlockTime:   10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewSubscriber: %v", err)
	}

	router, err := NewRouter(time.Second)
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}

	called := make(chan struct{}, 1)
	router.AddHandler("h", "stream", sub, func(context.Context, []byte, map[string]string) error {
		select {
		case called <- struct{}{}:
		default:
		}
		return errors.New("handler failed")
	})

	go func() { _ = router.Run(ctx) }()

	select {
	case <-called:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the handler to run")
	}

	time.Sleep(200 * time.Millisecond)
	if ids := reader.ackedIDs(); len(ids) != 0 {
		t.Errorf("acked = %v, want none — a failed handler must leave the entry pending", ids)
	}

	cancel()
	_ = router.Close()
	_ = sub.Close()
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./internal/watermill/... -run TestRouter -v`
Expected: FAIL — `undefined: NewRouter`, `undefined: MessageHandler`.

- [ ] **Step 3: Create `internal/watermill/router.go`**

```go
package watermill

import (
	"context"
	"fmt"
	"time"

	wm "github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

// MessageHandler processes one consumed message. It takes only plain types,
// so no watermill value reaches callers outside internal/ — the same
// boundary the publisher wrapper keeps.
//
// Returning nil acks the message; returning an error nacks it, leaving the
// entry pending for the reclaim loop.
type MessageHandler func(ctx context.Context, payload []byte, metadata map[string]string) error

// Router wraps watermill's Router, which supplies handler lifecycle, the
// middleware chain, ack/nack plumbing, and a bounded drain on shutdown.
type Router struct {
	router *message.Router
}

// NewRouter builds a Router whose shutdown drain is bounded by closeTimeout.
func NewRouter(closeTimeout time.Duration) (*Router, error) {
	router, err := message.NewRouter(
		message.RouterConfig{CloseTimeout: closeTimeout},
		wm.NewStdLogger(false, false),
	)
	if err != nil {
		return nil, fmt.Errorf("creating router: %w", err)
	}
	return &Router{router: router}, nil
}

// AddHandler subscribes h to stream. name identifies the handler within the
// router and must be unique.
func (r *Router) AddHandler(name, stream string, sub *Subscriber, h MessageHandler) {
	r.router.AddNoPublisherHandler(name, stream, sub, func(msg *message.Message) error {
		return h(msg.Context(), msg.Payload, msg.Metadata)
	})
}

// Run blocks until ctx is cancelled, then drains in-flight handlers within
// the configured close timeout.
func (r *Router) Run(ctx context.Context) error {
	if err := r.router.Run(ctx); err != nil {
		return fmt.Errorf("running router: %w", err)
	}
	return nil
}

// Close stops the router and releases its resources.
func (r *Router) Close() error {
	return r.router.Close()
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./internal/watermill/... -race -v`
Expected: PASS — all ten tests.

- [ ] **Step 5: Lint and commit**

Run: `make lint && make test`

```bash
git add internal/watermill/router.go internal/watermill/router_test.go
git commit -m "feat: add internal/watermill Router wrapper with plain-typed handler seam"
```

---

## Task 5: `Handler`, `TopicSelector`, and the registry

**Files:**
- Modify: `handler.go` (replace the Phase 0 stub comment)
- Create: `registry.go`
- Test: `registry_test.go`

**Interfaces:**
- Consumes: `Envelope[T]`, `EventDef`, `validateEnvelope` (Phase 1)
- Produces:
  - `type Handler[T any] interface { Handle(context.Context, Envelope[T]) error }`
  - `type TopicSelector struct { Topic string }`
  - `type routeKey struct { topic, eventType string; major int }`
  - `type dispatchFunc func(context.Context, Envelope[json.RawMessage]) error`
  - `type registry struct { … }`
  - `func newRegistry() *registry`
  - `func (r *registry) addTyped(topic, eventType string, major int, fn dispatchFunc) error`
  - `func (r *registry) addRaw(topic string, fn dispatchFunc) error`
  - `func (r *registry) lookup(topic, eventType string, major int) (dispatchFunc, bool)`
  - `func (r *registry) topicList() []string`
  - `func (r *registry) isEmpty() bool`
  - `func majorVersion(schemaVersion string) (int, error)`
  - `func typedDispatch[T any](h Handler[T]) dispatchFunc`

  Task 6 calls `addTyped`/`addRaw`; Task 7 calls `lookup` and `topicList`.

- [ ] **Step 1: Write the failing test**

Create `registry_test.go`:

```go
package messaging

import (
	"context"
	"encoding/json"
	"testing"
)

type recordingHandler struct {
	called  int
	lastEnv Envelope[testPayload]
}

func (h *recordingHandler) Handle(_ context.Context, env Envelope[testPayload]) error {
	h.called++
	h.lastEnv = env
	return nil
}

func rawEnvelope(eventType, version string, payload string) Envelope[json.RawMessage] {
	return Envelope[json.RawMessage]{
		EventID:       "01234567-89ab-7def-8000-000000000000",
		EventType:     eventType,
		SchemaVersion: version,
		CorrelationID: "corr-1",
		Source:        "test.service",
		Payload:       json.RawMessage(payload),
	}
}

func TestMajorVersion(t *testing.T) {
	cases := []struct {
		in   string
		want int
	}{
		{"1.0.0", 1},
		{"1.4.2", 1},
		{"2.0.0", 2},
		{"10.1.3", 10},
	}
	for _, c := range cases {
		got, err := majorVersion(c.in)
		if err != nil {
			t.Errorf("majorVersion(%q): %v", c.in, err)
			continue
		}
		if got != c.want {
			t.Errorf("majorVersion(%q) = %d, want %d", c.in, got, c.want)
		}
	}

	if _, err := majorVersion("not-a-version"); err == nil {
		t.Error("expected an error for a malformed schema version")
	}
}

func TestRegistry_LookupMatchesOnMajorVersionOnly(t *testing.T) {
	r := newRegistry()
	h := &recordingHandler{}

	if err := r.addTyped("documents", "document.created", 1, typedDispatch[testPayload](h)); err != nil {
		t.Fatalf("addTyped: %v", err)
	}

	// A handler registered for 1.0.0 must receive every 1.x.y.
	for _, version := range []string{"1.0.0", "1.1.0", "1.4.2"} {
		major, err := majorVersion(version)
		if err != nil {
			t.Fatalf("majorVersion(%q): %v", version, err)
		}
		fn, ok := r.lookup("documents", "document.created", major)
		if !ok {
			t.Fatalf("no handler found for version %q", version)
		}
		if err := fn(context.Background(), rawEnvelope("document.created", version, `{"name":"a.pdf"}`)); err != nil {
			t.Fatalf("dispatch for %q: %v", version, err)
		}
	}

	if h.called != 3 {
		t.Errorf("handler called %d times, want 3", h.called)
	}
	if h.lastEnv.Payload.Name != "a.pdf" {
		t.Errorf("Payload.Name = %q, want %q", h.lastEnv.Payload.Name, "a.pdf")
	}
	if h.lastEnv.SchemaVersion != "1.4.2" {
		t.Errorf("SchemaVersion = %q, want %q — header fields must survive dispatch", h.lastEnv.SchemaVersion, "1.4.2")
	}
	if h.lastEnv.CorrelationID != "corr-1" {
		t.Errorf("CorrelationID = %q, want %q", h.lastEnv.CorrelationID, "corr-1")
	}
}

func TestRegistry_DifferentMajorsCoexist(t *testing.T) {
	r := newRegistry()
	v1 := &recordingHandler{}
	v2 := &recordingHandler{}

	if err := r.addTyped("documents", "document.created", 1, typedDispatch[testPayload](v1)); err != nil {
		t.Fatalf("addTyped v1: %v", err)
	}
	if err := r.addTyped("documents", "document.created", 2, typedDispatch[testPayload](v2)); err != nil {
		t.Fatalf("addTyped v2: %v", err)
	}

	fn, ok := r.lookup("documents", "document.created", 2)
	if !ok {
		t.Fatal("no handler found for major 2")
	}
	if err := fn(context.Background(), rawEnvelope("document.created", "2.0.0", `{"name":"b.pdf"}`)); err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	if v1.called != 0 {
		t.Errorf("v1 handler called %d times, want 0", v1.called)
	}
	if v2.called != 1 {
		t.Errorf("v2 handler called %d times, want 1", v2.called)
	}
}

func TestRegistry_LookupMissReturnsFalse(t *testing.T) {
	r := newRegistry()
	if err := r.addTyped("documents", "document.created", 1, typedDispatch[testPayload](&recordingHandler{})); err != nil {
		t.Fatalf("addTyped: %v", err)
	}

	if _, ok := r.lookup("documents", "document.deleted", 1); ok {
		t.Error("expected no handler for an unregistered event type")
	}
	if _, ok := r.lookup("documents", "document.created", 2); ok {
		t.Error("expected no handler for an unregistered major version")
	}
	if _, ok := r.lookup("invoices", "document.created", 1); ok {
		t.Error("expected no handler on an unregistered topic")
	}
}

func TestRegistry_RejectsDuplicateRegistration(t *testing.T) {
	r := newRegistry()
	if err := r.addTyped("documents", "document.created", 1, typedDispatch[testPayload](&recordingHandler{})); err != nil {
		t.Fatalf("first addTyped: %v", err)
	}

	err := r.addTyped("documents", "document.created", 1, typedDispatch[testPayload](&recordingHandler{}))
	if err == nil {
		t.Fatal("expected an error registering a duplicate topic/type/major")
	}
}

func TestRegistry_RejectsTypedAndRawOnSameTopic(t *testing.T) {
	r := newRegistry()
	if err := r.addTyped("documents", "document.created", 1, typedDispatch[testPayload](&recordingHandler{})); err != nil {
		t.Fatalf("addTyped: %v", err)
	}
	if err := r.addRaw("documents", func(context.Context, Envelope[json.RawMessage]) error { return nil }); err == nil {
		t.Error("expected an error adding a raw handler to a topic that already has typed handlers")
	}

	r2 := newRegistry()
	if err := r2.addRaw("documents", func(context.Context, Envelope[json.RawMessage]) error { return nil }); err != nil {
		t.Fatalf("addRaw: %v", err)
	}
	if err := r2.addTyped("documents", "document.created", 1, typedDispatch[testPayload](&recordingHandler{})); err == nil {
		t.Error("expected an error adding a typed handler to a topic that already has a raw handler")
	}
	if err := r2.addRaw("documents", func(context.Context, Envelope[json.RawMessage]) error { return nil }); err == nil {
		t.Error("expected an error adding a second raw handler to the same topic")
	}
}

func TestRegistry_TopicListIsUnionOfTypedAndRaw(t *testing.T) {
	r := newRegistry()
	if err := r.addTyped("documents", "document.created", 1, typedDispatch[testPayload](&recordingHandler{})); err != nil {
		t.Fatalf("addTyped documents: %v", err)
	}
	if err := r.addTyped("documents", "document.deleted", 1, typedDispatch[testPayload](&recordingHandler{})); err != nil {
		t.Fatalf("addTyped documents 2: %v", err)
	}
	if err := r.addRaw("invoices", func(context.Context, Envelope[json.RawMessage]) error { return nil }); err != nil {
		t.Fatalf("addRaw invoices: %v", err)
	}

	topics := r.topicList()
	if len(topics) != 2 {
		t.Fatalf("topicList() = %v, want 2 distinct topics", topics)
	}
	seen := map[string]bool{}
	for _, topic := range topics {
		seen[topic] = true
	}
	if !seen["documents"] || !seen["invoices"] {
		t.Errorf("topicList() = %v, want documents and invoices", topics)
	}
}

func TestTypedDispatch_IgnoresUnknownPayloadFields(t *testing.T) {
	h := &recordingHandler{}
	fn := typedDispatch[testPayload](h)

	// PRD §11: consumers must deserialize leniently within a major version.
	err := fn(context.Background(), rawEnvelope("document.created", "1.2.0", `{"name":"a.pdf","added_later":true}`))
	if err != nil {
		t.Fatalf("dispatch must tolerate unknown fields, got: %v", err)
	}
	if h.lastEnv.Payload.Name != "a.pdf" {
		t.Errorf("Payload.Name = %q, want %q", h.lastEnv.Payload.Name, "a.pdf")
	}
}

func TestTypedDispatch_ReturnsErrorOnUndecodablePayload(t *testing.T) {
	fn := typedDispatch[testPayload](&recordingHandler{})

	err := fn(context.Background(), rawEnvelope("document.created", "1.0.0", `{"name":123}`))
	if err == nil {
		t.Fatal("expected an error decoding a payload with a mistyped field")
	}
}
```

Note: `testPayload` already exists in `envelope_test.go` as `struct{ Name string }`. Add a JSON tag so the tests above decode as written — modify it in `envelope_test.go` to:

```go
type testPayload struct {
	Name string `json:"name"`
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test . -run 'TestRegistry|TestMajorVersion|TestTypedDispatch' -v`
Expected: FAIL — `undefined: newRegistry`, `undefined: majorVersion`, `undefined: typedDispatch`.

- [ ] **Step 3: Replace `handler.go`**

```go
package messaging

import "context"

// Handler processes a typed event payload. Applications implement this
// interface and register implementations with a Consumer.
//
// Handlers must be idempotent: delivery is at-least-once, so the same event
// may arrive more than once (PRD §6). EventID is the deduplication key.
type Handler[T any] interface {
	Handle(context.Context, Envelope[T]) error
}

// TopicSelector identifies a topic for raw handler registration, for cases
// where several payload shapes share an event type or a service consumes
// events it has no typed contract for.
type TopicSelector struct {
	Topic string
}
```

- [ ] **Step 4: Create `registry.go`**

```go
package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// routeKey is the dispatch key: a topic, an event type, and a major schema
// version. Minor and patch versions deliberately do not participate.
type routeKey struct {
	topic     string
	eventType string
	major     int
}

// dispatchFunc handles an event whose payload is still raw JSON. Typed
// handlers are erased into this shape at registration time, so the payload
// is only deserialized once the right handler has been found.
type dispatchFunc func(context.Context, Envelope[json.RawMessage]) error

// registry stores handler registrations and resolves an event to one
// handler. A topic carries either typed handlers or a single raw handler,
// never both.
type registry struct {
	typed  map[routeKey]dispatchFunc
	raw    map[string]dispatchFunc
	topics map[string]struct{}
}

func newRegistry() *registry {
	return &registry{
		typed:  make(map[routeKey]dispatchFunc),
		raw:    make(map[string]dispatchFunc),
		topics: make(map[string]struct{}),
	}
}

// addTyped registers a typed handler. Duplicate topic/type/major
// registrations and typed/raw collisions fail here rather than at Run: the
// registry already has enough information, so deferring would only delay a
// deterministic error.
func (r *registry) addTyped(topic, eventType string, major int, fn dispatchFunc) error {
	if _, ok := r.raw[topic]; ok {
		return fmt.Errorf("registry: topic %q already has a raw handler; a topic may have typed handlers or one raw handler, not both", topic)
	}

	key := routeKey{topic: topic, eventType: eventType, major: major}
	if _, ok := r.typed[key]; ok {
		return fmt.Errorf("registry: a handler is already registered for topic %q, event type %q, major version %d", topic, eventType, major)
	}

	r.typed[key] = fn
	r.topics[topic] = struct{}{}
	return nil
}

// addRaw registers the single raw handler for a topic.
func (r *registry) addRaw(topic string, fn dispatchFunc) error {
	if _, ok := r.raw[topic]; ok {
		return fmt.Errorf("registry: topic %q already has a raw handler", topic)
	}
	for key := range r.typed {
		if key.topic == topic {
			return fmt.Errorf("registry: topic %q already has typed handlers; a topic may have typed handlers or one raw handler, not both", topic)
		}
	}

	r.raw[topic] = fn
	r.topics[topic] = struct{}{}
	return nil
}

// lookup resolves an event to its handler. A raw handler, when present,
// takes every event on its topic.
func (r *registry) lookup(topic, eventType string, major int) (dispatchFunc, bool) {
	if fn, ok := r.raw[topic]; ok {
		return fn, true
	}
	fn, ok := r.typed[routeKey{topic: topic, eventType: eventType, major: major}]
	return fn, ok
}

// topicList returns the distinct topics to subscribe to.
func (r *registry) topicList() []string {
	topics := make([]string, 0, len(r.topics))
	for topic := range r.topics {
		topics = append(topics, topic)
	}
	return topics
}

// isEmpty reports whether nothing has been registered.
func (r *registry) isEmpty() bool {
	return len(r.topics) == 0
}

// majorVersion extracts the major component of a semantic version.
func majorVersion(schemaVersion string) (int, error) {
	segment, _, found := strings.Cut(schemaVersion, ".")
	if !found {
		return 0, fmt.Errorf("schema version %q is not MAJOR.MINOR.PATCH", schemaVersion)
	}
	major, err := strconv.Atoi(segment)
	if err != nil {
		return 0, fmt.Errorf("schema version %q has a non-numeric major component: %w", schemaVersion, err)
	}
	return major, nil
}

// typedDispatch erases a typed handler into a dispatchFunc, deserializing
// the payload into T only once this handler has been selected.
//
// Decoding is deliberately lenient — unknown fields are ignored, never
// rejected — because within a major version producers may add fields
// without coordinating a consumer release (PRD §11).
func typedDispatch[T any](h Handler[T]) dispatchFunc {
	return func(ctx context.Context, raw Envelope[json.RawMessage]) error {
		var payload T
		if len(raw.Payload) > 0 {
			if err := json.Unmarshal(raw.Payload, &payload); err != nil {
				return fmt.Errorf("unmarshalling payload for event type %q: %w", raw.EventType, err)
			}
		}

		return h.Handle(ctx, Envelope[T]{
			EventID:       raw.EventID,
			EventType:     raw.EventType,
			Timestamp:     raw.Timestamp,
			SchemaVersion: raw.SchemaVersion,
			CorrelationID: raw.CorrelationID,
			Source:        raw.Source,
			Payload:       payload,
		})
	}
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `go test . -v`
Expected: PASS — the new registry tests plus all Phase 1 tests.

- [ ] **Step 6: Lint and commit**

Run: `make lint && make test`

```bash
git add handler.go registry.go registry_test.go envelope_test.go
git commit -m "feat: add Handler, TopicSelector, and the routing registry"
```

---

## Task 6: `ConsumerConfig` defaults and validation

**Files:**
- Modify: `config.go` (add `validateConsumer`, update `ConsumerConfig` doc comment)
- Test: `config_test.go` (append)

**Interfaces:**
- Consumes: `Config`, `ConsumerConfig` (Phase 1)
- Produces: `func (c *Config) validateConsumer() error`

  Task 7's `NewConsumer` calls `Validate` then `validateConsumer`.

- [ ] **Step 1: Write the failing test**

Append to `config_test.go`:

```go
func TestValidateConsumer_AppliesDefaults(t *testing.T) {
	cfg := Config{
		Source:   "test.service",
		Redis:    RedisConfig{Address: "localhost:6379"},
		Consumer: ConsumerConfig{Group: "test-group"},
	}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if err := cfg.validateConsumer(); err != nil {
		t.Fatalf("validateConsumer: %v", err)
	}

	if cfg.Consumer.Concurrency != 1 {
		t.Errorf("Concurrency = %d, want 1", cfg.Consumer.Concurrency)
	}
	if cfg.Consumer.ClaimInterval != 30*time.Second {
		t.Errorf("ClaimInterval = %v, want 30s", cfg.Consumer.ClaimInterval)
	}
	if cfg.Consumer.ClaimMinIdle != 60*time.Second {
		t.Errorf("ClaimMinIdle = %v, want 60s", cfg.Consumer.ClaimMinIdle)
	}
	if cfg.Consumer.MaxDeliveryAttempts != 5 {
		t.Errorf("MaxDeliveryAttempts = %d, want 5", cfg.Consumer.MaxDeliveryAttempts)
	}
	if cfg.Consumer.ShutdownTimeout != 30*time.Second {
		t.Errorf("ShutdownTimeout = %v, want 30s", cfg.Consumer.ShutdownTimeout)
	}
	if cfg.Consumer.ConsumerName == "" {
		t.Error("ConsumerName must be defaulted")
	}
}

func TestValidateConsumer_GeneratesDistinctConsumerNames(t *testing.T) {
	newCfg := func() Config {
		return Config{
			Source:   "test.service",
			Redis:    RedisConfig{Address: "localhost:6379"},
			Consumer: ConsumerConfig{Group: "test-group"},
		}
	}

	a, b := newCfg(), newCfg()
	if err := a.validateConsumer(); err != nil {
		t.Fatalf("validateConsumer a: %v", err)
	}
	if err := b.validateConsumer(); err != nil {
		t.Fatalf("validateConsumer b: %v", err)
	}

	if a.Consumer.ConsumerName == b.Consumer.ConsumerName {
		t.Errorf("two consumers on one host got the same name %q; they must be distinct", a.Consumer.ConsumerName)
	}
}

func TestValidateConsumer_RequiresGroup(t *testing.T) {
	cfg := Config{
		Source: "test.service",
		Redis:  RedisConfig{Address: "localhost:6379"},
	}

	err := cfg.validateConsumer()
	if err == nil {
		t.Fatal("expected an error when Consumer.Group is empty")
	}
	if !strings.Contains(err.Error(), "Consumer.Group") {
		t.Errorf("error = %q, want it to name Consumer.Group", err)
	}
}

func TestValidateConsumer_RejectsClaimMinIdleBelowClaimInterval(t *testing.T) {
	cfg := Config{
		Source: "test.service",
		Redis:  RedisConfig{Address: "localhost:6379"},
		Consumer: ConsumerConfig{
			Group:         "test-group",
			ClaimInterval: 60 * time.Second,
			ClaimMinIdle:  30 * time.Second,
		},
	}

	err := cfg.validateConsumer()
	if err == nil {
		t.Fatal("expected an error when ClaimMinIdle < ClaimInterval")
	}
	if !strings.Contains(err.Error(), "ClaimMinIdle") {
		t.Errorf("error = %q, want it to name ClaimMinIdle", err)
	}
}

func TestValidateConsumer_RejectsNegativeConcurrency(t *testing.T) {
	cfg := Config{
		Source: "test.service",
		Redis:  RedisConfig{Address: "localhost:6379"},
		Consumer: ConsumerConfig{
			Group:       "test-group",
			Concurrency: -1,
		},
	}

	if err := cfg.validateConsumer(); err == nil {
		t.Fatal("expected an error for negative Concurrency")
	}
}

func TestValidate_LeavesConsumerFieldsAloneForPublishers(t *testing.T) {
	cfg := Config{
		Source: "test.service",
		Redis:  RedisConfig{Address: "localhost:6379"},
	}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	// A publisher-only config must not be forced to supply a consumer group.
	if cfg.Consumer.Group != "" {
		t.Errorf("Consumer.Group = %q, want empty", cfg.Consumer.Group)
	}
	if cfg.Consumer.Concurrency != 0 {
		t.Errorf("Consumer.Concurrency = %d, want 0 — Validate must not default consumer fields", cfg.Consumer.Concurrency)
	}
}
```

Ensure `config_test.go` imports `strings` and `time`.

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test . -run TestValidateConsumer -v`
Expected: FAIL — `cfg.validateConsumer undefined`.

- [ ] **Step 3: Add `validateConsumer` to `config.go`**

Add these imports to `config.go`: `crypto/rand`, `encoding/hex`, `os`.

```go
const (
	defaultConcurrency         = 1
	defaultClaimInterval       = 30 * time.Second
	defaultClaimMinIdle        = 60 * time.Second
	defaultMaxDeliveryAttempts = 5
	defaultShutdownTimeout     = 30 * time.Second
)

// validateConsumer checks the consumer-only fields and fills in their
// defaults. It is called by NewConsumer after Validate.
//
// These checks live outside Validate because publisher-only applications
// never set Consumer fields, and requiring a consumer group from them would
// be wrong.
func (c *Config) validateConsumer() error {
	if c.Consumer.Group == "" {
		return fmt.Errorf("config: Consumer.Group is required for consumers")
	}
	if c.Consumer.Concurrency < 0 {
		return fmt.Errorf("config: Consumer.Concurrency must not be negative, got %d", c.Consumer.Concurrency)
	}

	if c.Consumer.Concurrency == 0 {
		c.Consumer.Concurrency = defaultConcurrency
	}
	if c.Consumer.ClaimInterval == 0 {
		c.Consumer.ClaimInterval = defaultClaimInterval
	}
	if c.Consumer.ClaimMinIdle == 0 {
		c.Consumer.ClaimMinIdle = defaultClaimMinIdle
	}
	if c.Consumer.MaxDeliveryAttempts == 0 {
		c.Consumer.MaxDeliveryAttempts = defaultMaxDeliveryAttempts
	}
	if c.Consumer.ShutdownTimeout == 0 {
		c.Consumer.ShutdownTimeout = defaultShutdownTimeout
	}

	// Reclaiming sooner than the sweep interval would let a message be
	// claimed while its previous delivery is still legitimately in flight.
	if c.Consumer.ClaimMinIdle < c.Consumer.ClaimInterval {
		return fmt.Errorf(
			"config: Consumer.ClaimMinIdle (%v) must be >= Consumer.ClaimInterval (%v)",
			c.Consumer.ClaimMinIdle, c.Consumer.ClaimInterval,
		)
	}

	if c.Consumer.ConsumerName == "" {
		name, err := defaultConsumerName()
		if err != nil {
			return err
		}
		c.Consumer.ConsumerName = name
	}

	return nil
}

// defaultConsumerName builds a name that identifies the host but stays
// unique across replicas and restarts on that host, so two processes never
// share a Redis consumer identity.
func defaultConsumerName() (string, error) {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}

	suffix := make([]byte, 3)
	if _, err := rand.Read(suffix); err != nil {
		return "", fmt.Errorf("generating consumer name suffix: %w", err)
	}

	return host + "-" + hex.EncodeToString(suffix), nil
}
```

Also update the `ConsumerConfig` doc comment, replacing the Phase 1 note that its validation comes later:

```go
// ConsumerConfig configures a Consumer. Its defaults and validation are
// applied by validateConsumer, which NewConsumer calls after Validate.
// MaxDeliveryAttempts is defaulted here but is not enforced until the
// delivery-attempt cap lands in Phase 2b.
type ConsumerConfig struct {
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test . -v`
Expected: PASS.

- [ ] **Step 5: Lint and commit**

Run: `make lint && make test`

```bash
git add config.go config_test.go
git commit -m "feat: add ConsumerConfig defaults and validation"
```

---

## Task 7: `Consumer`, registration, dispatch, and `Run`

**Files:**
- Modify: `consumer.go` (replace the Phase 0 stub comment)
- Modify: `doc.go` (drop the "consumer path is not implemented" note)
- Test: `consumer_test.go`

**Interfaces:**
- Consumes: `registry`, `typedDispatch`, `majorVersion` (Task 5); `validateConsumer` (Task 6); `internalwatermill.NewSubscriber`/`NewRouter`/`MessageHandler` (Tasks 2–4); `internalredis.NewClient`/`NewStreamReader` (Task 1); `validateEnvelope`, `contextWithCorrelationID`, `redisClientOptions` (Phase 1)
- Produces:
  - `type Consumer struct { … }`
  - `func NewConsumer(cfg Config) (*Consumer, error)`
  - `func RegisterHandler[T any](c *Consumer, def EventDef, h Handler[T]) error`
  - `func RegisterRawHandler(c *Consumer, sel TopicSelector, h Handler[json.RawMessage]) error`
  - `func (c *Consumer) Run(ctx context.Context) error`
  - `func (c *Consumer) Close() error`

- [ ] **Step 1: Write the failing test**

Create `consumer_test.go`:

```go
package messaging

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func testConsumerConfig() Config {
	return Config{
		Source:   "test.service",
		Redis:    RedisConfig{Address: "localhost:6379"},
		Consumer: ConsumerConfig{Group: "test-group"},
	}
}

type noopHandler struct{}

func (noopHandler) Handle(context.Context, Envelope[testPayload]) error { return nil }

type rawNoopHandler struct{}

func (rawNoopHandler) Handle(context.Context, Envelope[json.RawMessage]) error { return nil }

func TestNewConsumer_RequiresGroup(t *testing.T) {
	cfg := Config{
		Source: "test.service",
		Redis:  RedisConfig{Address: "localhost:6379"},
	}

	if _, err := NewConsumer(cfg); err == nil {
		t.Fatal("expected an error when Consumer.Group is empty")
	}
}

func TestNewConsumer_RequiresSource(t *testing.T) {
	cfg := Config{
		Redis:    RedisConfig{Address: "localhost:6379"},
		Consumer: ConsumerConfig{Group: "test-group"},
	}

	if _, err := NewConsumer(cfg); err == nil {
		t.Fatal("expected an error when Source is empty")
	}
}

func TestRegisterHandler_RejectsInvalidEventDef(t *testing.T) {
	cases := []struct {
		name string
		def  EventDef
	}{
		{"missing topic", EventDef{Type: "document.created", Version: "1.0.0"}},
		{"missing type", EventDef{Topic: "documents", Version: "1.0.0"}},
		{"missing version", EventDef{Topic: "documents", Type: "document.created"}},
		{"malformed version", EventDef{Topic: "documents", Type: "document.created", Version: "v1"}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			consumer, err := NewConsumer(testConsumerConfig())
			if err != nil {
				t.Fatalf("NewConsumer: %v", err)
			}
			if err := RegisterHandler(consumer, c.def, noopHandler{}); err == nil {
				t.Errorf("expected an error for %s", c.name)
			}
		})
	}
}

func TestRegisterHandler_RejectsDuplicate(t *testing.T) {
	consumer, err := NewConsumer(testConsumerConfig())
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	def := EventDef{Topic: "documents", Type: "document.created", Version: "1.0.0"}

	if err := RegisterHandler(consumer, def, noopHandler{}); err != nil {
		t.Fatalf("first RegisterHandler: %v", err)
	}
	// A different patch version is still major 1, so it collides.
	def2 := EventDef{Topic: "documents", Type: "document.created", Version: "1.3.0"}
	if err := RegisterHandler(consumer, def2, noopHandler{}); err == nil {
		t.Error("expected an error registering a second handler for the same topic/type/major")
	}
}

func TestRegisterRawHandler_RejectsMissingTopic(t *testing.T) {
	consumer, err := NewConsumer(testConsumerConfig())
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	if err := RegisterRawHandler(consumer, TopicSelector{}, rawNoopHandler{}); err == nil {
		t.Error("expected an error for an empty TopicSelector.Topic")
	}
}

func TestRegisterHandler_RejectsRegistrationAfterRun(t *testing.T) {
	consumer, err := NewConsumer(testConsumerConfig())
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}

	consumer.markRunning()

	def := EventDef{Topic: "documents", Type: "document.created", Version: "1.0.0"}
	err = RegisterHandler(consumer, def, noopHandler{})
	if err == nil {
		t.Fatal("expected an error registering after Run has started")
	}
	if !strings.Contains(err.Error(), "Run") {
		t.Errorf("error = %q, want it to mention Run", err)
	}
}

func TestRun_RejectsEmptyRegistry(t *testing.T) {
	consumer, err := NewConsumer(testConsumerConfig())
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}

	err = consumer.Run(context.Background())
	if err == nil {
		t.Fatal("expected an error running a consumer with no handlers")
	}
	if !strings.Contains(err.Error(), "no handlers") {
		t.Errorf("error = %q, want it to mention that no handlers are registered", err)
	}
}

func TestDispatch_InvokesTypedHandlerAndPropagatesCorrelationID(t *testing.T) {
	consumer, err := NewConsumer(testConsumerConfig())
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}

	type result struct {
		correlationFromCtx string
		payloadName        string
	}
	got := make(chan result, 1)

	def := EventDef{Topic: "documents", Type: "document.created", Version: "1.0.0"}
	if err := RegisterHandler(consumer, def, handlerFunc(func(ctx context.Context, env Envelope[testPayload]) error {
		id, _ := correlationIDFromContext(ctx)
		got <- result{correlationFromCtx: id, payloadName: env.Payload.Name}
		return nil
	})); err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}

	body := []byte(`{
		"event_id":"01234567-89ab-7def-8000-000000000000",
		"event_type":"document.created",
		"timestamp":"2026-04-23T10:00:00Z",
		"schema_version":"1.2.0",
		"correlation_id":"corr-42",
		"source":"other.service",
		"payload":{"name":"a.pdf"}
	}`)

	if err := consumer.dispatch(context.Background(), "documents", body); err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	r := <-got
	if r.correlationFromCtx != "corr-42" {
		t.Errorf("correlation ID in context = %q, want %q", r.correlationFromCtx, "corr-42")
	}
	if r.payloadName != "a.pdf" {
		t.Errorf("payload name = %q, want %q", r.payloadName, "a.pdf")
	}
}

func TestDispatch_AcksUnmatchedEventType(t *testing.T) {
	consumer, err := NewConsumer(testConsumerConfig())
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}

	called := false
	def := EventDef{Topic: "documents", Type: "document.created", Version: "1.0.0"}
	if err := RegisterHandler(consumer, def, handlerFunc(func(context.Context, Envelope[testPayload]) error {
		called = true
		return nil
	})); err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}

	body := []byte(`{
		"event_id":"01234567-89ab-7def-8000-000000000000",
		"event_type":"document.deleted",
		"timestamp":"2026-04-23T10:00:00Z",
		"schema_version":"1.0.0",
		"correlation_id":"corr-1",
		"source":"other.service",
		"payload":{}
	}`)

	// An unmatched event is a normal outcome, not an error: returning nil
	// acks it.
	if err := consumer.dispatch(context.Background(), "documents", body); err != nil {
		t.Errorf("dispatch of an unmatched event type must return nil, got: %v", err)
	}
	if called {
		t.Error("handler must not be invoked for an unmatched event type")
	}
}

func TestDispatch_AcksUnmatchedMajorVersion(t *testing.T) {
	consumer, err := NewConsumer(testConsumerConfig())
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}

	called := false
	def := EventDef{Topic: "documents", Type: "document.created", Version: "1.0.0"}
	if err := RegisterHandler(consumer, def, handlerFunc(func(context.Context, Envelope[testPayload]) error {
		called = true
		return nil
	})); err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}

	body := []byte(`{
		"event_id":"01234567-89ab-7def-8000-000000000000",
		"event_type":"document.created",
		"timestamp":"2026-04-23T10:00:00Z",
		"schema_version":"2.0.0",
		"correlation_id":"corr-1",
		"source":"other.service",
		"payload":{}
	}`)

	if err := consumer.dispatch(context.Background(), "documents", body); err != nil {
		t.Errorf("dispatch across a major version boundary must return nil, got: %v", err)
	}
	if called {
		t.Error("a handler registered for major 1 must not receive a 2.0.0 event")
	}
}

func TestDispatch_ReturnsErrorOnUndecodableEnvelope(t *testing.T) {
	consumer, err := NewConsumer(testConsumerConfig())
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	def := EventDef{Topic: "documents", Type: "document.created", Version: "1.0.0"}
	if err := RegisterHandler(consumer, def, noopHandler{}); err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}

	// In Phase 2a an undecodable entry nacks rather than being dropped;
	// Phase 2b routes it to the DLQ instead.
	if err := consumer.dispatch(context.Background(), "documents", []byte(`not json`)); err == nil {
		t.Error("expected an error for an undecodable envelope")
	}
}

func TestDispatch_ReturnsErrorOnInvalidEnvelope(t *testing.T) {
	consumer, err := NewConsumer(testConsumerConfig())
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
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

	if err := consumer.dispatch(context.Background(), "documents", body); err == nil {
		t.Error("expected an error for an envelope failing validation")
	}
}

func TestDispatch_PropagatesHandlerError(t *testing.T) {
	consumer, err := NewConsumer(testConsumerConfig())
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}

	def := EventDef{Topic: "documents", Type: "document.created", Version: "1.0.0"}
	if err := RegisterHandler(consumer, def, handlerFunc(func(context.Context, Envelope[testPayload]) error {
		return errHandlerFailed
	})); err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}

	body := []byte(`{
		"event_id":"01234567-89ab-7def-8000-000000000000",
		"event_type":"document.created",
		"timestamp":"2026-04-23T10:00:00Z",
		"schema_version":"1.0.0",
		"correlation_id":"corr-1",
		"source":"other.service",
		"payload":{}
	}`)

	// Phase 2a nacks on any handler error; Phase 2b classifies instead.
	if err := consumer.dispatch(context.Background(), "documents", body); err == nil {
		t.Error("expected the handler error to propagate so the message nacks")
	}
}

func TestDispatch_RawHandlerReceivesEveryEventOnTopic(t *testing.T) {
	consumer, err := NewConsumer(testConsumerConfig())
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}

	seen := make(chan string, 2)
	if err := RegisterRawHandler(consumer, TopicSelector{Topic: "documents"},
		rawHandlerFunc(func(_ context.Context, env Envelope[json.RawMessage]) error {
			seen <- env.EventType
			return nil
		})); err != nil {
		t.Fatalf("RegisterRawHandler: %v", err)
	}

	for _, eventType := range []string{"document.created", "document.deleted"} {
		body := []byte(`{
			"event_id":"01234567-89ab-7def-8000-000000000000",
			"event_type":"` + eventType + `",
			"timestamp":"2026-04-23T10:00:00Z",
			"schema_version":"1.0.0",
			"correlation_id":"corr-1",
			"source":"other.service",
			"payload":{}
		}`)
		if err := consumer.dispatch(context.Background(), "documents", body); err != nil {
			t.Fatalf("dispatch %s: %v", eventType, err)
		}
	}

	if got := <-seen; got != "document.created" {
		t.Errorf("first event = %q, want %q", got, "document.created")
	}
	if got := <-seen; got != "document.deleted" {
		t.Errorf("second event = %q, want %q", got, "document.deleted")
	}
}
```

Add these test helpers at the bottom of `consumer_test.go`:

```go
var errHandlerFailed = errors.New("handler failed")

// handlerFunc adapts a function to Handler[testPayload].
type handlerFunc func(context.Context, Envelope[testPayload]) error

func (f handlerFunc) Handle(ctx context.Context, env Envelope[testPayload]) error {
	return f(ctx, env)
}

// rawHandlerFunc adapts a function to Handler[json.RawMessage].
type rawHandlerFunc func(context.Context, Envelope[json.RawMessage]) error

func (f rawHandlerFunc) Handle(ctx context.Context, env Envelope[json.RawMessage]) error {
	return f(ctx, env)
}
```

Add `errors` to the test file's imports.

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test . -run 'TestNewConsumer|TestRegister|TestRun|TestDispatch' -v`
Expected: FAIL — `undefined: NewConsumer`, `undefined: RegisterHandler`.

- [ ] **Step 3: Replace `consumer.go`**

```go
package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	internalredis "github.com/bcgov/foi-messaging-go/internal/redis"
	internalwatermill "github.com/bcgov/foi-messaging-go/internal/watermill"
)

// Consumer subscribes to the topics its registered handlers cover and
// dispatches each event to the handler matching its event type and major
// schema version.
//
// A Consumer is created from a Config, has handlers registered against it,
// and is then run. Registration after Run has started is an error.
type Consumer struct {
	cfg Config

	mu       sync.Mutex
	registry *registry
	running  bool

	reader *internalredis.StreamReader
}

// NewConsumer validates cfg and returns a Consumer with no handlers
// registered. It opens no connections — the Redis client, subscriber, and
// router are built by Run, once the set of topics is known.
func NewConsumer(cfg Config) (*Consumer, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	if err := cfg.validateConsumer(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	return &Consumer{cfg: cfg, registry: newRegistry()}, nil
}

// RegisterHandler registers a typed handler for def. It is a function
// rather than a method because Go does not permit generic methods.
//
// The handler is invoked for every event on def.Topic whose event type
// matches def.Type and whose major schema version matches def.Version's.
// Minor and patch differences do not affect dispatch: producers may add
// optional fields without a coordinated consumer release, so handlers must
// tolerate any additive change within their major version.
func RegisterHandler[T any](c *Consumer, def EventDef, h Handler[T]) error {
	if err := validateEventDef(def); err != nil {
		return err
	}
	major, err := majorVersion(def.Version)
	if err != nil {
		return fmt.Errorf("event def: %w", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.running {
		return fmt.Errorf("consumer: handlers cannot be registered after Run has started")
	}

	return c.registry.addTyped(def.Topic, def.Type, major, typedDispatch(h))
}

// RegisterRawHandler registers a handler that receives every event on a
// topic with its payload left as raw JSON. Use it when several payload
// shapes share an event type, or to consume events without a typed
// contract.
//
// A topic may have typed handlers or one raw handler, never both.
func RegisterRawHandler(c *Consumer, sel TopicSelector, h Handler[json.RawMessage]) error {
	if sel.Topic == "" {
		return fmt.Errorf("topic selector: Topic is required")
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.running {
		return fmt.Errorf("consumer: handlers cannot be registered after Run has started")
	}

	return c.registry.addRaw(sel.Topic, func(ctx context.Context, env Envelope[json.RawMessage]) error {
		return h.Handle(ctx, env)
	})
}

// validateEventDef checks that a registration names a topic, a well-formed
// event type, and a semantic version.
func validateEventDef(def EventDef) error {
	if def.Topic == "" {
		return fmt.Errorf("event def: Topic is required")
	}
	if def.Type == "" {
		return fmt.Errorf("event def: Type is required")
	}
	if !eventTypePattern.MatchString(def.Type) {
		return fmt.Errorf("event def: Type %q must be 2-3 dot-separated lowercase segments", def.Type)
	}
	if def.Version == "" {
		return fmt.Errorf("event def: Version is required")
	}
	if !schemaVersionPattern.MatchString(def.Version) {
		return fmt.Errorf("event def: Version %q must be MAJOR.MINOR.PATCH", def.Version)
	}
	return nil
}

// markRunning flips the consumer into its running state, after which
// registration is refused. It reports false if Run has already started.
func (c *Consumer) markRunning() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.running {
		return false
	}
	c.running = true
	return true
}

// Run subscribes to every registered topic and blocks until ctx is
// cancelled, then drains in-flight handlers within cfg.Consumer.ShutdownTimeout
// before returning.
func (c *Consumer) Run(ctx context.Context) error {
	c.mu.Lock()
	empty := c.registry.isEmpty()
	topics := c.registry.topicList()
	c.mu.Unlock()

	if empty {
		return fmt.Errorf("consumer: no handlers registered; register at least one before calling Run")
	}
	if !c.markRunning() {
		return fmt.Errorf("consumer: Run has already been called")
	}

	client := internalredis.NewClient(redisClientOptions(c.cfg.Redis))
	reader := internalredis.NewStreamReader(client, c.cfg.Consumer.Group, c.cfg.Consumer.ConsumerName)

	c.mu.Lock()
	c.reader = reader
	c.mu.Unlock()

	subscriber, err := internalwatermill.NewSubscriber(internalwatermill.SubscriberOptions{
		Reader:        reader,
		Concurrency:   c.cfg.Consumer.Concurrency,
		ClaimInterval: c.cfg.Consumer.ClaimInterval,
		ClaimMinIdle:  c.cfg.Consumer.ClaimMinIdle,
	})
	if err != nil {
		_ = reader.Close()
		return fmt.Errorf("creating subscriber: %w", err)
	}

	router, err := internalwatermill.NewRouter(c.cfg.Consumer.ShutdownTimeout)
	if err != nil {
		_ = subscriber.Close()
		_ = reader.Close()
		return fmt.Errorf("creating router: %w", err)
	}

	for _, topic := range topics {
		router.AddHandler(topic, c.streamName(topic), subscriber,
			func(ctx context.Context, payload []byte, _ map[string]string) error {
				return c.dispatch(ctx, topic, payload)
			})
	}

	runErr := router.Run(ctx)

	closeErr := router.Close()
	subCloseErr := subscriber.Close()
	readerErr := reader.Close()

	if runErr != nil {
		return runErr
	}
	if closeErr != nil {
		return fmt.Errorf("closing router: %w", closeErr)
	}
	if subCloseErr != nil {
		return fmt.Errorf("closing subscriber: %w", subCloseErr)
	}
	if readerErr != nil {
		return fmt.Errorf("closing redis client: %w", readerErr)
	}
	return nil
}

// dispatch decodes one message and routes it to its handler.
//
// Returning nil acks the message; returning an error nacks it, leaving the
// entry pending for redelivery. In this phase every failure nacks — error
// classification, the delivery cap, and the DLQ arrive in Phase 2b.
func (c *Consumer) dispatch(ctx context.Context, topic string, payload []byte) error {
	log := c.cfg.Telemetry.Logger

	var env Envelope[json.RawMessage]
	if err := json.Unmarshal(payload, &env); err != nil {
		log.Error("messaging: undecodable event envelope",
			"topic", topic, "error", err)
		return fmt.Errorf("unmarshalling envelope on topic %q: %w", topic, err)
	}

	if err := validateEnvelope(env); err != nil {
		log.Error("messaging: invalid event envelope",
			"topic", topic, "event_id", env.EventID, "error", err)
		return fmt.Errorf("validating envelope on topic %q: %w", topic, err)
	}

	major, err := majorVersion(env.SchemaVersion)
	if err != nil {
		log.Error("messaging: unparseable schema version",
			"topic", topic, "event_id", env.EventID, "error", err)
		return fmt.Errorf("parsing schema version on topic %q: %w", topic, err)
	}

	c.mu.Lock()
	handler, ok := c.registry.lookup(topic, env.EventType, major)
	c.mu.Unlock()

	if !ok {
		// Topics are shared and services consume only the event types they
		// care about, so an unmatched event is a normal outcome, not a
		// failure. Phase 3 counts these as skipped{reason="no_handler"}.
		log.Debug("messaging: no handler for event",
			"topic", topic, "event_type", env.EventType,
			"schema_version", env.SchemaVersion, "event_id", env.EventID)
		return nil
	}

	ctx = contextWithCorrelationID(ctx, env.CorrelationID)
	return handler(ctx, env)
}

// streamName maps a logical topic to its Redis stream.
func (c *Consumer) streamName(topic string) string {
	return c.cfg.StreamPrefix + ":" + topic
}

// Close releases resources held by a Consumer that was created but never
// run. After Run returns, everything is already released. Close is
// idempotent and safe to defer unconditionally.
func (c *Consumer) Close() error {
	c.mu.Lock()
	reader := c.reader
	c.reader = nil
	c.mu.Unlock()

	if reader == nil {
		return nil
	}
	return reader.Close()
}
```

- [ ] **Step 4: Update `doc.go`**

```go
// Package messaging provides a transport-agnostic, strongly-typed
// asynchronous messaging library for FOI platform services, built on
// Watermill and Redis Streams.
//
// See docs/foi-messaging-go-prd-v1.1.md for the full design. The publish
// path (EventDef, Envelope, Config, and Publisher) and the consume path
// (Consumer, Handler, and routing) are implemented. Error classification,
// retry, the delivery-attempt cap, and the dead letter queue are not yet
// implemented, so a failing handler currently nacks and its message is
// redelivered by the reclaim loop indefinitely.
package messaging
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `go test . -race -v`
Expected: PASS — all consumer tests plus every Phase 1 and Task 5–6 test.

- [ ] **Step 6: Lint and commit**

Run: `make lint && make test`

```bash
git add consumer.go consumer_test.go doc.go
git commit -m "feat: implement Consumer, handler registration, dispatch, and Run"
```

---

## Task 8: End-to-end consume-path integration tests

**Files:**
- Create: `consumer_integration_test.go`

**Interfaces:**
- Consumes: the entire public API from Tasks 1–7, plus `Publisher` from Phase 1

- [ ] **Step 1: Write the integration tests**

Create `consumer_integration_test.go`:

```go
//go:build integration

package messaging_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	messaging "github.com/bcgov/foi-messaging-go"
	"github.com/bcgov/foi-messaging-go/internal/testsupport"
)

var errDeliberate = errors.New("deliberate handler failure")

var documentCreated = messaging.EventDef{
	Topic:   "documents",
	Type:    "document.created",
	Version: "1.0.0",
}

// consumeFixture starts Redis and returns a config pointed at it.
func consumeFixture(t *testing.T) messaging.Config {
	t.Helper()
	ctx := context.Background()

	addr, terminate, err := testsupport.StartRedis(ctx)
	if err != nil {
		t.Fatalf("StartRedis: %v", err)
	}
	t.Cleanup(func() {
		if err := terminate(ctx); err != nil {
			t.Errorf("terminate: %v", err)
		}
	})

	return messaging.Config{
		Source: "test.service",
		Redis:  messaging.RedisConfig{Address: addr},
		Consumer: messaging.ConsumerConfig{
			Group:           "test-group",
			Concurrency:     1,
			ClaimInterval:   1 * time.Second,
			ClaimMinIdle:    2 * time.Second,
			ShutdownTimeout: 5 * time.Second,
		},
	}
}

// collectingHandler records the envelopes it receives and can be told to
// fail a fixed number of times first.
type collectingHandler struct {
	mu        sync.Mutex
	received  []messaging.Envelope[documentCreatedPayload]
	failFirst int
	attempts  int
	notify    chan struct{}
}

func newCollectingHandler(failFirst int) *collectingHandler {
	return &collectingHandler{failFirst: failFirst, notify: make(chan struct{}, 16)}
}

func (h *collectingHandler) Handle(_ context.Context, env messaging.Envelope[documentCreatedPayload]) error {
	h.mu.Lock()
	h.attempts++
	shouldFail := h.attempts <= h.failFirst
	if !shouldFail {
		h.received = append(h.received, env)
	}
	h.mu.Unlock()

	select {
	case h.notify <- struct{}{}:
	default:
	}

	if shouldFail {
		return errDeliberate
	}
	return nil
}

func (h *collectingHandler) snapshot() []messaging.Envelope[documentCreatedPayload] {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]messaging.Envelope[documentCreatedPayload](nil), h.received...)
}

func (h *collectingHandler) attemptCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.attempts
}

// runConsumer starts consumer.Run in the background and returns a stop
// function that cancels it and waits for Run to return.
//
// stop is registered as a cleanup and may also be called explicitly by a
// test, so it must be safe to call twice — hence the sync.Once. Without it
// the second call would block waiting on an already-drained channel.
func runConsumer(t *testing.T, consumer *messaging.Consumer) func() {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- consumer.Run(ctx) }()

	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Errorf("consumer.Run: %v", err)
				}
			case <-time.After(30 * time.Second):
				t.Error("consumer.Run did not return within 30s of cancellation")
			}
		})
	}
	t.Cleanup(stop)
	return stop
}

func TestConsumer_ReceivesPublishedEvent(t *testing.T) {
	cfg := consumeFixture(t)

	publisher, err := messaging.NewPublisher(cfg)
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	t.Cleanup(func() {
		if err := publisher.Close(); err != nil {
			t.Errorf("publisher.Close: %v", err)
		}
	})

	consumer, err := messaging.NewConsumer(cfg)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	handler := newCollectingHandler(0)
	if err := messaging.RegisterHandler(consumer, documentCreated, handler); err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}
	runConsumer(t, consumer)

	result, err := publisher.Publish(context.Background(), documentCreated,
		documentCreatedPayload{EntityID: "e1", Name: "report.pdf"},
		messaging.WithCorrelationID("corr-99"))
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case <-handler.notify:
	case <-time.After(20 * time.Second):
		t.Fatal("timed out waiting for the handler to receive the event")
	}

	received := handler.snapshot()
	if len(received) != 1 {
		t.Fatalf("len(received) = %d, want 1", len(received))
	}
	if received[0].EventID != result.EventID {
		t.Errorf("EventID = %q, want %q", received[0].EventID, result.EventID)
	}
	if received[0].CorrelationID != "corr-99" {
		t.Errorf("CorrelationID = %q, want %q", received[0].CorrelationID, "corr-99")
	}
	if received[0].Payload.Name != "report.pdf" {
		t.Errorf("Payload.Name = %q, want %q", received[0].Payload.Name, "report.pdf")
	}
	if received[0].Source != "test.service" {
		t.Errorf("Source = %q, want %q", received[0].Source, "test.service")
	}
}

func TestConsumer_RedeliversNackedEventViaReclaim(t *testing.T) {
	cfg := consumeFixture(t)

	publisher, err := messaging.NewPublisher(cfg)
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	t.Cleanup(func() { _ = publisher.Close() })

	consumer, err := messaging.NewConsumer(cfg)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	// Fail once so the first delivery nacks and reclaim must redeliver it.
	handler := newCollectingHandler(1)
	if err := messaging.RegisterHandler(consumer, documentCreated, handler); err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}
	runConsumer(t, consumer)

	if _, err := publisher.Publish(context.Background(), documentCreated,
		documentCreatedPayload{EntityID: "e1", Name: "retry.pdf"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	deadline := time.After(30 * time.Second)
	for {
		if len(handler.snapshot()) == 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("event was never redelivered successfully; attempts = %d", handler.attemptCount())
		case <-time.After(200 * time.Millisecond):
		}
	}

	if got := handler.attemptCount(); got < 2 {
		t.Errorf("attempts = %d, want at least 2 (one failure then a reclaim)", got)
	}
}

func TestConsumer_PreservesOrderAtConcurrencyOne(t *testing.T) {
	cfg := consumeFixture(t)

	publisher, err := messaging.NewPublisher(cfg)
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	t.Cleanup(func() { _ = publisher.Close() })

	consumer, err := messaging.NewConsumer(cfg)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	handler := newCollectingHandler(0)
	if err := messaging.RegisterHandler(consumer, documentCreated, handler); err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}
	runConsumer(t, consumer)

	const count = 20
	want := make([]string, 0, count)
	for i := 0; i < count; i++ {
		name := "doc-" + string(rune('a'+i))
		want = append(want, name)
		if _, err := publisher.Publish(context.Background(), documentCreated,
			documentCreatedPayload{EntityID: "e", Name: name}); err != nil {
			t.Fatalf("Publish %d: %v", i, err)
		}
	}

	deadline := time.After(60 * time.Second)
	for {
		if len(handler.snapshot()) >= count {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("received %d of %d events before timeout", len(handler.snapshot()), count)
		case <-time.After(200 * time.Millisecond):
		}
	}

	got := handler.snapshot()
	for i := 0; i < count; i++ {
		if got[i].Payload.Name != want[i] {
			t.Fatalf("event %d = %q, want %q — Concurrency 1 must preserve order",
				i, got[i].Payload.Name, want[i])
		}
	}
}

func TestConsumer_SkipsUnmatchedEventType(t *testing.T) {
	cfg := consumeFixture(t)

	publisher, err := messaging.NewPublisher(cfg)
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	t.Cleanup(func() { _ = publisher.Close() })

	consumer, err := messaging.NewConsumer(cfg)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	handler := newCollectingHandler(0)
	if err := messaging.RegisterHandler(consumer, documentCreated, handler); err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}
	runConsumer(t, consumer)

	// Same topic, an event type this consumer has no handler for.
	other := messaging.EventDef{Topic: "documents", Type: "document.deleted", Version: "1.0.0"}
	if _, err := publisher.Publish(context.Background(), other,
		documentCreatedPayload{EntityID: "e1", Name: "ignored.pdf"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// Then a matching event, which must still arrive.
	if _, err := publisher.Publish(context.Background(), documentCreated,
		documentCreatedPayload{EntityID: "e2", Name: "wanted.pdf"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case <-handler.notify:
	case <-time.After(20 * time.Second):
		t.Fatal("timed out waiting for the matching event")
	}

	received := handler.snapshot()
	if len(received) != 1 {
		t.Fatalf("len(received) = %d, want exactly 1 — the unmatched event must be skipped", len(received))
	}
	if received[0].Payload.Name != "wanted.pdf" {
		t.Errorf("Payload.Name = %q, want %q", received[0].Payload.Name, "wanted.pdf")
	}
}

func TestConsumer_ReplaysEventsPublishedBeforeItStarted(t *testing.T) {
	cfg := consumeFixture(t)

	publisher, err := messaging.NewPublisher(cfg)
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	t.Cleanup(func() { _ = publisher.Close() })

	// Published before any consumer group exists.
	if _, err := publisher.Publish(context.Background(), documentCreated,
		documentCreatedPayload{EntityID: "e1", Name: "early.pdf"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	consumer, err := messaging.NewConsumer(cfg)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	handler := newCollectingHandler(0)
	if err := messaging.RegisterHandler(consumer, documentCreated, handler); err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}
	runConsumer(t, consumer)

	select {
	case <-handler.notify:
	case <-time.After(20 * time.Second):
		t.Fatal("timed out: a new group created at 0 must replay existing history")
	}

	received := handler.snapshot()
	if len(received) != 1 || received[0].Payload.Name != "early.pdf" {
		t.Fatalf("received = %+v, want one event named early.pdf", received)
	}
}

func TestConsumer_ShutdownDrainsInFlightHandler(t *testing.T) {
	cfg := consumeFixture(t)

	publisher, err := messaging.NewPublisher(cfg)
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	t.Cleanup(func() { _ = publisher.Close() })

	consumer, err := messaging.NewConsumer(cfg)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}

	started := make(chan struct{})
	finished := make(chan struct{})
	if err := messaging.RegisterHandler(consumer, documentCreated,
		slowHandler{started: started, finished: finished}); err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}

	stop := runConsumer(t, consumer)

	if _, err := publisher.Publish(context.Background(), documentCreated,
		documentCreatedPayload{EntityID: "e1", Name: "slow.pdf"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case <-started:
	case <-time.After(20 * time.Second):
		t.Fatal("timed out waiting for the handler to start")
	}

	// Cancel while the handler is mid-flight; it must be allowed to finish.
	stop()

	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Error("in-flight handler did not complete before shutdown returned")
	}
}

// slowHandler blocks briefly so a shutdown can overlap its execution.
type slowHandler struct {
	started  chan struct{}
	finished chan struct{}
}

func (h slowHandler) Handle(_ context.Context, _ messaging.Envelope[documentCreatedPayload]) error {
	close(h.started)
	time.Sleep(500 * time.Millisecond)
	close(h.finished)
	return nil
}
```

Note: `documentCreatedPayload` already exists in `publisher_integration_test.go` in this same `messaging_test` package — do not redeclare it.

- [ ] **Step 2: Run the integration suite**

Run: `go test -tags=integration . -v -timeout 30m`
Expected: PASS — all six consume-path tests plus the Phase 1 publish test.

- [ ] **Step 3: Run everything**

Run: `make lint && make test && make test-integration`
Expected: no findings; unit and integration suites both pass.

- [ ] **Step 4: Commit**

```bash
git add consumer_integration_test.go
git commit -m "test: add end-to-end consume-path integration tests"
```

---

## Task 9: Consumer example

**Files:**
- Create: `examples/consumer/main.go`
- Modify: `README.md` (add a consuming section)

**Interfaces:**
- Consumes: the full public API

- [ ] **Step 1: Create the example**

Create `examples/consumer/main.go`:

```go
// Command consumer shows how a service consumes events with the messaging
// library. It is a compile-checked illustration, not a runnable service.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	messaging "github.com/bcgov/foi-messaging-go"
)

// DocumentCreatedPayload would normally live in the shared contracts
// repository alongside its EventDef.
type DocumentCreatedPayload struct {
	EntityID string `json:"entity_id"`
	Name     string `json:"name"`
}

// DocumentCreated is the event contract: topic, type, and schema version
// declared once and shared by publishers and consumers.
var DocumentCreated = messaging.EventDef{
	Topic:   "documents",
	Type:    "document.created",
	Version: "1.0.0",
}

type documentHandler struct {
	log *slog.Logger
}

// Handle must be idempotent: delivery is at-least-once, so the same event
// may arrive more than once. EventID is the deduplication key.
func (h documentHandler) Handle(_ context.Context, env messaging.Envelope[DocumentCreatedPayload]) error {
	h.log.Info("document created",
		"event_id", env.EventID,
		"correlation_id", env.CorrelationID,
		"name", env.Payload.Name)
	return nil
}

func main() {
	log := slog.Default()

	cfg := messaging.Config{
		Source: "documents.service",
		Redis: messaging.RedisConfig{
			Address:  os.Getenv("REDIS_ADDRESS"),
			Username: os.Getenv("REDIS_USER"),
			Password: os.Getenv("REDIS_PASSWORD"),
		},
		Consumer: messaging.ConsumerConfig{
			Group: "documents-service",
		},
	}

	consumer, err := messaging.NewConsumer(cfg)
	if err != nil {
		log.Error("creating consumer", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			log.Error("closing consumer", "error", err)
		}
	}()

	if err := messaging.RegisterHandler(consumer, DocumentCreated, documentHandler{log: log}); err != nil {
		log.Error("registering handler", "error", err)
		os.Exit(1)
	}

	// Run blocks until the context is cancelled, then drains in-flight
	// handlers before returning.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := consumer.Run(ctx); err != nil {
		log.Error("running consumer", "error", err)
		os.Exit(1)
	}
}
```

- [ ] **Step 2: Verify it builds**

Run: `go build ./...`
Expected: success.

- [ ] **Step 3: Add a consuming section to `README.md`**

Insert after the existing publishing section:

````markdown
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
````

- [ ] **Step 4: Run everything and commit**

Run: `make lint && make test`

```bash
git add examples/consumer/main.go README.md
git commit -m "docs: add consumer example and README consuming section"
```

---

## Definition of Done

- [ ] `make lint` reports no findings
- [ ] `make test` passes
- [ ] `make test-integration` passes, including the Watermill Pub/Sub conformance suite
- [ ] `go build ./...` succeeds, including `examples/consumer`
- [ ] No Watermill or go-redis import exists outside `internal/` (enforced by `depguard`)
- [ ] `dlq.go` and `errors.go` still carry their Phase 0 stub comments — Phase 2b owns them
