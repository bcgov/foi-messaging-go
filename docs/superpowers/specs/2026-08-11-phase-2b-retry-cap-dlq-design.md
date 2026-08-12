# Phase 2b: Retry, Delivery Cap, and DLQ — Design

**Date:** 2026-08-11
**Status:** Approved
**Author:** brainstorming session (alvesfc + Claude)

## Purpose

Replace Phase 2a's blanket NACK with the three-layer failure handling of
[PRD v1.1](../../foi-messaging-go-prd-v1.1.md) §13–§15: error classification,
in-process immediate retry, the delivery-attempt cap, and the dead letter queue.

This design was driven by one constraint carried forward from Phase 2a's final
review ([2a spec §10](2026-08-11-phase-2a-consume-path-design.md)):

> **`Concurrency` now bounds in-flight handlers per subscribed topic**, not
> globally across the consumer. The retry middleware must not assume the older
> global meaning.

Following that through changed where the retry code lives, what the cap must be
ordered against, and which existing config invariants Phase 2b silently
redefines. Those consequences are the substance of this document.

## Scope

In scope:

- `errors.go`: `AsPermanent` / `AsRetryable` / `AsDiscard` and the
  `IsPermanent` / `IsRetryable` / `IsDiscard` predicates (PRD §15)
- `dlq.go`: the exported `DeadLetter` wrapper contract (PRD §14)
- The retry loop, the delivery-attempt cap, and DLQ routing, all inside
  `Consumer.dispatch`
- An `OnUndecodable` seam on `internal/watermill.Subscriber`
- A new `ClaimMinIdle` / backoff validation rule in `validateConsumer`
- Restating the `ClaimMinIdle` and `ShutdownTimeout` doc comments in terms of
  the retry multiplier
- An unexported `newPublisherWithClient` so the DLQ reuses `Run`'s Redis client

Out of scope:

- **Phase 3:** `messaging_dlq_publish_failures_total` and the skip metric named
  by PRD §14/§15. This phase logs the events those metrics will later count.
- **Phase 4:** `testing/` (`messagingtest`)
- DLQ replay tooling (PRD §22)
- Delayed/scheduled redelivery. Redelivery backoff remains bounded below by
  `ClaimMinIdle`, as PRD §13 Layer 2 already accepts.

## 1. Why the retry middleware is not a middleware

PRD §13 and 2a spec §9 both describe Layer 1 as "Watermill retry middleware."
That placement does not survive contact with the package boundary.

A classification-aware retry needs `IsRetryable` / `IsPermanent` / `IsDiscard`.
Those belong in the root `messaging` package alongside the `AsX` constructors
applications call. The root package already imports `internal/watermill`, so
`internal/watermill` importing the root is an import cycle — not merely a
`depguard` violation that could be waived.

The same pressure applies to the other two pieces:

- The cap reads `_foi_delivery_attempt`, which `dispatch` already receives and
  discards (2a spec §10).
- The DLQ needs a publisher, and `Publisher` is a root-package type. The
  wrapper it writes needs `consumer_group`, `consumer_name`,
  `delivery_attempts`, and `original_topic` — all of which are in scope at
  `dispatch` and none of which exist inside `internal/watermill`.

**Decision: retry, cap, and DLQ all live in the root package, inside
`dispatch`.** Retry is a plain Go loop, not Watermill middleware.
`internal/watermill` gains no retry code and no new seam for it. The one piece
of Phase 2b that does touch `internal/watermill` is the undecodable-entry hook
in §5, for a reason that has nothing to do with classification.

This is a correction to 2a spec §9, which should be read as superseded on this
point.

## 2. The dispatch pipeline

```
1. attempt := metadata[_foi_delivery_attempt]
2. attempt > MaxDeliveryAttempts  → DLQ(max_attempts_exceeded)  → ack
3. decode envelope        fails   → DLQ(deserialization_failed) → ack
4. validate envelope      fails   → DLQ(deserialization_failed) → ack
5. parse schema version   fails   → DLQ(deserialization_failed) → ack
6. no handler matched             → ack, debug log                (unchanged)
7. runWithRetry(handler, env)
```

### The cap is step 2, before decode

A message already over the cap must not spend four handler invocations proving
it. Ordering the cap first is also what makes the poison-backlog case in §4
survivable: a capped entry occupies its concurrency slot for one metadata read
and one DLQ publish rather than `(1+MaxImmediateRetries) × handler`.

This ordering matches PRD §13 Layer 3, which routes over-cap messages to the DLQ
"regardless of error classification" — classification has not yet been consulted
at step 2, and must not be.

The comparison is `>`, not `>=`: attempts 1–5 dispatch, attempt 6 is
dead-lettered. That is what makes PRD §13's stated worst case of
`MaxDeliveryAttempts × (1 + MaxImmediateRetries)` = 5 × 4 = 20 handler
invocations come out right, and it is worth an explicit boundary test.

### Steps 3–5 dead-letter immediately rather than NACKing

This is a behaviour change from Phase 2a, where all three NACK.

All three are definitionally permanent: malformed JSON does not become valid on
redelivery, and a missing `event_id` does not appear. Routing them through the
cap would mean five reclaim cycles and roughly five minutes to reach a
conclusion that was available on the first look, while occupying a concurrency
slot on each pass.

All three use PRD §14's `deserialization_failed` reason with the raw bytes in
`event_raw` (base64) rather than `event`. §14 already specifies exactly this for
"invalid JSON, missing envelope fields," so steps 4 and 5 conform even though
they did produce a syntactically valid envelope.

### The retry loop

```go
for i := 0; i <= c.cfg.Retry.MaxImmediateRetries; i++ {
    err := handler(ctx, env)
    switch {
    case err == nil:
        return nil                                  // ack
    case IsDiscard(err):
        return nil                                  // ack, log at warn, no DLQ
    case IsPermanent(err):
        return c.deadLetter(ctx, topic, payload, reasonPermanent, err, attempt)
    case i == c.cfg.Retry.MaxImmediateRetries:
        return err                                  // nack → pending → reclaim
    }
    if !sleepJitter(ctx, backoff(i)) {
        return err                                  // ctx cancelled mid-backoff
    }
}
```

Classification is re-evaluated on **every** attempt. A handler may return a
retryable error once and a permanent one the next time, and the loop must honour
the latest answer rather than the first.

`backoff(i)` is PRD §13's full jitter: `rand(0, min(MaxBackoff, InitialBackoff<<i))`.
With defaults that is `rand(0,100ms)`, `rand(0,200ms)`, `rand(0,400ms)` — a
worst case of 700ms across all three retries.

An unwrapped error is retryable (PRD §15's deliberate default), so retrying is
the switch's fallthrough and `IsRetryable` is never called by the loop itself.
The predicate is still exported, for tests and operational tooling.

`sleepJitter` selects on `ctx.Done()`. This is **not** the shutdown case:
`msgCtx` is derived from `context.WithoutCancel` and stays live for the whole
drain (2a spec §10), so a cancellation here means the subscriber closed, and
abandoning the retry to a NACK is correct.

### DLQ publish failure never acks

`deadLetter` returns its publish error, the entry stays pending, and the next
reclaim sweep retries the write. While the DLQ is unwritable this loops
indefinitely — the correct trade, since the alternative acks data into nothing.
PRD §14 specifies this behaviour directly.

## 3. Slot arithmetic: what per-topic `Concurrency` means here

`subscription.sem` is per-`Subscribe` call, and both the read loop
(`subscriber.go:185`) and the claim loop (`subscriber.go:249`) acquire from it.
A slot is held from acquire until `emit`'s ack-wait goroutine returns — that is,
for the entire handler run, retries and DLQ publish included.

So Phase 2b changes slot occupancy from `handler` to:

```
(1 + MaxImmediateRetries) × handler + Σjitter + DLQ publish
```

And the process-wide worst case is `Concurrency × len(topics)` of those, not
`Concurrency`.

Three consequences:

**At the default `Concurrency: 1`, Layer 1 retry is a topic-wide stall.** One
flaky message blocks its topic's read loop for the whole retry window. The
per-topic scoping means the blast radius is exactly one topic rather than the
whole consumer, which is a genuine improvement over the old global semaphore —
but the stall itself is new, and 1 is the default.

**This is accepted deliberately.** Releasing the slot across the backoff sleep
would keep the read loop moving, but it breaks per-topic ordering at
`Concurrency: 1` — a later message would overtake the retrying one — and
ordering at `Concurrency: 1` is a documented guarantee (PRD §6, README).
Retry-in-place is the only option that preserves it. Nacking without any
in-process sleep would preserve slots but raise minimum retry latency from
~100ms to `ClaimMinIdle` (~60s) and abandon PRD §13 Layer 1 outright.

**Retry multiplies 2a spec §10's poison-message finding by four.** A reclaim
sweep claims up to `claimBatchSize` (100) entries and each occupies a slot. With
Layer 1, each poison entry occupies its slot for four handler invocations rather
than one: at `Concurrency: 1`, a single sweep of 100 poison entries costs
roughly 35s of pure backoff even with instantaneous handlers, and because the
semaphore is shared between the loops it is the *read* loop that starves. The
cap ordering in §2 is what bounds this — a capped entry never reaches the retry
loop.

## 4. Config invariants Phase 2b redefines without touching a field

### `ClaimMinIdle`

Its doc comment (`config.go:54-60`) says it "must also exceed the longest a
handler is expected to run." Phase 2b silently redefines that quantity as
`(1+MaxImmediateRetries) × handler + Σbackoff`.

A 20s handler is correct today against the 60s default and self-reclaims once
retry lands: the entry is claimed while the original invocation is still
retrying, and the same process then processes the message concurrently with
itself — precisely the hazard the comment exists to prevent.

**The comment must be restated in Phase 2b's terms in the same commit that makes
retry live.** Documentation drift here is not cosmetic; it is the difference
between a config that is correct and one that silently duplicates work.

### A validatable slice of that invariant

Handler duration is unknowable at construction. Worst-case total backoff is not:

```
Σ min(MaxBackoff, InitialBackoff<<i) for i in [0, MaxImmediateRetries)
```

Defaults give 700ms against a 60s `ClaimMinIdle`. But
`MaxImmediateRetries: 10, MaxBackoff: 30s` yields roughly 150s, which guarantees
self-reclaim before the handler has finished backing off, with zero handler
runtime.

`validateConsumer` rejects a config whose worst-case backoff alone meets or
exceeds `ClaimMinIdle`. This rejects configurations that are legal today, which
is intended: they cannot behave correctly once retry is live.

### `ShutdownTimeout`

Retry extends the drain by up to `MaxImmediateRetries × handler + Σjitter` per
in-flight message, across `Concurrency × len(topics)` messages.

This is documented, not plumbed. Threading a shutdown signal into the retry loop
would contradict 2a spec §10's deliberate decision to keep message contexts live
through the drain, and a handler slow enough to blow the drain does so without
retry's help.

## 5. The undecodable-entry seam

`emit` fails before `dispatch` when Watermill's own marshaller cannot unmarshal
a stream entry (`subscriber.go:290`). Today it logs at ERROR and leaves the entry
pending, where it re-loops every `ClaimMinIdle` forever. With retry, cap, and DLQ
all in the root package, that path reaches neither: it is the one poison case the
cap cannot bound.

`SubscriberOptions` gains one field:

```go
// OnUndecodable is called for a stream entry that cannot be unmarshalled at
// all. Returning nil acks the entry; returning an error leaves it pending.
// Nil-safe: unset, the subscriber logs and leaves the entry pending.
OnUndecodable func(stream, entryID string, fields map[string]any) error
```

Plain types only, so the boundary holds. `Consumer.Run` wires it to a
`deserialization_failed` DLQ publish carrying the raw Redis fields map, since
there is no envelope and no payload byte slice to put in `event_raw` — an
extension to PRD §14, which did not anticipate a marshaller-level failure.

The entry is acked only after the hook returns nil, the same rule as everywhere
else in the pipeline. Left nil, the subscriber keeps 2a behaviour, which is what
`internal/watermill`'s own tests want.

## 6. DLQ publisher construction

`NewPublisher` builds its own Redis client (`publisher.go:63`) with no injection
seam. A DLQ publisher built that way inside a running consumer would open a
second connection pool.

**Corrected during planning.** An earlier draft of this section proposed an
unexported `newPublisherWithClient(cfg, client)` so `Run` could build a
`messaging.Publisher` over its existing client. That is the wrong type. A
`messaging.Publisher` wraps whatever it is given in an `Envelope`, but a DLQ
entry *is* a `DeadLetter` document, not an envelope containing one — routing
through `Publisher.Publish` would double-wrap it and break the contract in §7.

`Run` instead builds an `internalwatermill.NewPublisher(client)` directly, which
already accepts a client, and marshals the `DeadLetter` itself. `NewPublisher`
and `messaging.Publisher` are untouched by this phase.

One consequence to hold: `internalwatermill.Publisher.Close` closes the client
it was built over, and this one shares `Run`'s client with the reader. The DLQ
publisher must therefore never be `Close`d — `closeReader` owns that client, and
a second `Close` returns `ErrClosed` from go-redis's pool, which would surface
as a spurious teardown failure from `Run`.

## 7. Reason mapping

PRD §14 fixes three `reason` values. The pipeline's failure points map onto them:

| Pipeline step | `reason` | Body field |
|---|---|---|
| Step 2, over cap | `max_attempts_exceeded` | `event` |
| Steps 3–5, envelope unusable | `deserialization_failed` | `event_raw` |
| §5, entry unmarshallable | `deserialization_failed` | `event_raw` (raw fields) |
| Step 7, `IsPermanent` | `permanent` | `event` |

`IsDiscard` produces no DLQ entry at all: log at warn and ack, per PRD §15.

## 8. Testing

### Unit

- Classification × retry matrix, with test-scale backoffs: discard acks without
  a DLQ write, permanent dead-letters without retrying, retryable exhausts then
  NACKs, and reclassification mid-loop honours the latest answer.
- Cap boundary: attempt 5 dispatches, attempt 6 dead-letters.
- `validateConsumer` rejects `Σbackoff >= ClaimMinIdle`.
- The `AsX` / `IsX` predicates compose with `errors.Is` / `errors.As` chains and
  preserve the wrapped error (PRD §15).
- Backoff is real randomness at any scale, so assert on bounds and invocation
  counts, never on elapsed time.

### Integration (`-tags=integration`)

- **Per-topic isolation — the regression test for 2a spec §10's semantic.** Two
  topics at `Concurrency: 1`; topic A's handler always fails and retries, topic B
  must keep flowing at full rate. This test fails under a Subscriber-wide
  semaphore, so it is what would catch someone "simplifying" the per-subscription
  semaphore back to a shared one.
- **Poison backlog drains** — the test 2a spec §10 explicitly asks for. Roughly
  100 failing entries at `Concurrency: 1`; assert live traffic resumes once the
  cap routes them to the DLQ.
- **DLQ contents**: original envelope preserved byte-for-byte in `event`,
  failure metadata correct, original entry acked.
- **DLQ write failure NACKs** rather than acking, and the entry is still pending
  afterwards.
- **Undecodable entry** written directly to the stream reaches the DLQ and is
  acked.

## 9. Carried forward

- **2a spec §9 is superseded** on retry placement: there is no Watermill retry
  middleware. §1 explains why.
- **PRD §13 still specifies `XAUTOCLAIM`**, which the implementation does not
  use (2a spec §10). Phase 2b depends on the delivery counter `XAUTOCLAIM` does
  not return, so the PRD should be amended rather than carrying the discrepancy
  further.
- **PRD §14's `event_raw` has no defined shape for a marshaller-level failure.**
  §5 above picks one; the PRD should record it.
- **README already documents the `AsX` and `DeadLetter` signatures as planned.**
  Keep them or update the README in the same commit.

### Found during implementation

- **Retry silently invalidates 2a's reclaim test.**
  `TestConsumer_RedeliversNackedEventViaReclaim` proves the
  `XPENDING RetryCount + 1` arithmetic the cap compares against, by observing
  the delivery attempt stamped on a handler-error log. Immediate retry breaks
  it twice over: a delivery is now `1+MaxImmediateRetries` invocations, so a
  handler failing twice never reaches the reclaim path at all, and the log line
  it observes moved into `runWithRetry` under a new message. `failFirst` must
  cover two whole deliveries (8 at the defaults) and the message constant must
  follow the code. Left uncorrected the test passes while asserting nothing —
  the worst available outcome for the one test that pins the counter Layer 3
  depends on.
- **`Consumer.Run`'s `OnUndecodable` closure had no coverage.** §5's seam is
  tested against a fake reader inside `internal/watermill`, which exercises the
  hook but not the wiring: the stream-to-topic lookup, the marshalling of the
  raw fields into `event_raw`, and the detached, timeout-bounded DLQ context
  are all in the root package and all only reachable end to end. §8's
  "undecodable entry written directly to the stream" item is what covers them;
  it needs a `testsupport` XADD helper, since no publisher of ours can write an
  entry the marshaller rejects.
- **A poison-backlog test must wait on the DLQ count, not the pending count.**
  `XPENDING` reports `NOGROUP` until `Run` has created the group, and reports 0
  both before the first delivery and after the drain — so polling it alone
  passes instantly against a consumer that never started.
