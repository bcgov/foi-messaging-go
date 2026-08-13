# Phase 4 — Application-Facing `testing/` (`messagingtest`) Package

Design of record for the final phase of `github.com/bcgov/foi-messaging-go`.
Implements PRD §19 "Application Test Support" and the PRD §20 acceptance
criterion: *the `testing/` package allows applications to unit-test handlers
and publish paths without a Redis instance*.

Status: design approved 2026-08-13. Not yet implemented.

---

## 1. Purpose and governing principle

Applications consuming this library need to unit-test two things without
Docker or a Redis instance:

1. **Their publish paths** — that a service publishes the events it should,
   with the payloads and correlation IDs it should.
2. **Their handlers** — that a handler does the right thing with an event,
   and that the error it returns produces the delivery outcome the author
   intended.

The second is the harder and more valuable half. Whether `AsPermanent`,
`AsDiscard`, or an unclassified error is the right choice for a given
failure is a subtle judgement, and today it is only observable against
Testcontainers.

**The governing principle for this package:**

> `messagingtest` drives the library's *real* code. It never reimplements a
> decision the runtime makes. Where it cannot reach real code, it does less
> rather than reimplementing more.

Every design decision below follows from that sentence. The failure mode
being guarded against is `messagingtest` gradually becoming a second,
divergent implementation of the messaging runtime — one that reports green
for behaviour production does not have.

---

## 2. Responsibility boundary

Two entry points, drawn at two different boundaries. They are not variants
of each other and neither subsumes the other.

```
Transport → Router → Context setup → Handler → Result
   ▲          ▲      └──────────────────────────────┘
   │          │                  Deliver owns this
   │          │
   │          └── Dispatch owns from here in
   │
   └── nothing in messagingtest owns this
```

### `Deliver` — the handler boundary

Reproduces what the router installs around a handler invocation, and
nothing else.

| Does | Does not |
| --- | --- |
| Install the same context values the router installs | Retry |
| Propagate envelope metadata | ACK / NACK |
| Invoke the handler | Reclaim |
| Return the handler's error verbatim | Dead-letter |
| | Simulate Redis |
| | Interpret retry exhaustion |

### `Dispatch` — the router boundary

Runs the library's real `Consumer.dispatch` against the application's own
configured `*messaging.Consumer` and reports the terminal verdict. Covers
the delivery-attempt cap, envelope validation, registry lookup by event
type and major schema version, the immediate-retry loop, and dead-letter
routing.

`Dispatch` reports what the runtime **would do** with a delivery. It does
not perform the delivery: there is no ACK, no NACK, no pending entry, no
reclaim.

### Neither owns the transport

There is no in-memory Redis, no fake subscriber, no `Consumer.Run`
substitute. Anything requiring the transport lifecycle stays in the
existing `//go:build integration` tier.

The progression an application uses:

```go
messagingtest.Deliver(...)   // unit-test a handler
messagingtest.Dispatch(...)  // test messaging/router behaviour
// real Redis + real messaging stack  // full infrastructure test
```

---

## 3. Substitution model

**Decision: `messagingtest.Publisher` is a standalone type. The library
exports no publisher interface.**

Applications declare their own narrow interface at the point of
consumption — the Go idiom that the consumer, not the producer, defines the
interface:

```go
// application code
type eventPublisher interface {
    Publish(context.Context, messaging.EventDef, any,
        ...messaging.PublishOption) (messaging.PublishResult, error)
}

type Service struct{ pub eventPublisher }
```

Rejected: exporting `messaging.EventPublisher` from the root package. It
would guarantee the substitution centrally, but adds a permanent exported
name and turns `Publish`'s signature into a compatibility contract for a
benefit each application can obtain in two lines.

Because nothing in the library enforces that the fake's method set keeps
matching the real one, `messagingtest` carries a compile-time assertion of
its own (§5.1).

---

## 4. The seam

### 4.1 Why a seam is unavoidable

Three things `testing/` needs are unexported in the root package, and no
exported path reaches any of them:

| Need | Blocker |
| --- | --- |
| Install the correlation ID as the router does | `contextWithCorrelationID` unexported (`context.go:9`) |
| Run the real `dispatch` | Unexported; its only caller is the router, which `Run` builds, and `Run` needs Redis |
| Capture dead letters | `Consumer.dlq` is nil until `Run`; `deadLetter` returns `"no dead letter sink configured"` and nacks without it (`consumer.go:643`) |
| Interpret `PublishOption` values | `PublishOption` is `func(*publishOptions)` over an unexported struct — an option's value is unreadable outside the root |

Note the third row: there is no degraded mode. A `Consumer` constructed but
never run cannot dead-letter at all, so reusing the real dispatch *requires*
injecting a sink.

Moving `dispatch` into `internal/` is not an option. It needs `Envelope[T]`,
`IsPermanent`, `DeadLetter`, and the registry, so an `internal/` package
holding it would import the root and cycle — the same constraint that
already places OpenTelemetry outside the `internal/` boundary (CLAUDE.md,
"Architecture").

### 4.2 Shape

A new `internal/testseam` package holds function variables the root package
registers at `init`. `testing/` imports it.

```
messaging (root) ──→ internal/testseam ←── testing/ (messagingtest)
       ↑                                          │
       └──────────────────────────────────────────┘
```

`internal/testseam` imports nothing of ours, so it cannot participate in a
cycle. **The library's public API surface does not change** — `internal/` is
invisible to applications, and this is a root→internal import, the normal
direction. The existing `depguard` rule is unaffected: it denies Watermill
and go-redis outside `**/internal/**`, and `testseam` imports neither.

```go
// internal/testseam/testseam.go
package testseam

// Probe collects what a single delivery did. It travels on the context so
// Dispatch needs no lock and leaves the application's Consumer unmutated.
type Probe struct {
    Kind      string // "processed" | "skipped" | "failed"
    Reason    string // skip reason
    Category  string // failure category
    Err       error
    NoBackoff bool   // collapse retry sleeps; retry count still honoured

    // Sink stands in for Consumer.dlq, which is nil until Run. It receives
    // the marshalled DeadLetter; messagingtest's implementation records it.
    // Returning an error from it exercises the DLQ-write-failure path,
    // which nacks (consumer.go: "returning nil means the caller may ack").
    Sink func(ctx context.Context, stream string, body []byte) error
}

var (
    WithCorrelationID func(ctx context.Context, id string) context.Context

    NewRecordingPublisher func(source, streamPrefix string,
        record func(ctx context.Context, stream, id string,
            body []byte, metadata map[string]string) error) (any, error)

    Dispatch func(ctx context.Context, consumer any, topic string,
        body []byte, metadata map[string]string, p *Probe) error
)
```

The probe holds no dead-letter list of its own: `Sink` is the single channel
for them, and `messagingtest` accumulates on its side. `streamPrefix` is
passed to `NewRecordingPublisher` so `messagingtest` can strip it back off
the stream name the recorder receives and store the logical topic on
`Event.Topic`.

`consumer any` and the `any` return are the price of `testseam` being unable
to name root types. Both are type-asserted immediately — inside the root's
closure for the former, inside `messagingtest` for the latter, which *can*
name `*messaging.Publisher` because it imports the root.

### 4.3 Why the probe travels on the context

The probe could equally be a mutable field on `Consumer`. Context-carried
was chosen because it:

- needs no mutex — `Dispatch` is safe to call concurrently on one Consumer;
- leaves the application's `Consumer` unmutated after the call returns;
- cannot leak between deliveries.

**Accepted cost, to be documented inline where the lookups live:** two
`ctx.Value` lookups per delivery in production, always returning nil. This
is test scaffolding in the hot path. It was weighed against a guarded
mutable field and accepted; a future maintainer removing it should
understand they are trading concurrency-safety and non-mutation for it.

### 4.4 How the verdict is extracted

`Consumer.dispatch` returns only `error` — nil means "ack", which cannot
distinguish *processed* from *discarded* from *no handler matched*.

`deliveryRecorder` already holds exactly the verdict at `end()`: `kind`,
`reason`, `category`, `err` (`telemetry.go:273-284`). So the probe is
populated from two places and no new decision logic is written anywhere:

- `deliveryRecorder.end()` copies `kind`, `reason`, `category`, `err` into
  the probe when one is present on the delivery's context.
- `Consumer.deadLetter` uses `probe.Sink` in place of `c.dlq` when a probe is
  present, leaving the rest of that function — the marshalling, the metrics,
  the ack/nack contract on its return value — untouched.
- `runWithRetry` / `sleepWithJitter` honour `probe.NoBackoff`.

This is the whole reason `Dispatch` cannot drift: it is not modelling the
outcome, it is reading the same recorder state that produces the metrics.

---

## 5. Public API of `messagingtest`

### 5.0 Shared record

```go
// Event is a message as messagingtest models it.
type Event struct {
    Topic    string                              // logical topic, not the Redis stream
    Envelope messaging.Envelope[json.RawMessage] // marshalled, exactly as on the wire
    Payload  any                                 // original value; nil when built from JSON
}

func PayloadAs[T any](e Event) (T, error)
```

Both representations are kept deliberately. `Envelope` is what the wire
would carry, so a payload containing a channel or a NaN, or a struct with
wrong `json` tags, fails in the test rather than in production; `Payload`
is the caller's own value, so the common assertion is a direct type
assertion with no decode step.

`PayloadAs` is a free function rather than a method because Go has no
generic methods — the same reason `RegisterHandler` is one.

`Event` is the currency of the package: `Publisher.Published()` returns
`[]Event`, `NewEvent` produces one, and `Dispatch` consumes one. That is
what makes a two-service chain testable without Redis (§6.3).

### 5.1 Publisher fake

```go
func NewPublisher(opts ...PublisherOption) *Publisher
func WithSource(source string) PublisherOption // default "messagingtest"

func (p *Publisher) Publish(ctx context.Context, def messaging.EventDef,
    payload any, opts ...messaging.PublishOption) (messaging.PublishResult, error)
func (p *Publisher) Published() []Event
func (p *Publisher) Reset()
func (p *Publisher) FailWith(err error)
func (p *Publisher) Close() error
```

**`Publisher` wraps a real `*messaging.Publisher` whose `publishFn`
transport write is redirected into a recorder.** `publishFn` already exists
as a seam (`publisher.go:63`), placed there so publish-failure telemetry
could be tested; this reuses it.

Consequently the fake's `Publish` runs genuine library code: correlation-ID
resolution (explicit `WithCorrelationID` → context → new UUIDv7), envelope
construction, `validateEnvelope`, marshalling, and the producer span and
metrics. Everything but the Redis write.

This is what delivers the "real validation, no drift" property: an
application whose `EventDef.Type` is `"badformat"` or whose `Version` is
`"v1"` fails its unit test rather than failing on its first production
publish. It also sidesteps `PublishOption` being unreadable from outside the
root (§4.1, row 4) — the options are simply passed through to the real
`Publish`.

The original payload value is carried to the recorder on the publish
context, which keeps `Publish` correct under concurrent use without holding
a lock across the delegated call.

`FailWith` makes subsequent publishes fail with the given error, so an
application can test its own publish-failure path.

Since no library interface enforces the substitution (§3), this file
declares an unexported interface with `Publish`'s signature and asserts both
types against it, so a signature change to `messaging.Publisher.Publish`
breaks the library's own build rather than every application's:

```go
type publisher interface {
    Publish(context.Context, messaging.EventDef, any,
        ...messaging.PublishOption) (messaging.PublishResult, error)
}

var (
    _ publisher = (*messaging.Publisher)(nil)
    _ publisher = (*Publisher)(nil)
)
```

**Superseded decision.** An earlier round of this design extracted
`validateEnvelope`'s field checks into an `internal/validate` package so the
root and a hand-built fake could share one implementation. Delegating to a
real `*messaging.Publisher` achieves the same goal more directly: nothing is
duplicated, so there is nothing to keep in sync. **The `internal/validate`
extraction is dropped** — it would be a refactor with no remaining
justification.

### 5.2 `Deliver`

```go
func Deliver[T any](ctx context.Context, h messaging.Handler[T],
    env messaging.Envelope[T]) error
```

Installs the envelope's correlation ID on the context via
`testseam.WithCorrelationID`, calls `h.Handle`, returns its error verbatim.
That is the entire implementation.

The correlation-ID installation is the load-bearing part and the reason
`Deliver` needs a seam at all. That context value is what
`resolveCorrelationID` reads at publish time, so it is what makes a
correlation ID chain from a consumed event into a follow-on publish. Without
it, a handler that consumes and then publishes would appear to work in tests
while asserting nothing about the chain.

### 5.3 `Dispatch`

```go
func Dispatch(ctx context.Context, c *messaging.Consumer, e Event,
    opts ...DispatchOption) (Result, error)

func WithDeliveryAttempt(n int64) DispatchOption
func WithMetadata(md map[string]string) DispatchOption
func WithRealBackoff() DispatchOption

type Outcome int

const (
    OutcomeProcessed Outcome = iota
    OutcomeSkipped
    OutcomeDeadLettered
    OutcomeNacked
)

type Result struct {
    Outcome     Outcome
    Reason      string // skip reason only: "no_handler" or "discard"
    Category    string // failure category when the delivery failed
    Err         error  // the delivery's own error
    DeadLetters []messaging.DeadLetter
}
```

`Reason` carries the *skip* reason only. A dead letter's reason is read from
`DeadLetters[0].Reason`, where it sits alongside the rest of the wrapper the
application's operational tooling will actually see — one place for it
rather than two that can disagree.

The returned `error` is a **harness** error — a nil consumer, an
unmarshalable event, an unregistered seam. A delivery that failed is not a
harness error; it is reported in `Result`.

**Subject is the application's own `*messaging.Consumer`**, built exactly as
in production with `NewConsumer` plus its `RegisterHandler` calls. Because
the real registry lookup runs, `Dispatch` also tests the application's
wiring: that a `1.0.0` handler receives a `1.4.2` event, that an
unregistered event type is skipped rather than erroring, that a typed/raw
collision was caught at registration. None of that is reachable from a bare
handler, where the registry would be `messagingtest`'s rather than the
application's.

`Outcome` is derived from probe state, not decided independently:
`DeadLetters` non-empty → `OutcomeDeadLettered`; else `kind == "failed"` →
`OutcomeNacked`; else `processed`/`skipped` map directly.

`WithRealBackoff` is opt-in: by default `Dispatch` collapses retry sleeps to
zero so tests do not sleep, while honouring the retry *count*. At the
library defaults a retryable failure would otherwise sleep up to ~700ms per
test case.

### 5.4 Builders

```go
func NewEvent[T any](def messaging.EventDef, payload T,
    opts ...EventOption) (Event, error)

func WithEventID(id string) EventOption
func WithCorrelationID(id string) EventOption
func WithTimestamp(t time.Time) EventOption
func WithSource(s string) EventOption

func Config(opts ...ConfigOption) messaging.Config
```

`NewEvent` defaults: a fresh UUIDv7 event ID, a fresh UUIDv7 correlation ID,
`time.Now().UTC()`, and source `"messagingtest"` — the set that satisfies
`validateEnvelope`, so the default event is a valid one and the options
exist to make it invalid on purpose.

`Config` returns a `messaging.Config` that passes `Validate` without dialing
anything. It exists because `Validate` requires `Redis.Address` even for a
Consumer that never connects (`config.go:175`), which would otherwise force
every application test to invent a fake address.

Note the deliberate name collision: `messagingtest.WithCorrelationID` is an
`EventOption`, while `messaging.WithCorrelationID` is a `PublishOption`.
Different packages, different boundaries; the doc comment on each says so.

---

## 6. Worked usage

### 6.1 Publish path

```go
p := messagingtest.NewPublisher()
svc := NewService(p)

svc.CreateOrder(ctx, order)

pubs := p.Published()
if len(pubs) != 1 {
    t.Fatalf("got %d publishes, want 1", len(pubs))
}
if pubs[0].Envelope.EventType != "order.created" {
    t.Fatalf("got event type %q", pubs[0].Envelope.EventType)
}
payload, err := messagingtest.PayloadAs[OrderCreated](pubs[0])
```

### 6.2 Handler and router

```go
// handler boundary
err := messagingtest.Deliver(ctx, handler, env)

// router boundary
c, _ := messaging.NewConsumer(messagingtest.Config())
messaging.RegisterHandler(c, contracts.OrderCreated, handler)

e, _ := messagingtest.NewEvent(contracts.OrderCreated, payload)
res, _ := messagingtest.Dispatch(ctx, c, e)
// res.Outcome == messagingtest.OutcomeProcessed

res, _ = messagingtest.Dispatch(ctx, c, e,
    messagingtest.WithDeliveryAttempt(6))
// res.Outcome == messagingtest.OutcomeDeadLettered
// res.DeadLetters[0].Reason == messaging.ReasonMaxAttemptsExceeded
```

### 6.3 Two-service chain, no Redis

```go
res, _ := messagingtest.Dispatch(ctx, consumerB, pub.Published()[0])
```

Service A's recorded publish is service B's input, including the correlation
ID that A resolved.

---

## 7. Assertion style

**Decision: accessors only. `messagingtest` does not import `testing` and
ships no assertion helpers.**

The fake exposes recorded events; `Dispatch` returns a result struct;
applications write their own `if` / `t.Fatalf`. This matches the repository's
standard-library-only, no-testify convention, and avoids importing `testing`
from a non-test package — which registers test flags into any binary that
links it, and nothing prevents an application from importing `messagingtest`
outside a `_test.go` file.

Rejected: a local `TB` interface (`Helper`, `Fatalf`) satisfied structurally
by `*testing.T`. It would give ergonomic helpers with correct line
attribution and still no `testing` import, but adds an exported concept and
can only ever cover anticipated assertions.

---

## 8. Testing strategy for the package itself

`testing/` is part of the root module, so `go test ./...` and `make test`
cover it. No Docker. Tests live in `package messagingtest_test` and use the
package as an application would, matching the repository's external-test
convention.

Drift is largely impossible by construction — `Dispatch` calls the real
`dispatch`, the fake publisher calls the real `Publish`. **The only new
logic is probe wiring**, so that is where the tests concentrate:

- one case per terminal outcome: processed; skipped/`no_handler`;
  skipped/`discard`; dead-lettered × `permanent`,
  `max_attempts_exceeded`, `deserialization_failed`; nacked;
- retry count honoured with backoff collapsed, and `WithRealBackoff`
  restoring real sleeps;
- the delivery-attempt cap firing before decode, via `WithDeliveryAttempt`;
- major-version matching: a `1.0.0` registration receiving a `1.4.2` event;
- correlation ID chaining: `Deliver` into a handler that publishes through
  the fake, asserting the correlation ID carried;
- publisher fidelity: an invalid `EventDef` is rejected by the fake with the
  same error the real publisher gives.

**Seam registration is asserted explicitly.** `messagingtest` imports
`messaging`, so the root's `init` always runs first and a nil seam entry is
unreachable today. A test asserts every entry is non-nil anyway, so a future
refactor that drops a registration fails loudly here rather than
nil-panicking inside an application's test suite.

**One integration cross-check** (`//go:build integration`): run a
permanent-error scenario through real Redis and through `Dispatch`, and
assert both produce the same DLQ reason. Cheap insurance that the probe
reflects reality rather than only itself.

**Doc examples over prose.** `Example` functions compile and run under
`go test`, so usage documentation cannot rot: `ExampleNewPublisher`,
`ExampleDeliver`, `ExampleDispatch`, and the two-service chain.

---

## 9. Out of scope

Guarding the §1 principle:

- **No in-memory Redis, no fake transport lifecycle, no `Consumer.Run`
  substitute.** No reclaim, no `XPENDING`, no subscriber loops, no ACK/NACK
  simulation.
- **No `DispatchRaw`.** `Dispatch` takes an `Event`, so literally-malformed
  JSON is not reachable. Envelope *validation* failures remain reachable —
  an application can hand `Dispatch` a deliberately invalid `Event` and get
  `OutcomeDeadLettered` with reason `deserialization_failed`. Only the
  unparseable-bytes case drops out, and it is covered by the existing
  integration tier.
- **No `EventDef` builder**, despite PRD §19 naming one. An `EventDef` is
  three strings, and a test that invents its own stops testing the
  application's real contract. Applications import the same `EventDef` their
  production code publishes.
- **No assertion helpers, no `testing` import** (§7).
- **No DLQ replay tooling** — a post-v1.0 item in the README's future work.

---

## 10. Risks and open implementation questions

1. ~~**Does `internalwatermill.NewPublisher` dial Redis at construction?**~~
   **Resolved 2026-08-13: it does not.** `redisstream.NewPublisher`
   (v1.4.5) is struct construction plus config validation — no network call
   — and `internalredis.NewClient` documents go-redis's lazy connection.
   Measured against `192.0.2.1:6379` (TEST-NET-1, routed nowhere, so a dial
   attempt stalls rather than being refused):

   | Call | Result |
   | --- | --- |
   | `NewPublisher` | returned in 315µs, no error — no dial |
   | `Publish` | stalled the full 3s to the context deadline — the dial happens here |
   | `Close` | returned in 75µs, nil — safe without a connection |

   So §5.1 stands as written: swap `publishFn` on a fully-built
   `*messaging.Publisher`, and nothing ever dials, because `publishFn` is
   the only thing that touches the network. `messagingtest.Publisher.Close`
   can delegate straight through.

2. **Probe lookups in the production hot path.** Accepted in §4.3, but the
   inline comments must say why, or a later reader will delete them.

3. **`Consumer.deadLetter` and `deliveryRecorder.end()` gain a probe
   branch.** Both are on the failure path that Phase 2b and Phase 3 comment
   heavily. New branches must match that comment density (CLAUDE.md,
   "Conventions").

---

## 11. Documentation to update when the phase lands

CLAUDE.md records that past review rounds repeatedly caught the docs
claiming unimplemented behaviour.

| File | Change |
| --- | --- |
| `testing/doc.go` | Replace the "Phase 0 scaffolding only" stub with the real package doc |
| `doc.go:23` | Remove "The application-facing testing package (Phase 4) is not yet implemented." |
| `README.md:25, 238, 252, 258` | Remove the four "Planned: Phase 4" markers; write the Testing section |
| `CLAUDE.md` | Move Phase 4 out of "Not yet implemented"; document `internal/testseam` and why it exists |
| `docs/foi-messaging-go-prd-v1.1.md` §19 | Carry back the two cuts: no `EventDef` builder, no raw-bytes entry point |

---

## 12. Acceptance

Phase 4 is complete when:

- An application unit-tests its publish path with no Redis and no Docker,
  and a malformed `EventDef` fails that test.
- An application unit-tests a handler through `Deliver`, and a correlation
  ID chains from the delivered envelope into a follow-on publish.
- An application asserts, without Redis, that its handler's error
  classification produces the intended terminal outcome — including
  dead-lettering with the expected reason and the delivery-attempt cap.
- `make test` covers all of it; `make test-integration` still passes.
- The root `messaging` package's exported API surface is unchanged from
  Phase 3 — the only new exported names in the module are `messagingtest`'s.
- Every document in §11 is updated.
