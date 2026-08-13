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
