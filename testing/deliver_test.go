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
