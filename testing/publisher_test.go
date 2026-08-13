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
	for range 50 {
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
