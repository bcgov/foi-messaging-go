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
