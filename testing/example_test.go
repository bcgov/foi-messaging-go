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
