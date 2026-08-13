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
