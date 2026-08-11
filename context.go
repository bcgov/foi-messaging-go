package messaging

import "context"

type correlationIDContextKey struct{}

// contextWithCorrelationID returns a context carrying the given correlation
// ID. Used internally when resolving the correlation ID to publish with,
// and at dispatch to propagate the consumed event's correlation ID into the
// handler's context.
func contextWithCorrelationID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, correlationIDContextKey{}, id)
}

// correlationIDFromContext reads a correlation ID previously placed by
// contextWithCorrelationID. ok is false if none is present.
func correlationIDFromContext(ctx context.Context) (string, bool) {
	id, ok := ctx.Value(correlationIDContextKey{}).(string)
	return id, ok
}
