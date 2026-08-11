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
