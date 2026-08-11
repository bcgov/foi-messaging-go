package watermill

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
)

// MessageHandler processes one consumed message. It takes only plain types,
// so no watermill value reaches callers outside internal/ — the same
// boundary the publisher wrapper keeps.
//
// Returning nil acks the message; returning an error nacks it, leaving the
// entry pending for the reclaim loop.
type MessageHandler func(ctx context.Context, payload []byte, metadata map[string]string) error

// Router wraps watermill's Router, which supplies handler lifecycle, the
// middleware chain, ack/nack plumbing, and a bounded drain on shutdown.
type Router struct {
	router *message.Router
}

// NewRouter builds a Router whose shutdown drain is bounded by closeTimeout
// and which logs through logger — including watermill's own
// "Handler returned error" line, which would otherwise go to the stdlib
// logger in watermill's format and miss the application's log stream
// entirely. A nil logger falls back to slog's default.
func NewRouter(closeTimeout time.Duration, logger *slog.Logger) (*Router, error) {
	router, err := message.NewRouter(
		message.RouterConfig{CloseTimeout: closeTimeout},
		newLoggerAdapter(logger),
	)
	if err != nil {
		return nil, fmt.Errorf("creating router: %w", err)
	}
	return &Router{router: router}, nil
}

// AddHandler subscribes h to stream. name identifies the handler within the
// router and must be unique.
func (r *Router) AddHandler(name, stream string, sub *Subscriber, h MessageHandler) {
	r.router.AddConsumerHandler(name, stream, sharedSubscriber{sub}, func(msg *message.Message) error {
		return h(msg.Context(), msg.Payload, msg.Metadata)
	})
}

// sharedSubscriber hands the Subscriber to watermill with its Close
// suppressed.
//
// Watermill closes a handler's subscriber as soon as that handler stops —
// message/router.go's handleClose, which runs the moment the router context
// is cancelled and before the CloseTimeout drain — and every handler holds
// the same Subscriber here. Letting it through would mean the first handler
// to stop tore down every other topic's subscription, and closed the
// Subscriber whose shutdown signal settles in-flight messages, cancelling
// the contexts of the very handlers the drain exists to let finish.
//
// The Subscriber's owner is messaging.Consumer.Run, which closes it exactly
// once after the router has drained. Suppressing the early close costs
// nothing: each subscription's loops already exit on the handler context,
// so the message channel still closes and the handler still stops.
type sharedSubscriber struct {
	*Subscriber
}

func (sharedSubscriber) Close() error { return nil }

// Run blocks until ctx is cancelled, then drains in-flight handlers within
// the configured close timeout.
func (r *Router) Run(ctx context.Context) error {
	if err := r.router.Run(ctx); err != nil {
		return fmt.Errorf("running router: %w", err)
	}
	return nil
}

// Close stops the router and releases its resources.
func (r *Router) Close() error {
	return r.router.Close()
}
