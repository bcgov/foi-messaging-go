package watermill

import (
	"context"
	"fmt"
	"time"

	wm "github.com/ThreeDotsLabs/watermill"
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

// NewRouter builds a Router whose shutdown drain is bounded by closeTimeout.
func NewRouter(closeTimeout time.Duration) (*Router, error) {
	router, err := message.NewRouter(
		message.RouterConfig{CloseTimeout: closeTimeout},
		wm.NewStdLogger(false, false),
	)
	if err != nil {
		return nil, fmt.Errorf("creating router: %w", err)
	}
	return &Router{router: router}, nil
}

// AddHandler subscribes h to stream. name identifies the handler within the
// router and must be unique.
func (r *Router) AddHandler(name, stream string, sub *Subscriber, h MessageHandler) {
	r.router.AddConsumerHandler(name, stream, sub, func(msg *message.Message) error {
		return h(msg.Context(), msg.Payload, msg.Metadata)
	})
}

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
