// Package testseam carries the hooks the root messaging package registers
// at init so this module's testing/ package can drive the real publish and
// consume paths without a Redis instance.
//
// It exists because three things testing/ needs are unexported in the root
// package, and no exported path reaches any of them: the correlation-ID
// context setter, Consumer.dispatch, and a dead-letter sink — Consumer.dlq
// is nil until Run, and deadLetter refuses to run without one.
//
// Moving dispatch into internal/ instead was not an option: it needs
// Envelope[T], IsPermanent, DeadLetter, and the registry, so an internal
// package holding it would import the root and cycle. That is the same
// constraint that already places OpenTelemetry outside the internal/
// boundary.
//
// This package imports nothing of ours, so it cannot participate in a
// cycle. It lives under internal/, so no application can see it, and the
// root package's exported API is unchanged by its existence.
package testseam

import "context"

// Delivery kinds, mirroring the root package's outcomeKind. Declared here
// so both sides of the seam compare against the same strings rather than
// against literals of their own.
const (
	KindProcessed = "processed"
	KindSkipped   = "skipped"
	KindFailed    = "failed"
)

// Probe collects what a single delivery did.
//
// It travels on the delivery's context rather than living on the Consumer.
// That way messagingtest.Dispatch needs no lock, is safe to call
// concurrently on one Consumer, leaves the application's Consumer
// unmutated once it returns, and cannot leak state between deliveries.
type Probe struct {
	// Kind, Category, Reason, and Err are copied from the root package's
	// deliveryRecorder at the end of the delivery — the same state that
	// produces the metrics, which is why a probe cannot report an outcome
	// production would not.
	Kind     string
	Category string
	Reason   string
	Err      error

	// NoBackoff collapses the immediate-retry loop's jittered sleeps to
	// nothing. The retry *count* is unaffected; only the waiting is.
	NoBackoff bool

	// Sink stands in for Consumer.dlq, which is nil until Run. It receives
	// the marshalled DeadLetter. Returning an error from it exercises the
	// DLQ-write-failure path, which nacks rather than acking the event
	// into nothing.
	Sink func(ctx context.Context, stream string, body []byte) error
}

type probeKey struct{}

// NewContext returns ctx carrying p.
func NewContext(ctx context.Context, p *Probe) context.Context {
	return context.WithValue(ctx, probeKey{}, p)
}

// FromContext returns the probe on ctx, or nil when there is none — which
// is every delivery in a real service.
func FromContext(ctx context.Context) *Probe {
	p, _ := ctx.Value(probeKey{}).(*Probe)
	return p
}

// The hooks. Each is a whole path rather than a fragment, so testing/
// reuses real behaviour instead of reimplementing it. All three are
// registered by the root package's init (testhooks.go); testing/ imports
// the root, so registration always precedes any use.
var (
	// WithCorrelationID is messaging.contextWithCorrelationID. It is what
	// makes a correlation ID chain from a consumed event into a follow-on
	// publish, because resolveCorrelationID reads that context value.
	WithCorrelationID func(ctx context.Context, id string) context.Context

	// NewRecordingPublisher returns a *messaging.Publisher whose transport
	// write is record instead of Redis. The concrete type is returned as
	// any because this package cannot name it; messagingtest, which
	// imports the root, asserts it back.
	NewRecordingPublisher func(source, streamPrefix string,
		record func(ctx context.Context, stream, id string, body []byte,
			metadata map[string]string) error) (any, error)

	// Dispatch runs Consumer.dispatch against consumer — which must be a
	// *messaging.Consumer — with p attached to the context.
	Dispatch func(ctx context.Context, consumer any, topic string,
		body []byte, metadata map[string]string, p *Probe) error
)
