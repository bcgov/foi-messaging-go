package messagingtest

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"

	messaging "github.com/bcgov/foi-messaging-go"
	"github.com/bcgov/foi-messaging-go/internal/testseam"
	internalwatermill "github.com/bcgov/foi-messaging-go/internal/watermill"
)

// Outcome is the terminal verdict of a delivery.
type Outcome int

const (
	// OutcomeProcessed: the handler returned nil. Acked.
	OutcomeProcessed Outcome = iota
	// OutcomeSkipped: no handler matched, or the handler classified its
	// error with AsDiscard. Acked; see Result.Reason.
	OutcomeSkipped
	// OutcomeDeadLettered: written to the topic's DLQ and acked. See
	// Result.DeadLetters for the reason and the preserved event.
	OutcomeDeadLettered
	// OutcomeNacked: the entry stays pending for redelivery — a retryable
	// failure that exhausted its immediate retries, or a dead letter the
	// DLQ would not accept.
	OutcomeNacked
)

func (o Outcome) String() string {
	switch o {
	case OutcomeProcessed:
		return "processed"
	case OutcomeSkipped:
		return "skipped"
	case OutcomeDeadLettered:
		return "dead_lettered"
	case OutcomeNacked:
		return "nacked"
	default:
		return "unknown"
	}
}

// Result describes what the library would do with a delivery.
type Result struct {
	Outcome Outcome

	// Reason is the *skip* reason: "no_handler" or "discard". A dead
	// letter's reason lives on DeadLetters[0].Reason, alongside the rest
	// of the wrapper operational tooling actually sees — one place for it
	// rather than two that can disagree.
	Reason string

	// Category is the failure category when the delivery failed:
	// "permanent", "retryable", "deserialization", or "max_attempts".
	Category string

	// Err is the delivery's own error. It is not a harness failure —
	// Dispatch reports those through its second return value.
	Err error

	// DeadLetters holds every DeadLetter the delivery produced, decoded.
	DeadLetters []messaging.DeadLetter
}

type dispatchOptions struct {
	attempt     int64
	metadata    map[string]string
	realBackoff bool
	dlqErr      error
}

// DispatchOption customizes a Dispatch call.
type DispatchOption func(*dispatchOptions)

// WithDeliveryAttempt sets the delivery attempt number the consume path
// sees, which is how a test drives the delivery-attempt cap. Defaults to 1.
func WithDeliveryAttempt(n int64) DispatchOption {
	return func(o *dispatchOptions) { o.attempt = n }
}

// WithMetadata adds transport metadata to the delivery — a traceparent, a
// published_at. Keys the library sets itself take precedence.
func WithMetadata(md map[string]string) DispatchOption {
	return func(o *dispatchOptions) { o.metadata = md }
}

// WithRealBackoff restores the library's real jittered retry sleeps.
//
// By default Dispatch collapses them, because at the library defaults a
// single retryable failure would spend hundreds of milliseconds sleeping
// in a unit test. The retry *count* is honoured either way.
func WithRealBackoff() DispatchOption {
	return func(o *dispatchOptions) { o.realBackoff = true }
}

// WithFailingDLQ makes the dead-letter write fail with err, so a test can
// assert that an unwritable DLQ nacks rather than acking the event into
// nothing.
func WithFailingDLQ(err error) DispatchOption {
	return func(o *dispatchOptions) { o.dlqErr = err }
}

// Dispatch runs the library's real consume path against c and reports the
// terminal verdict, with no Redis involved.
//
// The subject is the application's own Consumer, built exactly as in
// production — messaging.NewConsumer plus its RegisterHandler calls — so
// the real registry lookup runs and the application's own wiring is under
// test too: that a 1.0.0 handler receives a 1.4.2 event, that an
// unregistered event type is skipped rather than erroring.
//
// Dispatch reports what the runtime *would do* with the delivery. It does
// not perform one: there is no ack, no nack, no pending entry, no reclaim.
//
// The returned error is a harness failure — a nil consumer, an
// unmarshalable event. A delivery that failed is not one of those; it is
// reported in Result.
func Dispatch(ctx context.Context, c *messaging.Consumer, e Event,
	opts ...DispatchOption) (Result, error) {
	if c == nil {
		return Result{}, fmt.Errorf("messagingtest: Dispatch needs a consumer, got nil")
	}

	o := dispatchOptions{attempt: 1}
	for _, opt := range opts {
		opt(&o)
	}

	body, err := json.Marshal(e.Envelope)
	if err != nil {
		return Result{}, fmt.Errorf("messagingtest: marshalling event: %w", err)
	}

	metadata := make(map[string]string, len(o.metadata)+1)
	for k, v := range o.metadata {
		metadata[k] = v
	}
	metadata[internalwatermill.MetadataDeliveryAttempt] = strconv.FormatInt(o.attempt, 10)

	// Guarded because a handler may dead-letter from more than one
	// goroutine in principle, and because -race should stay quiet on the
	// package's own tests.
	var (
		mu      sync.Mutex
		letters []messaging.DeadLetter
	)

	probe := &testseam.Probe{
		NoBackoff: !o.realBackoff,
		Sink: func(_ context.Context, _ string, body []byte) error {
			if o.dlqErr != nil {
				return o.dlqErr
			}
			var dl messaging.DeadLetter
			if err := json.Unmarshal(body, &dl); err != nil {
				return fmt.Errorf("messagingtest: decoding dead letter: %w", err)
			}
			mu.Lock()
			defer mu.Unlock()
			letters = append(letters, dl)
			return nil
		},
	}

	deliveryErr := testseam.Dispatch(ctx, c, e.Topic, body, metadata, probe)

	res := Result{
		Reason:      probe.Reason,
		Category:    probe.Category,
		Err:         deliveryErr,
		DeadLetters: letters,
	}
	// A dead-lettered delivery acks, so dispatch returns nil and the cause
	// is only on the probe.
	if res.Err == nil {
		res.Err = probe.Err
	}

	switch {
	case len(letters) > 0:
		res.Outcome = OutcomeDeadLettered
	case probe.Kind == testseam.KindFailed:
		res.Outcome = OutcomeNacked
	case probe.Kind == testseam.KindSkipped:
		res.Outcome = OutcomeSkipped
	default:
		res.Outcome = OutcomeProcessed
	}

	return res, nil
}
