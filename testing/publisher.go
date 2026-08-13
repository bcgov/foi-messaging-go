package messagingtest

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	messaging "github.com/bcgov/foi-messaging-go"
	"github.com/bcgov/foi-messaging-go/internal/testseam"
)

// publisher is the method set an application substitutes on.
//
// The library exports no publisher interface — applications declare their
// own narrow one at the point of consumption, which is the Go idiom. This
// assertion is what keeps the fake honest in the meantime: a change to
// messaging.Publisher.Publish's signature breaks the library's own build
// here, rather than breaking every application's tests silently.
type publisher interface {
	Publish(context.Context, messaging.EventDef, any,
		...messaging.PublishOption) (messaging.PublishResult, error)
}

var (
	_ publisher = (*messaging.Publisher)(nil)
	_ publisher = (*Publisher)(nil)
)

// Publisher records published events for assertion and never contacts
// Redis.
//
// It wraps a real *messaging.Publisher with only its transport write
// redirected, so Publish runs genuine library code: correlation-ID
// resolution, envelope construction, validation, marshalling, and the
// producer span and metrics. That is what makes a malformed EventDef fail
// here rather than on the first production publish.
type Publisher struct {
	real   *messaging.Publisher
	prefix string

	mu      sync.Mutex
	events  []Event
	failErr error
}

type publisherOptions struct{ source string }

// PublisherOption customizes a Publisher.
type PublisherOption func(*publisherOptions)

// WithPublisherSource sets the source stamped on published envelopes.
// Named for its boundary because WithSource is already this package's
// EventOption.
func WithPublisherSource(source string) PublisherOption {
	return func(o *publisherOptions) { o.source = source }
}

// NewPublisher returns a Publisher that records instead of publishing.
//
// It returns an error only for a library-internal failure — there is no
// input a caller can supply that makes it fail — but returns one anyway
// for consistency with messaging.NewPublisher and messaging.NewConsumer.
func NewPublisher(opts ...PublisherOption) (*Publisher, error) {
	o := publisherOptions{source: defaultSource}
	for _, opt := range opts {
		opt(&o)
	}

	p := &Publisher{prefix: defaultStreamPrefix}

	v, err := testseam.NewRecordingPublisher(o.source, defaultStreamPrefix, p.record)
	if err != nil {
		return nil, fmt.Errorf("messagingtest: building recording publisher: %w", err)
	}
	real, ok := v.(*messaging.Publisher)
	if !ok {
		return nil, fmt.Errorf("messagingtest: seam returned %T, want *messaging.Publisher", v)
	}
	p.real = real

	return p, nil
}

type payloadKey struct{}

// Publish delegates to the real publisher, whose transport write lands in
// record.
func (p *Publisher) Publish(ctx context.Context, def messaging.EventDef, payload any,
	opts ...messaging.PublishOption) (messaging.PublishResult, error) {
	// The original payload value rides the context, because record sees
	// only marshalled bytes. On the context rather than in a field so
	// concurrent Publish calls cannot cross their payloads — the
	// alternative is holding the lock across the whole delegated call.
	ctx = context.WithValue(ctx, payloadKey{}, payload)
	return p.real.Publish(ctx, def, payload, opts...)
}

// record is the transport write the real publisher calls in place of a
// Redis XADD.
func (p *Publisher) record(ctx context.Context, stream, _ string, body []byte,
	_ map[string]string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Failing here rather than short-circuiting Publish is deliberate: the
	// envelope is still built and validated on the way to this point, so a
	// simulated outage behaves exactly like a real one — a transport-stage
	// failure, not a validation-stage one.
	if p.failErr != nil {
		return p.failErr
	}

	var env messaging.Envelope[json.RawMessage]
	if err := json.Unmarshal(body, &env); err != nil {
		return fmt.Errorf("messagingtest: decoding recorded envelope: %w", err)
	}

	p.events = append(p.events, Event{
		Topic:    strings.TrimPrefix(stream, p.prefix+":"),
		Envelope: env,
		Payload:  ctx.Value(payloadKey{}),
	})
	return nil
}

// Published returns the events recorded so far, oldest first.
//
// The result is a copy: a caller that appends to it, or edits an element,
// must not be able to corrupt the recorder.
func (p *Publisher) Published() []Event {
	p.mu.Lock()
	defer p.mu.Unlock()

	out := make([]Event, len(p.events))
	copy(out, p.events)
	return out
}

// Reset discards everything recorded so far, for a test that reuses one
// Publisher across subtests.
func (p *Publisher) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = nil
}

// FailWith makes every subsequent Publish fail with err, so an application
// can test its own publish-failure path. Pass nil to stop failing.
func (p *Publisher) FailWith(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.failErr = err
}

// Close releases the underlying publisher. Safe to call on a Publisher
// that never connected to anything, which is all of them.
func (p *Publisher) Close() error {
	return p.real.Close()
}
