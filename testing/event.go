package messagingtest

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	messaging "github.com/bcgov/foi-messaging-go"
)

const (
	// defaultSource is what messagingtest stamps on envelopes and
	// publishes when the caller does not choose one. It satisfies the
	// envelope's event-source requirement without pretending to be a real
	// service name.
	defaultSource = "messagingtest"

	// defaultStreamPrefix mirrors the library's own default (PRD §8). It
	// is only ever used to strip the prefix back off a recorded stream
	// name, since nothing here writes to Redis.
	defaultStreamPrefix = "foi"
)

// Event is a message as messagingtest models it: the logical topic, the
// envelope as it would appear on the wire, and the payload value the
// caller passed.
//
// Both payload representations are kept deliberately. Envelope.Payload is
// marshalled JSON, so a payload holding a channel or a NaN, or a struct
// with wrong json tags, fails in the test rather than in production.
// Payload is the caller's own value, so the common assertion needs no
// decode step.
//
// Event is the currency of this package: Publisher.Published returns
// []Event, NewEvent produces one, and Dispatch consumes one. That is what
// makes a two-service chain testable without Redis.
type Event struct {
	Topic    string
	Envelope messaging.Envelope[json.RawMessage]

	// Payload is the value as passed. It is nil for an Event decoded from
	// bytes rather than built from a value.
	Payload any
}

// PayloadAs decodes e's marshalled payload into T.
//
// A free function rather than a method because Go does not permit generic
// methods — the same reason messaging.RegisterHandler is one.
func PayloadAs[T any](e Event) (T, error) {
	var payload T
	if len(e.Envelope.Payload) == 0 {
		return payload, nil
	}
	if err := json.Unmarshal(e.Envelope.Payload, &payload); err != nil {
		return payload, fmt.Errorf("messagingtest: decoding payload of event type %q: %w",
			e.Envelope.EventType, err)
	}
	return payload, nil
}

type eventOptions struct {
	eventID       string
	correlationID string
	source        string
	timestamp     time.Time
}

// EventOption customizes an Event built by NewEvent.
type EventOption func(*eventOptions)

// WithEventID sets the envelope's event_id. Pass "" to clear it, which is
// how a test provokes the dead-letter path for an invalid envelope.
func WithEventID(id string) EventOption {
	return func(o *eventOptions) { o.eventID = id }
}

// WithCorrelationID sets the envelope's correlation_id.
//
// Note the deliberate name collision: messaging.WithCorrelationID is a
// PublishOption for the publish boundary, this one is an EventOption for
// the consume boundary.
func WithCorrelationID(id string) EventOption {
	return func(o *eventOptions) { o.correlationID = id }
}

// WithSource sets the envelope's source.
func WithSource(source string) EventOption {
	return func(o *eventOptions) { o.source = source }
}

// WithTimestamp sets the envelope's timestamp. Pass the zero time to
// provoke the dead-letter path for an invalid envelope.
func WithTimestamp(t time.Time) EventOption {
	return func(o *eventOptions) { o.timestamp = t }
}

// NewEvent builds an Event for def carrying payload.
//
// The defaults produce an envelope that passes the library's validation:
// UUIDv7 event and correlation ids, the current time, and defaultSource.
// Options are applied over the defaults, so any of them can be cleared
// deliberately.
func NewEvent[T any](def messaging.EventDef, payload T, opts ...EventOption) (Event, error) {
	eventID, err := uuid.NewV7()
	if err != nil {
		return Event{}, fmt.Errorf("messagingtest: generating event id: %w", err)
	}
	correlationID, err := uuid.NewV7()
	if err != nil {
		return Event{}, fmt.Errorf("messagingtest: generating correlation id: %w", err)
	}

	o := eventOptions{
		eventID:       eventID.String(),
		correlationID: correlationID.String(),
		source:        defaultSource,
		timestamp:     time.Now().UTC(),
	}
	for _, opt := range opts {
		opt(&o)
	}

	raw, err := json.Marshal(payload)
	if err != nil {
		return Event{}, fmt.Errorf("messagingtest: marshalling payload for event type %q: %w",
			def.Type, err)
	}

	return Event{
		Topic: def.Topic,
		Envelope: messaging.Envelope[json.RawMessage]{
			EventID:       o.eventID,
			EventType:     def.Type,
			Timestamp:     o.timestamp,
			SchemaVersion: def.Version,
			CorrelationID: o.correlationID,
			Source:        o.source,
			Payload:       raw,
		},
		Payload: payload,
	}, nil
}

// Config returns a messaging.Config that passes validation without dialing
// anything, for building a Consumer to hand to Dispatch.
//
// It exists because Config.Validate requires Redis.Address even for a
// Consumer that never connects, which would otherwise make every
// application test invent a fake address. The returned value is a plain
// struct: adjust any field directly rather than reaching for options.
func Config() messaging.Config {
	return messaging.Config{
		Source:       defaultSource,
		StreamPrefix: defaultStreamPrefix,
		Redis:        messaging.RedisConfig{Address: "127.0.0.1:6379"},
		Consumer:     messaging.ConsumerConfig{Group: defaultSource},
	}
}
