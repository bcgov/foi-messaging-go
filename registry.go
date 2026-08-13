package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// routeKey is the dispatch key: a topic, an event type, and a major schema
// version. Minor and patch versions deliberately do not participate.
type routeKey struct {
	topic     string
	eventType string
	major     int
}

// dispatchFunc handles an event whose payload is still raw JSON. Typed
// handlers are erased into this shape at registration time, so the payload
// is only deserialized once the right handler has been found.
type dispatchFunc func(context.Context, Envelope[json.RawMessage]) error

// registry stores handler registrations and resolves an event to one
// handler. A topic carries either typed handlers or a single raw handler,
// never both.
type registry struct {
	typed  map[routeKey]dispatchFunc
	raw    map[string]dispatchFunc
	topics map[string]struct{}
}

func newRegistry() *registry {
	return &registry{
		typed:  make(map[routeKey]dispatchFunc),
		raw:    make(map[string]dispatchFunc),
		topics: make(map[string]struct{}),
	}
}

// addTyped registers a typed handler. Duplicate topic/type/major
// registrations and typed/raw collisions fail here rather than at Run: the
// registry already has enough information, so deferring would only delay a
// deterministic error.
func (r *registry) addTyped(topic, eventType string, major int, fn dispatchFunc) error {
	if _, ok := r.raw[topic]; ok {
		return fmt.Errorf("registry: topic %q already has a raw handler; a topic may have typed handlers or one raw handler, not both", topic)
	}

	key := routeKey{topic: topic, eventType: eventType, major: major}
	if _, ok := r.typed[key]; ok {
		return fmt.Errorf("registry: a handler is already registered for topic %q, event type %q, major version %d", topic, eventType, major)
	}

	r.typed[key] = fn
	r.topics[topic] = struct{}{}
	return nil
}

// addRaw registers the single raw handler for a topic.
func (r *registry) addRaw(topic string, fn dispatchFunc) error {
	if _, ok := r.raw[topic]; ok {
		return fmt.Errorf("registry: topic %q already has a raw handler", topic)
	}
	for key := range r.typed {
		if key.topic == topic {
			return fmt.Errorf("registry: topic %q already has typed handlers; a topic may have typed handlers or one raw handler, not both", topic)
		}
	}

	r.raw[topic] = fn
	r.topics[topic] = struct{}{}
	return nil
}

// routeMatch says how an event matched a handler.
//
// It exists for telemetry, not for dispatch, which treats both matches
// identically. A typed match means the event type came from a set fixed at
// registration time and is safe to use as a metric attribute; a raw match
// means it is whatever the wire said, and a raw handler takes every event
// on its topic — so a successful lookup alone is not evidence of a bounded
// event type. Attaching one as a label would let any producer mint
// unbounded series.
type routeMatch int

const (
	matchNone routeMatch = iota
	matchTyped
	matchRaw
)

// lookup resolves an event to its handler, reporting how it matched. A raw
// handler, when present, takes every event on its topic.
func (r *registry) lookup(topic, eventType string, major int) (dispatchFunc, routeMatch) {
	if fn, ok := r.raw[topic]; ok {
		return fn, matchRaw
	}
	if fn, ok := r.typed[routeKey{topic: topic, eventType: eventType, major: major}]; ok {
		return fn, matchTyped
	}
	return nil, matchNone
}

// topicList returns the distinct topics to subscribe to.
func (r *registry) topicList() []string {
	topics := make([]string, 0, len(r.topics))
	for topic := range r.topics {
		topics = append(topics, topic)
	}
	return topics
}

// isEmpty reports whether nothing has been registered.
func (r *registry) isEmpty() bool {
	return len(r.topics) == 0
}

// majorVersion extracts the major component of a semantic version.
func majorVersion(schemaVersion string) (int, error) {
	segment, _, found := strings.Cut(schemaVersion, ".")
	if !found {
		return 0, fmt.Errorf("schema version %q is not MAJOR.MINOR.PATCH", schemaVersion)
	}
	major, err := strconv.Atoi(segment)
	if err != nil {
		return 0, fmt.Errorf("schema version %q has a non-numeric major component: %w", schemaVersion, err)
	}
	return major, nil
}

// typedDispatch erases a typed handler into a dispatchFunc, deserializing
// the payload into T only once this handler has been selected.
//
// Decoding is deliberately lenient — unknown fields are ignored, never
// rejected — because within a major version producers may add fields
// without coordinating a consumer release (PRD §11).
func typedDispatch[T any](h Handler[T]) dispatchFunc {
	return func(ctx context.Context, raw Envelope[json.RawMessage]) error {
		var payload T
		if len(raw.Payload) > 0 {
			if err := json.Unmarshal(raw.Payload, &payload); err != nil {
				return fmt.Errorf("unmarshalling payload for event type %q: %w", raw.EventType, err)
			}
		}

		return h.Handle(ctx, Envelope[T]{
			EventID:       raw.EventID,
			EventType:     raw.EventType,
			Timestamp:     raw.Timestamp,
			SchemaVersion: raw.SchemaVersion,
			CorrelationID: raw.CorrelationID,
			Source:        raw.Source,
			Payload:       payload,
		})
	}
}
