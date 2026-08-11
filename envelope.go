package messaging

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Envelope wraps every published and consumed event. It carries business
// and workflow metadata only — transport state (retry counters, delivery
// attempts, trace context) travels in message metadata, never here.
type Envelope[T any] struct {
	EventID       string    `json:"event_id"`
	EventType     string    `json:"event_type"`
	Timestamp     time.Time `json:"timestamp"`
	SchemaVersion string    `json:"schema_version"`
	CorrelationID string    `json:"correlation_id"`
	Source        string    `json:"source"`
	Payload       T         `json:"payload"`
}

func newEnvelope[T any](def EventDef, source string, correlationID string, payload T) (Envelope[T], error) {
	id, err := uuid.NewV7()
	if err != nil {
		return Envelope[T]{}, fmt.Errorf("generating event id: %w", err)
	}

	return Envelope[T]{
		EventID:       id.String(),
		EventType:     def.Type,
		Timestamp:     time.Now().UTC(),
		SchemaVersion: def.Version,
		CorrelationID: correlationID,
		Source:        source,
		Payload:       payload,
	}, nil
}
