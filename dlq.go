package messaging

import (
	"encoding/json"
	"time"
)

// DLQ reason values, fixed by PRD §14. Exported so operational tooling
// consuming DLQ streams can compare against them rather than against string
// literals of its own.
const (
	// ReasonPermanent is a handler error classified with AsPermanent.
	ReasonPermanent = "permanent"
	// ReasonMaxAttemptsExceeded is the delivery-attempt cap firing,
	// regardless of how the failures were classified.
	ReasonMaxAttemptsExceeded = "max_attempts_exceeded"
	// ReasonDeserializationFailed covers every event that could not be read
	// well enough to dispatch: invalid JSON, an envelope failing validation,
	// an unparseable schema version, or a stream entry Watermill's own
	// marshaller rejected.
	ReasonDeserializationFailed = "deserialization_failed"
)

// DeadLetter is the wrapper written to a topic's DLQ stream.
//
// PRD §5 forbids failure metadata inside the event, so the original event
// travels verbatim alongside the metadata rather than being modified to
// carry it. That is what lets replay tooling republish the event without
// transformation.
//
// Exactly one of Event and EventRaw is set. Event holds the original bytes
// when they were valid JSON; EventRaw holds them — base64-encoded by
// encoding/json — when they were not, which is the only case where
// preserving unparseable input is what matters.
type DeadLetter struct {
	DeadLetteredAt   time.Time       `json:"dead_lettered_at"`
	Reason           string          `json:"reason"`
	Error            string          `json:"error"`
	DeliveryAttempts int64           `json:"delivery_attempts"`
	ConsumerGroup    string          `json:"consumer_group"`
	ConsumerName     string          `json:"consumer_name"`
	OriginalTopic    string          `json:"original_topic"`
	Event            json.RawMessage `json:"event,omitempty"`
	EventRaw         []byte          `json:"event_raw,omitempty"`
}

// deadLetterBody assigns payload to Event when it is valid JSON and to
// EventRaw otherwise.
//
// The validity check is load-bearing, not defensive: json.RawMessage is
// spliced into the document unquoted, so unparseable bytes placed there
// would make the DeadLetter itself invalid JSON — unreadable by the tooling
// it exists for, and undetectable until someone tried to replay it.
func deadLetterBody(payload []byte) (json.RawMessage, []byte) {
	if json.Valid(payload) {
		return json.RawMessage(payload), nil
	}
	return nil, payload
}
