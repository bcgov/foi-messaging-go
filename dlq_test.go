package messaging

import (
	"encoding/json"
	"testing"
	"time"
)

func TestDeadLetter_WireShapeMatchesTheContract(t *testing.T) {
	dl := DeadLetter{
		DeadLetteredAt:   time.Date(2026, 4, 23, 10, 5, 12, 0, time.UTC),
		Reason:           ReasonPermanent,
		Error:            "invoice 42 references unknown account",
		DeliveryAttempts: 5,
		ConsumerGroup:    "billing-service",
		ConsumerName:     "billing-7f9c4",
		OriginalTopic:    "documents",
		Event:            json.RawMessage(`{"event_id":"abc"}`),
	}

	body, err := json.Marshal(dl)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	// Field names are the operational contract PRD §14 publishes; replay
	// tooling reads them, so a rename is a breaking change.
	var got map[string]json.RawMessage
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	for _, key := range []string{
		"dead_lettered_at", "reason", "error", "delivery_attempts",
		"consumer_group", "consumer_name", "original_topic", "event",
	} {
		if _, ok := got[key]; !ok {
			t.Errorf("missing field %q in %s", key, body)
		}
	}
	if _, ok := got["event_raw"]; ok {
		t.Error("event_raw must be omitted when event is set")
	}
}

func TestDeadLetter_EventPreservedByteForByte(t *testing.T) {
	// PRD §14: replay tooling republishes event without transformation, so
	// key order and spacing must survive the round trip.
	original := `{"event_id":"abc","payload":{"b":1,"a":2}}`
	dl := DeadLetter{Reason: ReasonPermanent, Event: json.RawMessage(original)}

	body, err := json.Marshal(dl)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	var got DeadLetter
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if string(got.Event) != original {
		t.Errorf("Event = %s, want %s", got.Event, original)
	}
}

func TestDeadLetterBody_RoutesByValidity(t *testing.T) {
	tests := []struct {
		name    string
		payload string
		wantRaw bool
	}{
		{"valid json goes to event", `{"event_id":"abc"}`, false},
		{"invalid json goes to event_raw", `not json at all`, true},
		{"truncated json goes to event_raw", `{"event_id":`, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			event, raw := deadLetterBody([]byte(tc.payload))
			if tc.wantRaw {
				if event != nil {
					t.Errorf("Event = %s, want nil", event)
				}
				if string(raw) != tc.payload {
					t.Errorf("EventRaw = %s, want %s", raw, tc.payload)
				}
				return
			}
			if raw != nil {
				t.Errorf("EventRaw = %s, want nil", raw)
			}
			if string(event) != tc.payload {
				t.Errorf("Event = %s, want %s", event, tc.payload)
			}
		})
	}
}

func TestDeadLetter_UnparseableBytesSurviveAsBase64(t *testing.T) {
	// The whole point of event_raw: a DeadLetter carrying unparseable input
	// must itself still be valid JSON, or the DLQ entry cannot be read back
	// by the tooling the DLQ exists for.
	dl := DeadLetter{Reason: ReasonDeserializationFailed, EventRaw: []byte(`not json`)}

	body, err := json.Marshal(dl)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if !json.Valid(body) {
		t.Fatalf("dead letter document is not valid JSON: %s", body)
	}

	var got DeadLetter
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if string(got.EventRaw) != "not json" {
		t.Errorf("EventRaw = %q, want %q", got.EventRaw, "not json")
	}
}
