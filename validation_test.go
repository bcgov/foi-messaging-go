package messaging

import (
	"testing"
	"time"
)

func validEnvelope() Envelope[testPayload] {
	return Envelope[testPayload]{
		EventID:       "018f2e7a-1c6b-7c0a-9f8d-3e4a2b1c5d90",
		EventType:     "document.created",
		Timestamp:     time.Now().UTC(),
		SchemaVersion: "1.0.0",
		CorrelationID: "corr-1",
		Source:        "test.service",
		Payload:       testPayload{Name: "report.pdf"},
	}
}

func TestValidateEnvelope_Valid(t *testing.T) {
	if err := validateEnvelope(validEnvelope()); err != nil {
		t.Errorf("expected valid envelope to pass, got error: %v", err)
	}
}

func TestValidateEnvelope_Invalid(t *testing.T) {
	cases := []struct {
		name   string
		modify func(*Envelope[testPayload])
	}{
		{"empty EventID", func(e *Envelope[testPayload]) { e.EventID = "" }},
		{"empty EventType", func(e *Envelope[testPayload]) { e.EventType = "" }},
		{"single-segment EventType", func(e *Envelope[testPayload]) { e.EventType = "created" }},
		{"four-segment EventType", func(e *Envelope[testPayload]) { e.EventType = "a.b.c.d" }},
		{"uppercase EventType", func(e *Envelope[testPayload]) { e.EventType = "Document.Created" }},
		{"empty SchemaVersion", func(e *Envelope[testPayload]) { e.SchemaVersion = "" }},
		{"two-part SchemaVersion", func(e *Envelope[testPayload]) { e.SchemaVersion = "1.0" }},
		{"v-prefixed SchemaVersion", func(e *Envelope[testPayload]) { e.SchemaVersion = "v1.0.0" }},
		{"empty Source", func(e *Envelope[testPayload]) { e.Source = "" }},
		{"empty CorrelationID", func(e *Envelope[testPayload]) { e.CorrelationID = "" }},
		{"zero Timestamp", func(e *Envelope[testPayload]) { e.Timestamp = time.Time{} }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := validEnvelope()
			tc.modify(&env)
			if err := validateEnvelope(env); err == nil {
				t.Errorf("expected an error for %s, got nil", tc.name)
			}
		})
	}
}
