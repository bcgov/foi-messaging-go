package messaging

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

type testPayload struct {
	Name string `json:"name"`
}

func TestNewEnvelope_PopulatesFields(t *testing.T) {
	def := EventDef{Topic: "documents", Type: "document.created", Version: "1.0.0"}
	before := time.Now().UTC()

	env, err := newEnvelope(def, "test.service", "corr-1", testPayload{Name: "report.pdf"})
	if err != nil {
		t.Fatalf("newEnvelope: %v", err)
	}

	if env.EventID == "" {
		t.Error("EventID is empty")
	}
	if _, err := uuid.Parse(env.EventID); err != nil {
		t.Errorf("EventID %q is not a valid UUID: %v", env.EventID, err)
	}
	if env.EventType != "document.created" {
		t.Errorf("EventType = %q, want %q", env.EventType, "document.created")
	}
	if env.SchemaVersion != "1.0.0" {
		t.Errorf("SchemaVersion = %q, want %q", env.SchemaVersion, "1.0.0")
	}
	if env.CorrelationID != "corr-1" {
		t.Errorf("CorrelationID = %q, want %q", env.CorrelationID, "corr-1")
	}
	if env.Source != "test.service" {
		t.Errorf("Source = %q, want %q", env.Source, "test.service")
	}
	if env.Payload.Name != "report.pdf" {
		t.Errorf("Payload.Name = %q, want %q", env.Payload.Name, "report.pdf")
	}
	if env.Timestamp.Before(before) {
		t.Errorf("Timestamp %v is before call time %v", env.Timestamp, before)
	}
}

func TestNewEnvelope_GeneratesUniqueEventIDs(t *testing.T) {
	def := EventDef{Topic: "documents", Type: "document.created", Version: "1.0.0"}

	env1, err := newEnvelope(def, "test.service", "corr-1", testPayload{})
	if err != nil {
		t.Fatalf("newEnvelope: %v", err)
	}
	env2, err := newEnvelope(def, "test.service", "corr-1", testPayload{})
	if err != nil {
		t.Fatalf("newEnvelope: %v", err)
	}

	if env1.EventID == env2.EventID {
		t.Errorf("expected unique EventIDs, got the same value twice: %q", env1.EventID)
	}
}
