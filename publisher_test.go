package messaging

import (
	"context"
	"testing"

	"github.com/google/uuid"
)

func TestWithCorrelationID_SetsOption(t *testing.T) {
	var opts publishOptions
	WithCorrelationID("corr-99")(&opts)

	if opts.correlationID != "corr-99" {
		t.Errorf("correlationID = %q, want %q", opts.correlationID, "corr-99")
	}
}

func TestResolveCorrelationID_PrefersOption(t *testing.T) {
	ctx := contextWithCorrelationID(context.Background(), "from-context")
	opts := publishOptions{correlationID: "from-option"}

	id, err := resolveCorrelationID(ctx, opts)
	if err != nil {
		t.Fatalf("resolveCorrelationID: %v", err)
	}
	if id != "from-option" {
		t.Errorf("id = %q, want %q", id, "from-option")
	}
}

func TestResolveCorrelationID_FallsBackToContext(t *testing.T) {
	ctx := contextWithCorrelationID(context.Background(), "from-context")

	id, err := resolveCorrelationID(ctx, publishOptions{})
	if err != nil {
		t.Fatalf("resolveCorrelationID: %v", err)
	}
	if id != "from-context" {
		t.Errorf("id = %q, want %q", id, "from-context")
	}
}

func TestResolveCorrelationID_GeneratesUUIDv7WhenAbsent(t *testing.T) {
	id, err := resolveCorrelationID(context.Background(), publishOptions{})
	if err != nil {
		t.Fatalf("resolveCorrelationID: %v", err)
	}
	if id == "" {
		t.Fatal("expected a generated correlation ID, got empty string")
	}
	if _, err := uuid.Parse(id); err != nil {
		t.Errorf("generated id %q is not a valid UUID: %v", id, err)
	}
}
