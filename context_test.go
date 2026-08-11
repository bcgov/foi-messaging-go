package messaging

import (
	"context"
	"testing"
)

func TestContextWithCorrelationID_RoundTrip(t *testing.T) {
	ctx := contextWithCorrelationID(context.Background(), "corr-42")

	id, ok := correlationIDFromContext(ctx)
	if !ok {
		t.Fatal("expected ok=true, got false")
	}
	if id != "corr-42" {
		t.Errorf("id = %q, want %q", id, "corr-42")
	}
}

func TestCorrelationIDFromContext_Missing(t *testing.T) {
	_, ok := correlationIDFromContext(context.Background())
	if ok {
		t.Error("expected ok=false for a context with no correlation ID, got true")
	}
}
