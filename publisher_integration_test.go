//go:build integration

package messaging_test

import (
	"context"
	"encoding/json"
	"testing"

	messaging "github.com/bcgov/foi-messaging-go"
	"github.com/bcgov/foi-messaging-go/internal/testsupport"
)

type documentCreatedPayload struct {
	EntityID string `json:"entity_id"`
	Name     string `json:"name"`
}

func TestPublisher_Publish_WritesEnvelopeToRedisStream(t *testing.T) {
	ctx := context.Background()

	addr, terminate, err := testsupport.StartRedis(ctx)
	if err != nil {
		t.Fatalf("StartRedis: %v", err)
	}
	t.Cleanup(func() {
		if err := terminate(ctx); err != nil {
			t.Errorf("terminate: %v", err)
		}
	})

	cfg := messaging.Config{
		Source: "test.service",
		Redis:  messaging.RedisConfig{Address: addr},
	}
	pub, err := messaging.NewPublisher(cfg)
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	t.Cleanup(func() {
		if err := pub.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})

	def := messaging.EventDef{Topic: "documents", Type: "document.created", Version: "1.0.0"}
	payload := documentCreatedPayload{EntityID: "42", Name: "report.pdf"}

	result, err := pub.Publish(ctx, def, payload, messaging.WithCorrelationID("corr-integration"))
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if result.EventID == "" {
		t.Error("PublishResult.EventID is empty")
	}
	if result.Timestamp.IsZero() {
		t.Error("PublishResult.Timestamp is zero")
	}

	entries, err := testsupport.ReadStreamEntries(ctx, addr, "foi:documents")
	if err != nil {
		t.Fatalf("ReadStreamEntries: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("got %d stream entries, want 1", len(entries))
	}

	rawPayload, ok := entries[0].Fields["payload"]
	if !ok {
		t.Fatalf("entry has no %q field; got fields: %v", "payload", entries[0].Fields)
	}

	var env messaging.Envelope[documentCreatedPayload]
	if err := json.Unmarshal([]byte(rawPayload), &env); err != nil {
		t.Fatalf("unmarshaling stream entry payload: %v", err)
	}

	if env.EventID != result.EventID {
		t.Errorf("envelope EventID = %q, want %q", env.EventID, result.EventID)
	}
	if env.EventType != "document.created" {
		t.Errorf("envelope EventType = %q, want %q", env.EventType, "document.created")
	}
	if env.SchemaVersion != "1.0.0" {
		t.Errorf("envelope SchemaVersion = %q, want %q", env.SchemaVersion, "1.0.0")
	}
	if env.CorrelationID != "corr-integration" {
		t.Errorf("envelope CorrelationID = %q, want %q", env.CorrelationID, "corr-integration")
	}
	if env.Source != "test.service" {
		t.Errorf("envelope Source = %q, want %q", env.Source, "test.service")
	}
	if env.Payload != payload {
		t.Errorf("envelope Payload = %+v, want %+v", env.Payload, payload)
	}
}

func TestPublisher_Publish_ValidationFailureWritesNothing(t *testing.T) {
	ctx := context.Background()

	addr, terminate, err := testsupport.StartRedis(ctx)
	if err != nil {
		t.Fatalf("StartRedis: %v", err)
	}
	t.Cleanup(func() {
		if err := terminate(ctx); err != nil {
			t.Errorf("terminate: %v", err)
		}
	})

	cfg := messaging.Config{
		Source: "test.service",
		Redis:  messaging.RedisConfig{Address: addr},
	}
	pub, err := messaging.NewPublisher(cfg)
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	t.Cleanup(func() {
		if err := pub.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})

	// "bad_event_type" has only one segment, so envelope validation must
	// reject it before anything is written to the stream.
	badDef := messaging.EventDef{Topic: "documents", Type: "bad_event_type", Version: "1.0.0"}

	if _, err := pub.Publish(ctx, badDef, documentCreatedPayload{}); err == nil {
		t.Fatal("expected an error for an invalid event_type, got nil")
	}

	entries, err := testsupport.ReadStreamEntries(ctx, addr, "foi:documents")
	if err != nil {
		t.Fatalf("ReadStreamEntries: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("got %d stream entries after a validation failure, want 0", len(entries))
	}
}
