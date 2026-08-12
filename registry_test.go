package messaging

import (
	"context"
	"encoding/json"
	"testing"
)

type recordingHandler struct {
	called  int
	lastEnv Envelope[testPayload]
}

func (h *recordingHandler) Handle(_ context.Context, env Envelope[testPayload]) error {
	h.called++
	h.lastEnv = env
	return nil
}

func noopDispatch(context.Context, Envelope[json.RawMessage]) error { return nil }

func rawEnvelope(eventType, version string, payload string) Envelope[json.RawMessage] {
	return Envelope[json.RawMessage]{
		EventID:       "01234567-89ab-7def-8000-000000000000",
		EventType:     eventType,
		SchemaVersion: version,
		CorrelationID: "corr-1",
		Source:        "test.service",
		Payload:       json.RawMessage(payload),
	}
}

func TestMajorVersion(t *testing.T) {
	cases := []struct {
		in   string
		want int
	}{
		{"1.0.0", 1},
		{"1.4.2", 1},
		{"2.0.0", 2},
		{"10.1.3", 10},
	}
	for _, c := range cases {
		got, err := majorVersion(c.in)
		if err != nil {
			t.Errorf("majorVersion(%q): %v", c.in, err)
			continue
		}
		if got != c.want {
			t.Errorf("majorVersion(%q) = %d, want %d", c.in, got, c.want)
		}
	}

	if _, err := majorVersion("not-a-version"); err == nil {
		t.Error("expected an error for a malformed schema version")
	}
}

func TestRegistry_LookupMatchesOnMajorVersionOnly(t *testing.T) {
	r := newRegistry()
	h := &recordingHandler{}

	if err := r.addTyped("documents", "document.created", 1, typedDispatch[testPayload](h)); err != nil {
		t.Fatalf("addTyped: %v", err)
	}

	// A handler registered for 1.0.0 must receive every 1.x.y.
	for _, version := range []string{"1.0.0", "1.1.0", "1.4.2"} {
		major, err := majorVersion(version)
		if err != nil {
			t.Fatalf("majorVersion(%q): %v", version, err)
		}
		fn, match := r.lookup("documents", "document.created", major)
		if match == matchNone {
			t.Fatalf("no handler found for version %q", version)
		}
		if err := fn(context.Background(), rawEnvelope("document.created", version, `{"name":"a.pdf"}`)); err != nil {
			t.Fatalf("dispatch for %q: %v", version, err)
		}
	}

	if h.called != 3 {
		t.Errorf("handler called %d times, want 3", h.called)
	}
	if h.lastEnv.Payload.Name != "a.pdf" {
		t.Errorf("Payload.Name = %q, want %q", h.lastEnv.Payload.Name, "a.pdf")
	}
	if h.lastEnv.SchemaVersion != "1.4.2" {
		t.Errorf("SchemaVersion = %q, want %q — header fields must survive dispatch", h.lastEnv.SchemaVersion, "1.4.2")
	}
	if h.lastEnv.CorrelationID != "corr-1" {
		t.Errorf("CorrelationID = %q, want %q", h.lastEnv.CorrelationID, "corr-1")
	}
}

func TestRegistry_DifferentMajorsCoexist(t *testing.T) {
	r := newRegistry()
	v1 := &recordingHandler{}
	v2 := &recordingHandler{}

	if err := r.addTyped("documents", "document.created", 1, typedDispatch[testPayload](v1)); err != nil {
		t.Fatalf("addTyped v1: %v", err)
	}
	if err := r.addTyped("documents", "document.created", 2, typedDispatch[testPayload](v2)); err != nil {
		t.Fatalf("addTyped v2: %v", err)
	}

	fn, match := r.lookup("documents", "document.created", 2)
	if match == matchNone {
		t.Fatal("no handler found for major 2")
	}
	if err := fn(context.Background(), rawEnvelope("document.created", "2.0.0", `{"name":"b.pdf"}`)); err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	if v1.called != 0 {
		t.Errorf("v1 handler called %d times, want 0", v1.called)
	}
	if v2.called != 1 {
		t.Errorf("v2 handler called %d times, want 1", v2.called)
	}
}

func TestRegistry_LookupMissReturnsFalse(t *testing.T) {
	r := newRegistry()
	if err := r.addTyped("documents", "document.created", 1, typedDispatch[testPayload](&recordingHandler{})); err != nil {
		t.Fatalf("addTyped: %v", err)
	}

	if _, match := r.lookup("documents", "document.deleted", 1); match != matchNone {
		t.Error("expected no handler for an unregistered event type")
	}
	if _, match := r.lookup("documents", "document.created", 2); match != matchNone {
		t.Error("expected no handler for an unregistered major version")
	}
	if _, match := r.lookup("invoices", "document.created", 1); match != matchNone {
		t.Error("expected no handler on an unregistered topic")
	}
}

func TestRegistry_RejectsDuplicateRegistration(t *testing.T) {
	r := newRegistry()
	if err := r.addTyped("documents", "document.created", 1, typedDispatch[testPayload](&recordingHandler{})); err != nil {
		t.Fatalf("first addTyped: %v", err)
	}

	err := r.addTyped("documents", "document.created", 1, typedDispatch[testPayload](&recordingHandler{}))
	if err == nil {
		t.Fatal("expected an error registering a duplicate topic/type/major")
	}
}

func TestRegistry_RejectsTypedAndRawOnSameTopic(t *testing.T) {
	r := newRegistry()
	if err := r.addTyped("documents", "document.created", 1, typedDispatch[testPayload](&recordingHandler{})); err != nil {
		t.Fatalf("addTyped: %v", err)
	}
	if err := r.addRaw("documents", func(context.Context, Envelope[json.RawMessage]) error { return nil }); err == nil {
		t.Error("expected an error adding a raw handler to a topic that already has typed handlers")
	}

	r2 := newRegistry()
	if err := r2.addRaw("documents", func(context.Context, Envelope[json.RawMessage]) error { return nil }); err != nil {
		t.Fatalf("addRaw: %v", err)
	}
	if err := r2.addTyped("documents", "document.created", 1, typedDispatch[testPayload](&recordingHandler{})); err == nil {
		t.Error("expected an error adding a typed handler to a topic that already has a raw handler")
	}
	if err := r2.addRaw("documents", func(context.Context, Envelope[json.RawMessage]) error { return nil }); err == nil {
		t.Error("expected an error adding a second raw handler to the same topic")
	}
}

func TestRegistry_TopicListIsUnionOfTypedAndRaw(t *testing.T) {
	r := newRegistry()
	if err := r.addTyped("documents", "document.created", 1, typedDispatch[testPayload](&recordingHandler{})); err != nil {
		t.Fatalf("addTyped documents: %v", err)
	}
	if err := r.addTyped("documents", "document.deleted", 1, typedDispatch[testPayload](&recordingHandler{})); err != nil {
		t.Fatalf("addTyped documents 2: %v", err)
	}
	if err := r.addRaw("invoices", func(context.Context, Envelope[json.RawMessage]) error { return nil }); err != nil {
		t.Fatalf("addRaw invoices: %v", err)
	}

	topics := r.topicList()
	if len(topics) != 2 {
		t.Fatalf("topicList() = %v, want 2 distinct topics", topics)
	}
	seen := map[string]bool{}
	for _, topic := range topics {
		seen[topic] = true
	}
	if !seen["documents"] || !seen["invoices"] {
		t.Errorf("topicList() = %v, want documents and invoices", topics)
	}
}

func TestTypedDispatch_IgnoresUnknownPayloadFields(t *testing.T) {
	h := &recordingHandler{}
	fn := typedDispatch[testPayload](h)

	// PRD §11: consumers must deserialize leniently within a major version.
	err := fn(context.Background(), rawEnvelope("document.created", "1.2.0", `{"name":"a.pdf","added_later":true}`))
	if err != nil {
		t.Fatalf("dispatch must tolerate unknown fields, got: %v", err)
	}
	if h.lastEnv.Payload.Name != "a.pdf" {
		t.Errorf("Payload.Name = %q, want %q", h.lastEnv.Payload.Name, "a.pdf")
	}
}

func TestTypedDispatch_ReturnsErrorOnUndecodablePayload(t *testing.T) {
	fn := typedDispatch[testPayload](&recordingHandler{})

	err := fn(context.Background(), rawEnvelope("document.created", "1.0.0", `{"name":123}`))
	if err == nil {
		t.Fatal("expected an error decoding a payload with a mistyped field")
	}
}

func TestRegistry_IsEmpty(t *testing.T) {
	r := newRegistry()
	if !r.isEmpty() {
		t.Error("a freshly constructed registry should be empty")
	}

	if err := r.addTyped("documents", "document.created", 1, typedDispatch[testPayload](&recordingHandler{})); err != nil {
		t.Fatalf("addTyped: %v", err)
	}
	if r.isEmpty() {
		t.Error("a registry with a registered handler should not be empty")
	}
}

func TestRegistry_LookupReportsHowItMatched(t *testing.T) {
	// The distinction is telemetry's: a typed match means event_type came
	// from a set fixed at registration time and is safe as a metric
	// attribute; a raw match means it is whatever the wire said.
	t.Run("typed", func(t *testing.T) {
		r := newRegistry()
		if err := r.addTyped("documents", "document.created", 1, noopDispatch); err != nil {
			t.Fatalf("addTyped() = %v, want nil", err)
		}

		if _, match := r.lookup("documents", "document.created", 1); match != matchTyped {
			t.Fatalf("lookup() match = %v, want matchTyped", match)
		}
	})

	t.Run("raw takes any event type", func(t *testing.T) {
		r := newRegistry()
		if err := r.addRaw("documents", noopDispatch); err != nil {
			t.Fatalf("addRaw() = %v, want nil", err)
		}

		_, match := r.lookup("documents", "anything.at.all", 7)
		if match != matchRaw {
			t.Fatalf("lookup() match = %v, want matchRaw", match)
		}
	})

	t.Run("no match", func(t *testing.T) {
		r := newRegistry()
		if _, match := r.lookup("documents", "document.created", 1); match != matchNone {
			t.Fatalf("lookup() match = %v, want matchNone", match)
		}
	})
}
