package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/bcgov/foi-messaging-go/internal/testseam"
	internalwatermill "github.com/bcgov/foi-messaging-go/internal/watermill"
)

func TestSeamHooksRegistered(t *testing.T) {
	// A nil hook is unreachable today: testing/ imports this package, so
	// this init always runs first. Asserted anyway so a refactor that
	// drops a registration fails loudly here, rather than nil-panicking
	// inside some application's test suite.
	if testseam.WithCorrelationID == nil {
		t.Error("testseam.WithCorrelationID is nil")
	}
	if testseam.NewRecordingPublisher == nil {
		t.Error("testseam.NewRecordingPublisher is nil")
	}
	if testseam.Dispatch == nil {
		t.Error("testseam.Dispatch is nil")
	}
}

// seamProbe returns a probe whose Sink collects dead letters, plus the
// slice it collects into.
func seamProbe(sinkErr error) (*testseam.Probe, *[]DeadLetter) {
	var letters []DeadLetter
	p := &testseam.Probe{NoBackoff: true}
	p.Sink = func(_ context.Context, _ string, body []byte) error {
		if sinkErr != nil {
			return sinkErr
		}
		var dl DeadLetter
		if err := json.Unmarshal(body, &dl); err != nil {
			return err
		}
		letters = append(letters, dl)
		return nil
	}
	return p, &letters
}

func seamEnvelopeBody(t *testing.T) []byte {
	t.Helper()

	env := Envelope[json.RawMessage]{
		EventID:       "01900000-0000-7000-8000-000000000001",
		EventType:     "test.event",
		Timestamp:     time.Now().UTC(),
		SchemaVersion: "1.0.0",
		CorrelationID: "01900000-0000-7000-8000-000000000002",
		Source:        "test.service",
		Payload:       json.RawMessage(`{"value":"v"}`),
	}
	body, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshalling envelope: %v", err)
	}
	return body
}

type seamHandler struct{ err error }

func (h seamHandler) Handle(context.Context, Envelope[testPayload]) error { return h.err }

func TestSeamDispatch_Processed(t *testing.T) {
	c, err := NewConsumer(testConsumerConfig())
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	def := EventDef{Topic: "things", Type: "test.event", Version: "1.0.0"}
	if err := RegisterHandler(c, def, seamHandler{}); err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}

	p, letters := seamProbe(nil)
	derr := testseam.Dispatch(context.Background(), c, "things",
		seamEnvelopeBody(t), nil, p)

	if derr != nil {
		t.Fatalf("dispatch: %v", derr)
	}
	if p.Kind != testseam.KindProcessed {
		t.Fatalf("got kind %q, want %q", p.Kind, testseam.KindProcessed)
	}
	if len(*letters) != 0 {
		t.Fatalf("got %d dead letters, want 0", len(*letters))
	}
}

func TestSeamDispatch_NoHandlerSkips(t *testing.T) {
	c, err := NewConsumer(testConsumerConfig())
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	def := EventDef{Topic: "things", Type: "other.event", Version: "1.0.0"}
	if err := RegisterHandler(c, def, seamHandler{}); err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}

	p, _ := seamProbe(nil)
	if err := testseam.Dispatch(context.Background(), c, "things",
		seamEnvelopeBody(t), nil, p); err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	if p.Kind != testseam.KindSkipped {
		t.Fatalf("got kind %q, want %q", p.Kind, testseam.KindSkipped)
	}
	if p.Reason != reasonNoHandler {
		t.Fatalf("got reason %q, want %q", p.Reason, reasonNoHandler)
	}
}

func TestSeamDispatch_PermanentDeadLetters(t *testing.T) {
	c, err := NewConsumer(testConsumerConfig())
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	def := EventDef{Topic: "things", Type: "test.event", Version: "1.0.0"}
	want := errors.New("boom")
	if err := RegisterHandler(c, def, seamHandler{err: AsPermanent(want)}); err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}

	p, letters := seamProbe(nil)
	// nil, not an error: a dead-lettered event is acked.
	if err := testseam.Dispatch(context.Background(), c, "things",
		seamEnvelopeBody(t), nil, p); err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	if p.Kind != testseam.KindFailed {
		t.Fatalf("got kind %q, want %q", p.Kind, testseam.KindFailed)
	}
	if p.Category != categoryPermanent {
		t.Fatalf("got category %q, want %q", p.Category, categoryPermanent)
	}
	if len(*letters) != 1 {
		t.Fatalf("got %d dead letters, want 1", len(*letters))
	}
	if (*letters)[0].Reason != ReasonPermanent {
		t.Fatalf("got reason %q, want %q", (*letters)[0].Reason, ReasonPermanent)
	}
}

// The cap fires before decode, so this also proves the probe is reachable
// on dispatch's earliest exit path.
func TestSeamDispatch_CapDeadLetters(t *testing.T) {
	c, err := NewConsumer(testConsumerConfig())
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	def := EventDef{Topic: "things", Type: "test.event", Version: "1.0.0"}
	if err := RegisterHandler(c, def, seamHandler{}); err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}

	p, letters := seamProbe(nil)
	metadata := map[string]string{internalwatermill.MetadataDeliveryAttempt: "99"}
	if err := testseam.Dispatch(context.Background(), c, "things",
		seamEnvelopeBody(t), metadata, p); err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	if p.Category != categoryMaxAttempts {
		t.Fatalf("got category %q, want %q", p.Category, categoryMaxAttempts)
	}
	if len(*letters) != 1 || (*letters)[0].Reason != ReasonMaxAttemptsExceeded {
		t.Fatalf("got %+v, want one max_attempts_exceeded dead letter", *letters)
	}
}

// A DLQ write that fails must nack, not ack the event into nothing.
func TestSeamDispatch_SinkFailureNacks(t *testing.T) {
	c, err := NewConsumer(testConsumerConfig())
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	def := EventDef{Topic: "things", Type: "test.event", Version: "1.0.0"}
	if err := RegisterHandler(c, def, seamHandler{err: AsPermanent(errors.New("boom"))}); err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}

	p, _ := seamProbe(errors.New("dlq unavailable"))
	if err := testseam.Dispatch(context.Background(), c, "things",
		seamEnvelopeBody(t), nil, p); err == nil {
		t.Fatal("expected an error so the entry stays pending")
	}
}

// NoBackoff must not change how many times the handler runs.
func TestSeamDispatch_NoBackoffKeepsRetryCount(t *testing.T) {
	cfg := testConsumerConfig()
	cfg.Retry.MaxImmediateRetries = 2

	c, err := NewConsumer(cfg)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}

	var calls int
	def := EventDef{Topic: "things", Type: "test.event", Version: "1.0.0"}
	h := countingHandler{count: &calls, err: errors.New("transient")}
	if err := RegisterHandler(c, def, h); err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}

	p, _ := seamProbe(nil)
	start := time.Now()
	if err := testseam.Dispatch(context.Background(), c, "things",
		seamEnvelopeBody(t), nil, p); err == nil {
		t.Fatal("expected a nack after retries were exhausted")
	}

	if calls != 3 {
		t.Fatalf("got %d handler calls, want 3 (1 + 2 retries)", calls)
	}
	// The library default backoff would spend hundreds of milliseconds
	// here; NoBackoff is what keeps application test suites fast.
	if elapsed := time.Since(start); elapsed > 50*time.Millisecond {
		t.Fatalf("took %v; NoBackoff did not collapse the sleeps", elapsed)
	}
}

type countingHandler struct {
	count *int
	err   error
}

func (h countingHandler) Handle(context.Context, Envelope[testPayload]) error {
	*h.count++
	return h.err
}

func TestSeamNewRecordingPublisher_RecordsWithoutRedis(t *testing.T) {
	var got []byte
	v, err := testseam.NewRecordingPublisher("test.service", "foi",
		func(_ context.Context, _, _ string, body []byte, _ map[string]string) error {
			got = body
			return nil
		})
	if err != nil {
		t.Fatalf("NewRecordingPublisher: %v", err)
	}

	p, ok := v.(*Publisher)
	if !ok {
		t.Fatalf("got %T, want *messaging.Publisher", v)
	}
	def := EventDef{Topic: "things", Type: "test.event", Version: "1.0.0"}
	if _, err := p.Publish(context.Background(), def, testPayload{Name: "v"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	var env Envelope[json.RawMessage]
	if err := json.Unmarshal(got, &env); err != nil {
		t.Fatalf("decoding recorded body: %v", err)
	}
	if env.EventType != "test.event" {
		t.Fatalf("got event type %q, want test.event", env.EventType)
	}
	// Proof the real publish path ran: these are filled in by
	// newEnvelope and resolveCorrelationID, not by the recorder.
	if env.EventID == "" || env.CorrelationID == "" || env.Timestamp.IsZero() {
		t.Fatalf("recorded envelope was not built by the real path: %+v", env)
	}
}

func TestSeamWithCorrelationID(t *testing.T) {
	ctx := testseam.WithCorrelationID(context.Background(), "cid-1")

	got, ok := correlationIDFromContext(ctx)
	if !ok || got != "cid-1" {
		t.Fatalf("got (%q, %v), want (\"cid-1\", true)", got, ok)
	}
}
