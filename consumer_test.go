package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"

	internalredis "github.com/bcgov/foi-messaging-go/internal/redis"
)

func testConsumerConfig() Config {
	return Config{
		Source:   "test.service",
		Redis:    RedisConfig{Address: "localhost:6379"},
		Consumer: ConsumerConfig{Group: "test-group"},
	}
}

type noopHandler struct{}

func (noopHandler) Handle(context.Context, Envelope[testPayload]) error { return nil }

type rawNoopHandler struct{}

func (rawNoopHandler) Handle(context.Context, Envelope[json.RawMessage]) error { return nil }

func TestNewConsumer_RequiresGroup(t *testing.T) {
	cfg := Config{
		Source: "test.service",
		Redis:  RedisConfig{Address: "localhost:6379"},
	}

	if _, err := NewConsumer(cfg); err == nil {
		t.Fatal("expected an error when Consumer.Group is empty")
	}
}

func TestNewConsumer_RequiresSource(t *testing.T) {
	cfg := Config{
		Redis:    RedisConfig{Address: "localhost:6379"},
		Consumer: ConsumerConfig{Group: "test-group"},
	}

	if _, err := NewConsumer(cfg); err == nil {
		t.Fatal("expected an error when Source is empty")
	}
}

func TestRegisterHandler_RejectsInvalidEventDef(t *testing.T) {
	cases := []struct {
		name string
		def  EventDef
	}{
		{"missing topic", EventDef{Type: "document.created", Version: "1.0.0"}},
		{"missing type", EventDef{Topic: "documents", Version: "1.0.0"}},
		{"missing version", EventDef{Topic: "documents", Type: "document.created"}},
		{"malformed version", EventDef{Topic: "documents", Type: "document.created", Version: "v1"}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			consumer, err := NewConsumer(testConsumerConfig())
			if err != nil {
				t.Fatalf("NewConsumer: %v", err)
			}
			if err := RegisterHandler(consumer, c.def, noopHandler{}); err == nil {
				t.Errorf("expected an error for %s", c.name)
			}
		})
	}
}

func TestRegisterHandler_RejectsDuplicate(t *testing.T) {
	consumer, err := NewConsumer(testConsumerConfig())
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	def := EventDef{Topic: "documents", Type: "document.created", Version: "1.0.0"}

	if err := RegisterHandler(consumer, def, noopHandler{}); err != nil {
		t.Fatalf("first RegisterHandler: %v", err)
	}
	// A different patch version is still major 1, so it collides.
	def2 := EventDef{Topic: "documents", Type: "document.created", Version: "1.3.0"}
	if err := RegisterHandler(consumer, def2, noopHandler{}); err == nil {
		t.Error("expected an error registering a second handler for the same topic/type/major")
	}
}

func TestRegisterRawHandler_RejectsMissingTopic(t *testing.T) {
	consumer, err := NewConsumer(testConsumerConfig())
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	if err := RegisterRawHandler(consumer, TopicSelector{}, rawNoopHandler{}); err == nil {
		t.Error("expected an error for an empty TopicSelector.Topic")
	}
}

func TestRegisterHandler_RejectsRegistrationAfterRun(t *testing.T) {
	consumer, err := NewConsumer(testConsumerConfig())
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}

	consumer.markRunning()

	def := EventDef{Topic: "documents", Type: "document.created", Version: "1.0.0"}
	err = RegisterHandler(consumer, def, noopHandler{})
	if err == nil {
		t.Fatal("expected an error registering after Run has started")
	}
	if !strings.Contains(err.Error(), "Run") {
		t.Errorf("error = %q, want it to mention Run", err)
	}
}

// TestConsumer_CloseDoesNothingWhileRunning pins the guard that keeps a
// natural misreading of io.Closer from wedging the process. Closing the
// Redis client under a live read loop makes every ReadNew return
// "redis: client is closed", which is neither ctx.Done nor a shutdown
// signal: the loop backs off and retries it forever and Run never returns.
func TestConsumer_CloseDoesNothingWhileRunning(t *testing.T) {
	consumer, err := NewConsumer(testConsumerConfig())
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}

	// Stand in for what Run installs. Construction dials nothing.
	client := internalredis.NewClient(internalredis.ClientOptions{Address: "127.0.0.1:1"})
	reader := internalredis.NewStreamReader(client, "test-group", "test-consumer")
	consumer.mu.Lock()
	consumer.reader = reader
	consumer.mu.Unlock()
	consumer.markRunning()

	if err := consumer.Close(); err != nil {
		t.Fatalf("Close on a running consumer must return nil, got: %v", err)
	}

	consumer.mu.Lock()
	stillHeld := consumer.reader
	consumer.mu.Unlock()
	if stillHeld == nil {
		t.Error("Close cleared a running consumer's reader; only Run may release it")
	}

	// go-redis returns ErrClosed from a second Close on the same client, so
	// this succeeding is proof the first Close left the client alone.
	if err := reader.Close(); err != nil {
		t.Errorf("the Redis client was closed under the running consumer: %v", err)
	}
}

func TestRun_RejectsEmptyRegistry(t *testing.T) {
	consumer, err := NewConsumer(testConsumerConfig())
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}

	err = consumer.Run(context.Background())
	if err == nil {
		t.Fatal("expected an error running a consumer with no handlers")
	}
	if !strings.Contains(err.Error(), "no handlers") {
		t.Errorf("error = %q, want it to mention that no handlers are registered", err)
	}
}

func TestDispatch_InvokesTypedHandlerAndPropagatesCorrelationID(t *testing.T) {
	consumer, err := NewConsumer(testConsumerConfig())
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}

	type result struct {
		correlationFromCtx string
		payloadName        string
	}
	got := make(chan result, 1)

	def := EventDef{Topic: "documents", Type: "document.created", Version: "1.0.0"}
	if err := RegisterHandler(consumer, def, handlerFunc(func(ctx context.Context, env Envelope[testPayload]) error {
		id, _ := correlationIDFromContext(ctx)
		got <- result{correlationFromCtx: id, payloadName: env.Payload.Name}
		return nil
	})); err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}

	body := []byte(`{
		"event_id":"01234567-89ab-7def-8000-000000000000",
		"event_type":"document.created",
		"timestamp":"2026-04-23T10:00:00Z",
		"schema_version":"1.2.0",
		"correlation_id":"corr-42",
		"source":"other.service",
		"payload":{"name":"a.pdf"}
	}`)

	if err := consumer.dispatch(context.Background(), "documents", body, nil); err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	r := <-got
	if r.correlationFromCtx != "corr-42" {
		t.Errorf("correlation ID in context = %q, want %q", r.correlationFromCtx, "corr-42")
	}
	if r.payloadName != "a.pdf" {
		t.Errorf("payload name = %q, want %q", r.payloadName, "a.pdf")
	}
}

func TestDispatch_AcksUnmatchedEventType(t *testing.T) {
	consumer, err := NewConsumer(testConsumerConfig())
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}

	called := false
	def := EventDef{Topic: "documents", Type: "document.created", Version: "1.0.0"}
	if err := RegisterHandler(consumer, def, handlerFunc(func(context.Context, Envelope[testPayload]) error {
		called = true
		return nil
	})); err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}

	body := []byte(`{
		"event_id":"01234567-89ab-7def-8000-000000000000",
		"event_type":"document.deleted",
		"timestamp":"2026-04-23T10:00:00Z",
		"schema_version":"1.0.0",
		"correlation_id":"corr-1",
		"source":"other.service",
		"payload":{}
	}`)

	// An unmatched event is a normal outcome, not an error: returning nil
	// acks it.
	if err := consumer.dispatch(context.Background(), "documents", body, nil); err != nil {
		t.Errorf("dispatch of an unmatched event type must return nil, got: %v", err)
	}
	if called {
		t.Error("handler must not be invoked for an unmatched event type")
	}
}

func TestDispatch_AcksUnmatchedMajorVersion(t *testing.T) {
	consumer, err := NewConsumer(testConsumerConfig())
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}

	called := false
	def := EventDef{Topic: "documents", Type: "document.created", Version: "1.0.0"}
	if err := RegisterHandler(consumer, def, handlerFunc(func(context.Context, Envelope[testPayload]) error {
		called = true
		return nil
	})); err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}

	body := []byte(`{
		"event_id":"01234567-89ab-7def-8000-000000000000",
		"event_type":"document.created",
		"timestamp":"2026-04-23T10:00:00Z",
		"schema_version":"2.0.0",
		"correlation_id":"corr-1",
		"source":"other.service",
		"payload":{}
	}`)

	if err := consumer.dispatch(context.Background(), "documents", body, nil); err != nil {
		t.Errorf("dispatch across a major version boundary must return nil, got: %v", err)
	}
	if called {
		t.Error("a handler registered for major 1 must not receive a 2.0.0 event")
	}
}

func TestDispatch_ReturnsErrorOnUndecodableEnvelope(t *testing.T) {
	consumer, err := NewConsumer(testConsumerConfig())
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	def := EventDef{Topic: "documents", Type: "document.created", Version: "1.0.0"}
	if err := RegisterHandler(consumer, def, noopHandler{}); err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}

	// In Phase 2a an undecodable entry nacks rather than being dropped;
	// Phase 2b routes it to the DLQ instead.
	if err := consumer.dispatch(context.Background(), "documents", []byte(`not json`), nil); err == nil {
		t.Error("expected an error for an undecodable envelope")
	}
}

func TestDispatch_ReturnsErrorOnInvalidEnvelope(t *testing.T) {
	consumer, err := NewConsumer(testConsumerConfig())
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	def := EventDef{Topic: "documents", Type: "document.created", Version: "1.0.0"}
	if err := RegisterHandler(consumer, def, noopHandler{}); err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}

	// Valid JSON, but source and correlation_id are missing.
	body := []byte(`{
		"event_id":"01234567-89ab-7def-8000-000000000000",
		"event_type":"document.created",
		"timestamp":"2026-04-23T10:00:00Z",
		"schema_version":"1.0.0",
		"payload":{}
	}`)

	if err := consumer.dispatch(context.Background(), "documents", body, nil); err == nil {
		t.Error("expected an error for an envelope failing validation")
	}
}

func TestDispatch_ValidatesEnvelopeBeforeParsingMajorVersion(t *testing.T) {
	consumer, err := NewConsumer(testConsumerConfig())
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	def := EventDef{Topic: "documents", Type: "document.created", Version: "1.0.0"}
	if err := RegisterHandler(consumer, def, noopHandler{}); err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}

	// dispatch's ordering (json.Unmarshal -> validateEnvelope -> majorVersion
	// -> registry lookup) is load-bearing: validateEnvelope's
	// schemaVersionPattern (^\d+\.\d+\.\d+$) rejects a signed major before
	// majorVersion's strconv.Atoi ever sees it. If dispatch ever called
	// majorVersion first, "-1" would parse cleanly as major -1 and this
	// event would be routed (or silently acked) instead of nacked as an
	// invalid envelope.
	body := []byte(`{
		"event_id":"01234567-89ab-7def-8000-000000000000",
		"event_type":"document.created",
		"timestamp":"2026-04-23T10:00:00Z",
		"schema_version":"-1.0.0",
		"correlation_id":"corr-1",
		"source":"other.service",
		"payload":{}
	}`)

	if err := consumer.dispatch(context.Background(), "documents", body, nil); err == nil {
		t.Error("expected an error for a negative-major schema_version: validateEnvelope must reject it before majorVersion ever runs")
	}
}

func TestDispatch_PropagatesHandlerError(t *testing.T) {
	consumer, err := NewConsumer(testConsumerConfig())
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}

	def := EventDef{Topic: "documents", Type: "document.created", Version: "1.0.0"}
	if err := RegisterHandler(consumer, def, handlerFunc(func(context.Context, Envelope[testPayload]) error {
		return errHandlerFailed
	})); err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}

	body := []byte(`{
		"event_id":"01234567-89ab-7def-8000-000000000000",
		"event_type":"document.created",
		"timestamp":"2026-04-23T10:00:00Z",
		"schema_version":"1.0.0",
		"correlation_id":"corr-1",
		"source":"other.service",
		"payload":{}
	}`)

	// Phase 2a nacks on any handler error; Phase 2b classifies instead.
	if err := consumer.dispatch(context.Background(), "documents", body, nil); err == nil {
		t.Error("expected the handler error to propagate so the message nacks")
	}
}

func TestDispatch_RawHandlerReceivesEveryEventOnTopic(t *testing.T) {
	consumer, err := NewConsumer(testConsumerConfig())
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}

	seen := make(chan string, 2)
	if err := RegisterRawHandler(consumer, TopicSelector{Topic: "documents"},
		rawHandlerFunc(func(_ context.Context, env Envelope[json.RawMessage]) error {
			seen <- env.EventType
			return nil
		})); err != nil {
		t.Fatalf("RegisterRawHandler: %v", err)
	}

	for _, eventType := range []string{"document.created", "document.deleted"} {
		body := []byte(`{
			"event_id":"01234567-89ab-7def-8000-000000000000",
			"event_type":"` + eventType + `",
			"timestamp":"2026-04-23T10:00:00Z",
			"schema_version":"1.0.0",
			"correlation_id":"corr-1",
			"source":"other.service",
			"payload":{}
		}`)
		if err := consumer.dispatch(context.Background(), "documents", body, nil); err != nil {
			t.Fatalf("dispatch %s: %v", eventType, err)
		}
	}

	if got := <-seen; got != "document.created" {
		t.Errorf("first event = %q, want %q", got, "document.created")
	}
	if got := <-seen; got != "document.deleted" {
		t.Errorf("second event = %q, want %q", got, "document.deleted")
	}
}

var errHandlerFailed = errors.New("handler failed")

// handlerFunc adapts a function to Handler[testPayload].
type handlerFunc func(context.Context, Envelope[testPayload]) error

func (f handlerFunc) Handle(ctx context.Context, env Envelope[testPayload]) error {
	return f(ctx, env)
}

// rawHandlerFunc adapts a function to Handler[json.RawMessage].
type rawHandlerFunc func(context.Context, Envelope[json.RawMessage]) error

func (f rawHandlerFunc) Handle(ctx context.Context, env Envelope[json.RawMessage]) error {
	return f(ctx, env)
}

// recordingSink captures dead letters instead of publishing them, so
// dispatch's DLQ routing can be asserted without Redis.
type recordingSink struct {
	mu      sync.Mutex
	streams []string
	bodies  [][]byte
	err     error
}

func (s *recordingSink) publish(_ context.Context, stream string, body []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	s.streams = append(s.streams, stream)
	s.bodies = append(s.bodies, body)
	return nil
}

func (s *recordingSink) only(t *testing.T) DeadLetter {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.bodies) != 1 {
		t.Fatalf("expected exactly 1 dead letter, got %d", len(s.bodies))
	}
	var dl DeadLetter
	if err := json.Unmarshal(s.bodies[0], &dl); err != nil {
		t.Fatalf("unmarshalling dead letter: %v", err)
	}
	return dl
}

func TestDeadLetter_WritesToTheTopicsDLQStream(t *testing.T) {
	consumer, err := NewConsumer(testConsumerConfig())
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	sink := &recordingSink{}
	consumer.dlq = sink

	dl := consumer.newDeadLetter("documents", ReasonPermanent, errors.New("boom"), 3)
	dl.Event = json.RawMessage(`{"event_id":"abc"}`)
	if err := consumer.deadLetter(context.Background(), "documents", dl); err != nil {
		t.Fatalf("deadLetter: %v", err)
	}

	if got, want := sink.streams[0], "foi:documents.dlq"; got != want {
		t.Errorf("stream = %q, want %q", got, want)
	}
	got := sink.only(t)
	if got.Reason != ReasonPermanent {
		t.Errorf("Reason = %q, want %q", got.Reason, ReasonPermanent)
	}
	if got.Error != "boom" {
		t.Errorf("Error = %q, want %q", got.Error, "boom")
	}
	if got.DeliveryAttempts != 3 {
		t.Errorf("DeliveryAttempts = %d, want 3", got.DeliveryAttempts)
	}
	if got.OriginalTopic != "documents" {
		t.Errorf("OriginalTopic = %q, want %q", got.OriginalTopic, "documents")
	}
	if got.ConsumerGroup != "test-group" {
		t.Errorf("ConsumerGroup = %q, want %q", got.ConsumerGroup, "test-group")
	}
	if got.ConsumerName == "" {
		t.Error("ConsumerName must be set so an operator can identify the instance")
	}
	if got.DeadLetteredAt.IsZero() {
		t.Error("DeadLetteredAt must be set")
	}
}

func TestDeadLetter_PublishFailureReturnsAnError(t *testing.T) {
	// The caller nacks on a non-nil return. Acking here would drop the
	// event with nothing anywhere holding it (PRD §14).
	consumer, err := NewConsumer(testConsumerConfig())
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	consumer.dlq = &recordingSink{err: errors.New("redis down")}

	dl := consumer.newDeadLetter("documents", ReasonPermanent, errors.New("boom"), 1)
	if err := consumer.deadLetter(context.Background(), "documents", dl); err == nil {
		t.Error("expected a DLQ publish failure to be reported so the message nacks")
	}
}

func TestDeadLetter_WithoutASinkReturnsAnError(t *testing.T) {
	// Only reachable from a Consumer that was constructed but never run.
	// Erroring nacks, which is the safe direction.
	consumer, err := NewConsumer(testConsumerConfig())
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}

	dl := consumer.newDeadLetter("documents", ReasonPermanent, errors.New("boom"), 1)
	if err := consumer.deadLetter(context.Background(), "documents", dl); err == nil {
		t.Error("expected an error when no dead letter sink is configured")
	}
}

func (s *recordingSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.bodies)
}

// validEnvelopeJSON is an envelope that passes validation and routes to the
// documents/document.created/1.x.x handler.
func validEnvelopeJSON() []byte {
	return []byte(`{
		"event_id":"01234567-89ab-7def-8000-000000000000",
		"event_type":"document.created",
		"timestamp":"2026-04-23T10:00:00Z",
		"schema_version":"1.0.0",
		"correlation_id":"corr-1",
		"source":"other.service",
		"payload":{}
	}`)
}

func TestDeliveryAttempt_DefaultsToTheFirstDelivery(t *testing.T) {
	// A missing or malformed counter must not dead-letter an event. The cap
	// exists to bound redelivery, not to punish odd transport metadata.
	tests := []struct {
		name     string
		metadata map[string]string
		want     int64
	}{
		{"nil metadata", nil, 1},
		{"absent key", map[string]string{}, 1},
		{"unparseable", map[string]string{"_foi_delivery_attempt": "many"}, 1},
		{"zero", map[string]string{"_foi_delivery_attempt": "0"}, 1},
		{"negative", map[string]string{"_foi_delivery_attempt": "-4"}, 1},
		{"valid", map[string]string{"_foi_delivery_attempt": "4"}, 4},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := deliveryAttempt(tc.metadata); got != tc.want {
				t.Errorf("deliveryAttempt = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestDispatch_CapBoundary(t *testing.T) {
	// PRD §13: attempts 1..MaxDeliveryAttempts dispatch, and the next one
	// is dead-lettered. That is what makes the stated worst case of
	// MaxDeliveryAttempts × (1+MaxImmediateRetries) = 20 invocations right.
	tests := []struct {
		name        string
		attempt     string
		wantHandler bool
		wantDLQ     int
	}{
		{"at the cap still dispatches", "5", true, 0},
		{"over the cap dead-letters", "6", false, 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			consumer, err := NewConsumer(testConsumerConfig())
			if err != nil {
				t.Fatalf("NewConsumer: %v", err)
			}
			sink := &recordingSink{}
			consumer.dlq = sink

			var called bool
			def := EventDef{Topic: "documents", Type: "document.created", Version: "1.0.0"}
			h := handlerFunc(func(context.Context, Envelope[testPayload]) error {
				called = true
				return nil
			})
			if err := RegisterHandler(consumer, def, h); err != nil {
				t.Fatalf("RegisterHandler: %v", err)
			}

			metadata := map[string]string{"_foi_delivery_attempt": tc.attempt}
			if err := consumer.dispatch(context.Background(), "documents", validEnvelopeJSON(), metadata); err != nil {
				t.Fatalf("dispatch: %v", err)
			}

			if called != tc.wantHandler {
				t.Errorf("handler called = %v, want %v", called, tc.wantHandler)
			}
			if got := sink.count(); got != tc.wantDLQ {
				t.Errorf("dead letters = %d, want %d", got, tc.wantDLQ)
			}
		})
	}
}

func TestDispatch_CapDeadLettersBeforeDecoding(t *testing.T) {
	// The cap is checked before json.Unmarshal, so an over-cap event that
	// is ALSO undecodable still reports max_attempts_exceeded — and, more
	// importantly, never reaches the retry loop, where it would occupy its
	// topic's concurrency slot for four handler invocations.
	consumer, err := NewConsumer(testConsumerConfig())
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	sink := &recordingSink{}
	consumer.dlq = sink

	def := EventDef{Topic: "documents", Type: "document.created", Version: "1.0.0"}
	if err := RegisterHandler(consumer, def, noopHandler{}); err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}

	metadata := map[string]string{"_foi_delivery_attempt": "9"}
	if err := consumer.dispatch(context.Background(), "documents", []byte(`not json`), metadata); err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	got := sink.only(t)
	if got.Reason != ReasonMaxAttemptsExceeded {
		t.Errorf("Reason = %q, want %q — the cap must be checked before decoding", got.Reason, ReasonMaxAttemptsExceeded)
	}
	if got.DeliveryAttempts != 9 {
		t.Errorf("DeliveryAttempts = %d, want 9", got.DeliveryAttempts)
	}
}
