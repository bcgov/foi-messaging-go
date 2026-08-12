package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	internalredis "github.com/bcgov/foi-messaging-go/internal/redis"
	internalwatermill "github.com/bcgov/foi-messaging-go/internal/watermill"
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

func TestDispatch_DeadLettersUndecodableEnvelope(t *testing.T) {
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

	// Acked, not nacked: malformed JSON does not become valid on
	// redelivery, so burning the cap on it only delays the same verdict.
	if err := consumer.dispatch(context.Background(), "documents", []byte(`not json`), nil); err != nil {
		t.Fatalf("dispatch must ack after dead-lettering, got %v", err)
	}

	got := sink.only(t)
	if got.Reason != ReasonDeserializationFailed {
		t.Errorf("Reason = %q, want %q", got.Reason, ReasonDeserializationFailed)
	}
	if string(got.EventRaw) != "not json" {
		t.Errorf("EventRaw = %q, want the original bytes preserved", got.EventRaw)
	}
	if got.Event != nil {
		t.Error("unparseable bytes must not be spliced into event")
	}
}

func TestDispatch_DeadLettersInvalidEnvelope(t *testing.T) {
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

	// Valid JSON, but source and correlation_id are missing.
	body := []byte(`{
		"event_id":"01234567-89ab-7def-8000-000000000000",
		"event_type":"document.created",
		"timestamp":"2026-04-23T10:00:00Z",
		"schema_version":"1.0.0",
		"payload":{}
	}`)

	if err := consumer.dispatch(context.Background(), "documents", body, nil); err != nil {
		t.Fatalf("dispatch must ack after dead-lettering, got %v", err)
	}

	got := sink.only(t)
	if got.Reason != ReasonDeserializationFailed {
		t.Errorf("Reason = %q, want %q", got.Reason, ReasonDeserializationFailed)
	}
	// PRD §14 puts an envelope failing validation in event_raw too: it did
	// not deserialize into a usable event, whatever its syntax.
	if got.Event != nil {
		t.Error("an envelope failing validation belongs in event_raw, not event")
	}
	if len(got.EventRaw) == 0 {
		t.Error("the original bytes must be preserved")
	}
}

func TestDispatch_ValidatesEnvelopeBeforeParsingMajorVersion(t *testing.T) {
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

	// dispatch's ordering (json.Unmarshal -> validateEnvelope -> majorVersion
	// -> registry lookup) is load-bearing: validateEnvelope's
	// schemaVersionPattern (^\d+\.\d+\.\d+$) rejects a signed major before
	// majorVersion's strconv.Atoi ever sees it. If dispatch ever called
	// majorVersion first, "-1" would parse cleanly as major -1 and this
	// event would be routed (or silently acked) instead of dead-lettered.
	body := []byte(`{
		"event_id":"01234567-89ab-7def-8000-000000000000",
		"event_type":"document.created",
		"timestamp":"2026-04-23T10:00:00Z",
		"schema_version":"-1.0.0",
		"correlation_id":"corr-1",
		"source":"other.service",
		"payload":{}
	}`)

	if err := consumer.dispatch(context.Background(), "documents", body, nil); err != nil {
		t.Fatalf("dispatch must ack after dead-lettering, got %v", err)
	}

	got := sink.only(t)
	// The error text is how the ordering is observable now that both paths
	// end in the same reason: validateEnvelope names schema_version,
	// majorVersion's failure would name parsing instead.
	if !strings.Contains(got.Error, "validating envelope") {
		t.Errorf("Error = %q, want a validateEnvelope failure: it must reject the signed major before majorVersion runs", got.Error)
	}
}

// fastRetryConfig keeps the retry loop's structure but removes the waiting.
// Zero means "use the default" for RetryConfig, so retries cannot be
// switched off; they can only be made instant.
func fastRetryConfig(retries int) Config {
	cfg := testConsumerConfig()
	cfg.Retry = RetryConfig{
		MaxImmediateRetries: retries,
		InitialBackoff:      time.Nanosecond,
		MaxBackoff:          time.Nanosecond,
	}
	return cfg
}

func TestDispatch_RetriesThenNacksARetryableError(t *testing.T) {
	consumer, err := NewConsumer(fastRetryConfig(3))
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	sink := &recordingSink{}
	consumer.dlq = sink

	var calls int
	def := EventDef{Topic: "documents", Type: "document.created", Version: "1.0.0"}
	h := handlerFunc(func(context.Context, Envelope[testPayload]) error {
		calls++
		return errBoom
	})
	if err := RegisterHandler(consumer, def, h); err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}

	// Exhausting immediate retries nacks: the entry stays pending and the
	// reclaim loop redelivers it with its counter advanced toward the cap.
	if err := consumer.dispatch(context.Background(), "documents", validEnvelopeJSON(), nil); err == nil {
		t.Error("expected exhausted retries to nack")
	}
	if calls != 4 {
		t.Errorf("handler called %d times, want 4 (1 attempt + 3 retries)", calls)
	}
	if sink.count() != 0 {
		t.Error("exhausted retries must nack, not dead-letter: the cap decides that, not the retry loop")
	}
}

func TestDispatch_StopsRetryingOnceTheHandlerSucceeds(t *testing.T) {
	consumer, err := NewConsumer(fastRetryConfig(3))
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	consumer.dlq = &recordingSink{}

	var calls int
	def := EventDef{Topic: "documents", Type: "document.created", Version: "1.0.0"}
	h := handlerFunc(func(context.Context, Envelope[testPayload]) error {
		calls++
		if calls < 3 {
			return errBoom
		}
		return nil
	})
	if err := RegisterHandler(consumer, def, h); err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}

	if err := consumer.dispatch(context.Background(), "documents", validEnvelopeJSON(), nil); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if calls != 3 {
		t.Errorf("handler called %d times, want 3", calls)
	}
}

func TestDispatch_PermanentErrorDeadLettersWithoutRetrying(t *testing.T) {
	consumer, err := NewConsumer(fastRetryConfig(3))
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	sink := &recordingSink{}
	consumer.dlq = sink

	var calls int
	def := EventDef{Topic: "documents", Type: "document.created", Version: "1.0.0"}
	h := handlerFunc(func(context.Context, Envelope[testPayload]) error {
		calls++
		return AsPermanent(errBoom)
	})
	if err := RegisterHandler(consumer, def, h); err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}

	if err := consumer.dispatch(context.Background(), "documents", validEnvelopeJSON(), nil); err != nil {
		t.Fatalf("a dead-lettered event must ack, got %v", err)
	}
	if calls != 1 {
		t.Errorf("handler called %d times, want 1: a permanent error must not be retried", calls)
	}
	got := sink.only(t)
	if got.Reason != ReasonPermanent {
		t.Errorf("Reason = %q, want %q", got.Reason, ReasonPermanent)
	}
	if string(got.Event) == "" {
		t.Error("a decodable event belongs in event, so replay tooling can republish it")
	}
}

func TestDispatch_DiscardErrorAcksWithoutDLQOrRetry(t *testing.T) {
	consumer, err := NewConsumer(fastRetryConfig(3))
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	sink := &recordingSink{}
	consumer.dlq = sink

	var calls int
	def := EventDef{Topic: "documents", Type: "document.created", Version: "1.0.0"}
	h := handlerFunc(func(context.Context, Envelope[testPayload]) error {
		calls++
		return AsDiscard(errBoom)
	})
	if err := RegisterHandler(consumer, def, h); err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}

	if err := consumer.dispatch(context.Background(), "documents", validEnvelopeJSON(), nil); err != nil {
		t.Fatalf("a discarded event must ack, got %v", err)
	}
	if calls != 1 {
		t.Errorf("handler called %d times, want 1", calls)
	}
	if sink.count() != 0 {
		t.Error("a discarded event must produce no dead letter")
	}
}

func TestDispatch_ReclassifiesOnEveryAttempt(t *testing.T) {
	// A handler may fail transiently and then discover the failure is
	// permanent. Deciding classification once, on the first error, would
	// keep retrying an error the handler has already given up on.
	consumer, err := NewConsumer(fastRetryConfig(3))
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	sink := &recordingSink{}
	consumer.dlq = sink

	var calls int
	def := EventDef{Topic: "documents", Type: "document.created", Version: "1.0.0"}
	h := handlerFunc(func(context.Context, Envelope[testPayload]) error {
		calls++
		if calls == 1 {
			return errBoom // unclassified: retryable
		}
		return AsPermanent(errBoom)
	})
	if err := RegisterHandler(consumer, def, h); err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}

	if err := consumer.dispatch(context.Background(), "documents", validEnvelopeJSON(), nil); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if calls != 2 {
		t.Errorf("handler called %d times, want 2: the second attempt's permanent verdict must stop the loop", calls)
	}
	if got := sink.only(t); got.Reason != ReasonPermanent {
		t.Errorf("Reason = %q, want %q", got.Reason, ReasonPermanent)
	}
}

func TestDispatch_CancelledContextAbandonsRetry(t *testing.T) {
	// ctx here is the message context, which stays live through the whole
	// drain by design. Its cancellation means the subscriber closed, so
	// abandoning the retry to a nack is right — the entry is still pending.
	cfg := testConsumerConfig()
	cfg.Retry = RetryConfig{
		MaxImmediateRetries: 3,
		InitialBackoff:      10 * time.Second,
		MaxBackoff:          10 * time.Second,
	}
	consumer, err := NewConsumer(cfg)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	consumer.dlq = &recordingSink{}

	ctx, cancel := context.WithCancel(context.Background())
	var calls int
	def := EventDef{Topic: "documents", Type: "document.created", Version: "1.0.0"}
	h := handlerFunc(func(context.Context, Envelope[testPayload]) error {
		calls++
		cancel()
		return errBoom
	})
	if err := RegisterHandler(consumer, def, h); err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}

	start := time.Now()
	if err := consumer.dispatch(ctx, "documents", validEnvelopeJSON(), nil); err == nil {
		t.Error("expected an abandoned retry to nack")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("dispatch took %v: a cancelled context must abort the backoff, not sleep through it", elapsed)
	}
	if calls != 1 {
		t.Errorf("handler called %d times, want 1", calls)
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

var errBoom = errors.New("boom")

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

// newTestConsumerWithTelemetry builds a Consumer wired to mp, reusing the
// existing testConsumerConfig() so these tests stay in step with the rest
// of the suite's defaults. dispatch is reachable without Run, which is
// what makes the failure paths testable at all.
func newTestConsumerWithTelemetry(t *testing.T, mp metric.MeterProvider) *Consumer {
	t.Helper()

	cfg := testConsumerConfig()
	cfg.Telemetry.MeterProvider = mp

	c, err := NewConsumer(cfg)
	if err != nil {
		t.Fatalf("NewConsumer() = %v, want nil", err)
	}
	return c
}

func TestDispatch_TerminalInvariantAcrossExitPaths(t *testing.T) {
	// Spec §2's invariant, asserted against the real dispatch rather than
	// the recorder in isolation: every exit path must record exactly one
	// terminal counter and exactly one duration observation.
	terminal := []string{
		"messaging.events.processed",
		"messaging.events.failed",
		"messaging.events.skipped",
	}

	envelopeWith := func(mut func(*Envelope[json.RawMessage])) []byte {
		env := Envelope[json.RawMessage]{
			EventID:       "018f2e7a-1c6b-7c0a-9f8d-3e4a2b1c5d90",
			EventType:     "document.created",
			Timestamp:     time.Now().UTC(),
			SchemaVersion: "1.0.0",
			CorrelationID: "corr-1",
			Source:        "test",
			Payload:       json.RawMessage(`{}`),
		}
		mut(&env)
		b, err := json.Marshal(env)
		if err != nil {
			t.Fatalf("marshalling test envelope: %v", err)
		}
		return b
	}

	validEnvelope := func(eventType string) []byte {
		return envelopeWith(func(e *Envelope[json.RawMessage]) { e.EventType = eventType })
	}

	tests := []struct {
		name     string
		payload  []byte
		metadata map[string]string
		handler  func(context.Context, Envelope[json.RawMessage]) error
		register bool
		want     string

		// wantLabel and wantLabelValue pin the series, not merely the
		// counter. error_category and reason are the dimensions
		// operators cut alerts on, so a terminal counter firing with
		// the wrong label value is a silent, high-consequence defect —
		// and asserting only on the counter name cannot see it.
		wantLabel      string
		wantLabelValue string

		// wantEventType is the value the terminal series must carry on
		// event_type, or "" when it must carry none. Both halves of the
		// cardinality rule are asserted here: a typed match attaches it
		// (dashboards are cut by it), and every other path must not
		// (the wire value is attacker-influenced and unbounded).
		wantEventType string
	}{
		{
			name:     "processed",
			payload:  validEnvelope("document.created"),
			handler:  func(context.Context, Envelope[json.RawMessage]) error { return nil },
			register: true,
			want:     "messaging.events.processed",
			// The positive half of the cardinality rule. Without this
			// the setEventType call in dispatch can be deleted outright
			// and nothing fails, while every consume dashboard silently
			// loses its event_type dimension.
			wantEventType: "document.created",
		},
		{
			name:           "no handler",
			payload:        validEnvelope("document.created"),
			register:       false,
			want:           "messaging.events.skipped",
			wantLabel:      attrReason,
			wantLabelValue: reasonNoHandler,
		},
		{
			name:           "discard",
			payload:        validEnvelope("document.created"),
			handler:        func(context.Context, Envelope[json.RawMessage]) error { return AsDiscard(errors.New("nope")) },
			register:       true,
			want:           "messaging.events.skipped",
			wantLabel:      attrReason,
			wantLabelValue: reasonDiscard,
			// A skip is not attributed by event type even on a typed
			// match: deliveryRecorder.end drops it from the skipped
			// series deliberately.
			wantEventType: "",
		},
		{
			name:           "permanent",
			payload:        validEnvelope("document.created"),
			handler:        func(context.Context, Envelope[json.RawMessage]) error { return AsPermanent(errors.New("bad")) },
			register:       true,
			want:           "messaging.events.failed",
			wantLabel:      attrErrorCategory,
			wantLabelValue: categoryPermanent,
			wantEventType:  "document.created",
		},
		{
			name:           "undecodable",
			payload:        []byte("{not json"),
			want:           "messaging.events.failed",
			wantLabel:      attrErrorCategory,
			wantLabelValue: categoryDeserialization,
		},
		{
			// event_type with a single segment fails validateEnvelope's
			// eventTypePattern, which is the second of the three
			// deserialization exits and had no telemetry test at all.
			name:           "invalid envelope",
			payload:        envelopeWith(func(e *Envelope[json.RawMessage]) { e.EventType = "invalid" }),
			want:           "messaging.events.failed",
			wantLabel:      attrErrorCategory,
			wantLabelValue: categoryDeserialization,
		},
		{
			// The third deserialization exit, and the only way to reach
			// it: validateEnvelope's schemaVersionPattern (^\d+\.\d+\.\d+$)
			// rejects anything non-numeric before majorVersion is
			// called, so "abc" would fail one check earlier. A major
			// that is all digits but overflows int is the case that
			// passes the pattern and still fails strconv.Atoi.
			name:           "unparseable schema version",
			payload:        envelopeWith(func(e *Envelope[json.RawMessage]) { e.SchemaVersion = "99999999999999999999.0.0" }),
			want:           "messaging.events.failed",
			wantLabel:      attrErrorCategory,
			wantLabelValue: categoryDeserialization,
		},
		{
			name:           "cap exceeded",
			payload:        validEnvelope("document.created"),
			metadata:       map[string]string{internalwatermill.MetadataDeliveryAttempt: "99"},
			want:           "messaging.events.failed",
			wantLabel:      attrErrorCategory,
			wantLabelValue: categoryMaxAttempts,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mp, reader := newTestMeterProvider(t)
			c := newTestConsumerWithTelemetry(t, mp)
			c.dlq = &recordingSink{}

			if tt.register {
				h := tt.handler
				if err := c.registry.addTyped("documents", "document.created", 1,
					func(ctx context.Context, env Envelope[json.RawMessage]) error { return h(ctx, env) }); err != nil {
					t.Fatalf("addTyped() = %v, want nil", err)
				}
			}

			_ = c.dispatch(context.Background(), "documents", tt.payload, tt.metadata)

			got := readMetrics(t, reader)
			for _, name := range terminal {
				_, present := got[name]
				if name == tt.want && !present {
					t.Errorf("terminal counter %q was not recorded", name)
				}
				if name != tt.want && present {
					t.Errorf("terminal counter %q was recorded; want only %q", name, tt.want)
				}
			}
			if _, ok := got["messaging.processing.duration"]; !ok {
				t.Error("processing.duration was not recorded")
			}
			if _, ok := got["messaging.events.received"]; !ok {
				t.Error("messaging.events.received was not recorded")
			}

			attrs := terminalCounterAttrs(t, got[tt.want])

			if tt.wantLabel != "" {
				v, found := attrs.Value(attribute.Key(tt.wantLabel))
				if !found {
					t.Errorf("%s carried no %s attribute; want %q",
						tt.want, tt.wantLabel, tt.wantLabelValue)
				} else if v.AsString() != tt.wantLabelValue {
					t.Errorf("%s = %q, want %q", tt.wantLabel, v.AsString(), tt.wantLabelValue)
				}
			}

			v, found := attrs.Value(attribute.Key(attrEventType))
			switch {
			case tt.wantEventType == "" && found:
				t.Errorf("%s carried event_type=%q; the wire value is unbounded and must not be a metric attribute here",
					tt.want, v.AsString())
			case tt.wantEventType != "" && !found:
				t.Errorf("%s carried no event_type attribute; want %q", tt.want, tt.wantEventType)
			case tt.wantEventType != "" && v.AsString() != tt.wantEventType:
				t.Errorf("event_type = %q, want %q", v.AsString(), tt.wantEventType)
			}
		})
	}
}

func TestDispatch_EventTypeAttributeIsBounded(t *testing.T) {
	// A raw handler takes every event on its topic, so the wire event_type
	// must not become a metric attribute — that is the unbounded case the
	// rule exists for. The span carries it regardless.
	mp, reader := newTestMeterProvider(t)
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))

	c := newTestConsumerWithTelemetry(t, mp)
	c.tracer = tp.Tracer(telemetryScope)
	c.dlq = &recordingSink{}

	if err := c.registry.addRaw("documents", func(context.Context, Envelope[json.RawMessage]) error { return nil }); err != nil {
		t.Fatalf("addRaw() = %v, want nil", err)
	}

	env := Envelope[json.RawMessage]{
		EventID: "018f2e7a-1c6b-7c0a-9f8d-3e4a2b1c5d90", EventType: "attacker.controlled.value",
		Timestamp: time.Now().UTC(), SchemaVersion: "1.0.0", CorrelationID: "c", Source: "s",
		Payload: json.RawMessage(`{}`),
	}
	body, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshalling envelope: %v", err)
	}

	if err := c.dispatch(context.Background(), "documents", body, nil); err != nil {
		t.Fatalf("dispatch() = %v, want nil", err)
	}

	m := readMetrics(t, reader)["messaging.events.processed"]
	sum, ok := m.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("processed data = %T, want Sum[int64]", m.Data)
	}
	if _, found := sum.DataPoints[0].Attributes.Value(attribute.Key(attrEventType)); found {
		t.Error("event_type was attached as a metric attribute on a raw-handler match")
	}

	spans := recorder.Ended()
	if len(spans) != 1 {
		t.Fatalf("recorded %d spans, want 1", len(spans))
	}
	var sawEventType bool
	for _, a := range spans[0].Attributes() {
		if a.Key == "messaging.foi.event_type" && a.Value.AsString() == "attacker.controlled.value" {
			sawEventType = true
		}
	}
	if !sawEventType {
		t.Error("span did not carry the wire event_type")
	}
}

func TestRunWithRetry_RecordsRetriesAsCounterAndSpanEvents(t *testing.T) {
	// Spec §3: one span per delivery, with each immediate retry recorded as
	// a span event rather than a child span. This asserts both halves — the
	// counter increments once per retry, and the retries are visible as
	// events on the single delivery span, not as separate spans.
	mp, reader := newTestMeterProvider(t)
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))

	c := newTestConsumerWithTelemetry(t, mp)
	c.tracer = tp.Tracer(telemetryScope)
	c.dlq = &recordingSink{}
	// Keep the backoff out of the test's runtime.
	c.cfg.Retry.InitialBackoff = time.Nanosecond
	c.cfg.Retry.MaxBackoff = time.Nanosecond

	var calls int
	if err := c.registry.addTyped("documents", "document.created", 1,
		func(context.Context, Envelope[json.RawMessage]) error {
			calls++
			if calls < 3 {
				return errors.New("transient")
			}
			return nil
		}); err != nil {
		t.Fatalf("addTyped() = %v, want nil", err)
	}

	env := Envelope[json.RawMessage]{
		EventID: "018f2e7a-1c6b-7c0a-9f8d-3e4a2b1c5d90", EventType: "document.created",
		Timestamp: time.Now().UTC(), SchemaVersion: "1.0.0", CorrelationID: "c", Source: "s",
		Payload: json.RawMessage(`{}`),
	}
	body, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshalling envelope: %v", err)
	}

	if err := c.dispatch(context.Background(), "documents", body, nil); err != nil {
		t.Fatalf("dispatch() = %v, want nil", err)
	}

	m, ok := readMetrics(t, reader)["messaging.retries"]
	if !ok {
		t.Fatal("messaging.retries was not recorded")
	}
	sum, ok := m.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("retries data = %T, want Sum[int64]", m.Data)
	}
	if got := sum.DataPoints[0].Value; got != 2 {
		t.Errorf("retries = %d, want 2", got)
	}

	spans := recorder.Ended()
	if len(spans) != 1 {
		t.Fatalf("recorded %d spans, want 1 span per delivery regardless of retries", len(spans))
	}
	var retryEvents int
	for _, e := range spans[0].Events() {
		if e.Name == "retry" {
			retryEvents++
		}
	}
	if retryEvents != 2 {
		t.Errorf("retry span events = %d, want 2", retryEvents)
	}
}

// terminalCounterAttrs returns the attribute set of a terminal counter that
// was recorded exactly once.
//
// Insisting on a single data point is part of the assertion, not just
// convenience: two data points on one delivery would mean two different
// attribute sets were recorded, which is the same defect as two counters
// firing and would otherwise hide behind an index of [0].
func terminalCounterAttrs(t *testing.T, m metricdata.Metrics) attribute.Set {
	t.Helper()

	sum, ok := m.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("%s data = %T, want Sum[int64]", m.Name, m.Data)
	}
	if len(sum.DataPoints) != 1 {
		t.Fatalf("%s has %d data points, want 1", m.Name, len(sum.DataPoints))
	}
	return sum.DataPoints[0].Attributes
}
