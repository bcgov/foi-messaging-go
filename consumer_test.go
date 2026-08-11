package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
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
