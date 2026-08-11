//go:build integration

package messaging_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	messaging "github.com/bcgov/foi-messaging-go"
	"github.com/bcgov/foi-messaging-go/internal/testsupport"
)

var errDeliberate = errors.New("deliberate handler failure")

var documentCreated = messaging.EventDef{
	Topic:   "documents",
	Type:    "document.created",
	Version: "1.0.0",
}

// consumeFixture starts Redis and returns a config pointed at it.
func consumeFixture(t *testing.T) messaging.Config {
	t.Helper()
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

	return messaging.Config{
		Source: "test.service",
		Redis:  messaging.RedisConfig{Address: addr},
		Consumer: messaging.ConsumerConfig{
			Group:           "test-group",
			Concurrency:     1,
			ClaimInterval:   1 * time.Second,
			ClaimMinIdle:    2 * time.Second,
			ShutdownTimeout: 5 * time.Second,
		},
	}
}

// collectingHandler records the envelopes it receives and can be told to
// fail a fixed number of times first.
type collectingHandler struct {
	mu        sync.Mutex
	received  []messaging.Envelope[documentCreatedPayload]
	failFirst int
	attempts  int
	notify    chan struct{}
}

func newCollectingHandler(failFirst int) *collectingHandler {
	return &collectingHandler{failFirst: failFirst, notify: make(chan struct{}, 16)}
}

func (h *collectingHandler) Handle(_ context.Context, env messaging.Envelope[documentCreatedPayload]) error {
	h.mu.Lock()
	h.attempts++
	shouldFail := h.attempts <= h.failFirst
	if !shouldFail {
		h.received = append(h.received, env)
	}
	h.mu.Unlock()

	select {
	case h.notify <- struct{}{}:
	default:
	}

	if shouldFail {
		return errDeliberate
	}
	return nil
}

func (h *collectingHandler) snapshot() []messaging.Envelope[documentCreatedPayload] {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]messaging.Envelope[documentCreatedPayload](nil), h.received...)
}

func (h *collectingHandler) attemptCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.attempts
}

// runConsumer starts consumer.Run in the background and returns a stop
// function that cancels it and waits for Run to return.
//
// stop is registered as a cleanup and may also be called explicitly by a
// test, so it must be safe to call twice — hence the sync.Once. Without it
// the second call would block waiting on an already-drained channel.
func runConsumer(t *testing.T, consumer *messaging.Consumer) func() {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- consumer.Run(ctx) }()

	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Errorf("consumer.Run: %v", err)
				}
			case <-time.After(30 * time.Second):
				t.Error("consumer.Run did not return within 30s of cancellation")
			}
		})
	}
	t.Cleanup(stop)
	return stop
}

func TestConsumer_ReceivesPublishedEvent(t *testing.T) {
	cfg := consumeFixture(t)

	publisher, err := messaging.NewPublisher(cfg)
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	t.Cleanup(func() {
		if err := publisher.Close(); err != nil {
			t.Errorf("publisher.Close: %v", err)
		}
	})

	consumer, err := messaging.NewConsumer(cfg)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	t.Cleanup(func() {
		if err := consumer.Close(); err != nil {
			t.Errorf("consumer.Close: %v", err)
		}
	})
	handler := newCollectingHandler(0)
	if err := messaging.RegisterHandler(consumer, documentCreated, handler); err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}
	runConsumer(t, consumer)

	result, err := publisher.Publish(context.Background(), documentCreated,
		documentCreatedPayload{EntityID: "e1", Name: "report.pdf"},
		messaging.WithCorrelationID("corr-99"))
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case <-handler.notify:
	case <-time.After(20 * time.Second):
		t.Fatal("timed out waiting for the handler to receive the event")
	}

	received := handler.snapshot()
	if len(received) != 1 {
		t.Fatalf("len(received) = %d, want 1", len(received))
	}
	if received[0].EventID != result.EventID {
		t.Errorf("EventID = %q, want %q", received[0].EventID, result.EventID)
	}
	if received[0].CorrelationID != "corr-99" {
		t.Errorf("CorrelationID = %q, want %q", received[0].CorrelationID, "corr-99")
	}
	if received[0].Payload.Name != "report.pdf" {
		t.Errorf("Payload.Name = %q, want %q", received[0].Payload.Name, "report.pdf")
	}
	if received[0].Source != "test.service" {
		t.Errorf("Source = %q, want %q", received[0].Source, "test.service")
	}
}

func TestConsumer_RedeliversNackedEventViaReclaim(t *testing.T) {
	cfg := consumeFixture(t)

	publisher, err := messaging.NewPublisher(cfg)
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	t.Cleanup(func() { _ = publisher.Close() })

	consumer, err := messaging.NewConsumer(cfg)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	// Fail once so the first delivery nacks and reclaim must redeliver it.
	handler := newCollectingHandler(1)
	if err := messaging.RegisterHandler(consumer, documentCreated, handler); err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}
	runConsumer(t, consumer)

	if _, err := publisher.Publish(context.Background(), documentCreated,
		documentCreatedPayload{EntityID: "e1", Name: "retry.pdf"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	deadline := time.After(30 * time.Second)
	for {
		if len(handler.snapshot()) == 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("event was never redelivered successfully; attempts = %d", handler.attemptCount())
		case <-time.After(200 * time.Millisecond):
		}
	}

	if got := handler.attemptCount(); got < 2 {
		t.Errorf("attempts = %d, want at least 2 (one failure then a reclaim)", got)
	}
}

func TestConsumer_PreservesOrderAtConcurrencyOne(t *testing.T) {
	cfg := consumeFixture(t)

	publisher, err := messaging.NewPublisher(cfg)
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	t.Cleanup(func() { _ = publisher.Close() })

	consumer, err := messaging.NewConsumer(cfg)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	handler := newCollectingHandler(0)
	if err := messaging.RegisterHandler(consumer, documentCreated, handler); err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}
	runConsumer(t, consumer)

	const count = 20
	want := make([]string, 0, count)
	for i := 0; i < count; i++ {
		name := "doc-" + string(rune('a'+i))
		want = append(want, name)
		if _, err := publisher.Publish(context.Background(), documentCreated,
			documentCreatedPayload{EntityID: "e", Name: name}); err != nil {
			t.Fatalf("Publish %d: %v", i, err)
		}
	}

	deadline := time.After(60 * time.Second)
	for {
		if len(handler.snapshot()) >= count {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("received %d of %d events before timeout", len(handler.snapshot()), count)
		case <-time.After(200 * time.Millisecond):
		}
	}

	got := handler.snapshot()
	for i := 0; i < count; i++ {
		if got[i].Payload.Name != want[i] {
			t.Fatalf("event %d = %q, want %q — Concurrency 1 must preserve order",
				i, got[i].Payload.Name, want[i])
		}
	}
}

func TestConsumer_SkipsUnmatchedEventType(t *testing.T) {
	cfg := consumeFixture(t)

	publisher, err := messaging.NewPublisher(cfg)
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	t.Cleanup(func() { _ = publisher.Close() })

	consumer, err := messaging.NewConsumer(cfg)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	handler := newCollectingHandler(0)
	if err := messaging.RegisterHandler(consumer, documentCreated, handler); err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}
	runConsumer(t, consumer)

	// Same topic, an event type this consumer has no handler for.
	other := messaging.EventDef{Topic: "documents", Type: "document.deleted", Version: "1.0.0"}
	if _, err := publisher.Publish(context.Background(), other,
		documentCreatedPayload{EntityID: "e1", Name: "ignored.pdf"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// Then a matching event, which must still arrive.
	if _, err := publisher.Publish(context.Background(), documentCreated,
		documentCreatedPayload{EntityID: "e2", Name: "wanted.pdf"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case <-handler.notify:
	case <-time.After(20 * time.Second):
		t.Fatal("timed out waiting for the matching event")
	}

	received := handler.snapshot()
	if len(received) != 1 {
		t.Fatalf("len(received) = %d, want exactly 1 — the unmatched event must be skipped", len(received))
	}
	if received[0].Payload.Name != "wanted.pdf" {
		t.Errorf("Payload.Name = %q, want %q", received[0].Payload.Name, "wanted.pdf")
	}
}

func TestConsumer_ReplaysEventsPublishedBeforeItStarted(t *testing.T) {
	cfg := consumeFixture(t)

	publisher, err := messaging.NewPublisher(cfg)
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	t.Cleanup(func() { _ = publisher.Close() })

	// Published before any consumer group exists.
	if _, err := publisher.Publish(context.Background(), documentCreated,
		documentCreatedPayload{EntityID: "e1", Name: "early.pdf"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	consumer, err := messaging.NewConsumer(cfg)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	handler := newCollectingHandler(0)
	if err := messaging.RegisterHandler(consumer, documentCreated, handler); err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}
	runConsumer(t, consumer)

	select {
	case <-handler.notify:
	case <-time.After(20 * time.Second):
		t.Fatal("timed out: a new group created at 0 must replay existing history")
	}

	received := handler.snapshot()
	if len(received) != 1 || received[0].Payload.Name != "early.pdf" {
		t.Fatalf("received = %+v, want one event named early.pdf", received)
	}
}

func TestConsumer_ShutdownDrainsInFlightHandler(t *testing.T) {
	cfg := consumeFixture(t)

	publisher, err := messaging.NewPublisher(cfg)
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	t.Cleanup(func() { _ = publisher.Close() })

	consumer, err := messaging.NewConsumer(cfg)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}

	started := make(chan struct{})
	finished := make(chan struct{})
	if err := messaging.RegisterHandler(consumer, documentCreated,
		slowHandler{started: started, finished: finished}); err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}

	stop := runConsumer(t, consumer)

	if _, err := publisher.Publish(context.Background(), documentCreated,
		documentCreatedPayload{EntityID: "e1", Name: "slow.pdf"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case <-started:
	case <-time.After(20 * time.Second):
		t.Fatal("timed out waiting for the handler to start")
	}

	// Cancel while the handler is mid-flight; it must be allowed to finish.
	stop()

	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Error("in-flight handler did not complete before shutdown returned")
	}
}

// slowHandler blocks briefly so a shutdown can overlap its execution.
type slowHandler struct {
	started  chan struct{}
	finished chan struct{}
}

func (h slowHandler) Handle(_ context.Context, _ messaging.Envelope[documentCreatedPayload]) error {
	close(h.started)
	time.Sleep(500 * time.Millisecond)
	close(h.finished)
	return nil
}
