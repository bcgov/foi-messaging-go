//go:build integration

package messaging_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
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
		Retry: messaging.RetryConfig{
			MaxImmediateRetries: 3,
			// Nanosecond backoff keeps the retry loop's structure without
			// its waiting: these tests assert on attempt counts and
			// outcomes, never on timing.
			InitialBackoff: time.Nanosecond,
			MaxBackoff:     time.Nanosecond,
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

// waitForPendingCountZero polls the pending entries list for group on
// stream until it reaches zero or deadline elapses. Acks are issued by
// each message's own per-message goroutine (internal/watermill's emit),
// which can lag slightly behind a handler returning or a skip decision
// being made, so this polls rather than asserting immediately.
//
// This exists because "the handler received the expected events" cannot
// distinguish an acked message from one that was silently left pending
// forever — see dispatch's unmatched-event-type branch, which must ack
// (return nil) rather than nack, and Phase 2b's not-yet-built DLQ means an
// unbounded pending list would otherwise go unnoticed.
func waitForPendingCountZero(t *testing.T, addr, stream, group string) {
	t.Helper()

	deadline := time.After(10 * time.Second)
	for {
		count, err := testsupport.PendingCount(context.Background(), addr, stream, group)
		if err != nil {
			t.Fatalf("PendingCount: %v", err)
		}
		if count == 0 {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("pending entries for stream %q group %q = %d, want 0", stream, group, count)
		case <-time.After(100 * time.Millisecond):
		}
	}
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

	waitForPendingCountZero(t, cfg.Redis.Address, "foi:documents", cfg.Consumer.Group)
}

// TestConsumer_RedeliversNackedEventViaReclaim is spec §8 integration
// scenario 2. Counting attempts alone would be satisfied by any redelivery
// mechanism; what has to be proved is that the reclaimed delivery carries
// _foi_delivery_attempt == 2, i.e. that the XPENDING RetryCount → +1 →
// stamp path is right against real Redis and not just against a fake whose
// pending list the test wrote itself.
//
// Recovering that counter is the entire stated reason for not using
// watermill-redisstream's subscriber, and Phase 2b's delivery-attempt cap
// compares against exactly this value.
//
// The stamped attempt is observed through the log line dispatch already
// emits for a failing handler, which carries the transport metadata as
// attributes — no test-only seam and no widening of the public API. The
// handler fails twice so the second (reclaimed) delivery is itself logged.
func TestConsumer_RedeliversNackedEventViaReclaim(t *testing.T) {
	cfg := consumeFixture(t)
	logs := &logStore{}
	cfg.Telemetry.Logger = slog.New(&captureHandler{store: logs})

	publisher, err := messaging.NewPublisher(cfg)
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	t.Cleanup(func() { _ = publisher.Close() })

	consumer, err := messaging.NewConsumer(cfg)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	// Each delivery is 1+MaxImmediateRetries = 4 invocations, so failFirst
	// must cover two whole deliveries for the reclaimed one to fail and be
	// logged with its stamped attempt: invocations 1-4 nack delivery 1,
	// 5-8 nack the reclaimed delivery 2, and invocation 9 — delivery 3 —
	// succeeds. Anything less and delivery 1's own immediate retries
	// succeed, so the reclaim path this test exists for never runs.
	handler := newCollectingHandler(8)
	if err := messaging.RegisterHandler(consumer, documentCreated, handler); err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}
	runConsumer(t, consumer)

	if _, err := publisher.Publish(context.Background(), documentCreated,
		documentCreatedPayload{EntityID: "e1", Name: "retry.pdf"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	deadline := time.After(60 * time.Second)
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

	if got := handler.attemptCount(); got < 9 {
		t.Errorf("attempts = %d, want at least 9 (two deliveries of four invocations, then a reclaim)", got)
	}

	// The first delivery comes from XREADGROUP and is attempt 1 by
	// construction; the redelivery comes from XPENDING + XCLAIM and must
	// carry the recovered counter.
	if !logs.has(handlerErrorMsg, "delivery_attempt", "1") {
		t.Errorf("no handler-error log stamped delivery_attempt=1 for the first delivery; got %v",
			logs.attemptsFor(handlerErrorMsg))
	}
	if !logs.has(handlerErrorMsg, "delivery_attempt", "2") {
		t.Errorf("reclaim redelivery was not stamped delivery_attempt=2; got %v — "+
			"the XPENDING RetryCount + 1 arithmetic is what Phase 2b's cap compares against",
			logs.attemptsFor(handlerErrorMsg))
	}
}

// handlerErrorMsg is the log runWithRetry emits once a delivery's immediate
// retries are exhausted and the message is about to nack. It carries the
// delivery attempt the subscriber stamped, which is what the cap compares
// against.
const handlerErrorMsg = "messaging: handler returned error, immediate retries exhausted"

type logRecord struct {
	msg   string
	attrs map[string]string
}

type logStore struct {
	mu      sync.Mutex
	records []logRecord
}

func (s *logStore) add(r logRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records = append(s.records, r)
}

func (s *logStore) has(msg, key, value string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.records {
		if r.msg == msg && r.attrs[key] == value {
			return true
		}
	}
	return false
}

// attemptsFor lists the delivery attempts seen on a given log message, for
// failure output.
func (s *logStore) attemptsFor(msg string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for _, r := range s.records {
		if r.msg == msg {
			out = append(out, r.attrs["delivery_attempt"])
		}
	}
	return out
}

// captureHandler is a slog.Handler that records what the library logged,
// including attributes added through Logger.With.
type captureHandler struct {
	store *logStore
	attrs []slog.Attr
}

func (h *captureHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *captureHandler) Handle(_ context.Context, r slog.Record) error {
	rec := logRecord{msg: r.Message, attrs: make(map[string]string, r.NumAttrs()+len(h.attrs))}
	for _, a := range h.attrs {
		rec.attrs[a.Key] = a.Value.String()
	}
	r.Attrs(func(a slog.Attr) bool {
		rec.attrs[a.Key] = a.Value.String()
		return true
	})
	h.store.add(rec)
	return nil
}

func (h *captureHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &captureHandler{
		store: h.store,
		attrs: append(append([]slog.Attr(nil), h.attrs...), attrs...),
	}
}

func (h *captureHandler) WithGroup(string) slog.Handler { return h }

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

	// Both events must be acknowledged: the unmatched one via the skip
	// path (dispatch returning nil), the matching one via the handler
	// returning nil. Checking only what the handler received cannot tell
	// an ack apart from a silently growing pending entries list — if the
	// skip path ever regressed to nacking, the matching event would still
	// arrive right on schedule (Concurrency: 1 only gates on slot release,
	// not on acknowledgement) and this test would stay green without this
	// assertion.
	waitForPendingCountZero(t, cfg.Redis.Address, "foi:documents", cfg.Consumer.Group)
}

// TestConsumer_CloseDuringRunLeavesConsumerWorking is the end-to-end form
// of the guard in Close: a service that does `go consumer.Run(ctx)` and
// wires Close into its shutdown handler — a natural misreading of
// io.Closer — used to close the Redis client under the live read loop.
// Every read then failed with "redis: client is closed", which is neither
// ctx.Done nor a shutdown signal, so the loop retried forever, Run never
// returned, and the process never exited.
func TestConsumer_CloseDuringRunLeavesConsumerWorking(t *testing.T) {
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

	// runConsumer's cleanup asserts Run returns within 30s of cancellation.
	runConsumer(t, consumer)

	// Give Run time to build its client and start reading, then Close.
	time.Sleep(2 * time.Second)
	if err := consumer.Close(); err != nil {
		t.Fatalf("Close during Run must return nil, got: %v", err)
	}

	if _, err := publisher.Publish(context.Background(), documentCreated,
		documentCreatedPayload{EntityID: "e1", Name: "after-close.pdf"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case <-handler.notify:
	case <-time.After(20 * time.Second):
		t.Fatal("the consumer stopped delivering after Close was called during Run; " +
			"Close must not tear down a running consumer's Redis client")
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

	// finished must already be closed by the time stop() returns: stop()
	// blocks until consumer.Run returns, and Run is documented to drain
	// in-flight handlers before returning. A separate grace window here
	// (waiting on <-finished with its own timeout after stop() has already
	// returned) cannot tell "Run drained the handler" apart from "Run
	// returned immediately and the handler happened to finish on its own
	// moments later" — slowHandler's 500ms sleep is unpreemptible and
	// ignores ctx, so it completes on the same wall-clock schedule either
	// way. A non-blocking check is the only form of this assertion that
	// can fail when the drain is missing.
	select {
	case <-finished:
	default:
		t.Error("consumer.Run returned before the in-flight handler completed; " +
			"Run must drain in-flight handlers within ShutdownTimeout before returning")
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

// ctxAwareHandler is the canonical shape of a real handler: it passes its
// context into blocking work — `return db.ExecContext(ctx, ...)` — and so
// fails immediately if that context is already cancelled.
type ctxAwareHandler struct {
	started chan struct{}
	work    time.Duration

	mu  sync.Mutex
	ran bool
	err error
}

func (h *ctxAwareHandler) Handle(ctx context.Context, _ messaging.Envelope[documentCreatedPayload]) error {
	close(h.started)

	var err error
	select {
	case <-ctx.Done():
		err = ctx.Err()
	case <-time.After(h.work):
	}

	h.mu.Lock()
	h.ran, h.err = true, err
	h.mu.Unlock()
	return err
}

func (h *ctxAwareHandler) result() (bool, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.ran, h.err
}

// TestConsumer_ShutdownGivesInFlightHandlerALiveContext is the assertion
// TestConsumer_ShutdownDrainsInFlightHandler structurally cannot make: its
// slowHandler ignores its context and sleeps unpreemptibly, so it completes
// on the same schedule whether or not the drain works.
//
// A handler that honours its context is the normal case, and the drain the
// library documents is worthless to it if the message context is cancelled
// the instant shutdown begins: the handler fails with context.Canceled,
// the message nacks, and it is redelivered ClaimMinIdle later. In-flight
// handlers must instead keep a live context for the whole ShutdownTimeout.
func TestConsumer_ShutdownGivesInFlightHandlerALiveContext(t *testing.T) {
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

	// Well inside the 5s ShutdownTimeout the fixture configures.
	handler := &ctxAwareHandler{started: make(chan struct{}), work: 500 * time.Millisecond}
	if err := messaging.RegisterHandler(consumer, documentCreated, handler); err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}

	stop := runConsumer(t, consumer)

	if _, err := publisher.Publish(context.Background(), documentCreated,
		documentCreatedPayload{EntityID: "e1", Name: "ctx.pdf"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case <-handler.started:
	case <-time.After(20 * time.Second):
		t.Fatal("timed out waiting for the handler to start")
	}

	// Shut down while the handler is mid-flight.
	stop()

	ran, handlerErr := handler.result()
	if !ran {
		t.Fatal("consumer.Run returned before the in-flight handler completed")
	}
	if handlerErr != nil {
		t.Errorf("handler returned %v; a handler in flight when shutdown begins must keep "+
			"a live context for the whole ShutdownTimeout, not be cancelled at the start of the drain",
			handlerErr)
	}

	// A handler that completed during the drain must have had its entry
	// acked, not left pending for a redelivery ClaimMinIdle later.
	waitForPendingCountZero(t, cfg.Redis.Address, "foi:documents", cfg.Consumer.Group)
}

// TestConsumer_RetryStormOnOneTopicDoesNotStallAnother is the regression
// test for per-topic concurrency. Concurrency bounds in-flight handlers per
// subscribed topic, so one topic exhausting its single slot on retries must
// not stop another topic from consuming.
//
// Under a Subscriber-wide semaphore — which is what this codebase had
// before — topic A's retrying handler holds the only slot and topic B stops
// entirely. This test is what would catch someone "simplifying" the
// per-subscription semaphore back to a shared one.
func TestConsumer_RetryStormOnOneTopicDoesNotStallAnother(t *testing.T) {
	cfg := consumeFixture(t)
	cfg.Consumer.Concurrency = 1
	// Real backoff, so topic A genuinely occupies its slot for a while.
	cfg.Retry = messaging.RetryConfig{
		MaxImmediateRetries: 3,
		InitialBackoff:      200 * time.Millisecond,
		MaxBackoff:          200 * time.Millisecond,
	}

	stalling := messaging.EventDef{Topic: "stalling", Type: "document.created", Version: "1.0.0"}
	flowing := messaging.EventDef{Topic: "flowing", Type: "document.created", Version: "1.0.0"}

	consumer, err := messaging.NewConsumer(cfg)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	t.Cleanup(func() { _ = consumer.Close() })

	// Always fails: every delivery burns all four attempts and the full
	// backoff, holding the stalling topic's only slot throughout.
	stallingHandler := newCollectingHandler(1 << 30)
	if err := messaging.RegisterHandler(consumer, stalling, stallingHandler); err != nil {
		t.Fatalf("RegisterHandler(stalling): %v", err)
	}
	flowingHandler := newCollectingHandler(0)
	if err := messaging.RegisterHandler(consumer, flowing, flowingHandler); err != nil {
		t.Fatalf("RegisterHandler(flowing): %v", err)
	}

	publisher, err := messaging.NewPublisher(cfg)
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	t.Cleanup(func() { _ = publisher.Close() })

	ctx := context.Background()
	const flowingCount = 5
	for i := 0; i < flowingCount; i++ {
		if _, err := publisher.Publish(ctx, stalling, documentCreatedPayload{}); err != nil {
			t.Fatalf("Publish(stalling): %v", err)
		}
		if _, err := publisher.Publish(ctx, flowing, documentCreatedPayload{}); err != nil {
			t.Fatalf("Publish(flowing): %v", err)
		}
	}

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- consumer.Run(runCtx) }()

	// The flowing topic must drain on its own slot while the stalling topic
	// is still working through its first message's retries.
	deadline := time.After(15 * time.Second)
	for len(flowingHandler.snapshot()) < flowingCount {
		select {
		case <-flowingHandler.notify:
		case <-deadline:
			cancel()
			<-done
			t.Fatalf("flowing topic received %d/%d events: one topic's retries are stalling another",
				len(flowingHandler.snapshot()), flowingCount)
		}
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestConsumer_PermanentErrorReachesTheDLQStream(t *testing.T) {
	cfg := consumeFixture(t)

	consumer, err := messaging.NewConsumer(cfg)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	t.Cleanup(func() { _ = consumer.Close() })

	handled := make(chan struct{}, 1)
	h := permanentlyFailingHandler{done: handled}
	if err := messaging.RegisterHandler(consumer, documentCreated, h); err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}

	publisher, err := messaging.NewPublisher(cfg)
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	t.Cleanup(func() { _ = publisher.Close() })

	ctx := context.Background()
	published, err := publisher.Publish(ctx, documentCreated, documentCreatedPayload{})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- consumer.Run(runCtx) }()

	select {
	case <-handled:
	case <-time.After(10 * time.Second):
		cancel()
		<-done
		t.Fatal("handler was never called")
	}

	// Let the DLQ publish and the ack settle before tearing down.
	time.Sleep(500 * time.Millisecond)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}

	entries, err := testsupport.ReadStreamEntries(ctx, cfg.Redis.Address, "foi:documents.dlq")
	if err != nil {
		t.Fatalf("ReadStreamEntries: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("DLQ has %d entries, want 1", len(entries))
	}

	var dl messaging.DeadLetter
	if err := json.Unmarshal([]byte(entries[0].Fields["payload"]), &dl); err != nil {
		t.Fatalf("unmarshalling dead letter: %v", err)
	}
	if dl.Reason != messaging.ReasonPermanent {
		t.Errorf("Reason = %q, want %q", dl.Reason, messaging.ReasonPermanent)
	}
	if dl.OriginalTopic != "documents" {
		t.Errorf("OriginalTopic = %q, want %q", dl.OriginalTopic, "documents")
	}
	if dl.ConsumerGroup != cfg.Consumer.Group {
		t.Errorf("ConsumerGroup = %q, want %q", dl.ConsumerGroup, cfg.Consumer.Group)
	}

	// The original envelope must survive byte-for-byte so replay tooling
	// can republish it without transformation (PRD §14).
	var env messaging.Envelope[documentCreatedPayload]
	if err := json.Unmarshal(dl.Event, &env); err != nil {
		t.Fatalf("dead-lettered event is not a readable envelope: %v", err)
	}
	if env.EventID != published.EventID {
		t.Errorf("EventID = %q, want %q", env.EventID, published.EventID)
	}

	// And the original entry must be acked: a dead-lettered event is
	// durably recorded elsewhere, so leaving it pending would redeliver it.
	pending, err := testsupport.PendingCount(ctx, cfg.Redis.Address, "foi:documents", cfg.Consumer.Group)
	if err != nil {
		t.Fatalf("PendingCount: %v", err)
	}
	if pending != 0 {
		t.Errorf("pending = %d, want 0: a dead-lettered event must be acked", pending)
	}
}

// permanentlyFailingHandler classifies its failure as permanent, so it is
// dead-lettered on the first delivery without retry.
type permanentlyFailingHandler struct {
	done chan struct{}
}

func (h permanentlyFailingHandler) Handle(context.Context, messaging.Envelope[documentCreatedPayload]) error {
	select {
	case h.done <- struct{}{}:
	default:
	}
	return messaging.AsPermanent(errDeliberate)
}

// TestConsumer_PoisonBacklogDrainsAndLiveTrafficResumes is 2a spec §10's
// requested test. A reclaim sweep claims up to 100 entries and each occupies
// a concurrency slot for a full claim/decode/release cycle — with retry,
// for four handler invocations. At Concurrency 1 an accumulated poison
// backlog therefore starves the read loop, and it is the delivery-attempt
// cap that bounds it: once each entry is dead-lettered and acked, the
// backlog is gone for good and live traffic moves again.
func TestConsumer_PoisonBacklogDrainsAndLiveTrafficResumes(t *testing.T) {
	cfg := consumeFixture(t)
	cfg.Consumer.Concurrency = 1
	cfg.Consumer.MaxDeliveryAttempts = 2
	cfg.Consumer.ClaimInterval = 500 * time.Millisecond
	cfg.Consumer.ClaimMinIdle = 500 * time.Millisecond

	consumer, err := messaging.NewConsumer(cfg)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	t.Cleanup(func() { _ = consumer.Close() })

	// Fails every delivery: unclassified, so retryable, so it is the cap
	// rather than classification that ends it.
	handler := newCollectingHandler(1 << 30)
	if err := messaging.RegisterHandler(consumer, documentCreated, handler); err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}

	publisher, err := messaging.NewPublisher(cfg)
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	t.Cleanup(func() { _ = publisher.Close() })

	ctx := context.Background()
	const poisonCount = 20
	for i := 0; i < poisonCount; i++ {
		if _, err := publisher.Publish(ctx, documentCreated, documentCreatedPayload{}); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- consumer.Run(runCtx) }()

	// Every poison entry must end up in the DLQ and be acked, leaving the
	// pending list empty — the state in which live traffic can flow again.
	//
	// The DLQ count is the condition waited on, not the pending count.
	// Pending is 0 both before the consumer has read anything and after it
	// has drained, so polling it alone would pass instantly against a
	// consumer that never started: XPENDING even reports NOGROUP until Run
	// has created the group.
	deadline := time.After(60 * time.Second)
	var entries []testsupport.StreamEntry
	for {
		var err error
		entries, err = testsupport.ReadStreamEntries(ctx, cfg.Redis.Address, "foi:documents.dlq")
		if err != nil {
			t.Fatalf("ReadStreamEntries: %v", err)
		}
		if len(entries) >= poisonCount {
			break
		}
		select {
		case <-deadline:
			cancel()
			<-done
			t.Fatalf("DLQ has %d of %d entries: the cap is not draining the poison backlog",
				len(entries), poisonCount)
		case <-time.After(200 * time.Millisecond):
		}
	}
	if len(entries) != poisonCount {
		t.Errorf("DLQ has %d entries, want %d", len(entries), poisonCount)
	}

	// Each dead-lettered entry must also be acked, or the backlog would
	// keep being reclaimed and live traffic would still be starved.
	for {
		pending, err := testsupport.PendingCount(ctx, cfg.Redis.Address, "foi:documents", cfg.Consumer.Group)
		if err != nil {
			t.Fatalf("PendingCount: %v", err)
		}
		if pending == 0 {
			break
		}
		select {
		case <-deadline:
			cancel()
			<-done
			t.Fatalf("%d entries still pending: a dead-lettered event must be acked", pending)
		case <-time.After(200 * time.Millisecond):
		}
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
}
