package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	internalredis "github.com/bcgov/foi-messaging-go/internal/redis"
	internalwatermill "github.com/bcgov/foi-messaging-go/internal/watermill"
)

// dlqPublishTimeout bounds a dead letter written from the subscriber's
// undecodable-entry hook, whose context is deliberately detached from Run's
// so a drain in progress still records the entry.
const dlqPublishTimeout = 5 * time.Second

// Consumer subscribes to the topics its registered handlers cover and
// dispatches each event to the handler matching its event type and major
// schema version.
//
// A Consumer is created from a Config, has handlers registered against it,
// and is then run. Registration after Run has started is an error.
type Consumer struct {
	cfg Config

	mu       sync.Mutex
	registry *registry
	running  bool
	// dlq is nil until Run builds it. Guarded by mu for the same reason
	// reader is: Run writes it while callers may be reading.
	dlq deadLetterSink

	reader *internalredis.StreamReader

	// inst and tracer are built once by NewConsumer, not by Run: dispatch
	// is reachable without Run (tests, and the failure paths that are
	// hardest to provoke against a live broker), and a nil instrument set
	// there would panic rather than simply not record.
	inst   *instruments
	tracer trace.Tracer
}

// NewConsumer validates cfg and returns a Consumer with no handlers
// registered. It opens no connections — the Redis client, subscriber, and
// router are built by Run, once the set of topics is known.
func NewConsumer(cfg Config) (*Consumer, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	if err := cfg.validateConsumer(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	return &Consumer{
		cfg:      cfg,
		registry: newRegistry(),
		inst:     newInstruments(cfg.Telemetry.MeterProvider, cfg.Telemetry.Logger),
		tracer:   cfg.Telemetry.TracerProvider.Tracer(telemetryScope),
	}, nil
}

// RegisterHandler registers a typed handler for def. It is a function
// rather than a method because Go does not permit generic methods.
//
// The handler is invoked for every event on def.Topic whose event type
// matches def.Type and whose major schema version matches def.Version's.
// Minor and patch differences do not affect dispatch: producers may add
// optional fields without a coordinated consumer release, so handlers must
// tolerate any additive change within their major version.
func RegisterHandler[T any](c *Consumer, def EventDef, h Handler[T]) error {
	if err := validateEventDef(def); err != nil {
		return err
	}
	major, err := majorVersion(def.Version)
	if err != nil {
		return fmt.Errorf("event def: %w", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.running {
		return fmt.Errorf("consumer: handlers cannot be registered after Run has started")
	}

	return c.registry.addTyped(def.Topic, def.Type, major, typedDispatch(h))
}

// RegisterRawHandler registers a handler that receives every event on a
// topic with its payload left as raw JSON. Use it when several payload
// shapes share an event type, or to consume events without a typed
// contract.
//
// A topic may have typed handlers or one raw handler, never both.
func RegisterRawHandler(c *Consumer, sel TopicSelector, h Handler[json.RawMessage]) error {
	if sel.Topic == "" {
		return fmt.Errorf("topic selector: Topic is required")
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.running {
		return fmt.Errorf("consumer: handlers cannot be registered after Run has started")
	}

	return c.registry.addRaw(sel.Topic, func(ctx context.Context, env Envelope[json.RawMessage]) error {
		return h.Handle(ctx, env)
	})
}

// validateEventDef checks that a registration names a topic, a well-formed
// event type, and a semantic version.
func validateEventDef(def EventDef) error {
	if def.Topic == "" {
		return fmt.Errorf("event def: Topic is required")
	}
	if def.Type == "" {
		return fmt.Errorf("event def: Type is required")
	}
	if !eventTypePattern.MatchString(def.Type) {
		return fmt.Errorf("event def: Type %q must be 2-3 dot-separated lowercase segments", def.Type)
	}
	if def.Version == "" {
		return fmt.Errorf("event def: Version is required")
	}
	if !schemaVersionPattern.MatchString(def.Version) {
		return fmt.Errorf("event def: Version %q must be MAJOR.MINOR.PATCH", def.Version)
	}
	return nil
}

// markRunning flips the consumer into its running state, after which
// registration is refused. It reports false if Run has already started.
func (c *Consumer) markRunning() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.running {
		return false
	}
	c.running = true
	return true
}

// Run subscribes to every registered topic and blocks until ctx is
// cancelled, then drains in-flight handlers within cfg.Consumer.ShutdownTimeout
// before returning.
func (c *Consumer) Run(ctx context.Context) error {
	// The empty-registry check, the already-running check, and the topics
	// snapshot must happen in one critical section. Splitting them (as an
	// earlier version did, reading topics then calling a separately locked
	// markRunning) leaves a window where a concurrent RegisterHandler can
	// add a topic after topics is captured but before running flips true:
	// the registration would succeed silently and its topic would never be
	// subscribed to, with no error anywhere to explain why the handler is
	// never called.
	c.mu.Lock()
	if c.registry.isEmpty() {
		c.mu.Unlock()
		return fmt.Errorf("consumer: no handlers registered; register at least one before calling Run")
	}
	if c.running {
		c.mu.Unlock()
		return fmt.Errorf("consumer: Run has already been called")
	}
	topics := c.registry.topicList()
	c.running = true
	c.mu.Unlock()

	client := internalredis.NewClient(redisClientOptions(c.cfg.Redis))
	reader := internalredis.NewStreamReader(client, c.cfg.Consumer.Group, c.cfg.Consumer.ConsumerName)

	// The DLQ publisher shares Run's client rather than opening a second
	// connection pool. It is deliberately never Closed here:
	// internalwatermill.Publisher.Close closes the client it was built
	// over, and closeReader already owns that client — a second Close
	// returns ErrClosed from go-redis's pool and would surface as a
	// spurious teardown failure.
	dlqPublisher, err := internalwatermill.NewPublisher(client)
	if err != nil {
		_ = c.closeReader(reader)
		return fmt.Errorf("creating dead letter publisher: %w", err)
	}

	c.mu.Lock()
	c.reader = reader
	c.dlq = redisDeadLetterSink{pub: dlqPublisher}
	c.mu.Unlock()

	// The hook below is handed a Redis stream name, but dead-lettering is
	// expressed in logical topics, so the mapping Run already computes for
	// AddHandler is inverted once here rather than parsed back out of the
	// stream name.
	topicByStream := make(map[string]string, len(topics))
	for _, topic := range topics {
		topicByStream[c.streamName(topic)] = topic
	}

	// The application's logger is threaded into both halves: without it the
	// subscriber's read-loop, claim-loop and ack failures, and watermill's
	// own handler-error line, all go to a NopLogger or the stdlib logger
	// and never reach the operator.
	subscriber, err := internalwatermill.NewSubscriber(internalwatermill.SubscriberOptions{
		Reader:        reader,
		Concurrency:   c.cfg.Consumer.Concurrency,
		ClaimInterval: c.cfg.Consumer.ClaimInterval,
		ClaimMinIdle:  c.cfg.Consumer.ClaimMinIdle,
		Logger:        c.cfg.Telemetry.Logger,
		OnUndecodable: func(stream, entryID string, fields map[string]any) error {
			topic, ok := topicByStream[stream]
			if !ok {
				return fmt.Errorf("no topic registered for stream %q", stream)
			}

			// The original bytes are unreachable — the marshaller failed
			// before producing a payload — so the raw Redis fields are what
			// gets preserved. PRD §14 did not anticipate a
			// marshaller-level failure; this is the nearest thing to
			// "the raw bytes" that exists at this point.
			raw, err := json.Marshal(fields)
			if err != nil {
				return fmt.Errorf("marshalling fields of undecodable entry %q: %w", entryID, err)
			}

			dl := c.newDeadLetter(topic, ReasonDeserializationFailed,
				fmt.Errorf("stream entry %q could not be unmarshalled", entryID), 1)
			dl.EventRaw = raw

			// Detached from Run's ctx, and timeout-bounded, for the same
			// reason the subscriber's ack path is: an entry reaching this
			// hook during the drain must still be recorded, and Run's ctx
			// is already cancelled by then. Without this the DLQ write
			// fails with context.Canceled at exactly the moment there is a
			// backlog to clear.
			//
			// The same reasoning does not apply to the DLQ writes inside
			// dispatch: those run on the message context, which is already
			// context.WithoutCancel-derived and stays live for the whole
			// drain.
			dlqCtx, cancelDLQ := context.WithTimeout(context.WithoutCancel(ctx), dlqPublishTimeout)
			defer cancelDLQ()
			return c.deadLetter(dlqCtx, topic, dl)
		},
	})
	if err != nil {
		_ = c.closeReader(reader)
		return fmt.Errorf("creating subscriber: %w", err)
	}

	router, err := internalwatermill.NewRouter(c.cfg.Consumer.ShutdownTimeout, c.cfg.Telemetry.Logger)
	if err != nil {
		_ = subscriber.Close()
		_ = c.closeReader(reader)
		return fmt.Errorf("creating router: %w", err)
	}

	for _, topic := range topics {
		router.AddHandler(topic, c.streamName(topic), subscriber,
			func(ctx context.Context, payload []byte, metadata map[string]string) error {
				return c.dispatch(ctx, topic, payload, metadata)
			})
	}

	runErr := router.Run(ctx)

	// Every teardown step runs, and every failure is reported: returning
	// only the first would mask a teardown failure behind an earlier one.
	closeErr := router.Close()
	if closeErr != nil {
		// Watermill's Close returns "router close timeout" but still
		// closes its done channel, so without this the drain expiring —
		// handlers abandoned mid-processing, their messages left pending —
		// would be invisible.
		c.cfg.Telemetry.Logger.Error("messaging: router did not shut down cleanly",
			"shutdown_timeout", c.cfg.Consumer.ShutdownTimeout, "error", closeErr)
		closeErr = fmt.Errorf("closing router: %w", closeErr)
	}

	subCloseErr := subscriber.Close()
	if subCloseErr != nil {
		subCloseErr = fmt.Errorf("closing subscriber: %w", subCloseErr)
	}

	readerErr := c.closeReader(reader)
	if readerErr != nil {
		readerErr = fmt.Errorf("closing redis client: %w", readerErr)
	}

	return errors.Join(runErr, closeErr, subCloseErr, readerErr)
}

// closeReader closes reader and clears it from Consumer state so a later
// Close call sees nothing to close. This is the single call site Run uses
// to close its reader — on every exit path, not just the normal-teardown
// one — so c.reader can never be left pointing at an already-closed
// client. go-redis's connection pool uses a CompareAndSwap guard and
// returns ErrClosed from a second Close on the same client
// (internal/pool.ConnPool.Close), so leaving c.reader set after closing it
// would make a documented-idempotent Consumer.Close call fail.
func (c *Consumer) closeReader(reader *internalredis.StreamReader) error {
	err := reader.Close()
	c.mu.Lock()
	c.reader = nil
	// Cleared alongside the reader so a Consumer that has finished running
	// holds no publisher over an already-closed client.
	c.dlq = nil
	c.mu.Unlock()
	return err
}

// dispatch decodes one message and routes it to its handler.
//
// metadata carries the transport-only fields the subscriber stamped on the
// message — the Redis entry ID and the delivery attempt. They are logged,
// never merged into the envelope: PRD §5 keeps transport state out of the
// event contract. The delivery attempt is read here for the cap.
//
// Returning nil acks the message; returning an error nacks it, leaving the
// entry pending for redelivery. The order of the checks below is
// load-bearing: the delivery-attempt cap fires before decoding, so an
// over-cap event never spends handler invocations — or its concurrency
// slot — proving what its counter already said; then the three
// deserialization failures dead-letter rather than nack, being permanent by
// definition; then runWithRetry runs the handler.
//
// Telemetry is stated, not recorded, at each of dispatch's six return
// statements — one of which delegates to runWithRetry's own five
// outcome-stating branches, for ten call sites in total across the two
// functions: each calls one rec.processed/failed/skipped and the single
// deferred rec.end() below turns that into the span status, the duration
// observation, and exactly one terminal counter. Instrumenting each site in
// place would thread twenty-odd statements through the subtlest function in
// the repository and make every exit path added later a chance to forget
// one.
func (c *Consumer) dispatch(ctx context.Context, topic string, payload []byte, metadata map[string]string) error {
	// Extracted before the span opens so the consumer span parents to the
	// producer's (PRD §5). A message with no traceparent simply starts a
	// new trace here.
	ctx = c.cfg.Telemetry.Propagator.Extract(ctx, propagation.MapCarrier(metadata))

	stream := c.streamName(topic)
	ctx, span := c.tracer.Start(ctx, "process "+topic,
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("messaging.system", "redis"),
			attribute.String("messaging.operation.name", "process"),
			attribute.String("messaging.destination.name", stream),
			attribute.String("messaging.consumer.group.name", c.cfg.Consumer.Group),
			attribute.String("messaging.foi.stream_id", metadata[internalwatermill.MetadataStreamID]),
		))

	rec := newDeliveryRecorder(c.inst, span, topic, c.cfg.Consumer.Group, c.cfg.Telemetry.Logger)
	// The only thing guaranteeing the span is closed when a handler is
	// abandoned at the drain deadline: message contexts are
	// context.WithoutCancel-derived and stay live for the whole
	// ShutdownTimeout, so nothing else will end it.
	defer rec.end()

	c.inst.received.Add(ctx, 1, metric.WithAttributes(attribute.String(attrTopic, topic)))

	log := c.cfg.Telemetry.Logger.With(
		"stream_id", metadata[internalwatermill.MetadataStreamID],
		"delivery_attempt", metadata[internalwatermill.MetadataDeliveryAttempt],
	)

	attempt := deliveryAttempt(metadata)
	span.SetAttributes(attribute.Int64("messaging.foi.delivery_attempt", attempt))

	if attempt > int64(c.cfg.Consumer.MaxDeliveryAttempts) {
		// Checked before decoding, and before any classification is
		// consulted — PRD §13 Layer 3 caps regardless of classification.
		//
		// The ordering is load-bearing for throughput, not just tidiness.
		// A capped event occupies its topic's concurrency slot for one
		// metadata read and one DLQ publish; run through the retry loop
		// instead it would hold that slot for (1+MaxImmediateRetries)
		// handler invocations. At the default Concurrency of 1 a reclaim
		// sweep of accumulated poison entries is what starves the read
		// loop, so this bound is what keeps live traffic moving.
		log.Warn("messaging: delivery attempt cap exceeded",
			"topic", topic, "max_delivery_attempts", c.cfg.Consumer.MaxDeliveryAttempts)

		err := fmt.Errorf("delivery attempt %d exceeded MaxDeliveryAttempts %d",
			attempt, c.cfg.Consumer.MaxDeliveryAttempts)
		rec.failed(categoryMaxAttempts, err)

		dl := c.newDeadLetter(topic, ReasonMaxAttemptsExceeded, err, attempt)
		dl.Event, dl.EventRaw = deadLetterBody(payload)
		return c.deadLetter(ctx, topic, dl)
	}

	var env Envelope[json.RawMessage]
	if err := json.Unmarshal(payload, &env); err != nil {
		log.Error("messaging: undecodable event envelope",
			"topic", topic, "error", err)
		// Dead-lettered rather than nacked. These three failures are
		// definitionally permanent — malformed JSON does not become valid
		// on redelivery, and a missing event_id does not appear — so
		// routing them through the cap would spend five reclaim cycles,
		// each holding a concurrency slot, to reach a verdict that was
		// available on the first look.
		//
		// EventRaw is set directly rather than through deadLetterBody:
		// PRD §14 puts everything that failed to deserialize into a usable
		// event in event_raw, including a syntactically valid envelope
		// that failed validation.
		rec.failed(categoryDeserialization, err)

		dl := c.newDeadLetter(topic, ReasonDeserializationFailed,
			fmt.Errorf("unmarshalling envelope on topic %q: %w", topic, err), attempt)
		dl.EventRaw = payload
		return c.deadLetter(ctx, topic, dl)
	}

	// Set on the span, never on the metric attributes at this point: the
	// event type here is whatever the wire said, and only a typed registry
	// match downstream proves it came from a bounded set. Spans are not
	// aggregated by attribute value, so they carry it unconditionally.
	span.SetAttributes(
		attribute.String("messaging.message.id", env.EventID),
		attribute.String("messaging.foi.event_type", env.EventType),
		attribute.String("messaging.foi.schema_version", env.SchemaVersion),
		attribute.String("messaging.foi.correlation_id", env.CorrelationID),
	)

	if err := validateEnvelope(env); err != nil {
		log.Error("messaging: invalid event envelope",
			"topic", topic, "event_id", env.EventID, "error", err)
		rec.failed(categoryDeserialization, err)

		dl := c.newDeadLetter(topic, ReasonDeserializationFailed,
			fmt.Errorf("validating envelope on topic %q: %w", topic, err), attempt)
		dl.EventRaw = payload
		return c.deadLetter(ctx, topic, dl)
	}

	major, err := majorVersion(env.SchemaVersion)
	if err != nil {
		log.Error("messaging: unparseable schema version",
			"topic", topic, "event_id", env.EventID, "error", err)
		rec.failed(categoryDeserialization, err)

		dl := c.newDeadLetter(topic, ReasonDeserializationFailed,
			fmt.Errorf("parsing schema version on topic %q: %w", topic, err), attempt)
		dl.EventRaw = payload
		return c.deadLetter(ctx, topic, dl)
	}

	c.mu.Lock()
	handler, match := c.registry.lookup(topic, env.EventType, major)
	c.mu.Unlock()

	if match == matchNone {
		// Topics are shared and services consume only the event types they
		// care about, so an unmatched event is a normal outcome, not a
		// failure. It counts as skipped{reason="no_handler"}.
		log.Debug("messaging: no handler for event",
			"topic", topic, "event_type", env.EventType,
			"schema_version", env.SchemaVersion, "event_id", env.EventID)
		rec.skipped(reasonNoHandler)
		return nil
	}

	// Only a typed match proves the event type came from a bounded set
	// fixed at registration; a raw handler takes whatever the wire said.
	if match == matchTyped {
		rec.setEventType(env.EventType)
	}

	ctx = contextWithCorrelationID(ctx, env.CorrelationID)
	return c.runWithRetry(ctx, topic, payload, attempt, handler, env, rec)
}

// streamName maps a logical topic to its Redis stream.
func (c *Consumer) streamName(topic string) string {
	return c.cfg.StreamPrefix + ":" + topic
}

// Close releases the Redis client held by a Consumer that was constructed
// but never run. It is idempotent, returns nil when there is nothing to
// release, and is safe to defer unconditionally.
//
// Close is not how a running consumer is stopped. Cancel the context passed
// to Run: Run drains its handlers and releases everything itself before
// returning. While Run is in progress — and after it has returned, when
// there is nothing left to release — Close does nothing and returns nil.
//
// That guard matters. Closing the client under a live read loop wedged the
// consumer permanently and silently: "redis: client is closed" is neither
// ctx.Done nor a shutdown signal, so the read loop backed off and retried
// it forever, Run never returned, and the process never exited.
func (c *Consumer) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.running {
		return nil
	}

	reader := c.reader
	c.reader = nil
	if reader == nil {
		return nil
	}
	return reader.Close()
}

// deadLetterSink publishes DeadLetter documents to a DLQ stream.
//
// It is an interface so dispatch's DLQ routing is unit-testable without
// Redis: the failure paths it guards are exactly the ones hardest to
// provoke against a live broker.
type deadLetterSink interface {
	publish(ctx context.Context, stream string, body []byte) error
}

// redisDeadLetterSink writes dead letters through the Redis client Run
// already holds for the reader.
type redisDeadLetterSink struct {
	pub *internalwatermill.Publisher
}

func (s redisDeadLetterSink) publish(ctx context.Context, stream string, body []byte) error {
	// A dead letter is a new stream entry with no meaningful predecessor,
	// so it gets a fresh id rather than reusing the original event's —
	// which may not even be readable, on the deserialization paths.
	id, err := uuid.NewV7()
	if err != nil {
		return fmt.Errorf("generating dead letter id: %w", err)
	}
	return s.pub.Publish(ctx, stream, id.String(), body, nil)
}

// newDeadLetter fills in the fields every dead letter carries. The caller
// sets Event or EventRaw, because only the caller knows whether the bytes
// it holds are a parseable envelope.
func (c *Consumer) newDeadLetter(topic, reason string, cause error, attempt int64) DeadLetter {
	return DeadLetter{
		DeadLetteredAt:   time.Now().UTC(),
		Reason:           reason,
		Error:            cause.Error(),
		DeliveryAttempts: attempt,
		ConsumerGroup:    c.cfg.Consumer.Group,
		ConsumerName:     c.cfg.Consumer.ConsumerName,
		OriginalTopic:    topic,
	}
}

// deadLetter publishes dl to topic's DLQ stream.
//
// Returning nil means the caller may ack: the event is durably recorded
// somewhere else. Returning an error means it must nack — the entry stays
// pending and the next reclaim sweep retries the DLQ write. While the DLQ
// is unwritable this loops, which is the correct trade: the alternative
// acks the event into nothing (PRD §14).
func (c *Consumer) deadLetter(ctx context.Context, topic string, dl DeadLetter) error {
	c.mu.Lock()
	sink := c.dlq
	c.mu.Unlock()

	stream := c.streamName(topic) + ".dlq"

	if sink == nil {
		// Only reachable from a Consumer constructed but never run.
		// Erroring nacks, which keeps the event rather than dropping it.
		return fmt.Errorf("dead-lettering to %q: no dead letter sink configured", stream)
	}

	body, err := json.Marshal(dl)
	if err != nil {
		return fmt.Errorf("marshalling dead letter for %q: %w", stream, err)
	}

	if err := sink.publish(ctx, stream, body); err != nil {
		c.cfg.Telemetry.Logger.Error("messaging: dead letter publish failed",
			"topic", topic, "dlq_stream", stream, "reason", dl.Reason,
			"delivery_attempts", dl.DeliveryAttempts, "error", err)
		return fmt.Errorf("publishing dead letter to %q: %w", stream, err)
	}

	// Warn, not Info: a dead letter is an event no handler will ever
	// process, and it needs to be visible without turning on debug logging.
	c.cfg.Telemetry.Logger.Warn("messaging: event dead-lettered",
		"topic", topic, "dlq_stream", stream, "reason", dl.Reason,
		"delivery_attempts", dl.DeliveryAttempts, "error", dl.Error)
	return nil
}

// deliveryAttempt reads the attempt counter the subscriber stamped on the
// message.
//
// A missing, unparseable, or nonsensical value is treated as the first
// delivery. The cap exists to bound redelivery of events that keep failing,
// not to dead-letter an event whose transport metadata was odd — and every
// caller of dispatch outside the router (tests, future tooling) passes nil.
func deliveryAttempt(metadata map[string]string) int64 {
	n, err := strconv.ParseInt(metadata[internalwatermill.MetadataDeliveryAttempt], 10, 64)
	if err != nil || n < 1 {
		return 1
	}
	return n
}

// runWithRetry is PRD §13 Layer 1: immediate in-process retry with full
// jitter, ending in an ack, a dead letter, or a nack.
//
// Every attempt runs inside the message's per-topic concurrency slot, which
// is held for the whole loop. At the default Concurrency of 1 that means a
// retrying message blocks its topic's read loop until the loop finishes.
// That is deliberate rather than overlooked: releasing the slot across the
// sleep would let a later message overtake the retrying one, and per-topic
// ordering at Concurrency 1 is a documented guarantee (PRD §6). The bound
// being per topic is what keeps the stall from reaching other topics.
//
// rec is dispatch's recorder rather than one of this loop's own: the whole
// loop is one delivery, so each of its five outcome-stating branches
// (success, discard, permanent failure, retries exhausted, and abandoned
// mid-backoff) states the outcome for the delivery dispatch already opened
// a span and started a timer for.
func (c *Consumer) runWithRetry(
	ctx context.Context,
	topic string,
	payload []byte,
	attempt int64,
	handler dispatchFunc,
	env Envelope[json.RawMessage],
	rec *deliveryRecorder,
) error {
	log := c.cfg.Telemetry.Logger.With(
		"topic", topic, "event_type", env.EventType,
		"schema_version", env.SchemaVersion, "event_id", env.EventID,
		"delivery_attempt", attempt,
	)

	for i := 0; ; i++ {
		err := handler(ctx, env)

		// Classification is re-read on every attempt rather than decided
		// once from the first error: a handler may fail transiently and
		// then discover the failure is permanent, and the latest verdict is
		// the one that should apply.
		switch {
		case err == nil:
			rec.processed()
			return nil

		case IsDiscard(err):
			log.Warn("messaging: handler discarded event", "error", err)
			rec.skipped(reasonDiscard)
			return nil

		case IsPermanent(err):
			log.Error("messaging: handler returned a permanent error", "error", err)
			rec.failed(categoryPermanent, err)
			dl := c.newDeadLetter(topic, ReasonPermanent, err, attempt)
			dl.Event, dl.EventRaw = deadLetterBody(payload)
			return c.deadLetter(ctx, topic, dl)

		case i >= c.cfg.Retry.MaxImmediateRetries:
			// Nack, not a dead letter. The cap decides when to give up on
			// an event; this loop only decides when to stop trying within
			// one delivery. The entry stays pending and the reclaim loop
			// redelivers it with its counter advanced.
			log.Error("messaging: handler returned error, immediate retries exhausted",
				"immediate_attempts", i+1, "error", err)
			rec.failed(categoryRetryable, err)
			return err
		}

		log.Debug("messaging: retrying handler", "immediate_attempt", i+1, "error", err)
		rec.retry(i+1, err)
		if !sleepWithJitter(ctx, backoffUpperBound(c.cfg.Retry, i)) {
			// Abandoned mid-backoff. Nack so the entry survives.
			rec.failed(categoryRetryable, err)
			return err
		}
	}
}

// sleepWithJitter sleeps a random duration in [0, upper) — PRD §13's full
// jitter — reporting false if ctx was cancelled first.
//
// A cancellation here is not the shutdown drain: message contexts are
// derived from context.WithoutCancel and stay live for the whole
// ShutdownTimeout, so ctx is Done only once the subscriber has closed.
// Retries are therefore never interrupted by a graceful shutdown, which is
// why ShutdownTimeout has to be budgeted with the retry multiplier in mind.
func sleepWithJitter(ctx context.Context, upper time.Duration) bool {
	if upper <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(time.Duration(rand.Int64N(int64(upper))))
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
