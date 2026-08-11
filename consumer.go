package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	internalredis "github.com/bcgov/foi-messaging-go/internal/redis"
	internalwatermill "github.com/bcgov/foi-messaging-go/internal/watermill"
)

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

	reader *internalredis.StreamReader
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

	return &Consumer{cfg: cfg, registry: newRegistry()}, nil
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

	c.mu.Lock()
	c.reader = reader
	c.mu.Unlock()

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
	c.mu.Unlock()
	return err
}

// dispatch decodes one message and routes it to its handler.
//
// metadata carries the transport-only fields the subscriber stamped on the
// message — the Redis entry ID and the delivery attempt. They are logged,
// never merged into the envelope: PRD §5 keeps transport state out of the
// event contract. Phase 2b reads the delivery attempt here for the cap.
//
// Returning nil acks the message; returning an error nacks it, leaving the
// entry pending for redelivery. In this phase every failure nacks — error
// classification, the delivery cap, and the DLQ arrive in Phase 2b.
func (c *Consumer) dispatch(ctx context.Context, topic string, payload []byte, metadata map[string]string) error {
	log := c.cfg.Telemetry.Logger.With(
		"stream_id", metadata[internalwatermill.MetadataStreamID],
		"delivery_attempt", metadata[internalwatermill.MetadataDeliveryAttempt],
	)

	var env Envelope[json.RawMessage]
	if err := json.Unmarshal(payload, &env); err != nil {
		log.Error("messaging: undecodable event envelope",
			"topic", topic, "error", err)
		return fmt.Errorf("unmarshalling envelope on topic %q: %w", topic, err)
	}

	if err := validateEnvelope(env); err != nil {
		log.Error("messaging: invalid event envelope",
			"topic", topic, "event_id", env.EventID, "error", err)
		return fmt.Errorf("validating envelope on topic %q: %w", topic, err)
	}

	major, err := majorVersion(env.SchemaVersion)
	if err != nil {
		log.Error("messaging: unparseable schema version",
			"topic", topic, "event_id", env.EventID, "error", err)
		return fmt.Errorf("parsing schema version on topic %q: %w", topic, err)
	}

	c.mu.Lock()
	handler, ok := c.registry.lookup(topic, env.EventType, major)
	c.mu.Unlock()

	if !ok {
		// Topics are shared and services consume only the event types they
		// care about, so an unmatched event is a normal outcome, not a
		// failure. Phase 3 counts these as skipped{reason="no_handler"}.
		log.Debug("messaging: no handler for event",
			"topic", topic, "event_type", env.EventType,
			"schema_version", env.SchemaVersion, "event_id", env.EventID)
		return nil
	}

	ctx = contextWithCorrelationID(ctx, env.CorrelationID)
	if err := handler(ctx, env); err != nil {
		// Logged here so a handler failure reaches the same slog.Logger
		// as the other three nack paths above, rather than only
		// watermill's own "Handler returned error".
		log.Error("messaging: handler returned error",
			"topic", topic, "event_type", env.EventType,
			"schema_version", env.SchemaVersion, "event_id", env.EventID,
			"error", err)
		return err
	}
	return nil
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
