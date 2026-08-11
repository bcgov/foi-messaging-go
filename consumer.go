package messaging

import (
	"context"
	"encoding/json"
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
	c.mu.Lock()
	empty := c.registry.isEmpty()
	topics := c.registry.topicList()
	c.mu.Unlock()

	if empty {
		return fmt.Errorf("consumer: no handlers registered; register at least one before calling Run")
	}
	if !c.markRunning() {
		return fmt.Errorf("consumer: Run has already been called")
	}

	client := internalredis.NewClient(redisClientOptions(c.cfg.Redis))
	reader := internalredis.NewStreamReader(client, c.cfg.Consumer.Group, c.cfg.Consumer.ConsumerName)

	c.mu.Lock()
	c.reader = reader
	c.mu.Unlock()

	subscriber, err := internalwatermill.NewSubscriber(internalwatermill.SubscriberOptions{
		Reader:        reader,
		Concurrency:   c.cfg.Consumer.Concurrency,
		ClaimInterval: c.cfg.Consumer.ClaimInterval,
		ClaimMinIdle:  c.cfg.Consumer.ClaimMinIdle,
	})
	if err != nil {
		_ = reader.Close()
		return fmt.Errorf("creating subscriber: %w", err)
	}

	router, err := internalwatermill.NewRouter(c.cfg.Consumer.ShutdownTimeout)
	if err != nil {
		_ = subscriber.Close()
		_ = reader.Close()
		return fmt.Errorf("creating router: %w", err)
	}

	for _, topic := range topics {
		router.AddHandler(topic, c.streamName(topic), subscriber,
			func(ctx context.Context, payload []byte, _ map[string]string) error {
				return c.dispatch(ctx, topic, payload)
			})
	}

	runErr := router.Run(ctx)

	closeErr := router.Close()
	subCloseErr := subscriber.Close()
	readerErr := reader.Close()

	// Everything is already released, so clear c.reader: an unconditionally
	// deferred Close call must be a no-op here, not a second close of an
	// already-closed Redis client.
	c.mu.Lock()
	c.reader = nil
	c.mu.Unlock()

	if runErr != nil {
		return runErr
	}
	if closeErr != nil {
		return fmt.Errorf("closing router: %w", closeErr)
	}
	if subCloseErr != nil {
		return fmt.Errorf("closing subscriber: %w", subCloseErr)
	}
	if readerErr != nil {
		return fmt.Errorf("closing redis client: %w", readerErr)
	}
	return nil
}

// dispatch decodes one message and routes it to its handler.
//
// Returning nil acks the message; returning an error nacks it, leaving the
// entry pending for redelivery. In this phase every failure nacks — error
// classification, the delivery cap, and the DLQ arrive in Phase 2b.
func (c *Consumer) dispatch(ctx context.Context, topic string, payload []byte) error {
	log := c.cfg.Telemetry.Logger

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
	return handler(ctx, env)
}

// streamName maps a logical topic to its Redis stream.
func (c *Consumer) streamName(topic string) string {
	return c.cfg.StreamPrefix + ":" + topic
}

// Close releases resources held by a Consumer that was created but never
// run. After Run returns, everything is already released. Close is
// idempotent and safe to defer unconditionally.
func (c *Consumer) Close() error {
	c.mu.Lock()
	reader := c.reader
	c.reader = nil
	c.mu.Unlock()

	if reader == nil {
		return nil
	}
	return reader.Close()
}
