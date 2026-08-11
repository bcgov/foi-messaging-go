package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	internalredis "github.com/bcgov/foi-messaging-go/internal/redis"
	internalwatermill "github.com/bcgov/foi-messaging-go/internal/watermill"
)

// PublishResult is returned by a successful Publish.
type PublishResult struct {
	EventID   string
	Timestamp time.Time
}

type publishOptions struct {
	correlationID string
}

// PublishOption customizes a single Publish call.
type PublishOption func(*publishOptions)

// WithCorrelationID explicitly sets the correlation ID for a publish call,
// taking priority over any correlation ID carried on the context.
func WithCorrelationID(id string) PublishOption {
	return func(o *publishOptions) {
		o.correlationID = id
	}
}

func resolveCorrelationID(ctx context.Context, opts publishOptions) (string, error) {
	if opts.correlationID != "" {
		return opts.correlationID, nil
	}
	if id, ok := correlationIDFromContext(ctx); ok && id != "" {
		return id, nil
	}
	id, err := uuid.NewV7()
	if err != nil {
		return "", fmt.Errorf("generating correlation id: %w", err)
	}
	return id.String(), nil
}

// Publisher publishes typed payloads to Redis streams without exposing
// Watermill or go-redis to callers.
type Publisher struct {
	cfg Config
	wm  *internalwatermill.Publisher
}

// NewPublisher validates cfg and builds a Publisher backed by it.
func NewPublisher(cfg Config) (*Publisher, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	client := internalredis.NewClient(redisClientOptions(cfg.Redis))

	wmPublisher, err := internalwatermill.NewPublisher(client)
	if err != nil {
		return nil, fmt.Errorf("creating watermill publisher: %w", err)
	}

	return &Publisher{cfg: cfg, wm: wmPublisher}, nil
}

// Publish builds a standard envelope around payload and writes it to the
// stream named by cfg.StreamPrefix + ":" + def.Topic. Errors are returned
// synchronously; the library does not buffer or retry publishes.
func (p *Publisher) Publish(ctx context.Context, def EventDef, payload any, opts ...PublishOption) (PublishResult, error) {
	if def.Topic == "" {
		return PublishResult{}, fmt.Errorf("event def: topic is required")
	}

	var options publishOptions
	for _, opt := range opts {
		opt(&options)
	}

	correlationID, err := resolveCorrelationID(ctx, options)
	if err != nil {
		return PublishResult{}, err
	}

	env, err := newEnvelope(def, p.cfg.Source, correlationID, payload)
	if err != nil {
		return PublishResult{}, err
	}

	if err := validateEnvelope(env); err != nil {
		return PublishResult{}, err
	}

	body, err := json.Marshal(env)
	if err != nil {
		return PublishResult{}, fmt.Errorf("marshaling envelope: %w", err)
	}

	stream := p.cfg.StreamPrefix + ":" + def.Topic
	if err := p.wm.Publish(ctx, stream, env.EventID, body, map[string]string{}); err != nil {
		return PublishResult{}, fmt.Errorf("publishing to stream %q: %w", stream, err)
	}

	return PublishResult{EventID: env.EventID, Timestamp: env.Timestamp}, nil
}

// Close releases the Publisher's underlying resources.
func (p *Publisher) Close() error {
	return p.wm.Close()
}

// redisClientOptions maps a RedisConfig to the internal/redis client
// options, so the field mapping can be tested independently of a real
// Redis connection.
func redisClientOptions(c RedisConfig) internalredis.ClientOptions {
	return internalredis.ClientOptions{
		Address:  c.Address,
		Username: c.Username,
		Password: c.Password,
		TLS:      c.TLS,
		DB:       c.DB,
		PoolSize: c.PoolSize,
	}
}
