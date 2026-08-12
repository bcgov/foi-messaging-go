package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

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
	cfg    Config
	wm     *internalwatermill.Publisher
	inst   *instruments
	tracer trace.Tracer

	// publishFn is the transport write. It defaults to wm.Publish and is
	// replaced in tests: the telemetry around a publish — span status,
	// failure stage attribution — is most interesting on the failure
	// paths, which are the hardest to provoke against a live broker.
	publishFn func(ctx context.Context, stream, id string, body []byte, metadata map[string]string) error
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

	p := &Publisher{
		cfg:    cfg,
		wm:     wmPublisher,
		inst:   newInstruments(cfg.Telemetry.MeterProvider, cfg.Telemetry.Logger),
		tracer: cfg.Telemetry.TracerProvider.Tracer(telemetryScope),
	}
	p.publishFn = p.wm.Publish
	return p, nil
}

// Publish builds a standard envelope around payload and writes it to the
// stream named by cfg.StreamPrefix + ":" + def.Topic. Errors are returned
// synchronously; the library does not buffer or retry publishes.
func (p *Publisher) Publish(ctx context.Context, def EventDef, payload any, opts ...PublishOption) (PublishResult, error) {
	if def.Topic == "" {
		return PublishResult{}, fmt.Errorf("event def: topic is required")
	}

	stream := p.cfg.StreamPrefix + ":" + def.Topic

	// The span opens before validation and marshalling, not just around
	// the transport write, so a rejected envelope is as visible in a trace
	// as an unreachable Redis is — and so the span and
	// publish.failures{stage} describe the same call.
	ctx, span := p.tracer.Start(ctx, "publish "+def.Topic,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "redis"),
			attribute.String("messaging.operation.name", "publish"),
			attribute.String("messaging.destination.name", stream),
			attribute.String("messaging.foi.event_type", def.Type),
			attribute.String("messaging.foi.schema_version", def.Version),
		))
	defer span.End()

	attrs := []attribute.KeyValue{
		attribute.String(attrTopic, def.Topic),
		attribute.String(attrEventType, def.Type),
	}

	fail := func(stage string, err error) (PublishResult, error) {
		p.inst.publishFailures.Add(ctx, 1, metric.WithAttributes(
			append(attrs, attribute.String(attrStage, stage))...))
		span.RecordError(err)
		span.SetStatus(codes.Error, stage)
		return PublishResult{}, err
	}

	var options publishOptions
	for _, opt := range opts {
		opt(&options)
	}

	correlationID, err := resolveCorrelationID(ctx, options)
	if err != nil {
		return fail(stageValidation, err)
	}

	env, err := newEnvelope(def, p.cfg.Source, correlationID, payload)
	if err != nil {
		return fail(stageValidation, err)
	}

	if err := validateEnvelope(env); err != nil {
		return fail(stageValidation, err)
	}

	span.SetAttributes(
		attribute.String("messaging.message.id", env.EventID),
		attribute.String("messaging.foi.correlation_id", env.CorrelationID),
	)

	body, err := json.Marshal(env)
	if err != nil {
		return fail(stageMarshal, fmt.Errorf("marshaling envelope: %w", err))
	}

	// Transport metadata (PRD §5): trace context so the consumer span can
	// parent to this one, and published_at so queue latency is measurable
	// separately from handler duration. Neither is ever merged into the
	// envelope.
	metadata := map[string]string{metadataPublishedAt: publishedAtNow()}
	p.cfg.Telemetry.Propagator.Inject(ctx, propagation.MapCarrier(metadata))

	if err := p.publishFn(ctx, stream, env.EventID, body, metadata); err != nil {
		return fail(stageTransport, fmt.Errorf("publishing to stream %q: %w", stream, err))
	}

	p.inst.published.Add(ctx, 1, metric.WithAttributes(attrs...))
	span.SetStatus(codes.Ok, "")

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
