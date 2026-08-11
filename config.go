package messaging

import (
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// RedisConfig configures the Redis connection the library uses internally.
// Applications never construct a go-redis client themselves.
type RedisConfig struct {
	Address  string
	Username string
	Password string
	TLS      *tls.Config
	DB       int
	PoolSize int
}

// ConsumerConfig configures a Consumer. Its defaults and validation are
// applied by validateConsumer, which NewConsumer calls after Validate.
// MaxDeliveryAttempts is defaulted here but is not enforced until the
// delivery-attempt cap lands in Phase 2b.
type ConsumerConfig struct {
	// Group is the Redis consumer group name. Required.
	Group string

	// ConsumerName identifies this instance within Group. Defaults to
	// <hostname>-<random suffix>, which stays unique across replicas and
	// restarts on one host.
	ConsumerName string

	// Concurrency bounds how many messages may be in flight at once *per
	// subscribed topic*, not across the consumer as a whole: a consumer
	// registered on three topics at Concurrency 3 can be running nine
	// handlers. The bound is per topic because each topic has its own read
	// loop, and a shared bound would let an idle topic's blocking read
	// throttle a busy one. Defaults to 1, at which per-topic ordering is
	// preserved.
	Concurrency int

	// ClaimInterval is how often the reclaim sweep runs. Defaults to 30s.
	ClaimInterval time.Duration

	// ClaimMinIdle is how long an entry must sit unacknowledged before
	// another consumer may reclaim it. It must be >= ClaimInterval, and it
	// must also exceed the longest a handler is expected to run: at
	// Concurrency > 1 a handler still running after ClaimMinIdle has its
	// own message reclaimed and processed concurrently by this same
	// process. Defaults to 60s.
	ClaimMinIdle time.Duration

	// MaxDeliveryAttempts is defaulted to 5 but is inert until Phase 2b.
	MaxDeliveryAttempts int

	// ShutdownTimeout bounds the drain of in-flight handlers after the Run
	// context is cancelled. Defaults to 30s.
	ShutdownTimeout time.Duration
}

// RetryConfig configures the in-process immediate-retry layer (PRD §13
// Layer 1).
type RetryConfig struct {
	MaxImmediateRetries int
	InitialBackoff      time.Duration
	MaxBackoff          time.Duration
}

// TelemetryConfig configures observability integration. Logger is defaulted
// by Validate and is used throughout the consume path — the subscriber's
// loops, dispatch, and watermill's own router logs all go through it. The
// tracer and meter providers are defaulted but inert: span creation and
// metric emission arrive in Phase 3.
type TelemetryConfig struct {
	TracerProvider trace.TracerProvider
	MeterProvider  metric.MeterProvider
	Logger         *slog.Logger
	LogPayloads    bool
}

// Config is the library's single configuration object. A minimal config is
// three fields: Source, Redis.Address, and — for consumers — Consumer.Group.
// Every other field has a working default.
type Config struct {
	Source       string
	StreamPrefix string
	Redis        RedisConfig
	Consumer     ConsumerConfig
	Retry        RetryConfig
	Telemetry    TelemetryConfig
}

const defaultPoolSizeMultiplier = 10

const (
	defaultConcurrency         = 1
	defaultClaimInterval       = 30 * time.Second
	defaultClaimMinIdle        = 60 * time.Second
	defaultMaxDeliveryAttempts = 5
	defaultShutdownTimeout     = 30 * time.Second
)

// Validate checks required fields and fills in defaults for everything
// else. Called by NewPublisher, and by NewConsumer — which then also calls
// validateConsumer for the consumer-only fields.
func (c *Config) Validate() error {
	if c.Source == "" {
		return fmt.Errorf("config: Source is required")
	}
	if c.Redis.Address == "" {
		return fmt.Errorf("config: Redis.Address is required")
	}

	if c.StreamPrefix == "" {
		c.StreamPrefix = "foi"
	}
	if c.Redis.PoolSize == 0 {
		c.Redis.PoolSize = defaultPoolSizeMultiplier * runtime.GOMAXPROCS(0)
	}
	if c.Retry.MaxImmediateRetries == 0 {
		c.Retry.MaxImmediateRetries = 3
	}
	if c.Retry.InitialBackoff == 0 {
		c.Retry.InitialBackoff = 100 * time.Millisecond
	}
	if c.Retry.MaxBackoff == 0 {
		c.Retry.MaxBackoff = 5 * time.Second
	}
	if c.Telemetry.TracerProvider == nil {
		c.Telemetry.TracerProvider = otel.GetTracerProvider()
	}
	if c.Telemetry.MeterProvider == nil {
		c.Telemetry.MeterProvider = otel.GetMeterProvider()
	}
	if c.Telemetry.Logger == nil {
		c.Telemetry.Logger = slog.Default()
	}

	return nil
}

// validateConsumer checks the consumer-only fields and fills in their
// defaults. It is called by NewConsumer after Validate.
//
// These checks live outside Validate because publisher-only applications
// never set Consumer fields, and requiring a consumer group from them would
// be wrong.
func (c *Config) validateConsumer() error {
	if c.Consumer.Group == "" {
		return fmt.Errorf("config: Consumer.Group is required for consumers")
	}
	if c.Consumer.Concurrency < 0 {
		return fmt.Errorf("config: Consumer.Concurrency must not be negative, got %d", c.Consumer.Concurrency)
	}

	// Negative durations have to be rejected explicitly: zero means "use the
	// default" here, so a negative value would sail past the defaulting and
	// past the ClaimMinIdle >= ClaimInterval check below (any ClaimMinIdle
	// exceeds a negative ClaimInterval), then silently disable the reclaim
	// loop, which starts only when claimInterval > 0. Nacked messages would
	// never be redelivered and nothing would report why.
	if c.Consumer.ClaimInterval < 0 {
		return fmt.Errorf("config: Consumer.ClaimInterval must not be negative, got %v", c.Consumer.ClaimInterval)
	}
	if c.Consumer.ClaimMinIdle < 0 {
		return fmt.Errorf("config: Consumer.ClaimMinIdle must not be negative, got %v", c.Consumer.ClaimMinIdle)
	}
	if c.Consumer.ShutdownTimeout < 0 {
		return fmt.Errorf("config: Consumer.ShutdownTimeout must not be negative, got %v", c.Consumer.ShutdownTimeout)
	}

	if c.Consumer.Concurrency == 0 {
		c.Consumer.Concurrency = defaultConcurrency
	}
	if c.Consumer.ClaimInterval == 0 {
		c.Consumer.ClaimInterval = defaultClaimInterval
	}
	if c.Consumer.ClaimMinIdle == 0 {
		c.Consumer.ClaimMinIdle = defaultClaimMinIdle
	}
	if c.Consumer.MaxDeliveryAttempts == 0 {
		c.Consumer.MaxDeliveryAttempts = defaultMaxDeliveryAttempts
	}
	if c.Consumer.ShutdownTimeout == 0 {
		c.Consumer.ShutdownTimeout = defaultShutdownTimeout
	}

	// Reclaiming sooner than the sweep interval would let a message be
	// claimed while its previous delivery is still legitimately in flight.
	if c.Consumer.ClaimMinIdle < c.Consumer.ClaimInterval {
		return fmt.Errorf(
			"config: Consumer.ClaimMinIdle (%v) must be >= Consumer.ClaimInterval (%v)",
			c.Consumer.ClaimMinIdle, c.Consumer.ClaimInterval,
		)
	}

	if c.Consumer.ConsumerName == "" {
		name, err := defaultConsumerName()
		if err != nil {
			return err
		}
		c.Consumer.ConsumerName = name
	}

	return nil
}

// defaultConsumerName builds a name that identifies the host but stays
// unique across replicas and restarts on that host, so two processes never
// share a Redis consumer identity.
func defaultConsumerName() (string, error) {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}

	suffix := make([]byte, 3)
	if _, err := rand.Read(suffix); err != nil {
		return "", fmt.Errorf("generating consumer name suffix: %w", err)
	}

	return host + "-" + hex.EncodeToString(suffix), nil
}
