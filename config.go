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
	Group               string
	ConsumerName        string
	Concurrency         int
	ClaimInterval       time.Duration
	ClaimMinIdle        time.Duration
	MaxDeliveryAttempts int
	ShutdownTimeout     time.Duration
}

// RetryConfig configures the in-process immediate-retry layer (PRD §13
// Layer 1).
type RetryConfig struct {
	MaxImmediateRetries int
	InitialBackoff      time.Duration
	MaxBackoff          time.Duration
}

// TelemetryConfig configures observability integration. The providers and
// logger are defaulted by Validate but are not yet used by Publish — span
// creation and metric emission are added in a later phase.
type TelemetryConfig struct {
	TracerProvider trace.TracerProvider
	MeterProvider  metric.MeterProvider
	Logger         *slog.Logger
	LogPayloads    bool
}

// Config is the library's single configuration object. A minimal config is
// three fields: Source, Redis.Address, and (for consumers, in a later
// phase) Consumer.Group. Every other field has a working default.
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
// else. Called by NewPublisher; a later consumer phase adds an additional
// Consumer.Group check in NewConsumer.
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
