package messaging

import (
	"crypto/tls"
	"fmt"
	"log/slog"
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

// ConsumerConfig configures a Consumer. Its defaulting and Group-required
// validation are added in a later phase alongside NewConsumer; the struct
// exists now so Config compiles as documented in PRD §17.
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
