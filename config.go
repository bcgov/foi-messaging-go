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
	// must also exceed the longest one delivery can occupy its concurrency
	// slot — which, with immediate retry, is
	//
	//	(1 + Retry.MaxImmediateRetries) × handler duration + worst-case backoff
	//
	// not one handler run. A 20s handler at the defaults occupies its slot
	// for up to 80.7s, so the 60s default is already too short for it: the
	// entry is reclaimed and processed concurrently by this same process
	// while the first delivery is still retrying.
	//
	// The backoff term alone is checked at construction; the handler term
	// cannot be. Defaults to 60s.
	ClaimMinIdle time.Duration

	// MaxDeliveryAttempts bounds redeliveries. On the delivery whose
	// attempt exceeds it, the event is dead-lettered and acked before it is
	// even decoded — regardless of how its failures were classified
	// (PRD §13 Layer 3). Attempts 1..MaxDeliveryAttempts dispatch;
	// attempt MaxDeliveryAttempts+1 is dead-lettered. Defaults to 5.
	MaxDeliveryAttempts int

	// ShutdownTimeout bounds the drain of in-flight handlers after the Run
	// context is cancelled.
	//
	// Immediate retry extends the drain: a message that fails on the last
	// attempt before shutdown can still spend
	// Retry.MaxImmediateRetries × handler duration + worst-case backoff
	// finishing, across Concurrency × (number of subscribed topics)
	// messages. Retries are deliberately not interrupted by shutdown —
	// message contexts stay live for the whole drain — so budget for it
	// here. Defaults to 30s.
	ShutdownTimeout time.Duration
}

// RetryConfig configures the in-process immediate-retry layer (PRD §13
// Layer 1): the retries a handler gets within one delivery, before the
// message is nacked and left for the reclaim loop.
//
// Zero values mean "use the default" — 3 retries, 100ms initial, 5s max —
// so there is no way to express "no retries" by zeroing MaxImmediateRetries.
// Set InitialBackoff and MaxBackoff to a nanosecond in tests that need the
// loop to run without waiting.
//
// Retries sleep inside the handler's concurrency slot, so these values
// interact with Consumer.ClaimMinIdle; see its documentation.
type RetryConfig struct {
	// MaxImmediateRetries is the number of retries after the first
	// attempt, so a message gets 1+MaxImmediateRetries invocations per
	// delivery. Defaults to 3.
	MaxImmediateRetries int

	// InitialBackoff is the upper bound of the first retry's jittered
	// sleep, doubling per retry. Defaults to 100ms.
	InitialBackoff time.Duration

	// MaxBackoff caps that doubling. Defaults to 5s.
	MaxBackoff time.Duration
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
	// Zero means "use the default" for all three, so a negative value would
	// otherwise skip defaulting entirely and feed the backoff arithmetic a
	// nonsense bound.
	if c.Retry.MaxImmediateRetries < 0 {
		return fmt.Errorf("config: Retry.MaxImmediateRetries must not be negative, got %d", c.Retry.MaxImmediateRetries)
	}
	if c.Retry.InitialBackoff < 0 {
		return fmt.Errorf("config: Retry.InitialBackoff must not be negative, got %v", c.Retry.InitialBackoff)
	}
	if c.Retry.MaxBackoff < 0 {
		return fmt.Errorf("config: Retry.MaxBackoff must not be negative, got %v", c.Retry.MaxBackoff)
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

	// Layer 1 retry sleeps inside the handler goroutine, which holds the
	// message's per-topic concurrency slot for the whole loop. Backoff
	// therefore counts against ClaimMinIdle exactly as handler runtime
	// does. A config whose backoff alone reaches ClaimMinIdle guarantees
	// the entry is reclaimed — and processed a second time by this very
	// process — before the first delivery has finished sleeping, with zero
	// handler runtime needed to trigger it.
	if wc := worstCaseBackoff(c.Retry); wc >= c.Consumer.ClaimMinIdle {
		return fmt.Errorf(
			"config: worst-case retry backoff (%v) must be < Consumer.ClaimMinIdle (%v); "+
				"lower Retry.MaxImmediateRetries or Retry.MaxBackoff, or raise Consumer.ClaimMinIdle",
			wc, c.Consumer.ClaimMinIdle,
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

// backoffUpperBound returns the upper bound of retry i's jittered sleep:
// InitialBackoff doubled per retry, capped at MaxBackoff (PRD §13 Layer 1).
// i is zero-based, so retry 0 is the sleep before the second attempt.
//
// The retry loop jitters within this bound rather than sleeping it exactly;
// worstCaseBackoff sums it. Both callers go through here so the number
// config validation guards is the number the loop can actually spend.
func backoffUpperBound(r RetryConfig, i int) time.Duration {
	d := r.InitialBackoff
	for range i {
		if d >= r.MaxBackoff {
			return r.MaxBackoff
		}
		d *= 2
	}
	if d > r.MaxBackoff {
		return r.MaxBackoff
	}
	return d
}

// worstCaseBackoff is the total time the retry loop can spend sleeping
// across all immediate retries, taking every jittered sleep at its bound.
//
// It exists because it is the only part of the ClaimMinIdle invariant that
// is computable at construction: handler duration, the other term, is not
// knowable until the handler runs.
func worstCaseBackoff(r RetryConfig) time.Duration {
	var total time.Duration
	for i := range r.MaxImmediateRetries {
		total += backoffUpperBound(r, i)
	}
	return total
}
