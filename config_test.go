package messaging

import (
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestConfigValidate_RequiredFields(t *testing.T) {
	cases := []struct {
		name string
		cfg  Config
	}{
		{"missing Source", Config{Redis: RedisConfig{Address: "localhost:6379"}}},
		{"missing Redis.Address", Config{Source: "test.service"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.cfg.Validate(); err == nil {
				t.Error("expected an error, got nil")
			}
		})
	}
}

func TestConfigValidate_Defaults(t *testing.T) {
	cfg := Config{
		Source: "test.service",
		Redis:  RedisConfig{Address: "localhost:6379"},
	}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	if cfg.StreamPrefix != "foi" {
		t.Errorf("StreamPrefix = %q, want %q", cfg.StreamPrefix, "foi")
	}
	if want := 10 * runtime.GOMAXPROCS(0); cfg.Redis.PoolSize != want {
		t.Errorf("Redis.PoolSize = %d, want %d", cfg.Redis.PoolSize, want)
	}
	if cfg.Retry.MaxImmediateRetries != 3 {
		t.Errorf("Retry.MaxImmediateRetries = %d, want 3", cfg.Retry.MaxImmediateRetries)
	}
	if cfg.Retry.InitialBackoff != 100*time.Millisecond {
		t.Errorf("Retry.InitialBackoff = %v, want 100ms", cfg.Retry.InitialBackoff)
	}
	if cfg.Retry.MaxBackoff != 5*time.Second {
		t.Errorf("Retry.MaxBackoff = %v, want 5s", cfg.Retry.MaxBackoff)
	}
	if cfg.Telemetry.TracerProvider == nil {
		t.Error("Telemetry.TracerProvider is nil, want a default provider")
	}
	if cfg.Telemetry.MeterProvider == nil {
		t.Error("Telemetry.MeterProvider is nil, want a default provider")
	}
	if cfg.Telemetry.Logger == nil {
		t.Error("Telemetry.Logger is nil, want slog.Default()")
	}
}

func TestConfigValidate_PreservesExplicitValues(t *testing.T) {
	cfg := Config{
		Source:       "test.service",
		StreamPrefix: "custom",
		Redis:        RedisConfig{Address: "localhost:6379", PoolSize: 42},
		Retry:        RetryConfig{MaxImmediateRetries: 7},
	}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	if cfg.StreamPrefix != "custom" {
		t.Errorf("StreamPrefix = %q, want %q", cfg.StreamPrefix, "custom")
	}
	if cfg.Redis.PoolSize != 42 {
		t.Errorf("Redis.PoolSize = %d, want 42", cfg.Redis.PoolSize)
	}
	if cfg.Retry.MaxImmediateRetries != 7 {
		t.Errorf("Retry.MaxImmediateRetries = %d, want 7", cfg.Retry.MaxImmediateRetries)
	}
}

func TestValidateConsumer_AppliesDefaults(t *testing.T) {
	cfg := Config{
		Source:   "test.service",
		Redis:    RedisConfig{Address: "localhost:6379"},
		Consumer: ConsumerConfig{Group: "test-group"},
	}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if err := cfg.validateConsumer(); err != nil {
		t.Fatalf("validateConsumer: %v", err)
	}

	if cfg.Consumer.Concurrency != 1 {
		t.Errorf("Concurrency = %d, want 1", cfg.Consumer.Concurrency)
	}
	if cfg.Consumer.ClaimInterval != 30*time.Second {
		t.Errorf("ClaimInterval = %v, want 30s", cfg.Consumer.ClaimInterval)
	}
	if cfg.Consumer.ClaimMinIdle != 60*time.Second {
		t.Errorf("ClaimMinIdle = %v, want 60s", cfg.Consumer.ClaimMinIdle)
	}
	if cfg.Consumer.MaxDeliveryAttempts != 5 {
		t.Errorf("MaxDeliveryAttempts = %d, want 5", cfg.Consumer.MaxDeliveryAttempts)
	}
	if cfg.Consumer.ShutdownTimeout != 30*time.Second {
		t.Errorf("ShutdownTimeout = %v, want 30s", cfg.Consumer.ShutdownTimeout)
	}
	if cfg.Consumer.ConsumerName == "" {
		t.Error("ConsumerName must be defaulted")
	}
}

func TestValidateConsumer_GeneratesDistinctConsumerNames(t *testing.T) {
	newCfg := func() Config {
		return Config{
			Source:   "test.service",
			Redis:    RedisConfig{Address: "localhost:6379"},
			Consumer: ConsumerConfig{Group: "test-group"},
		}
	}

	a, b := newCfg(), newCfg()
	if err := a.validateConsumer(); err != nil {
		t.Fatalf("validateConsumer a: %v", err)
	}
	if err := b.validateConsumer(); err != nil {
		t.Fatalf("validateConsumer b: %v", err)
	}

	if a.Consumer.ConsumerName == b.Consumer.ConsumerName {
		t.Errorf("two consumers on one host got the same name %q; they must be distinct", a.Consumer.ConsumerName)
	}
}

func TestValidateConsumer_RequiresGroup(t *testing.T) {
	cfg := Config{
		Source: "test.service",
		Redis:  RedisConfig{Address: "localhost:6379"},
	}

	err := cfg.validateConsumer()
	if err == nil {
		t.Fatal("expected an error when Consumer.Group is empty")
	}
	if !strings.Contains(err.Error(), "Consumer.Group") {
		t.Errorf("error = %q, want it to name Consumer.Group", err)
	}
}

func TestValidateConsumer_RejectsClaimMinIdleBelowClaimInterval(t *testing.T) {
	cfg := Config{
		Source: "test.service",
		Redis:  RedisConfig{Address: "localhost:6379"},
		Consumer: ConsumerConfig{
			Group:         "test-group",
			ClaimInterval: 60 * time.Second,
			ClaimMinIdle:  30 * time.Second,
		},
	}

	err := cfg.validateConsumer()
	if err == nil {
		t.Fatal("expected an error when ClaimMinIdle < ClaimInterval")
	}
	if !strings.Contains(err.Error(), "ClaimMinIdle") {
		t.Errorf("error = %q, want it to name ClaimMinIdle", err)
	}
}

func TestValidateConsumer_RejectsNegativeConcurrency(t *testing.T) {
	cfg := Config{
		Source: "test.service",
		Redis:  RedisConfig{Address: "localhost:6379"},
		Consumer: ConsumerConfig{
			Group:       "test-group",
			Concurrency: -1,
		},
	}

	err := cfg.validateConsumer()
	if err == nil {
		t.Fatal("expected an error for negative Concurrency")
	}
	if !strings.Contains(err.Error(), "Consumer.Concurrency") {
		t.Errorf("error = %q, want it to name Consumer.Concurrency", err)
	}
}

// TestValidateConsumer_RejectsNegativeDurations covers the gap that let a
// negative ClaimInterval through: zero means "use the default", so a
// negative value skipped defaulting, then passed the
// ClaimMinIdle >= ClaimInterval check (any ClaimMinIdle beats a negative
// interval), and finally failed the subscriber's `claimInterval > 0` guard
// — silently disabling reclaim, so nacked messages were never redelivered
// and nothing reported it.
func TestValidateConsumer_RejectsNegativeDurations(t *testing.T) {
	cases := []struct {
		name     string
		mutate   func(*ConsumerConfig)
		wantName string
	}{
		{"claim interval", func(c *ConsumerConfig) { c.ClaimInterval = -time.Second }, "Consumer.ClaimInterval"},
		{"claim min idle", func(c *ConsumerConfig) { c.ClaimMinIdle = -time.Second }, "Consumer.ClaimMinIdle"},
		{"shutdown timeout", func(c *ConsumerConfig) { c.ShutdownTimeout = -time.Second }, "Consumer.ShutdownTimeout"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{
				Source:   "test.service",
				Redis:    RedisConfig{Address: "localhost:6379"},
				Consumer: ConsumerConfig{Group: "test-group"},
			}
			tc.mutate(&cfg.Consumer)

			err := cfg.validateConsumer()
			if err == nil {
				t.Fatalf("expected an error for a negative %s", tc.name)
			}
			if !strings.Contains(err.Error(), tc.wantName) {
				t.Errorf("error = %q, want it to name %s", err, tc.wantName)
			}
		})
	}
}

func TestValidate_LeavesConsumerFieldsAloneForPublishers(t *testing.T) {
	cfg := Config{
		Source: "test.service",
		Redis:  RedisConfig{Address: "localhost:6379"},
	}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	// A publisher-only config must not be forced to supply a consumer group.
	if cfg.Consumer.Group != "" {
		t.Errorf("Consumer.Group = %q, want empty", cfg.Consumer.Group)
	}
	if cfg.Consumer.Concurrency != 0 {
		t.Errorf("Consumer.Concurrency = %d, want 0 — Validate must not default consumer fields", cfg.Consumer.Concurrency)
	}
}

func TestBackoffUpperBound_DoublesThenCaps(t *testing.T) {
	r := RetryConfig{
		MaxImmediateRetries: 6,
		InitialBackoff:      100 * time.Millisecond,
		MaxBackoff:          500 * time.Millisecond,
	}

	want := []time.Duration{
		100 * time.Millisecond,
		200 * time.Millisecond,
		400 * time.Millisecond,
		500 * time.Millisecond, // capped
		500 * time.Millisecond,
		500 * time.Millisecond,
	}
	for i, w := range want {
		if got := backoffUpperBound(r, i); got != w {
			t.Errorf("backoffUpperBound(%d) = %v, want %v", i, got, w)
		}
	}
}

func TestWorstCaseBackoff_SumsTheDefaults(t *testing.T) {
	// The library defaults: 100ms + 200ms + 400ms across three retries.
	r := RetryConfig{
		MaxImmediateRetries: 3,
		InitialBackoff:      100 * time.Millisecond,
		MaxBackoff:          5 * time.Second,
	}
	if got, want := worstCaseBackoff(r), 700*time.Millisecond; got != want {
		t.Errorf("worstCaseBackoff = %v, want %v", got, want)
	}
}

func TestWorstCaseBackoff_ZeroRetriesIsZero(t *testing.T) {
	r := RetryConfig{MaxImmediateRetries: 0, InitialBackoff: time.Second, MaxBackoff: time.Minute}
	if got := worstCaseBackoff(r); got != 0 {
		t.Errorf("worstCaseBackoff = %v, want 0", got)
	}
}

func TestValidateConsumer_RejectsBackoffReachingClaimMinIdle(t *testing.T) {
	// 10 retries capped at 30s is ~150s of backoff against a 60s
	// ClaimMinIdle: the entry is reclaimed and processed a second time by
	// this same process before the first delivery has finished sleeping.
	cfg := Config{
		Source: "test.service",
		Redis:  RedisConfig{Address: "localhost:6379"},
		Consumer: ConsumerConfig{
			Group:        "test-group",
			ClaimMinIdle: 60 * time.Second,
		},
		Retry: RetryConfig{
			MaxImmediateRetries: 10,
			InitialBackoff:      time.Second,
			MaxBackoff:          30 * time.Second,
		},
	}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	err := cfg.validateConsumer()
	if err == nil {
		t.Fatal("expected worst-case backoff exceeding ClaimMinIdle to be rejected")
	}
	if !strings.Contains(err.Error(), "ClaimMinIdle") {
		t.Errorf("error should name ClaimMinIdle, got %q", err)
	}
}

func TestValidateConsumer_AcceptsDefaultBackoffAgainstDefaultClaimMinIdle(t *testing.T) {
	cfg := Config{
		Source:   "test.service",
		Redis:    RedisConfig{Address: "localhost:6379"},
		Consumer: ConsumerConfig{Group: "test-group"},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if err := cfg.validateConsumer(); err != nil {
		t.Fatalf("the library's own defaults must validate: %v", err)
	}
}

func TestValidate_RejectsNegativeRetryFields(t *testing.T) {
	// Zero means "use the default" for every field in this config, so a
	// negative value would otherwise sail past defaulting into the backoff
	// arithmetic.
	tests := []struct {
		name  string
		retry RetryConfig
		field string
	}{
		{"retries", RetryConfig{MaxImmediateRetries: -1}, "MaxImmediateRetries"},
		{"initial", RetryConfig{InitialBackoff: -time.Second}, "InitialBackoff"},
		{"max", RetryConfig{MaxBackoff: -time.Second}, "MaxBackoff"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{
				Source: "test.service",
				Redis:  RedisConfig{Address: "localhost:6379"},
				Retry:  tc.retry,
			}
			err := cfg.Validate()
			if err == nil {
				t.Fatalf("expected a negative %s to be rejected", tc.field)
			}
			if !strings.Contains(err.Error(), tc.field) {
				t.Errorf("error should name %s, got %q", tc.field, err)
			}
		})
	}
}
