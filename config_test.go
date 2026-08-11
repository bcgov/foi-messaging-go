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

	if err := cfg.validateConsumer(); err == nil {
		t.Fatal("expected an error for negative Concurrency")
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
