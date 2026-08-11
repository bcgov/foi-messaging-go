package messaging

import (
	"runtime"
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
