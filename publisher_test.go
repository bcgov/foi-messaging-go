package messaging

import (
	"context"
	"crypto/tls"
	"reflect"
	"testing"

	"github.com/google/uuid"

	internalredis "github.com/bcgov/foi-messaging-go/internal/redis"
)

func TestWithCorrelationID_SetsOption(t *testing.T) {
	var opts publishOptions
	WithCorrelationID("corr-99")(&opts)

	if opts.correlationID != "corr-99" {
		t.Errorf("correlationID = %q, want %q", opts.correlationID, "corr-99")
	}
}

func TestResolveCorrelationID_PrefersOption(t *testing.T) {
	ctx := contextWithCorrelationID(context.Background(), "from-context")
	opts := publishOptions{correlationID: "from-option"}

	id, err := resolveCorrelationID(ctx, opts)
	if err != nil {
		t.Fatalf("resolveCorrelationID: %v", err)
	}
	if id != "from-option" {
		t.Errorf("id = %q, want %q", id, "from-option")
	}
}

func TestResolveCorrelationID_FallsBackToContext(t *testing.T) {
	ctx := contextWithCorrelationID(context.Background(), "from-context")

	id, err := resolveCorrelationID(ctx, publishOptions{})
	if err != nil {
		t.Fatalf("resolveCorrelationID: %v", err)
	}
	if id != "from-context" {
		t.Errorf("id = %q, want %q", id, "from-context")
	}
}

func TestResolveCorrelationID_GeneratesUUIDv7WhenAbsent(t *testing.T) {
	id, err := resolveCorrelationID(context.Background(), publishOptions{})
	if err != nil {
		t.Fatalf("resolveCorrelationID: %v", err)
	}
	if id == "" {
		t.Fatal("expected a generated correlation ID, got empty string")
	}
	if _, err := uuid.Parse(id); err != nil {
		t.Errorf("generated id %q is not a valid UUID: %v", id, err)
	}
}

func TestRedisClientOptions_MapsAllFields(t *testing.T) {
	tlsCfg := &tls.Config{ServerName: "redis.example.com"}
	rc := RedisConfig{
		Address:  "redis.example.com:6379",
		Username: "app-user",
		Password: "s3cr3t",
		TLS:      tlsCfg,
		DB:       7,
		PoolSize: 42,
	}

	got := redisClientOptions(rc)

	if got.Address != rc.Address {
		t.Errorf("Address = %q, want %q", got.Address, rc.Address)
	}
	if got.Username != rc.Username {
		t.Errorf("Username = %q, want %q", got.Username, rc.Username)
	}
	if got.Password != rc.Password {
		t.Errorf("Password = %q, want %q", got.Password, rc.Password)
	}
	if got.TLS != tlsCfg {
		t.Errorf("TLS pointer = %p, want %p (same *tls.Config)", got.TLS, tlsCfg)
	}
	if got.DB != rc.DB {
		t.Errorf("DB = %d, want %d", got.DB, rc.DB)
	}
	if got.PoolSize != rc.PoolSize {
		t.Errorf("PoolSize = %d, want %d", got.PoolSize, rc.PoolSize)
	}

	if !reflect.DeepEqual(got, internalredis.ClientOptions{
		Address:  rc.Address,
		Username: rc.Username,
		Password: rc.Password,
		TLS:      tlsCfg,
		DB:       rc.DB,
		PoolSize: rc.PoolSize,
	}) {
		t.Errorf("redisClientOptions(%+v) = %+v, want a field-for-field match", rc, got)
	}
}
