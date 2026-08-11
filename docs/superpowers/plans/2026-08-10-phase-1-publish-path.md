# Phase 1: Publish Path Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement `EventDef`, `Envelope[T]`, `Config`, and a `Publisher` that genuinely publishes to a Redis stream via Watermill, replacing the Phase 0 stub files with real logic for the write path only.

**Architecture:** The root `messaging` package builds and validates an `Envelope[T]` and hands raw bytes to `internal/watermill`, which wraps `watermill-redisstream` using a `*redis.Client` built by `internal/redis`. No `watermill` or `go-redis` type ever crosses into the root package — `internal/watermill`'s `Publish` method takes plain `(topic, id string, payload []byte, metadata map[string]string)`.

**Tech Stack:** Go 1.25 (per current `go.mod`), `github.com/ThreeDotsLabs/watermill`, `github.com/ThreeDotsLabs/watermill-redisstream`, `github.com/redis/go-redis/v9`, `github.com/google/uuid` (already an indirect dependency, promoted to direct).

## Global Constraints

- Module path: `github.com/bcgov/foi-messaging-go` (unchanged)
- No consumer, `Handler[T]`, `RegisterHandler`, routing, retry, DLQ, or error-classification wrappers in this phase — write path only
- No OpenTelemetry span creation or Prometheus metric emission on `Publish` this phase — `Config.Telemetry` carries defaulted providers but they are unused
- No `testing/` package (`messagingtest.Publisher`) this phase
- `depguard` must continue to block `github.com/ThreeDotsLabs/watermill`, `github.com/ThreeDotsLabs/watermill-redisstream`, and `github.com/redis/go-redis` imports outside `internal/` — add the missing `watermill-redisstream` entry
- `internal/watermill`'s public API never exposes a `watermill.Message` or any go-redis type
- Stream name is `{Config.StreamPrefix}:{EventDef.Topic}` (default prefix `"foi"`)

---

## Task 1: EventDef, Envelope, and validation

**Files:**
- Create: `eventdef.go`
- Create: `envelope.go`
- Create: `validation.go`
- Test: `envelope_test.go`
- Test: `validation_test.go`
- Modify: `go.mod`, `go.sum` (promote `github.com/google/uuid` to a direct dependency)

**Interfaces:**
- Consumes: nothing (first task)
- Produces:
  - `type EventDef struct { Topic, Type, Version string }`
  - `type Envelope[T any] struct { EventID, EventType string; Timestamp time.Time; SchemaVersion, CorrelationID, Source string; Payload T }`
  - `func newEnvelope[T any](def EventDef, source string, correlationID string, payload T) (Envelope[T], error)`
  - `func validateEnvelope[T any](e Envelope[T]) error`

  Task 6 calls all three; Task 7's integration test relies on `Envelope[T]`'s JSON tags to decode the raw stream payload.

- [ ] **Step 1: Write the failing envelope test**

Create `envelope_test.go`:
```go
package messaging

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

type testPayload struct {
	Name string
}

func TestNewEnvelope_PopulatesFields(t *testing.T) {
	def := EventDef{Topic: "documents", Type: "document.created", Version: "1.0.0"}
	before := time.Now().UTC()

	env, err := newEnvelope(def, "test.service", "corr-1", testPayload{Name: "report.pdf"})
	if err != nil {
		t.Fatalf("newEnvelope: %v", err)
	}

	if env.EventID == "" {
		t.Error("EventID is empty")
	}
	if _, err := uuid.Parse(env.EventID); err != nil {
		t.Errorf("EventID %q is not a valid UUID: %v", env.EventID, err)
	}
	if env.EventType != "document.created" {
		t.Errorf("EventType = %q, want %q", env.EventType, "document.created")
	}
	if env.SchemaVersion != "1.0.0" {
		t.Errorf("SchemaVersion = %q, want %q", env.SchemaVersion, "1.0.0")
	}
	if env.CorrelationID != "corr-1" {
		t.Errorf("CorrelationID = %q, want %q", env.CorrelationID, "corr-1")
	}
	if env.Source != "test.service" {
		t.Errorf("Source = %q, want %q", env.Source, "test.service")
	}
	if env.Payload.Name != "report.pdf" {
		t.Errorf("Payload.Name = %q, want %q", env.Payload.Name, "report.pdf")
	}
	if env.Timestamp.Before(before) {
		t.Errorf("Timestamp %v is before call time %v", env.Timestamp, before)
	}
}

func TestNewEnvelope_GeneratesUniqueEventIDs(t *testing.T) {
	def := EventDef{Topic: "documents", Type: "document.created", Version: "1.0.0"}

	env1, err := newEnvelope(def, "test.service", "corr-1", testPayload{})
	if err != nil {
		t.Fatalf("newEnvelope: %v", err)
	}
	env2, err := newEnvelope(def, "test.service", "corr-1", testPayload{})
	if err != nil {
		t.Fatalf("newEnvelope: %v", err)
	}

	if env1.EventID == env2.EventID {
		t.Errorf("expected unique EventIDs, got the same value twice: %q", env1.EventID)
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./... -run TestNewEnvelope -v`
Expected: FAIL — `undefined: EventDef` / `undefined: newEnvelope` (types don't exist yet).

- [ ] **Step 3: Create eventdef.go**

```go
package messaging

// EventDef identifies an event contract: which topic it publishes on,
// its event type, and its schema version. Declared once per event in the
// contract package that owns the payload type, and shared by publishers
// and consumers.
type EventDef struct {
	Topic   string
	Type    string
	Version string
}
```

- [ ] **Step 4: Create envelope.go**

```go
package messaging

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Envelope wraps every published and consumed event. It carries business
// and workflow metadata only — transport state (retry counters, delivery
// attempts, trace context) travels in message metadata, never here.
type Envelope[T any] struct {
	EventID       string    `json:"event_id"`
	EventType     string    `json:"event_type"`
	Timestamp     time.Time `json:"timestamp"`
	SchemaVersion string    `json:"schema_version"`
	CorrelationID string    `json:"correlation_id"`
	Source        string    `json:"source"`
	Payload       T         `json:"payload"`
}

func newEnvelope[T any](def EventDef, source string, correlationID string, payload T) (Envelope[T], error) {
	id, err := uuid.NewV7()
	if err != nil {
		return Envelope[T]{}, fmt.Errorf("generating event id: %w", err)
	}

	return Envelope[T]{
		EventID:       id.String(),
		EventType:     def.Type,
		Timestamp:     time.Now().UTC(),
		SchemaVersion: def.Version,
		CorrelationID: correlationID,
		Source:        source,
		Payload:       payload,
	}, nil
}
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `go test ./... -run TestNewEnvelope -v`
Expected: PASS

- [ ] **Step 6: Write the failing validation test**

Create `validation_test.go`:
```go
package messaging

import (
	"testing"
	"time"
)

func validEnvelope() Envelope[testPayload] {
	return Envelope[testPayload]{
		EventID:       "018f2e7a-1c6b-7c0a-9f8d-3e4a2b1c5d90",
		EventType:     "document.created",
		Timestamp:     time.Now().UTC(),
		SchemaVersion: "1.0.0",
		CorrelationID: "corr-1",
		Source:        "test.service",
		Payload:       testPayload{Name: "report.pdf"},
	}
}

func TestValidateEnvelope_Valid(t *testing.T) {
	if err := validateEnvelope(validEnvelope()); err != nil {
		t.Errorf("expected valid envelope to pass, got error: %v", err)
	}
}

func TestValidateEnvelope_Invalid(t *testing.T) {
	cases := []struct {
		name   string
		modify func(*Envelope[testPayload])
	}{
		{"empty EventID", func(e *Envelope[testPayload]) { e.EventID = "" }},
		{"empty EventType", func(e *Envelope[testPayload]) { e.EventType = "" }},
		{"single-segment EventType", func(e *Envelope[testPayload]) { e.EventType = "created" }},
		{"four-segment EventType", func(e *Envelope[testPayload]) { e.EventType = "a.b.c.d" }},
		{"uppercase EventType", func(e *Envelope[testPayload]) { e.EventType = "Document.Created" }},
		{"empty SchemaVersion", func(e *Envelope[testPayload]) { e.SchemaVersion = "" }},
		{"two-part SchemaVersion", func(e *Envelope[testPayload]) { e.SchemaVersion = "1.0" }},
		{"v-prefixed SchemaVersion", func(e *Envelope[testPayload]) { e.SchemaVersion = "v1.0.0" }},
		{"empty Source", func(e *Envelope[testPayload]) { e.Source = "" }},
		{"empty CorrelationID", func(e *Envelope[testPayload]) { e.CorrelationID = "" }},
		{"zero Timestamp", func(e *Envelope[testPayload]) { e.Timestamp = time.Time{} }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := validEnvelope()
			tc.modify(&env)
			if err := validateEnvelope(env); err == nil {
				t.Errorf("expected an error for %s, got nil", tc.name)
			}
		})
	}
}
```

- [ ] **Step 7: Run the test to verify it fails**

Run: `go test ./... -run TestValidateEnvelope -v`
Expected: FAIL — `undefined: validateEnvelope`

- [ ] **Step 8: Create validation.go**

```go
package messaging

import (
	"fmt"
	"regexp"
)

var (
	eventTypePattern     = regexp.MustCompile(`^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*){1,2}$`)
	schemaVersionPattern = regexp.MustCompile(`^\d+\.\d+\.\d+$`)
)

// validateEnvelope checks required fields, event_type format, schema_version
// format, and timestamp presence. Used at publish time (Task 6) and, in a
// later phase, at consume time.
func validateEnvelope[T any](e Envelope[T]) error {
	if e.EventID == "" {
		return fmt.Errorf("envelope: event_id is required")
	}
	if e.EventType == "" {
		return fmt.Errorf("envelope: event_type is required")
	}
	if !eventTypePattern.MatchString(e.EventType) {
		return fmt.Errorf("envelope: event_type %q must be 2-3 dot-separated lowercase segments", e.EventType)
	}
	if e.SchemaVersion == "" {
		return fmt.Errorf("envelope: schema_version is required")
	}
	if !schemaVersionPattern.MatchString(e.SchemaVersion) {
		return fmt.Errorf("envelope: schema_version %q must be MAJOR.MINOR.PATCH", e.SchemaVersion)
	}
	if e.Source == "" {
		return fmt.Errorf("envelope: source is required")
	}
	if e.CorrelationID == "" {
		return fmt.Errorf("envelope: correlation_id is required")
	}
	if e.Timestamp.IsZero() {
		return fmt.Errorf("envelope: timestamp is required")
	}
	return nil
}
```

- [ ] **Step 9: Run the tests to verify they pass**

Run: `go test ./... -run 'TestNewEnvelope|TestValidateEnvelope' -v`
Expected: PASS for all subtests.

- [ ] **Step 10: Tidy go.mod and verify the full build**

Run:
```bash
go mod tidy
go build ./...
go vet ./...
```
Expected: all exit 0. `go.mod` now lists `github.com/google/uuid` as a direct dependency (no longer `// indirect`).

- [ ] **Step 11: Commit**

```bash
git add eventdef.go envelope.go validation.go envelope_test.go validation_test.go go.mod go.sum
git commit -m "feat: implement EventDef, Envelope, and envelope validation"
```

---

## Task 2: Correlation ID context helpers

**Files:**
- Create: `context.go`
- Test: `context_test.go`

**Interfaces:**
- Consumes: nothing from Task 1
- Produces:
  - `func contextWithCorrelationID(ctx context.Context, id string) context.Context`
  - `func correlationIDFromContext(ctx context.Context) (string, bool)`

  Task 6's `resolveCorrelationID` calls `correlationIDFromContext`. Both stay unexported this phase — the PRD's only public correlation API is the `WithCorrelationID` publish option (Task 6); a later consumer phase reuses `contextWithCorrelationID` to place the value, with no public API change.

- [ ] **Step 1: Write the failing test**

Create `context_test.go`:
```go
package messaging

import (
	"context"
	"testing"
)

func TestContextWithCorrelationID_RoundTrip(t *testing.T) {
	ctx := contextWithCorrelationID(context.Background(), "corr-42")

	id, ok := correlationIDFromContext(ctx)
	if !ok {
		t.Fatal("expected ok=true, got false")
	}
	if id != "corr-42" {
		t.Errorf("id = %q, want %q", id, "corr-42")
	}
}

func TestCorrelationIDFromContext_Missing(t *testing.T) {
	_, ok := correlationIDFromContext(context.Background())
	if ok {
		t.Error("expected ok=false for a context with no correlation ID, got true")
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./... -run TestContextWithCorrelationID -v && go test ./... -run TestCorrelationIDFromContext -v`
Expected: FAIL — `undefined: contextWithCorrelationID`

- [ ] **Step 3: Create context.go**

```go
package messaging

import "context"

type correlationIDContextKey struct{}

// contextWithCorrelationID returns a context carrying the given correlation
// ID. Used internally when resolving the correlation ID to publish with
// (Task 6), and by a later consumer phase to propagate it to handlers.
func contextWithCorrelationID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, correlationIDContextKey{}, id)
}

// correlationIDFromContext reads a correlation ID previously placed by
// contextWithCorrelationID. ok is false if none is present.
func correlationIDFromContext(ctx context.Context) (string, bool) {
	id, ok := ctx.Value(correlationIDContextKey{}).(string)
	return id, ok
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./... -run 'TestContextWithCorrelationID|TestCorrelationIDFromContext' -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add context.go context_test.go
git commit -m "feat: add correlation ID context helpers"
```

---

## Task 3: Config

**Files:**
- Create: `config.go`
- Test: `config_test.go`

**Interfaces:**
- Consumes: nothing from Tasks 1-2
- Produces:
  - `type RedisConfig struct { Address, Username, Password string; TLS *tls.Config; DB, PoolSize int }`
  - `type ConsumerConfig struct { Group, ConsumerName string; Concurrency int; ClaimInterval, ClaimMinIdle time.Duration; MaxDeliveryAttempts int; ShutdownTimeout time.Duration }`
  - `type RetryConfig struct { MaxImmediateRetries int; InitialBackoff, MaxBackoff time.Duration }`
  - `type TelemetryConfig struct { TracerProvider trace.TracerProvider; MeterProvider metric.MeterProvider; Logger *slog.Logger; LogPayloads bool }`
  - `type Config struct { Source, StreamPrefix string; Redis RedisConfig; Consumer ConsumerConfig; Retry RetryConfig; Telemetry TelemetryConfig }`
  - `func (c *Config) Validate() error`

  Task 6's `NewPublisher(cfg Config)` calls `cfg.Validate()` and reads `cfg.Source`, `cfg.StreamPrefix`, and `cfg.Redis.*`.

- [ ] **Step 1: Write the failing test**

Create `config_test.go`:
```go
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
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./... -run TestConfigValidate -v`
Expected: FAIL — `undefined: Config`

- [ ] **Step 3: Create config.go**

```go
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
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./... -run TestConfigValidate -v`
Expected: PASS for all subtests.

- [ ] **Step 5: Build and vet**

Run:
```bash
go build ./...
go vet ./...
```
Expected: both exit 0.

- [ ] **Step 6: Commit**

```bash
git add config.go config_test.go
git commit -m "feat: implement Config with PRD §17 defaults and validation"
```

---

## Task 4: internal/redis client construction

**Files:**
- Create: `internal/redis/client.go`
- Test: `internal/redis/client_test.go`
- Modify: `go.mod`, `go.sum` (add `github.com/redis/go-redis/v9`)

**Interfaces:**
- Consumes: nothing from Tasks 1-3 (deliberately decoupled from the root `Config` type — see Task 6 for how fields map across)
- Produces: `type ClientOptions struct { Address, Username, Password string; TLS *tls.Config; DB, PoolSize int }` and `func NewClient(opts ClientOptions) *redis.Client` (the `*redis.Client` return type is `github.com/redis/go-redis/v9`'s type — safe to expose from `internal/redis` since only other `internal/` packages consume it). Task 5 passes this client into `internal/watermill.NewPublisher`; Task 6 constructs `ClientOptions` from `Config.Redis`.

- [ ] **Step 1: Add the go-redis dependency**

Run:
```bash
go get github.com/redis/go-redis/v9@latest
```
Expected: exits 0, adds an entry to `go.mod`/`go.sum`.

- [ ] **Step 2: Write the failing test**

Create `internal/redis/client_test.go`:
```go
package redis

import "testing"

func TestNewClient_SetsOptions(t *testing.T) {
	client := NewClient(ClientOptions{
		Address:  "localhost:6380",
		Username: "svc",
		Password: "secret",
		DB:       2,
		PoolSize: 15,
	})
	defer client.Close()

	opts := client.Options()
	if opts.Addr != "localhost:6380" {
		t.Errorf("Addr = %q, want %q", opts.Addr, "localhost:6380")
	}
	if opts.Username != "svc" {
		t.Errorf("Username = %q, want %q", opts.Username, "svc")
	}
	if opts.Password != "secret" {
		t.Errorf("Password = %q, want %q", opts.Password, "secret")
	}
	if opts.DB != 2 {
		t.Errorf("DB = %d, want 2", opts.DB)
	}
	if opts.PoolSize != 15 {
		t.Errorf("PoolSize = %d, want 15", opts.PoolSize)
	}
}
```

This test never dials Redis — `redis.NewClient` builds a lazily-connecting client, and `.Options()` just reflects back what was configured.

- [ ] **Step 3: Run the test to verify it fails**

Run: `go test ./internal/redis/... -run TestNewClient -v`
Expected: FAIL — `undefined: NewClient`

- [ ] **Step 4: Replace internal/redis/doc.go's package with client.go**

Create `internal/redis/client.go` (this supersedes the Phase 0 stub comment in `internal/redis/doc.go` — leave `doc.go`'s package doc comment in place, just add this new file alongside it):
```go
package redis

import (
	"crypto/tls"

	goredis "github.com/redis/go-redis/v9"
)

// ClientOptions configures the go-redis client this package constructs.
type ClientOptions struct {
	Address  string
	Username string
	Password string
	TLS      *tls.Config
	DB       int
	PoolSize int
}

// NewClient builds a go-redis client. Construction does not dial Redis —
// connections are established lazily on first use.
func NewClient(opts ClientOptions) *goredis.Client {
	return goredis.NewClient(&goredis.Options{
		Addr:      opts.Address,
		Username:  opts.Username,
		Password:  opts.Password,
		TLSConfig: opts.TLS,
		DB:        opts.DB,
		PoolSize:  opts.PoolSize,
	})
}
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `go test ./internal/redis/... -run TestNewClient -v`
Expected: PASS

- [ ] **Step 6: Build and vet**

Run:
```bash
go build ./...
go vet ./...
```
Expected: both exit 0.

- [ ] **Step 7: Commit**

```bash
git add internal/redis/client.go internal/redis/client_test.go go.mod go.sum
git commit -m "feat: add internal/redis go-redis client construction"
```

---

## Task 5: internal/watermill publisher wrapper

**Files:**
- Create: `internal/watermill/publisher.go`
- Modify: `.golangci.yml`
- Modify: `go.mod`, `go.sum` (add `github.com/ThreeDotsLabs/watermill`, `github.com/ThreeDotsLabs/watermill-redisstream`)

**Interfaces:**
- Consumes: `internal/redis.NewClient` (Task 4) — specifically its `*goredis.Client` return value
- Produces: `func NewPublisher(client *goredis.Client) (*Publisher, error)`, `func (p *Publisher) Publish(topic string, id string, payload []byte, metadata map[string]string) error`, `func (p *Publisher) Close() error`. Task 6's root `Publisher.Publish` calls this `Publish` method with the computed stream name, the envelope's `EventID`, and the marshaled envelope bytes.

No committed test file for this task — a live Redis connection is required to prove `Publish` actually works, and that end-to-end proof belongs to Task 7's integration test once the root `Publisher` exists to drive it. This task is verified by a manual throwaway script, the same pattern Phase 0 used for `internal/testsupport`.

- [ ] **Step 1: Add the Watermill dependencies**

Run:
```bash
go get github.com/ThreeDotsLabs/watermill@latest
go get github.com/ThreeDotsLabs/watermill-redisstream@latest
```
Expected: both exit 0, add entries to `go.mod`/`go.sum`.

- [ ] **Step 2: Add the depguard deny entry for watermill-redisstream**

Open `.golangci.yml` and add a second `deny` entry to the existing `internal-only` rule, alongside the existing `watermill` and `go-redis` entries:

```yaml
version: "2"
linters:
  enable:
    - govet
    - staticcheck
    - errcheck
    - unused
    - gofmt
    - goimports
  settings:
    depguard:
      rules:
        internal-only:
          files:
            - "!**/internal/**"
          deny:
            - pkg: github.com/ThreeDotsLabs/watermill
              desc: "Watermill must stay inside internal/ (PRD §12, §21)"
            - pkg: github.com/ThreeDotsLabs/watermill-redisstream
              desc: "watermill-redisstream must stay inside internal/ (PRD §12, §21)"
            - pkg: github.com/redis/go-redis
              desc: "go-redis must stay inside internal/ (PRD §12, §21)"
```

- [ ] **Step 3: Create internal/watermill/publisher.go**

This supersedes the Phase 0 stub comment in `internal/watermill/doc.go` — leave `doc.go`'s package doc comment in place, just add this new file alongside it:

```go
package watermill

import (
	"fmt"

	wm "github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill-redisstream/pkg/redisstream"
	goredis "github.com/redis/go-redis/v9"
)

// Publisher wraps a watermill-redisstream Publisher. Its methods take and
// return only primitive types and byte slices, so no watermill or go-redis
// type crosses into callers outside internal/.
type Publisher struct {
	pub *redisstream.Publisher
}

// NewPublisher builds a Publisher backed by client.
func NewPublisher(client *goredis.Client) (*Publisher, error) {
	pub, err := redisstream.NewPublisher(
		redisstream.PublisherConfig{
			Client:     client,
			Marshaller: redisstream.DefaultMarshallerUnmarshaller{},
		},
		wm.NewStdLogger(false, false),
	)
	if err != nil {
		return nil, fmt.Errorf("creating redis stream publisher: %w", err)
	}

	return &Publisher{pub: pub}, nil
}

// Publish writes payload to topic as a single Redis Streams entry with
// message id id, carrying metadata as Watermill message metadata.
func (p *Publisher) Publish(topic string, id string, payload []byte, metadata map[string]string) error {
	msg := message.NewMessage(id, payload)
	for k, v := range metadata {
		msg.Metadata.Set(k, v)
	}

	if err := p.pub.Publish(topic, msg); err != nil {
		return fmt.Errorf("publishing to topic %q: %w", topic, err)
	}
	return nil
}

// Close releases the underlying publisher's resources.
func (p *Publisher) Close() error {
	return p.pub.Close()
}
```

If `go build` reports a signature mismatch against the installed `watermill-redisstream` version, run `go doc github.com/ThreeDotsLabs/watermill-redisstream/pkg/redisstream.PublisherConfig` and `go doc github.com/ThreeDotsLabs/watermill-redisstream/pkg/redisstream.NewPublisher` to check the current signatures and adjust — `Client` and `Marshaller` fields on `PublisherConfig`, and the `(config, logger) (*Publisher, error)` shape of `NewPublisher`, are current as of the latest published version.

- [ ] **Step 4: Verify it compiles**

Run:
```bash
go build ./...
go vet ./...
```
Expected: both exit 0.

- [ ] **Step 5: Manually verify it actually publishes to a real Redis stream**

This task commits no test file, so verify by hand with a throwaway `go run`, the same pattern Phase 0 used for `internal/testsupport`. Create a temporary file `/tmp/verify_watermill_publisher.go`:
```go
package main

import (
	"context"
	"fmt"
	"log"

	internalredis "github.com/bcgov/foi-messaging-go/internal/redis"
	internalwatermill "github.com/bcgov/foi-messaging-go/internal/watermill"
	"github.com/bcgov/foi-messaging-go/internal/testsupport"
)

func main() {
	ctx := context.Background()
	addr, terminate, err := testsupport.StartRedis(ctx)
	if err != nil {
		log.Fatalf("StartRedis: %v", err)
	}
	defer terminate(ctx)
	fmt.Println("redis started at:", addr)

	client := internalredis.NewClient(internalredis.ClientOptions{Address: addr})
	defer client.Close()

	pub, err := internalwatermill.NewPublisher(client)
	if err != nil {
		log.Fatalf("NewPublisher: %v", err)
	}
	defer pub.Close()

	if err := pub.Publish("verify-topic", "msg-1", []byte(`{"hello":"world"}`), map[string]string{"k": "v"}); err != nil {
		log.Fatalf("Publish: %v", err)
	}
	fmt.Println("published successfully")
}
```
Run (Docker must be running):
```bash
go run /tmp/verify_watermill_publisher.go
```
Expected output: `redis started at: <host>:<port>` then `published successfully`.

Delete the throwaway file afterward — it is not part of the repo:
```bash
rm /tmp/verify_watermill_publisher.go
```

- [ ] **Step 6: Commit**

```bash
git add internal/watermill/publisher.go .golangci.yml go.mod go.sum
git commit -m "feat: add internal/watermill publisher wrapper over watermill-redisstream"
```

---

## Task 6: Publisher (NewPublisher, Publish, Close)

**Files:**
- Create: `publisher.go`
- Test: `publisher_test.go`

**Interfaces:**
- Consumes: `newEnvelope`, `validateEnvelope` (Task 1); `correlationIDFromContext` (Task 2); `Config`, `Config.Validate` (Task 3); `internal/redis.NewClient`, `internal/redis.ClientOptions` (Task 4); `internal/watermill.NewPublisher`, `(*internal/watermill.Publisher).Publish`, `.Close` (Task 5)
- Produces:
  - `type PublishResult struct { EventID string; Timestamp time.Time }`
  - `type PublishOption func(*publishOptions)`
  - `func WithCorrelationID(id string) PublishOption`
  - `type Publisher struct { ... }`
  - `func NewPublisher(cfg Config) (*Publisher, error)`
  - `func (p *Publisher) Publish(ctx context.Context, def EventDef, payload any, opts ...PublishOption) (PublishResult, error)`
  - `func (p *Publisher) Close() error`

  Task 7's integration test calls `NewPublisher` and `Publish` directly.

- [ ] **Step 1: Write the failing unit tests**

These test the pure, network-free pieces of `publisher.go`: option handling and correlation ID resolution order. `NewPublisher`/`Publish` themselves require a real Redis connection and are proven end-to-end in Task 7.

Create `publisher_test.go`:
```go
package messaging

import (
	"context"
	"testing"

	"github.com/google/uuid"
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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./... -run 'TestWithCorrelationID|TestResolveCorrelationID' -v`
Expected: FAIL — `undefined: publishOptions`

- [ ] **Step 3: Create publisher.go**

```go
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

	client := internalredis.NewClient(internalredis.ClientOptions{
		Address:  cfg.Redis.Address,
		Username: cfg.Redis.Username,
		Password: cfg.Redis.Password,
		TLS:      cfg.Redis.TLS,
		DB:       cfg.Redis.DB,
		PoolSize: cfg.Redis.PoolSize,
	})

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
	if err := p.wm.Publish(stream, env.EventID, body, map[string]string{}); err != nil {
		return PublishResult{}, fmt.Errorf("publishing to stream %q: %w", stream, err)
	}

	return PublishResult{EventID: env.EventID, Timestamp: env.Timestamp}, nil
}

// Close releases the Publisher's underlying resources.
func (p *Publisher) Close() error {
	return p.wm.Close()
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./... -run 'TestWithCorrelationID|TestResolveCorrelationID' -v`
Expected: PASS for all subtests.

- [ ] **Step 5: Build and vet**

Run:
```bash
go build ./...
go vet ./...
```
Expected: both exit 0.

- [ ] **Step 6: Commit**

```bash
git add publisher.go publisher_test.go
git commit -m "feat: implement Publisher, NewPublisher, and Publish"
```

---

## Task 7: Integration test — real end-to-end publish

**Files:**
- Modify: `internal/testsupport/redis.go` (add `StreamEntry` and `ReadStreamEntries`)
- Create: `publisher_integration_test.go`

**Interfaces:**
- Consumes: `testsupport.StartRedis` (existing, Phase 0); `Config`, `NewPublisher`, `(*Publisher).Publish`, `(*Publisher).Close`, `EventDef`, `PublishResult` (Tasks 3, 6)
- Produces: `type testsupport.StreamEntry struct { ID string; Fields map[string]string }` and `func testsupport.ReadStreamEntries(ctx context.Context, addr string, stream string) ([]testsupport.StreamEntry, error)` — read-back helper, consumed only by this task's test.

This is the phase's proof that the write path genuinely works, not just that it compiles.

- [ ] **Step 1: Add the read-back helper to internal/testsupport**

Modify `internal/testsupport/redis.go` to add (after the existing `StartRedis` function):
```go
// StreamEntry is a single raw Redis Streams entry.
type StreamEntry struct {
	ID     string
	Fields map[string]string
}

// ReadStreamEntries connects to addr and reads every entry currently on
// stream, for integration-test assertions against what a Publisher wrote.
func ReadStreamEntries(ctx context.Context, addr string, stream string) ([]StreamEntry, error) {
	client := goredis.NewClient(&goredis.Options{Addr: addr})
	defer client.Close()

	raw, err := client.XRange(ctx, stream, "-", "+").Result()
	if err != nil {
		return nil, fmt.Errorf("reading stream %q: %w", stream, err)
	}

	entries := make([]StreamEntry, 0, len(raw))
	for _, msg := range raw {
		fields := make(map[string]string, len(msg.Values))
		for k, v := range msg.Values {
			if s, ok := v.(string); ok {
				fields[k] = s
			}
		}
		entries = append(entries, StreamEntry{ID: msg.ID, Fields: fields})
	}
	return entries, nil
}
```

Add `goredis "github.com/redis/go-redis/v9"` to the file's import block (alongside the existing `context`, `errors`, `fmt`, and `tcredis` imports).

- [ ] **Step 2: Write the integration test**

Create `publisher_integration_test.go`:
```go
//go:build integration

package messaging_test

import (
	"context"
	"encoding/json"
	"testing"

	messaging "github.com/bcgov/foi-messaging-go"
	"github.com/bcgov/foi-messaging-go/internal/testsupport"
)

type documentCreatedPayload struct {
	EntityID string `json:"entity_id"`
	Name     string `json:"name"`
}

func TestPublisher_Publish_WritesEnvelopeToRedisStream(t *testing.T) {
	ctx := context.Background()

	addr, terminate, err := testsupport.StartRedis(ctx)
	if err != nil {
		t.Fatalf("StartRedis: %v", err)
	}
	t.Cleanup(func() {
		if err := terminate(ctx); err != nil {
			t.Errorf("terminate: %v", err)
		}
	})

	cfg := messaging.Config{
		Source: "test.service",
		Redis:  messaging.RedisConfig{Address: addr},
	}
	pub, err := messaging.NewPublisher(cfg)
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	t.Cleanup(func() {
		if err := pub.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})

	def := messaging.EventDef{Topic: "documents", Type: "document.created", Version: "1.0.0"}
	payload := documentCreatedPayload{EntityID: "42", Name: "report.pdf"}

	result, err := pub.Publish(ctx, def, payload, messaging.WithCorrelationID("corr-integration"))
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if result.EventID == "" {
		t.Error("PublishResult.EventID is empty")
	}
	if result.Timestamp.IsZero() {
		t.Error("PublishResult.Timestamp is zero")
	}

	entries, err := testsupport.ReadStreamEntries(ctx, addr, "foi:documents")
	if err != nil {
		t.Fatalf("ReadStreamEntries: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("got %d stream entries, want 1", len(entries))
	}

	rawPayload, ok := entries[0].Fields["payload"]
	if !ok {
		t.Fatalf("entry has no %q field; got fields: %v", "payload", entries[0].Fields)
	}

	var env messaging.Envelope[documentCreatedPayload]
	if err := json.Unmarshal([]byte(rawPayload), &env); err != nil {
		t.Fatalf("unmarshaling stream entry payload: %v", err)
	}

	if env.EventID != result.EventID {
		t.Errorf("envelope EventID = %q, want %q", env.EventID, result.EventID)
	}
	if env.EventType != "document.created" {
		t.Errorf("envelope EventType = %q, want %q", env.EventType, "document.created")
	}
	if env.SchemaVersion != "1.0.0" {
		t.Errorf("envelope SchemaVersion = %q, want %q", env.SchemaVersion, "1.0.0")
	}
	if env.CorrelationID != "corr-integration" {
		t.Errorf("envelope CorrelationID = %q, want %q", env.CorrelationID, "corr-integration")
	}
	if env.Source != "test.service" {
		t.Errorf("envelope Source = %q, want %q", env.Source, "test.service")
	}
	if env.Payload != payload {
		t.Errorf("envelope Payload = %+v, want %+v", env.Payload, payload)
	}
}

func TestPublisher_Publish_ValidationFailureWritesNothing(t *testing.T) {
	ctx := context.Background()

	addr, terminate, err := testsupport.StartRedis(ctx)
	if err != nil {
		t.Fatalf("StartRedis: %v", err)
	}
	t.Cleanup(func() {
		if err := terminate(ctx); err != nil {
			t.Errorf("terminate: %v", err)
		}
	})

	cfg := messaging.Config{
		Source: "test.service",
		Redis:  messaging.RedisConfig{Address: addr},
	}
	pub, err := messaging.NewPublisher(cfg)
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	t.Cleanup(func() {
		if err := pub.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})

	// "bad_event_type" has only one segment, so envelope validation must
	// reject it before anything is written to the stream.
	badDef := messaging.EventDef{Topic: "documents", Type: "bad_event_type", Version: "1.0.0"}

	if _, err := pub.Publish(ctx, badDef, documentCreatedPayload{}); err == nil {
		t.Fatal("expected an error for an invalid event_type, got nil")
	}

	entries, err := testsupport.ReadStreamEntries(ctx, addr, "foi:documents")
	if err != nil {
		t.Fatalf("ReadStreamEntries: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("got %d stream entries after a validation failure, want 0", len(entries))
	}
}
```

The field name `"payload"` matches `watermill-redisstream`'s `DefaultMarshallerUnmarshaller`, which writes the message body under a `payload` key (alongside `_watermill_message_uuid` and `metadata`) when it calls `XAdd`. If `go test` fails with a missing-field error, run `go doc github.com/ThreeDotsLabs/watermill-redisstream/pkg/redisstream.DefaultMarshallerUnmarshaller.Marshal` to confirm the installed version's key names and adjust.

- [ ] **Step 3: Run the integration test**

Run (Docker must be running):
```bash
go test -tags=integration ./... -run TestPublisher_Publish -v
```
Expected: both `TestPublisher_Publish_WritesEnvelopeToRedisStream` and `TestPublisher_Publish_ValidationFailureWritesNothing` PASS.

- [ ] **Step 4: Run the full test suite and lint**

Run:
```bash
go build ./...
go vet ./...
go test ./...
go test -tags=integration ./...
make lint
```
Expected: all exit 0. `make lint`'s `depguard` rule should report no violations — `publisher_integration_test.go` and `internal/testsupport/redis.go` never import `watermill` or `watermill-redisstream` directly, and go-redis usage in `internal/testsupport` is inside `internal/`, exempting it from the rule.

- [ ] **Step 5: Commit**

```bash
git add internal/testsupport/redis.go publisher_integration_test.go
git commit -m "test: add integration test proving Publish writes to a real Redis stream"
```

---

## Post-plan verification

After all seven tasks are committed, run from the repo root:
```bash
go build ./...
go vet ./...
go test ./...
go test -tags=integration ./...
make lint
```
All five should succeed, confirming Phase 1's deliverable: applications can call `messaging.NewPublisher(cfg)` and `publisher.Publish(ctx, def, payload)` to write a validated, standard envelope to a real Redis stream, entirely through the typed public API — with `watermill` and `go-redis` never crossing outside `internal/`, enforced by `depguard`. Ready for Phase 2 to build the consumer/handler/routing/retry/DLQ read path against this Publisher and the same `internal/redis` and `internal/watermill` foundations.
