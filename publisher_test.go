package messaging

import (
	"context"
	"crypto/tls"
	"errors"
	"log/slog"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"go.opentelemetry.io/otel/sdk/metric/metricdata"

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

func TestPublish_RecordsSpanAndMetadata(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	mp, reader := newTestMeterProvider(t)

	captured := make(map[string]string)
	p := &Publisher{
		cfg: Config{
			Source:       "billing.service",
			StreamPrefix: "foi",
			Telemetry: TelemetryConfig{
				TracerProvider: tp,
				MeterProvider:  mp,
				Propagator:     propagation.TraceContext{},
				Logger:         slog.Default(),
			},
		},
		inst:   newInstruments(mp, slog.Default()),
		tracer: tp.Tracer(telemetryScope),
		wm:     nil,
	}
	p.publishFn = func(ctx context.Context, stream, id string, body []byte, md map[string]string) error {
		for k, v := range md {
			captured[k] = v
		}
		return nil
	}

	def := EventDef{Topic: "documents", Type: "document.created", Version: "1.0.0"}
	if _, err := p.Publish(context.Background(), def, map[string]string{"a": "b"}); err != nil {
		t.Fatalf("Publish() = %v, want nil", err)
	}

	if _, ok := captured["traceparent"]; !ok {
		t.Errorf("metadata = %v, want a traceparent key", captured)
	}
	if _, ok := captured[metadataPublishedAt]; !ok {
		t.Errorf("metadata = %v, want a %s key", captured, metadataPublishedAt)
	}

	spans := recorder.Ended()
	if len(spans) != 1 {
		t.Fatalf("recorded %d spans, want 1", len(spans))
	}
	if got, want := spans[0].Name(), "publish documents"; got != want {
		t.Errorf("span name = %q, want %q", got, want)
	}
	if got := spans[0].SpanKind(); got != trace.SpanKindProducer {
		t.Errorf("span kind = %v, want Producer", got)
	}

	if _, ok := readMetrics(t, reader)["messaging.events.published"]; !ok {
		t.Error("messaging.events.published was not recorded")
	}
}

func TestPublish_RecordsValidationFailureStage(t *testing.T) {
	// An envelope that fails validateEnvelope must be attributed to the
	// validation stage, not lumped in with transport failures — the whole
	// point of the stage attribute is separating "this service is emitting
	// garbage" from "Redis is unreachable".
	mp, reader := newTestMeterProvider(t)
	tp := sdktrace.NewTracerProvider()

	p := &Publisher{
		cfg: Config{
			// An empty Source makes the envelope fail validation.
			Source:       "",
			StreamPrefix: "foi",
			Telemetry: TelemetryConfig{
				TracerProvider: tp,
				MeterProvider:  mp,
				Propagator:     propagation.TraceContext{},
				Logger:         slog.Default(),
			},
		},
		inst:   newInstruments(mp, slog.Default()),
		tracer: tp.Tracer(telemetryScope),
	}
	p.publishFn = func(context.Context, string, string, []byte, map[string]string) error {
		t.Error("transport was reached despite an invalid envelope")
		return nil
	}

	def := EventDef{Topic: "documents", Type: "document.created", Version: "1.0.0"}
	if _, err := p.Publish(context.Background(), def, map[string]string{}); err == nil {
		t.Fatal("Publish() = nil, want a validation error")
	}

	m, ok := readMetrics(t, reader)["messaging.publish.failures"]
	if !ok {
		t.Fatal("messaging.publish.failures was not recorded")
	}
	sum, ok := m.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("publish.failures data = %T, want Sum[int64]", m.Data)
	}
	stage, found := sum.DataPoints[0].Attributes.Value(attribute.Key(attrStage))
	if !found || stage.AsString() != stageValidation {
		t.Errorf("stage = %v, want %q", stage.AsString(), stageValidation)
	}
}

func TestPublish_RecordsMarshalFailureStage(t *testing.T) {
	// validateEnvelope never inspects Payload, so an unmarshalable payload
	// passes validation and fails at json.Marshal — which is exactly the
	// gap the marshal stage exists to name.
	mp, reader := newTestMeterProvider(t)
	tp := sdktrace.NewTracerProvider()

	p := &Publisher{
		cfg: Config{
			Source:       "billing.service",
			StreamPrefix: "foi",
			Telemetry: TelemetryConfig{
				TracerProvider: tp,
				MeterProvider:  mp,
				Propagator:     propagation.TraceContext{},
				Logger:         slog.Default(),
			},
		},
		inst:   newInstruments(mp, slog.Default()),
		tracer: tp.Tracer(telemetryScope),
	}
	p.publishFn = func(context.Context, string, string, []byte, map[string]string) error {
		t.Error("transport was reached despite an unmarshalable payload")
		return nil
	}

	def := EventDef{Topic: "documents", Type: "document.created", Version: "1.0.0"}
	// A channel cannot be JSON-encoded.
	if _, err := p.Publish(context.Background(), def, make(chan int)); err == nil {
		t.Fatal("Publish() = nil, want a marshalling error")
	}

	m, ok := readMetrics(t, reader)["messaging.publish.failures"]
	if !ok {
		t.Fatal("messaging.publish.failures was not recorded")
	}
	sum, ok := m.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("publish.failures data = %T, want Sum[int64]", m.Data)
	}
	stage, found := sum.DataPoints[0].Attributes.Value(attribute.Key(attrStage))
	if !found || stage.AsString() != stageMarshal {
		t.Errorf("stage = %v, want %q", stage.AsString(), stageMarshal)
	}
}

func TestPublish_RecordsFailureStage(t *testing.T) {
	mp, reader := newTestMeterProvider(t)
	tp := sdktrace.NewTracerProvider()

	p := &Publisher{
		cfg: Config{
			Source:       "billing.service",
			StreamPrefix: "foi",
			Telemetry: TelemetryConfig{
				TracerProvider: tp,
				MeterProvider:  mp,
				Propagator:     propagation.TraceContext{},
				Logger:         slog.Default(),
			},
		},
		inst:   newInstruments(mp, slog.Default()),
		tracer: tp.Tracer(telemetryScope),
	}
	p.publishFn = func(context.Context, string, string, []byte, map[string]string) error {
		return errors.New("redis is down")
	}

	def := EventDef{Topic: "documents", Type: "document.created", Version: "1.0.0"}
	if _, err := p.Publish(context.Background(), def, map[string]string{}); err == nil {
		t.Fatal("Publish() = nil, want an error")
	}

	got := readMetrics(t, reader)
	m, ok := got["messaging.publish.failures"]
	if !ok {
		t.Fatal("messaging.publish.failures was not recorded")
	}
	sum, ok := m.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("publish.failures data = %T, want Sum[int64]", m.Data)
	}
	stage, found := sum.DataPoints[0].Attributes.Value(attribute.Key(attrStage))
	if !found || stage.AsString() != stageTransport {
		t.Errorf("stage = %v, want %q", stage.AsString(), stageTransport)
	}

	if _, ok := got["messaging.events.published"]; ok {
		t.Error("messaging.events.published was recorded for a failed publish")
	}
}

func TestParsePublishedAt_RoundTripsPublishedAtNow(t *testing.T) {
	// publishedAtNow/parsePublishedAt are a pair: Publish writes the wire
	// format, Task 10's queue-latency observation reads it back. Nothing in
	// this task's Publish path calls parsePublishedAt, so this test is what
	// keeps it from being dead code between now and Task 10.
	before := time.Now().UTC()
	metadata := map[string]string{metadataPublishedAt: publishedAtNow()}
	after := time.Now().UTC()

	got, ok := parsePublishedAt(metadata)
	if !ok {
		t.Fatalf("parsePublishedAt(%v) ok = false, want true", metadata)
	}
	if got.Before(before.Add(-time.Second)) || got.After(after.Add(time.Second)) {
		t.Errorf("parsePublishedAt round trip = %v, want between %v and %v", got, before, after)
	}
}

func TestParsePublishedAt_MissingOrUnparseable(t *testing.T) {
	if _, ok := parsePublishedAt(map[string]string{}); ok {
		t.Error("parsePublishedAt(no key) ok = true, want false")
	}
	if _, ok := parsePublishedAt(map[string]string{metadataPublishedAt: "not-a-timestamp"}); ok {
		t.Error("parsePublishedAt(garbage) ok = true, want false")
	}
}
