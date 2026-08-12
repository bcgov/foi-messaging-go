package messaging

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

// readMetrics collects everything recorded through reader, keyed by
// instrument name, so tests can assert on one instrument without
// reconstructing the whole ResourceMetrics tree at every call site.
func readMetrics(t *testing.T, reader *metric.ManualReader) map[string]metricdata.Metrics {
	t.Helper()

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect() = %v, want nil", err)
	}

	out := make(map[string]metricdata.Metrics)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			out[m.Name] = m
		}
	}
	return out
}

// newTestMeterProvider returns a provider and the reader collecting from it.
func newTestMeterProvider(t *testing.T) (*metric.MeterProvider, *metric.ManualReader) {
	t.Helper()
	reader := metric.NewManualReader()
	return metric.NewMeterProvider(metric.WithReader(reader)), reader
}

func TestNewInstruments_CreatesEveryInstrument(t *testing.T) {
	mp, reader := newTestMeterProvider(t)
	inst := newInstruments(mp, slog.Default())

	ctx := context.Background()
	inst.published.Add(ctx, 1)
	inst.publishFailures.Add(ctx, 1)
	inst.received.Add(ctx, 1)
	inst.processed.Add(ctx, 1)
	inst.failed.Add(ctx, 1)
	inst.skipped.Add(ctx, 1)
	inst.retries.Add(ctx, 1)
	inst.dlq.Add(ctx, 1)
	inst.dlqPublishFailures.Add(ctx, 1)
	inst.processingDuration.Record(ctx, 0.1)
	inst.queueLatency.Record(ctx, 0.1)

	got := readMetrics(t, reader)

	// The names are the library's contract with every dashboard built on
	// it, so they are asserted literally rather than derived.
	want := []string{
		"messaging.events.published",
		"messaging.publish.failures",
		"messaging.events.received",
		"messaging.events.processed",
		"messaging.events.failed",
		"messaging.events.skipped",
		"messaging.retries",
		"messaging.dlq",
		"messaging.dlq.publish.failures",
		"messaging.processing.duration",
		"messaging.queue.latency",
	}
	for _, name := range want {
		if _, ok := got[name]; !ok {
			t.Errorf("instrument %q was not recorded", name)
		}
	}
	if len(got) != len(want) {
		t.Errorf("recorded %d instruments, want %d", len(got), len(want))
	}
}

func TestNewInstruments_NilProviderFallsBackToNoop(t *testing.T) {
	// A telemetry failure must never stop a service delivering messages,
	// so construction degrades to no-op instruments rather than erroring.
	inst := newInstruments(nil, slog.Default())

	if inst == nil {
		t.Fatal("newInstruments(nil) = nil, want no-op instruments")
	}
	// Recording through the fallback must not panic.
	inst.published.Add(context.Background(), 1)
	inst.processingDuration.Record(context.Background(), 0.1)
}

func TestMetricLabelContract(t *testing.T) {
	// The attribute key and reason/error-category constants are the metric
	// label contract — the same way instrument names are the metric name
	// contract. Changing attrTopic from "topic" to "host", or
	// reasonNoHandler from "no_handler" to "nohandler", silently breaks
	// every dashboard query and alert built on this library, with nothing
	// failing to warn you. These assertions guard that boundary.
	const (
		wantAttrTopic         = "topic"
		wantAttrEventType     = "event_type"
		wantAttrGroup         = "group"
		wantAttrReason        = "reason"
		wantAttrErrorCategory = "error_category"
		wantAttrStage         = "stage"

		wantStageValidation = "validation"
		wantStageMarshal    = "marshal"
		wantStageTransport  = "transport"

		wantReasonNoHandler = "no_handler"
		wantReasonDiscard   = "discard"

		wantCategoryPermanent       = "permanent"
		wantCategoryRetryable       = "retryable"
		wantCategoryDeserialization = "deserialization"
		wantCategoryMaxAttempts     = "max_attempts"
	)

	if attrTopic != wantAttrTopic {
		t.Errorf("attrTopic = %q, want %q", attrTopic, wantAttrTopic)
	}
	if attrEventType != wantAttrEventType {
		t.Errorf("attrEventType = %q, want %q", attrEventType, wantAttrEventType)
	}
	if attrGroup != wantAttrGroup {
		t.Errorf("attrGroup = %q, want %q", attrGroup, wantAttrGroup)
	}
	if attrReason != wantAttrReason {
		t.Errorf("attrReason = %q, want %q", attrReason, wantAttrReason)
	}
	if attrErrorCategory != wantAttrErrorCategory {
		t.Errorf("attrErrorCategory = %q, want %q", attrErrorCategory, wantAttrErrorCategory)
	}
	if attrStage != wantAttrStage {
		t.Errorf("attrStage = %q, want %q", attrStage, wantAttrStage)
	}

	if stageValidation != wantStageValidation {
		t.Errorf("stageValidation = %q, want %q", stageValidation, wantStageValidation)
	}
	if stageMarshal != wantStageMarshal {
		t.Errorf("stageMarshal = %q, want %q", stageMarshal, wantStageMarshal)
	}
	if stageTransport != wantStageTransport {
		t.Errorf("stageTransport = %q, want %q", stageTransport, wantStageTransport)
	}

	if reasonNoHandler != wantReasonNoHandler {
		t.Errorf("reasonNoHandler = %q, want %q", reasonNoHandler, wantReasonNoHandler)
	}
	if reasonDiscard != wantReasonDiscard {
		t.Errorf("reasonDiscard = %q, want %q", reasonDiscard, wantReasonDiscard)
	}

	if categoryPermanent != wantCategoryPermanent {
		t.Errorf("categoryPermanent = %q, want %q", categoryPermanent, wantCategoryPermanent)
	}
	if categoryRetryable != wantCategoryRetryable {
		t.Errorf("categoryRetryable = %q, want %q", categoryRetryable, wantCategoryRetryable)
	}
	if categoryDeserialization != wantCategoryDeserialization {
		t.Errorf("categoryDeserialization = %q, want %q", categoryDeserialization, wantCategoryDeserialization)
	}
	if categoryMaxAttempts != wantCategoryMaxAttempts {
		t.Errorf("categoryMaxAttempts = %q, want %q", categoryMaxAttempts, wantCategoryMaxAttempts)
	}
}

func TestConsumeAttrs_Contract(t *testing.T) {
	// consumeAttrs builds the attribute set for consume-path metrics. The
	// contract is: topic and group are always present; event_type is
	// present only when non-empty. This omission of empty event_type is the
	// whole point of the function — it prevents an unbounded wire value
	// from exploding the metric cardinality when a raw handler takes every
	// event on a topic and a bad producer sends event_type values we've
	// never instrumented for.

	// Typed match: event_type is non-empty, so it is included.
	typedAttrs := consumeAttrs("orders", "consumer-1", "order.created.v1")

	if len(typedAttrs) != 3 {
		t.Fatalf("consumeAttrs with non-empty event_type: got %d attrs, want 3 (topic, group, event_type)", len(typedAttrs))
	}
	if typedAttrs[0].Key != "topic" || typedAttrs[0].Value.AsString() != "orders" {
		t.Errorf("attr 0: got {%s=%s}, want {topic=orders}", typedAttrs[0].Key, typedAttrs[0].Value.AsString())
	}
	if typedAttrs[1].Key != "group" || typedAttrs[1].Value.AsString() != "consumer-1" {
		t.Errorf("attr 1: got {%s=%s}, want {group=consumer-1}", typedAttrs[1].Key, typedAttrs[1].Value.AsString())
	}
	if typedAttrs[2].Key != "event_type" || typedAttrs[2].Value.AsString() != "order.created.v1" {
		t.Errorf("attr 2: got {%s=%s}, want {event_type=order.created.v1}", typedAttrs[2].Key, typedAttrs[2].Value.AsString())
	}

	// Raw handler or deserialization path: event_type is empty, so it is
	// omitted. This keeps an unbounded cardinality sink out of the metrics.
	rawAttrs := consumeAttrs("orders", "consumer-1", "")

	if len(rawAttrs) != 2 {
		t.Fatalf("consumeAttrs with empty event_type: got %d attrs, want 2 (topic, group only)", len(rawAttrs))
	}
	if rawAttrs[0].Key != "topic" || rawAttrs[0].Value.AsString() != "orders" {
		t.Errorf("attr 0: got {%s=%s}, want {topic=orders}", rawAttrs[0].Key, rawAttrs[0].Value.AsString())
	}
	if rawAttrs[1].Key != "group" || rawAttrs[1].Value.AsString() != "consumer-1" {
		t.Errorf("attr 1: got {%s=%s}, want {group=consumer-1}", rawAttrs[1].Key, rawAttrs[1].Value.AsString())
	}

	// Extra attributes are appended.
	extraAttrs := consumeAttrs("orders", "consumer-1", "", []attribute.KeyValue{
		attribute.String("delivery_attempt", "2"),
		attribute.Int64("retry_count", 1),
	}...)

	if len(extraAttrs) != 4 {
		t.Fatalf("consumeAttrs with extra attrs: got %d attrs, want 4", len(extraAttrs))
	}
	if extraAttrs[2].Key != "delivery_attempt" {
		t.Errorf("extra attr 0: got key %q, want delivery_attempt", extraAttrs[2].Key)
	}
	if extraAttrs[3].Key != "retry_count" {
		t.Errorf("extra attr 1: got key %q, want retry_count", extraAttrs[3].Key)
	}
}

func TestNewInstruments_ErroringProviderFallsBackToNoop(t *testing.T) {
	// When a metric.MeterProvider fails to create instruments — whether due
	// to a broken SDK or misconfiguration — newInstruments must detect that
	// failure and rebuild from the no-op provider. A regression that let the
	// error bubble out would mean a broken metrics pipeline stops a service
	// delivering messages, which is precisely what the design forbids.

	// Construct a provider that fails when building the first instrument.
	// Embed noop.MeterProvider and override Meter to return a failing meter.
	failingProvider := &failingMeterProvider{
		MeterProvider: noop.NewMeterProvider(),
	}

	// newInstruments should detect the failure and rebuild from no-op.
	inst := newInstruments(failingProvider, slog.Default())

	if inst == nil {
		t.Fatal("newInstruments with erroring provider = nil, want non-nil no-op instruments")
	}

	// Recording through the rebuilt instruments must not panic.
	ctx := context.Background()
	inst.published.Add(ctx, 1)
	inst.publishFailures.Add(ctx, 1)
	inst.processingDuration.Record(ctx, 0.1)
	inst.queueLatency.Record(ctx, 0.1)
}

// failingMeterProvider embeds noop.MeterProvider and returns a failing meter.
type failingMeterProvider struct {
	otelmetric.MeterProvider
}

func (fp *failingMeterProvider) Meter(name string, opts ...otelmetric.MeterOption) otelmetric.Meter {
	return &failingMeter{}
}

// failingMeter embeds noop.Meter and overrides Int64Counter to fail.
type failingMeter struct {
	noop.Meter
}

func (fm *failingMeter) Int64Counter(name string, opts ...otelmetric.Int64CounterOption) (otelmetric.Int64Counter, error) {
	return nil, errors.New("simulated instrument creation failure")
}

func TestDeliveryRecorder_TerminalInvariant(t *testing.T) {
	// Spec §2: every delivery increments exactly one of processed /
	// failed / skipped, and records processing.duration exactly once.
	// This is the test that keeps that true as exit paths are added.
	terminal := []string{
		"messaging.events.processed",
		"messaging.events.failed",
		"messaging.events.skipped",
	}

	tests := []struct {
		name string
		act  func(r *deliveryRecorder)
		want string
	}{
		{"processed", func(r *deliveryRecorder) { r.processed() }, "messaging.events.processed"},
		{"failed", func(r *deliveryRecorder) { r.failed(categoryPermanent, errors.New("boom")) }, "messaging.events.failed"},
		{"skipped", func(r *deliveryRecorder) { r.skipped(reasonNoHandler) }, "messaging.events.skipped"},
		{"unset defaults to failed", func(r *deliveryRecorder) {}, "messaging.events.failed"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mp, reader := newTestMeterProvider(t)
			inst := newInstruments(mp, slog.Default())
			r := newDeliveryRecorder(inst, tracenoop.Span{}, "documents", "billing", slog.Default())

			tt.act(r)
			r.end()

			got := readMetrics(t, reader)

			for _, name := range terminal {
				_, present := got[name]
				if name == tt.want && !present {
					t.Errorf("terminal counter %q was not recorded", name)
				}
				if name != tt.want && present {
					t.Errorf("terminal counter %q was recorded; want only %q", name, tt.want)
				}
			}

			if _, ok := got["messaging.processing.duration"]; !ok {
				t.Error("processing.duration was not recorded")
			}
		})
	}
}

func TestDeliveryRecorder_EndIsIdempotent(t *testing.T) {
	// The recorder is invoked from a defer on a path that also returns
	// early; a double end would double-count every delivery.
	mp, reader := newTestMeterProvider(t)
	inst := newInstruments(mp, slog.Default())
	r := newDeliveryRecorder(inst, tracenoop.Span{}, "documents", "billing", slog.Default())

	r.processed()
	r.end()
	r.end()

	got := readMetrics(t, reader)
	sum, ok := got["messaging.events.processed"].Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("processed data = %T, want Sum[int64]", got["messaging.events.processed"].Data)
	}
	if len(sum.DataPoints) != 1 || sum.DataPoints[0].Value != 1 {
		t.Fatalf("processed = %+v, want a single data point of 1", sum.DataPoints)
	}
}

func TestDeliveryRecorder_SetEventTypeAttributesTheOutcome(t *testing.T) {
	// setEventType is called only on a typed registry match (see
	// consumeAttrs); confirm it actually reaches the recorded attributes
	// rather than being tracked and silently dropped at end().
	mp, reader := newTestMeterProvider(t)
	inst := newInstruments(mp, slog.Default())
	r := newDeliveryRecorder(inst, tracenoop.Span{}, "documents", "billing", slog.Default())

	r.setEventType("document.filed.v1")
	r.processed()
	r.end()

	got := readMetrics(t, reader)
	sum, ok := got["messaging.events.processed"].Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("processed data = %T, want Sum[int64]", got["messaging.events.processed"].Data)
	}
	if len(sum.DataPoints) != 1 {
		t.Fatalf("processed data points = %d, want 1", len(sum.DataPoints))
	}
	val, ok := sum.DataPoints[0].Attributes.Value(attrEventType)
	if !ok || val.AsString() != "document.filed.v1" {
		t.Errorf("event_type attribute = %v (present=%v), want %q", val, ok, "document.filed.v1")
	}
}
