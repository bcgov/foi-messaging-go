package messaging

import (
	"log/slog"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

// telemetryScope is the instrumentation scope every instrument and tracer
// is created under. It shows up as otel_scope_name on exported Prometheus
// series, so it is stable API in practice.
const telemetryScope = "github.com/bcgov/foi-messaging-go"

// Metric attribute keys. Named constants because the same key is set from
// several call sites across publisher.go and consumer.go, and a typo would
// silently split one series into two rather than failing anything.
const (
	attrTopic         = "topic"
	attrEventType     = "event_type"
	attrGroup         = "group"
	attrReason        = "reason"
	attrErrorCategory = "error_category"
	attrStage         = "stage"
)

// Publish failure stages (spec §2). They separate "this service is
// emitting garbage" from "Redis is unreachable".
const (
	stageValidation = "validation"
	stageMarshal    = "marshal"
	stageTransport  = "transport"
)

// Skip reasons and error categories, fixed by PRD §16 and spec §2.
const (
	reasonNoHandler = "no_handler"
	reasonDiscard   = "discard"

	categoryPermanent       = "permanent"
	categoryRetryable       = "retryable"
	categoryDeserialization = "deserialization"
	categoryMaxAttempts     = "max_attempts"
)

// instruments holds every metric the library records. It is built once per
// Publisher and per Consumer; recording through it is safe from any
// goroutine, as OTel instruments are.
//
// Renaming anything here is a breaking change for every dashboard built on
// this library. Two tests must be updated together:
// TestNewInstruments_CreatesEveryInstrument in this package (the real
// rename guard), and TestPrometheusNames in examples/telemetry, which is a
// separate module and therefore holds a copy of the list.
type instruments struct {
	published          metric.Int64Counter
	publishFailures    metric.Int64Counter
	received           metric.Int64Counter
	processed          metric.Int64Counter
	failed             metric.Int64Counter
	skipped            metric.Int64Counter
	retries            metric.Int64Counter
	dlq                metric.Int64Counter
	dlqPublishFailures metric.Int64Counter
	processingDuration metric.Float64Histogram
	queueLatency       metric.Float64Histogram
}

// newInstruments builds every instrument from mp.
//
// Instrument creation can fail, but every name here is a compile-time
// constant, so a failure means something is wrong with the provider rather
// than with this call. Rather than propagate that into NewPublisher /
// NewConsumer — where it would stop a service delivering messages over a
// broken metrics pipeline — the whole set is rebuilt from the no-op
// provider, which cannot fail, and the reason is logged once. Dropping
// metrics is strictly better than dropping messages.
//
// A nil mp takes the same path, which is what makes instruments usable from
// tests that never configure telemetry.
func newInstruments(mp metric.MeterProvider, log *slog.Logger) *instruments {
	if mp != nil {
		if inst, err := buildInstruments(mp); err == nil {
			return inst
		} else if log != nil {
			log.Warn("messaging: metric instrument creation failed; metrics are disabled for this instance",
				"error", err)
		}
	}

	// Cannot fail: the no-op meter returns no errors.
	inst, _ := buildInstruments(noop.NewMeterProvider())
	return inst
}

// buildInstruments creates the full set, returning on the first failure so
// newInstruments never hands back a partially-wired struct with nil
// instruments in it — recording through one of those panics.
func buildInstruments(mp metric.MeterProvider) (*instruments, error) {
	m := mp.Meter(telemetryScope)
	var inst instruments
	var err error

	// The {event} and {retry} units are UCUM annotations. The Prometheus
	// exporter drops them rather than suffixing the metric name, so they
	// are free documentation; "s" by contrast becomes the _seconds suffix
	// that lands these on the PRD §16 spellings.
	if inst.published, err = m.Int64Counter("messaging.events.published",
		metric.WithDescription("Events successfully published."),
		metric.WithUnit("{event}")); err != nil {
		return nil, err
	}
	if inst.publishFailures, err = m.Int64Counter("messaging.publish.failures",
		metric.WithDescription("Publish attempts that returned an error, by stage."),
		metric.WithUnit("{event}")); err != nil {
		return nil, err
	}
	if inst.received, err = m.Int64Counter("messaging.events.received",
		metric.WithDescription("Stream entries taken off a stream for delivery."),
		metric.WithUnit("{event}")); err != nil {
		return nil, err
	}
	if inst.processed, err = m.Int64Counter("messaging.events.processed",
		metric.WithDescription("Deliveries whose handler returned nil."),
		metric.WithUnit("{event}")); err != nil {
		return nil, err
	}
	if inst.failed, err = m.Int64Counter("messaging.events.failed",
		metric.WithDescription("Deliveries that ended in failure, by error category."),
		metric.WithUnit("{event}")); err != nil {
		return nil, err
	}
	if inst.skipped, err = m.Int64Counter("messaging.events.skipped",
		metric.WithDescription("Deliveries acked without processing, by reason."),
		metric.WithUnit("{event}")); err != nil {
		return nil, err
	}
	if inst.retries, err = m.Int64Counter("messaging.retries",
		metric.WithDescription("Immediate in-process handler retries."),
		metric.WithUnit("{retry}")); err != nil {
		return nil, err
	}
	if inst.dlq, err = m.Int64Counter("messaging.dlq",
		metric.WithDescription("Events written to a dead letter queue, by reason."),
		metric.WithUnit("{event}")); err != nil {
		return nil, err
	}
	if inst.dlqPublishFailures, err = m.Int64Counter("messaging.dlq.publish.failures",
		metric.WithDescription("Failed attempts to write a dead letter."),
		metric.WithUnit("{event}")); err != nil {
		return nil, err
	}
	if inst.processingDuration, err = m.Float64Histogram("messaging.processing.duration",
		metric.WithDescription("End-to-end dispatch duration for one delivery, including immediate retries."),
		metric.WithUnit("s")); err != nil {
		return nil, err
	}
	if inst.queueLatency, err = m.Float64Histogram("messaging.queue.latency",
		metric.WithDescription("Elapsed time from publish to the start of dispatch."),
		metric.WithUnit("s")); err != nil {
		return nil, err
	}

	return &inst, nil
}

// consumeAttrs builds the attribute set shared by the consume-path
// instruments.
//
// eventType is attached only when it came from a typed registry match. A
// raw handler takes every event on its topic, and the deserialization and
// cap paths never reach a lookup at all, so in those cases the value is
// whatever the wire said — unbounded, and one bad producer away from
// exploding the metric store. Spans carry it regardless; see spec §2.
func consumeAttrs(topic, group, eventType string, extra ...attribute.KeyValue) []attribute.KeyValue {
	attrs := make([]attribute.KeyValue, 0, 3+len(extra))
	attrs = append(attrs, attribute.String(attrTopic, topic), attribute.String(attrGroup, group))
	if eventType != "" {
		attrs = append(attrs, attribute.String(attrEventType, eventType))
	}
	return append(attrs, extra...)
}
