package messaging

import (
	"context"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
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

// outcomeKind is the terminal disposition of one delivery. Exactly one is
// recorded per delivery — see deliveryRecorder.end.
type outcomeKind int

const (
	outcomeUnset outcomeKind = iota
	outcomeProcessed
	outcomeFailed
	outcomeSkipped
)

// deliveryRecorder accumulates the outcome of one delivery and records all
// of it at once.
//
// dispatch has seven terminal exit paths. Instrumenting each in place means
// twenty-odd statements threaded through the subtlest code in the
// repository, and every exit path added later is a chance to forget one.
// Instead each path states its outcome and a single deferred end() records
// the span status, the duration histogram, and exactly one counter.
//
// It also owns the one thing per-site instrumentation cannot get right: the
// duration histogram and the span need a single start and a single end, and
// holding both here is the only shape where they cannot drift apart.
type deliveryRecorder struct {
	inst  *instruments
	span  trace.Span
	log   *slog.Logger
	topic string
	group string
	start time.Time

	kind      outcomeKind
	category  string
	reason    string
	eventType string
	err       error
	ended     bool
}

func newDeliveryRecorder(inst *instruments, span trace.Span, topic, group string, log *slog.Logger) *deliveryRecorder {
	return &deliveryRecorder{
		inst:  inst,
		span:  span,
		log:   log,
		topic: topic,
		group: group,
		start: time.Now(),
	}
}

// setEventType records the event type for metric attribution. It is called
// only on a typed registry match; see consumeAttrs.
func (r *deliveryRecorder) setEventType(eventType string) { r.eventType = eventType }

func (r *deliveryRecorder) processed() { r.kind = outcomeProcessed }

func (r *deliveryRecorder) failed(category string, err error) {
	r.kind = outcomeFailed
	r.category = category
	r.err = err
}

func (r *deliveryRecorder) skipped(reason string) {
	r.kind = outcomeSkipped
	r.reason = reason
}

// end records the delivery. It is idempotent: it is called from a defer on
// paths that also return early, and a second recording would double-count
// every delivery.
func (r *deliveryRecorder) end() {
	if r.ended {
		return
	}
	r.ended = true

	// context.Background() deliberately, not the delivery's own context:
	// metric recording only reads a context for exemplars and cancellation,
	// and the delivery's own context may be cancelled at the shutdown drain
	// deadline — recording through a cancelled context would silently drop
	// the observation for exactly the deliveries most worth counting.
	ctx := context.Background()
	elapsed := time.Since(r.start).Seconds()
	base := consumeAttrs(r.topic, r.group, r.eventType)

	r.inst.processingDuration.Record(ctx, elapsed, metric.WithAttributes(base...))

	switch r.kind {
	case outcomeProcessed:
		r.inst.processed.Add(ctx, 1, metric.WithAttributes(base...))
		r.span.SetStatus(codes.Ok, "")

	case outcomeSkipped:
		r.inst.skipped.Add(ctx, 1, metric.WithAttributes(
			consumeAttrs(r.topic, r.group, "", attribute.String(attrReason, r.reason))...))

	case outcomeFailed:
		r.recordFailure(ctx, base)

	default:
		// Unreachable by design: every dispatch exit path states an
		// outcome. Treated as a failure rather than silently skipped so
		// the "exactly one terminal counter" invariant stays literally
		// true even if a future exit path forgets, and so the omission
		// is visible in the logs rather than as a quiet gap between
		// received and the terminal counters.
		if r.log != nil {
			r.log.Error("messaging: delivery ended with no recorded outcome; this is a bug in the library",
				"topic", r.topic)
		}
		r.category = "unknown"
		r.recordFailure(ctx, base)
	}

	r.span.End()
}

func (r *deliveryRecorder) recordFailure(ctx context.Context, base []attribute.KeyValue) {
	r.inst.failed.Add(ctx, 1, metric.WithAttributes(
		append(base, attribute.String(attrErrorCategory, r.category))...))
	if r.err != nil {
		r.span.RecordError(r.err)
	}
	r.span.SetStatus(codes.Error, r.category)
}
