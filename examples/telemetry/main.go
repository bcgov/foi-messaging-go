// Command telemetry shows how to expose foi-messaging-go's metrics to
// Prometheus.
//
// The library records through the OpenTelemetry metric API only. It has no
// Prometheus dependency and exposes no HTTP handler, so applications stay
// free to choose their own pipeline; this is the recipe for the Prometheus
// one. It lives in its own Go module precisely so that choice stays free:
// the exporter pulls in client_golang, and putting that in the library's
// go.mod would hand a Prometheus dependency to every service importing the
// library, including the ones exporting over OTLP.
//
// The View below is not optional. Without it the exporter's default
// histogram boundaries apply, and those are millisecond-scaled
// (0, 5, 10, ... 10000). Both of the library's histograms are in seconds,
// so every realistic observation lands in the first bucket and the
// histograms render as flat lines. The library cannot fix this itself — the
// application owns the MeterProvider and therefore owns the Views. This is
// the single most likely way to finish integrating and still be unable to
// see your own latency.
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/attribute"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// scopeName must match the library's instrumentation scope. It is also what
// appears as the otel_scope_name label on every exported series.
const scopeName = "github.com/bcgov/foi-messaging-go"

const readHeaderTimeout = 5 * time.Second

// NewMeterProvider builds a MeterProvider that exports to reg.
//
// Pass the result as messaging.Config.Telemetry.MeterProvider.
func NewMeterProvider(reg prometheus.Registerer) (*sdkmetric.MeterProvider, error) {
	exporter, err := otelprom.New(otelprom.WithRegisterer(reg))
	if err != nil {
		return nil, fmt.Errorf("creating prometheus exporter: %w", err)
	}

	secondsBuckets := sdkmetric.Stream{
		Aggregation: sdkmetric.AggregationExplicitBucketHistogram{
			Boundaries: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		},
	}

	// Both histograms are named explicitly. A single wildcard is a trap
	// here: the two are not consistently suffixed
	// (messaging.processing.duration versus messaging.queue.latency), so a
	// pattern matching one would silently miss the other — and a broader
	// selector such as "messaging.*" would sweep in the nine counters, for
	// which an explicit-bucket-histogram aggregation is invalid.
	return sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(exporter),
		sdkmetric.WithView(
			sdkmetric.NewView(sdkmetric.Instrument{Name: "messaging.processing.duration"}, secondsBuckets),
			sdkmetric.NewView(sdkmetric.Instrument{Name: "messaging.queue.latency"}, secondsBuckets),
		),
	), nil
}

// counterNames and histogramNames mirror the library's instrument set.
//
// They are a COPY, and that is a real limitation worth stating plainly:
// this module cannot reach the library's unexported instruments, so
// renaming one in telemetry.go will NOT fail the tests in this package. The
// rename guard is TestNewInstruments_CreatesEveryInstrument in the library
// itself, which asserts against the real instruments. What the tests here
// guard is the OTel-to-Prometheus name translation — that dots become
// underscores, monotonic counters gain _total, unit "s" becomes _seconds,
// and annotation units like {event} are dropped rather than suffixed.
//
// Both lists must be updated together; telemetry.go carries a comment
// saying so.
var (
	counterNames = []struct{ name, unit string }{
		{"messaging.events.published", "{event}"},
		{"messaging.publish.failures", "{event}"},
		{"messaging.events.received", "{event}"},
		{"messaging.events.processed", "{event}"},
		{"messaging.events.failed", "{event}"},
		{"messaging.events.skipped", "{event}"},
		{"messaging.retries", "{retry}"},
		{"messaging.dlq", "{event}"},
		{"messaging.dlq.publish.failures", "{event}"},
	}
	histogramNames = []string{
		"messaging.processing.duration",
		"messaging.queue.latency",
	}
)

// recordOneOfEach touches every instrument the library defines, so the
// exporter has a series for each. It exists for this package's tests, which
// pin the exported Prometheus names; a real application records nothing
// itself and simply hands the provider to messaging.Config.
func recordOneOfEach(ctx context.Context, mp *sdkmetric.MeterProvider) error {
	m := mp.Meter(scopeName)
	attrs := metric.WithAttributes(attribute.String("topic", "documents"))

	for _, c := range counterNames {
		counter, err := m.Int64Counter(c.name, metric.WithUnit(c.unit))
		if err != nil {
			return fmt.Errorf("creating %s: %w", c.name, err)
		}
		counter.Add(ctx, 1, attrs)
	}

	for _, name := range histogramNames {
		h, err := m.Float64Histogram(name, metric.WithUnit("s"))
		if err != nil {
			return fmt.Errorf("creating %s: %w", name, err)
		}
		h.Record(ctx, 0.42, attrs)
	}

	return nil
}

func main() {
	reg := prometheus.NewRegistry()

	mp, err := NewMeterProvider(reg)
	if err != nil {
		log.Fatalf("building meter provider: %v", err)
	}
	defer func() {
		if err := mp.Shutdown(context.Background()); err != nil {
			log.Printf("shutting down meter provider: %v", err)
		}
	}()

	// A real application wires the provider into the library and records
	// nothing itself:
	//
	//	cfg := messaging.Config{
	//	    Source: "billing.service",
	//	    Redis:  messaging.RedisConfig{Address: "redis:6379"},
	//	    Telemetry: messaging.TelemetryConfig{MeterProvider: mp},
	//	}
	//
	// Here we record one of each so /metrics is non-empty on first scrape.
	if err := recordOneOfEach(context.Background(), mp); err != nil {
		log.Fatalf("recording sample metrics: %v", err)
	}

	http.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	log.Println("serving metrics on :2112/metrics")

	srv := &http.Server{Addr: ":2112", ReadHeaderTimeout: readHeaderTimeout}
	if err := srv.ListenAndServe(); err != nil {
		log.Fatalf("serving metrics: %v", err)
	}
}
