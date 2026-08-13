package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// scrape builds a provider exporting to a fresh registry, records one of
// every instrument, and returns the Prometheus text exposition.
func scrape(t *testing.T) string {
	t.Helper()

	reg := prometheus.NewRegistry()
	mp, err := NewMeterProvider(reg)
	if err != nil {
		t.Fatalf("NewMeterProvider() = %v, want nil", err)
	}
	if err := recordOneOfEach(context.Background(), mp); err != nil {
		t.Fatalf("recordOneOfEach() = %v, want nil", err)
	}

	srv := httptest.NewServer(promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("GET %s = %v, want nil", srv.URL, err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading body = %v, want nil", err)
	}
	return string(body)
}

// TestPrometheusNames verifies the OTel-to-Prometheus name translation: that
// dotted instrument names become underscored ones, that monotonic counters
// gain _total, that unit "s" becomes _seconds, and that {event} and {retry}
// annotation units are dropped rather than suffixed.
//
// It is NOT a rename guard for the library's instruments — see the comment
// on counterNames in main.go. The guard is
// TestNewInstruments_CreatesEveryInstrument in the library module.
func TestPrometheusNames(t *testing.T) {
	out := scrape(t)

	want := []string{
		"messaging_events_published_total",
		"messaging_publish_failures_total",
		"messaging_events_received_total",
		"messaging_events_processed_total",
		"messaging_events_failed_total",
		"messaging_events_skipped_total",
		"messaging_retries_total",
		"messaging_dlq_total",
		"messaging_dlq_publish_failures_total",
		"messaging_processing_duration_seconds",
		"messaging_queue_latency_seconds",
	}
	for _, name := range want {
		if !strings.Contains(out, name) {
			t.Errorf("exporter output is missing %q", name)
		}
	}

	// The annotation units must not leak into names. A regression here
	// would produce messaging_events_published_event_total and break every
	// dashboard silently.
	for _, bad := range []string{"_event_total", "_retry_total", "_{event}", "_{retry}"} {
		if strings.Contains(out, bad) {
			t.Errorf("exporter output contains %q; annotation units must be dropped, not suffixed", bad)
		}
	}
}

// TestSecondScaleBuckets guards the View. Without it the exporter's default
// boundaries are millisecond-scaled (0, 5, 10, ... 10000), and every
// realistic duration in seconds lands in the first bucket, making both
// histograms render as flat lines.
func TestSecondScaleBuckets(t *testing.T) {
	out := scrape(t)

	for _, histogram := range []string{
		"messaging_processing_duration_seconds",
		"messaging_queue_latency_seconds",
	} {
		if !strings.Contains(out, histogram+`_bucket{`) {
			t.Fatalf("%s has no buckets", histogram)
		}
	}

	// A second-scale boundary the default millisecond boundaries do not
	// contain. Its presence proves the View was applied.
	if !strings.Contains(out, `le="0.005"`) {
		t.Error(`no le="0.005" bucket; the seconds View was not applied and both histograms are unusable`)
	}

	// 0.42s must land below the 0.5 boundary and above 0.25. If the
	// millisecond defaults were in force it would sit in the first bucket
	// instead, which the boundary check above would miss on its own.
	if !strings.Contains(out, `le="0.5"} 1`) {
		t.Errorf("a 0.42s observation did not land in the le=0.5 bucket\n%s", out)
	}
	if !strings.Contains(out, `le="0.25"} 0`) {
		t.Errorf("a 0.42s observation was counted below le=0.25\n%s", out)
	}
}
