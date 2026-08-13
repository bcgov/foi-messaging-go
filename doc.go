// Package messaging provides a transport-agnostic, strongly-typed
// asynchronous messaging library for FOI platform services, built on
// Watermill and Redis Streams.
//
// See docs/foi-messaging-go-prd-v1.1.md for the full design. The publish
// path (EventDef, Envelope, Config, and Publisher) and the consume path
// (Consumer, Handler, and routing) are implemented, as is the failure
// path: handlers classify errors with AsPermanent, AsRetryable, or
// AsDiscard, a retryable failure is retried in-process with exponential
// backoff and full jitter before the message nacks, redelivery is bounded
// by Consumer.MaxDeliveryAttempts, and an event that reaches the end of
// that path is published to its topic's dead letter queue as a DeadLetter
// and acked.
//
// Observability is live: Publish opens a producer span and dispatch opens a
// consumer span parented to it via traceparent transport metadata, and both
// paths record OpenTelemetry metrics — eleven instruments covering publish,
// receive, process, failure, skip, retry, and dead-letter counts plus
// processing-duration and queue-latency histograms. The library depends on
// the OTel metric API only; see examples/telemetry for Prometheus wiring,
// including the histogram bucket View that recipe requires.
//
// The application-facing testing package is implemented: import
// github.com/bcgov/foi-messaging-go/testing to record publishes, invoke a
// handler at the router's boundary, and assert what the consume path would
// do with a delivery — all without Redis.
package messaging
