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
// OpenTelemetry spans and Prometheus metrics (Phase 3) and the
// application-facing testing package (Phase 4) are not yet implemented.
package messaging
