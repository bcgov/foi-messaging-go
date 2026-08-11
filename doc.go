// Package messaging provides a transport-agnostic, strongly-typed
// asynchronous messaging library for FOI platform services, built on
// Watermill and Redis Streams.
//
// See docs/foi-messaging-go-prd-v1.1.md for the full design. The publish
// path (EventDef, Envelope, Config, and Publisher) is implemented; the
// consumer path (routing, retry, DLQ, and telemetry instrumentation) is
// not yet implemented.
package messaging
