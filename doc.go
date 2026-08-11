// Package messaging provides a transport-agnostic, strongly-typed
// asynchronous messaging library for FOI platform services, built on
// Watermill and Redis Streams.
//
// See docs/foi-messaging-go-prd-v1.1.md for the full design. The publish
// path (EventDef, Envelope, Config, and Publisher) and the consume path
// (Consumer, Handler, and routing) are implemented. Error classification,
// retry, the delivery-attempt cap, and the dead letter queue are not yet
// implemented, so a failing handler currently nacks and its message is
// redelivered by the reclaim loop indefinitely.
package messaging
