// Package messagingtest (imported from the testing/ directory) lets
// applications that consume this library unit-test their publish paths and
// handlers without a running Redis instance.
//
// It drives the library's real code rather than reimplementing it.
// Publisher wraps a genuine messaging.Publisher with only its transport
// write redirected, so envelope construction, correlation-ID resolution,
// and validation are the real ones; Dispatch runs the library's real
// consume path, so the delivery-attempt cap, error classification, the
// immediate-retry loop, and dead-lettering behave exactly as they do in
// production.
//
// Three boundaries, in increasing order of what they cover:
//
//	messagingtest.Deliver    // unit-test a handler
//	messagingtest.Dispatch   // test messaging and router behaviour
//	// real Redis + real messaging stack — see the integration test tier
//
// Deliver reproduces the handler boundary only: it installs the context
// values the router installs, invokes the handler, and returns its error.
// It does not retry, ack, nack, reclaim, or dead-letter. Dispatch covers
// all of that, reporting what the runtime would do with a delivery without
// performing one.
//
// What is deliberately absent: there is no in-memory Redis, no
// Consumer.Run substitute, and no assertion helpers — this package exposes
// accessors and leaves the assertions to the standard library's testing
// package, which it does not import.
//
// See docs/foi-messaging-go-prd-v1.1.md §19.
package messagingtest
