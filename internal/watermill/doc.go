// Package watermill wraps Watermill's Publisher and Subscriber over Redis
// Streams: Publisher publishes messages via watermill-redisstream, and
// Subscriber implements message.Subscriber with a bounded-concurrency read
// loop plus a claim loop that reclaims nacked or abandoned pending entries.
// This package is internal: no application code, and no other package in
// this module outside internal/, may import Watermill directly (PRD §12,
// §21).
//
// See docs/foi-messaging-go-prd-v1.1.md §12.
package watermill
