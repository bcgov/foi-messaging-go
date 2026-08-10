// Package redis wraps the go-redis client and the Redis Streams
// adapter. This package is internal: no application code, and no other
// package in this module outside internal/, may import go-redis
// directly (PRD §12, §21).
//
// See docs/foi-messaging-go-prd-v1.1.md §12. Not yet implemented — Phase
// 0 scaffolding only.
package redis
