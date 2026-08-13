# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Note that this file versions **the library**. An event's `schema_version` is a
separate, per-contract version owned by the contracts repository, and handler
dispatch matches on its major component only (PRD §11).

## [Unreleased]

## [0.1.0] - 2026-08-13

First tagged release. The library is feature-complete against
[PRD v1.1](docs/foi-messaging-go-prd-v1.1.md), but the API is not yet frozen:
under `v0.x` the Go toolchain will not auto-upgrade across minor versions, and
breaking changes cost only a minor bump. Promotion to `v1.0.0` follows the
first FOI service integration.

### Added

- Strongly-typed event envelope with generic payloads, `event_id` as UUIDv7,
  and publish-time timestamping.
- `Publisher` and `Publish`, keyed on a typed `EventDef` (topic + event type +
  schema version), writing to `{StreamPrefix}:{Topic}` Redis streams. Errors
  are returned synchronously; there is no buffering or publish retry.
- `Consumer`, `RegisterHandler[T]`, and `RegisterRawHandler`, with two-level
  routing: topic to Redis stream, then `event_type` + **major** schema version
  to handler. A `1.0.0` handler receives `1.4.2` events.
- Correlation-ID propagation across service chains, resolved from an explicit
  option, then the context, then a fresh UUIDv7.
- At-least-once delivery: in-process retry with full-jitter backoff, a
  delivery-attempt cap enforced before decoding, reclaim of pending entries
  over `ClaimMinIdle`, and per-topic bounded concurrency.
- Handler error classification (`AsPermanent`, `AsDiscard`) and a Dead Letter
  Queue with a defined wrapper contract. Undecodable, invalid, and unversioned
  envelopes are dead-lettered rather than retried — all three are permanent by
  definition.
- OpenTelemetry spans across the publish and consume paths, with consumer
  spans as children of the producer span, plus delivery-outcome metrics
  exportable to Prometheus.
- Structured `slog` logging with `trace_id`/`span_id` correlation.
- `testing/` (`messagingtest`): `Publisher`, `Deliver`, `Dispatch`,
  `NewEvent`, and `Config`, for unit-testing handlers and publish paths
  without Redis or Docker.

### Notes

- Requires Go 1.25+ and Redis 7.0+.
- Watermill, go-redis, and watermill-redisstream do not cross the library
  boundary; applications import only the root `messaging` package.

[Unreleased]: https://github.com/bcgov/foi-messaging-go/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/bcgov/foi-messaging-go/releases/tag/v0.1.0
