# Recreating a lost consumer group — design

## Problem

`EnsureGroup` (`XGROUP CREATE … 0 MKSTREAM`) runs once per stream, in
`Subscriber.Subscribe`, when `Consumer.Run` starts. If the group disappears while
the consumer is running — `FLUSHALL`, a Redis restart without persistence, a
failover to a replica that never had the group, `DEL` on the stream,
`XGROUP DESTROY` — every later `XREADGROUP`, `XPENDING` and `XCLAIM` fails with
`NOGROUP`. The read loop and the claim loop treat that like any other transient
error: log, sleep `readErrorBackoff`, retry, forever. `Run` never returns, so the
consumer looks alive, but nothing on that stream is consumed again until the
process restarts. A restart is the only thing that fixes it, because that is the
only time `EnsureGroup` runs.

## Decision

A `NOGROUP` reply is not transient; it means the group is gone. When the read
loop or the claim loop sees one, it recreates the group with the same
`EnsureGroup` the startup path uses, then carries on.

- **Recreate at ID `0`**, exactly as at startup. If only the group was lost and
  the stream survived, everything still on the stream is redelivered. Delivery is
  already at-least-once and handlers must already be idempotent, so a burst of
  duplicates is preferred over `$`, which would silently skip every event
  published between the loss and the recreation. If the stream was lost too,
  there is nothing to replay.
- The old group's Pending Entries List is gone with it. Entries that were in
  flight are not recoverable through reclaim; with `0` they are redelivered as
  new reads instead, provided the stream itself survived.
- An `XACK` for a delivery made under the old group does not fail on the new
  one — the ID is simply not pending — so the ack path needs no change.

## Shape

- `internal/redis` exports `ErrNoGroup`. `ReadNew`, `PendingOverIdle` and
  `Claim` wrap a `NOGROUP` reply so `errors.Is(err, ErrNoGroup)` holds, while
  keeping the original error in the chain. Detection is by reply prefix, the
  same way `EnsureGroup` already recognises `BUSYGROUP`.
- `internal/watermill.Subscriber` checks `errors.Is(err, internalredis.ErrNoGroup)`
  in both loops and calls `recreateGroup`, which logs a WARN
  (`messaging: consumer group missing, recreating`) and calls `EnsureGroup`.
  On success the loop retries immediately; if recreation itself fails it logs
  the ERROR and falls back to `readErrorBackoff`, as for any other failure.
- Recreation is idempotent: the read loop and the claim loop of one
  subscription, and every other instance in the group, may all race to
  recreate. `EnsureGroup` already tolerates `BUSYGROUP`.
- The `StreamReader` interface is unchanged; the root package is untouched.

## Out of scope

- A blocked `XREADGROUP` whose stream is deleted mid-block is released by Redis
  with an `UNBLOCKED …` error rather than `NOGROUP`. That is logged as an
  ordinary read failure; the next read then gets `NOGROUP` and recovers. No
  special case is needed.

## Tests

- `internal/redis` integration: `ReadNew`, `PendingOverIdle`, `Claim` against a
  destroyed group each return an error satisfying `errors.Is(err, ErrNoGroup)`.
- `internal/watermill` unit: a reader whose group is "lost" returns `ErrNoGroup`
  until `EnsureGroup` is called again; the subscriber recreates it and delivers
  the next entry. Same for the claim loop.
- Root integration: a running consumer keeps consuming after `XGROUP DESTROY`,
  and after `DEL` of the stream.
