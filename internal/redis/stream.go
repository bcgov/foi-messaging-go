package redis

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// ErrNoGroup reports that the consumer group, or the stream holding it, no
// longer exists on the server — a FLUSHALL, a restart without persistence, a
// failover to a replica that never had the group, a DEL of the stream, or an
// XGROUP DESTROY. It is not transient: retrying the same command can never
// succeed until the group is created again, so callers test for it with
// errors.Is and call EnsureGroup rather than backing off.
var ErrNoGroup = errors.New("consumer group does not exist")

// classifyGroupErr marks a NOGROUP reply with ErrNoGroup, keeping the
// original error in the chain. Detection is by reply prefix, the same way
// EnsureGroup recognises BUSYGROUP: go-redis exposes no typed error for it.
func classifyGroupErr(err error) error {
	if strings.HasPrefix(err.Error(), "NOGROUP") {
		return fmt.Errorf("%w: %w", ErrNoGroup, err)
	}
	return err
}

// Entry is a single Redis Streams entry, reduced to plain types so no
// go-redis value escapes this package.
type Entry struct {
	ID     string
	Fields map[string]any
}

// PendingEntry describes one entry in a consumer group's Pending Entries
// List. RetryCount is the number of deliveries that have already happened;
// a claim or read issued now is the next one.
type PendingEntry struct {
	ID         string
	RetryCount int64
	Idle       time.Duration
}

// StreamReader issues the Redis Streams commands the consumer needs,
// bound to a single consumer group and consumer name.
type StreamReader struct {
	client   *goredis.Client
	group    string
	consumer string
}

// NewStreamReader builds a StreamReader. It takes ownership of client and
// closes it when Close is called.
func NewStreamReader(client *goredis.Client, group, consumer string) *StreamReader {
	return &StreamReader{client: client, group: group, consumer: consumer}
}

// EnsureGroup creates the consumer group on stream at ID "0", creating the
// stream if absent. An already-existing group is not an error, so every
// consumer instance can call this unconditionally on startup.
//
// Starting at "0" means a newly created group replays whatever history is
// still on the stream, so a service deployed after its producer does not
// silently miss events.
func (r *StreamReader) EnsureGroup(ctx context.Context, stream string) error {
	err := r.client.XGroupCreateMkStream(ctx, stream, r.group, "0").Err()
	if err == nil || strings.Contains(err.Error(), "BUSYGROUP") {
		return nil
	}
	return fmt.Errorf("creating consumer group %q on stream %q: %w", r.group, stream, err)
}

// ReadNew issues XREADGROUP for never-delivered entries. Every entry it
// returns is a first delivery. An empty result is not an error.
func (r *StreamReader) ReadNew(ctx context.Context, stream string, count int64, block time.Duration) ([]Entry, error) {
	streams, err := r.client.XReadGroup(ctx, &goredis.XReadGroupArgs{
		Group:    r.group,
		Consumer: r.consumer,
		Streams:  []string{stream, ">"},
		Count:    count,
		Block:    block,
	}).Result()
	if err == goredis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("reading group %q on stream %q: %w", r.group, stream, classifyGroupErr(err))
	}

	var entries []Entry
	for _, s := range streams {
		for _, m := range s.Messages {
			entries = append(entries, Entry{ID: m.ID, Fields: m.Values})
		}
	}
	return entries, nil
}

// PendingOverIdle lists entries pending longer than minIdle. It is the only
// source of delivery counts, and is already the reclaim discovery step, so
// exact attempt counts cost no extra round-trip.
func (r *StreamReader) PendingOverIdle(ctx context.Context, stream string, minIdle time.Duration, count int64) ([]PendingEntry, error) {
	pending, err := r.client.XPendingExt(ctx, &goredis.XPendingExtArgs{
		Stream: stream,
		Group:  r.group,
		Idle:   minIdle,
		Start:  "-",
		End:    "+",
		Count:  count,
	}).Result()
	if err == goredis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("listing pending entries on stream %q: %w", stream, classifyGroupErr(err))
	}

	entries := make([]PendingEntry, 0, len(pending))
	for _, p := range pending {
		entries = append(entries, PendingEntry{
			ID:         p.ID,
			RetryCount: p.RetryCount,
			Idle:       p.Idle,
		})
	}
	return entries, nil
}

// Claim takes ownership of pending entries via XCLAIM. minIdle is passed as
// MINIDLE so two instances cannot claim the same entry concurrently.
//
// XCLAIM increments each entry's delivery counter — it is deliberately not
// issued with JUSTID — so a claimed entry's true attempt number is the
// RetryCount reported by PendingOverIdle plus one.
func (r *StreamReader) Claim(ctx context.Context, stream string, minIdle time.Duration, ids []string) ([]Entry, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	messages, err := r.client.XClaim(ctx, &goredis.XClaimArgs{
		Stream:   stream,
		Group:    r.group,
		Consumer: r.consumer,
		MinIdle:  minIdle,
		Messages: ids,
	}).Result()
	if err == goredis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("claiming entries on stream %q: %w", stream, classifyGroupErr(err))
	}

	entries := make([]Entry, 0, len(messages))
	for _, m := range messages {
		entries = append(entries, Entry{ID: m.ID, Fields: m.Values})
	}
	return entries, nil
}

// Ack acknowledges entries, removing them from the group's pending list.
func (r *StreamReader) Ack(ctx context.Context, stream string, ids ...string) error {
	if len(ids) == 0 {
		return nil
	}
	if err := r.client.XAck(ctx, stream, r.group, ids...).Err(); err != nil {
		return fmt.Errorf("acking entries on stream %q: %w", stream, err)
	}
	return nil
}

// Close releases the underlying Redis client.
func (r *StreamReader) Close() error {
	return r.client.Close()
}
