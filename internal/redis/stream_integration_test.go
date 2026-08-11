//go:build integration

package redis_test

import (
	"context"
	"testing"
	"time"

	internalredis "github.com/bcgov/foi-messaging-go/internal/redis"
	"github.com/bcgov/foi-messaging-go/internal/testsupport"
	goredis "github.com/redis/go-redis/v9"
)

const (
	testGroup    = "test-group"
	testConsumer = "test-consumer"
)

// newReader starts a Redis container and returns a StreamReader bound to it.
func newReader(t *testing.T) (*internalredis.StreamReader, *goredis.Client) {
	t.Helper()
	ctx := context.Background()

	addr, terminate, err := testsupport.StartRedis(ctx)
	if err != nil {
		t.Fatalf("StartRedis: %v", err)
	}
	t.Cleanup(func() {
		if err := terminate(ctx); err != nil {
			t.Errorf("terminate: %v", err)
		}
	})

	client := internalredis.NewClient(internalredis.ClientOptions{Address: addr})
	reader := internalredis.NewStreamReader(client, testGroup, testConsumer)
	t.Cleanup(func() {
		if err := reader.Close(); err != nil {
			t.Errorf("reader.Close: %v", err)
		}
	})
	return reader, client
}

func TestStreamReader_EnsureGroup_IsIdempotent(t *testing.T) {
	ctx := context.Background()
	reader, _ := newReader(t)

	if err := reader.EnsureGroup(ctx, "s1"); err != nil {
		t.Fatalf("first EnsureGroup: %v", err)
	}
	if err := reader.EnsureGroup(ctx, "s1"); err != nil {
		t.Fatalf("second EnsureGroup should tolerate BUSYGROUP, got: %v", err)
	}
}

func TestStreamReader_EnsureGroup_ReadsFromOldest(t *testing.T) {
	ctx := context.Background()
	reader, client := newReader(t)

	// Publish BEFORE the group exists. Creating at "0" must still see it.
	if err := client.XAdd(ctx, &goredis.XAddArgs{
		Stream: "s2",
		Values: map[string]any{"payload": "before-group"},
	}).Err(); err != nil {
		t.Fatalf("XAdd: %v", err)
	}

	if err := reader.EnsureGroup(ctx, "s2"); err != nil {
		t.Fatalf("EnsureGroup: %v", err)
	}

	entries, err := reader.ReadNew(ctx, "s2", 10, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("ReadNew: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("len(entries) = %d, want 1 (group created at 0 must replay history)", len(entries))
	}
	if got := entries[0].Fields["payload"]; got != "before-group" {
		t.Errorf("payload = %v, want %q", got, "before-group")
	}
}

func TestStreamReader_ReadNew_ReturnsEmptyWhenNothingPending(t *testing.T) {
	ctx := context.Background()
	reader, _ := newReader(t)

	if err := reader.EnsureGroup(ctx, "s3"); err != nil {
		t.Fatalf("EnsureGroup: %v", err)
	}

	entries, err := reader.ReadNew(ctx, "s3", 10, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("ReadNew on empty stream must not error, got: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("len(entries) = %d, want 0", len(entries))
	}
}

func TestStreamReader_PendingOverIdle_ReportsRetryCount(t *testing.T) {
	ctx := context.Background()
	reader, client := newReader(t)

	if err := reader.EnsureGroup(ctx, "s4"); err != nil {
		t.Fatalf("EnsureGroup: %v", err)
	}
	if err := client.XAdd(ctx, &goredis.XAddArgs{
		Stream: "s4",
		Values: map[string]any{"payload": "p"},
	}).Err(); err != nil {
		t.Fatalf("XAdd: %v", err)
	}

	// One delivery, never acked.
	entries, err := reader.ReadNew(ctx, "s4", 10, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("ReadNew: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("len(entries) = %d, want 1", len(entries))
	}

	time.Sleep(150 * time.Millisecond)

	pending, err := reader.PendingOverIdle(ctx, "s4", 100*time.Millisecond, 10)
	if err != nil {
		t.Fatalf("PendingOverIdle: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("len(pending) = %d, want 1", len(pending))
	}
	if pending[0].ID != entries[0].ID {
		t.Errorf("pending ID = %q, want %q", pending[0].ID, entries[0].ID)
	}
	if pending[0].RetryCount != 1 {
		t.Errorf("RetryCount = %d, want 1 after a single delivery", pending[0].RetryCount)
	}
}

func TestStreamReader_Claim_IncrementsDeliveryCount(t *testing.T) {
	ctx := context.Background()
	reader, client := newReader(t)

	if err := reader.EnsureGroup(ctx, "s5"); err != nil {
		t.Fatalf("EnsureGroup: %v", err)
	}
	if err := client.XAdd(ctx, &goredis.XAddArgs{
		Stream: "s5",
		Values: map[string]any{"payload": "p"},
	}).Err(); err != nil {
		t.Fatalf("XAdd: %v", err)
	}
	if _, err := reader.ReadNew(ctx, "s5", 10, 100*time.Millisecond); err != nil {
		t.Fatalf("ReadNew: %v", err)
	}

	time.Sleep(150 * time.Millisecond)

	pending, err := reader.PendingOverIdle(ctx, "s5", 100*time.Millisecond, 10)
	if err != nil {
		t.Fatalf("PendingOverIdle: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("len(pending) = %d, want 1", len(pending))
	}

	claimed, err := reader.Claim(ctx, "s5", 100*time.Millisecond, []string{pending[0].ID})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("len(claimed) = %d, want 1", len(claimed))
	}

	time.Sleep(150 * time.Millisecond)

	after, err := reader.PendingOverIdle(ctx, "s5", 100*time.Millisecond, 10)
	if err != nil {
		t.Fatalf("PendingOverIdle after claim: %v", err)
	}
	if len(after) != 1 {
		t.Fatalf("len(after) = %d, want 1", len(after))
	}
	// This is why the subscriber stamps RetryCount+1: XCLAIM is itself a delivery.
	if after[0].RetryCount != 2 {
		t.Errorf("RetryCount after claim = %d, want 2", after[0].RetryCount)
	}
}

func TestStreamReader_Ack_RemovesFromPending(t *testing.T) {
	ctx := context.Background()
	reader, client := newReader(t)

	if err := reader.EnsureGroup(ctx, "s6"); err != nil {
		t.Fatalf("EnsureGroup: %v", err)
	}
	if err := client.XAdd(ctx, &goredis.XAddArgs{
		Stream: "s6",
		Values: map[string]any{"payload": "p"},
	}).Err(); err != nil {
		t.Fatalf("XAdd: %v", err)
	}
	entries, err := reader.ReadNew(ctx, "s6", 10, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("ReadNew: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("len(entries) = %d, want 1", len(entries))
	}

	if err := reader.Ack(ctx, "s6", entries[0].ID); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	pending, err := reader.PendingOverIdle(ctx, "s6", 0, 10)
	if err != nil {
		t.Fatalf("PendingOverIdle: %v", err)
	}
	if len(pending) != 0 {
		t.Errorf("len(pending) = %d, want 0 after Ack", len(pending))
	}
}
