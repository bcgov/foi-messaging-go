package watermill

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	internalredis "github.com/bcgov/foi-messaging-go/internal/redis"
)

// fakeReader is a StreamReader that serves canned entries and records calls,
// so the subscriber's slot and ack behaviour can be tested without Redis.
type fakeReader struct {
	mu sync.Mutex

	queued  []internalredis.Entry
	pending []internalredis.PendingEntry
	claimed map[string]internalredis.Entry

	acked []string
	reads int
}

func newFakeReader(entries ...internalredis.Entry) *fakeReader {
	return &fakeReader{queued: entries, claimed: map[string]internalredis.Entry{}}
}

func (f *fakeReader) EnsureGroup(context.Context, string) error { return nil }

func (f *fakeReader) ReadNew(ctx context.Context, _ string, count int64, block time.Duration) ([]internalredis.Entry, error) {
	f.mu.Lock()
	f.reads++
	if len(f.queued) > 0 {
		n := int(count)
		if n > len(f.queued) {
			n = len(f.queued)
		}
		out := f.queued[:n]
		f.queued = f.queued[n:]
		f.mu.Unlock()
		return out, nil
	}
	f.mu.Unlock()

	// Nothing left: emulate a blocking read that times out.
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(block):
		return nil, nil
	}
}

func (f *fakeReader) PendingOverIdle(context.Context, string, time.Duration, int64) ([]internalredis.PendingEntry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := f.pending
	f.pending = nil
	return out, nil
}

func (f *fakeReader) Claim(_ context.Context, _ string, _ time.Duration, ids []string) ([]internalredis.Entry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []internalredis.Entry
	for _, id := range ids {
		if e, ok := f.claimed[id]; ok {
			out = append(out, e)
		}
	}
	return out, nil
}

func (f *fakeReader) Ack(_ context.Context, _ string, ids ...string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.acked = append(f.acked, ids...)
	return nil
}

func (f *fakeReader) ackedIDs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.acked...)
}

func (f *fakeReader) readCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.reads
}

// entry builds a stream entry in the wire format the Phase 1 publisher writes.
func entry(id, uuid, payload string) internalredis.Entry {
	return internalredis.Entry{
		ID: id,
		Fields: map[string]any{
			"_watermill_message_uuid": uuid,
			"payload":                 payload,
		},
	}
}

func TestSubscriber_DeliversEntryWithMetadata(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reader := newFakeReader(entry("1-0", "event-a", `{"hello":"world"}`))
	sub, err := NewSubscriber(SubscriberOptions{
		Reader:      reader,
		Concurrency: 1,
		BlockTime:   10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewSubscriber: %v", err)
	}
	defer func() {
		if err := sub.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()

	out, err := sub.Subscribe(ctx, "stream")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	select {
	case msg := <-out:
		if msg.UUID != "event-a" {
			t.Errorf("UUID = %q, want %q", msg.UUID, "event-a")
		}
		if string(msg.Payload) != `{"hello":"world"}` {
			t.Errorf("Payload = %q, want %q", msg.Payload, `{"hello":"world"}`)
		}
		if got := msg.Metadata.Get(MetadataStreamID); got != "1-0" {
			t.Errorf("%s = %q, want %q", MetadataStreamID, got, "1-0")
		}
		if got := msg.Metadata.Get(MetadataDeliveryAttempt); got != "1" {
			t.Errorf("%s = %q, want %q for a fresh read", MetadataDeliveryAttempt, got, "1")
		}
		msg.Ack()
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for a message")
	}
}

func TestSubscriber_AcksEntryOnMessageAck(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reader := newFakeReader(entry("1-0", "event-a", "{}"))
	sub, err := NewSubscriber(SubscriberOptions{
		Reader:      reader,
		Concurrency: 1,
		BlockTime:   10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewSubscriber: %v", err)
	}
	defer func() { _ = sub.Close() }()

	out, err := sub.Subscribe(ctx, "stream")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	msg := <-out
	msg.Ack()

	deadline := time.After(2 * time.Second)
	for {
		if ids := reader.ackedIDs(); len(ids) == 1 && ids[0] == "1-0" {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("acked = %v, want [1-0]", reader.ackedIDs())
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestSubscriber_DoesNotAckOnNack(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reader := newFakeReader(entry("1-0", "event-a", "{}"))
	sub, err := NewSubscriber(SubscriberOptions{
		Reader:      reader,
		Concurrency: 1,
		BlockTime:   10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewSubscriber: %v", err)
	}
	defer func() { _ = sub.Close() }()

	out, err := sub.Subscribe(ctx, "stream")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	msg := <-out
	msg.Nack()

	time.Sleep(200 * time.Millisecond)
	if ids := reader.ackedIDs(); len(ids) != 0 {
		t.Errorf("acked = %v, want none — a nacked entry must stay pending", ids)
	}
}

func TestSubscriber_AcquiresSlotBeforeReading(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reader := newFakeReader(
		entry("1-0", "event-a", "{}"),
		entry("2-0", "event-b", "{}"),
	)
	sub, err := NewSubscriber(SubscriberOptions{
		Reader:      reader,
		Concurrency: 1,
		BlockTime:   10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewSubscriber: %v", err)
	}
	defer func() { _ = sub.Close() }()

	out, err := sub.Subscribe(ctx, "stream")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	first := <-out
	// Hold the only slot. The loop must not read again while it is held,
	// so an unread entry must never be fetched into a local buffer where
	// its Redis idle clock would run down.
	countWhileHeld := reader.readCount()
	time.Sleep(200 * time.Millisecond)
	if got := reader.readCount(); got != countWhileHeld {
		t.Errorf("reads went %d → %d while the slot was held; must not read without a free slot", countWhileHeld, got)
	}

	first.Ack()

	select {
	case second := <-out:
		if second.UUID != "event-b" {
			t.Errorf("UUID = %q, want %q", second.UUID, "event-b")
		}
		second.Ack()
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the second message after the slot freed")
	}
}

func TestSubscriber_ClosesOutputChannelOnClose(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sub, err := NewSubscriber(SubscriberOptions{
		Reader:      newFakeReader(),
		Concurrency: 1,
		BlockTime:   10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewSubscriber: %v", err)
	}

	out, err := sub.Subscribe(ctx, "stream")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	if err := sub.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := sub.Close(); err != nil {
		t.Fatalf("second Close must be idempotent, got: %v", err)
	}

	select {
	case _, open := <-out:
		if open {
			t.Error("expected the output channel to be closed")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the output channel to close")
	}
}

func TestNewSubscriber_RejectsMissingReader(t *testing.T) {
	_, err := NewSubscriber(SubscriberOptions{Concurrency: 1})
	if err == nil {
		t.Fatal("expected an error when Reader is nil")
	}
	if !errors.Is(err, ErrNoReader) {
		t.Errorf("err = %v, want ErrNoReader", err)
	}
}

func TestSubscriber_ClaimLoopRedeliversWithIncrementedAttempt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reader := newFakeReader()
	// One entry already delivered once and left pending.
	reader.pending = []internalredis.PendingEntry{
		{ID: "1-0", RetryCount: 1, Idle: time.Second},
	}
	reader.claimed["1-0"] = entry("1-0", "event-a", "{}")

	sub, err := NewSubscriber(SubscriberOptions{
		Reader:        reader,
		Concurrency:   1,
		ClaimInterval: 20 * time.Millisecond,
		ClaimMinIdle:  10 * time.Millisecond,
		BlockTime:     10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewSubscriber: %v", err)
	}
	defer func() { _ = sub.Close() }()

	out, err := sub.Subscribe(ctx, "stream")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	select {
	case msg := <-out:
		if msg.UUID != "event-a" {
			t.Errorf("UUID = %q, want %q", msg.UUID, "event-a")
		}
		// XPENDING reported 1 prior delivery; the XCLAIM just issued is
		// the second, so the stamped attempt must be 2.
		if got := msg.Metadata.Get(MetadataDeliveryAttempt); got != "2" {
			t.Errorf("%s = %q, want %q (RetryCount+1)", MetadataDeliveryAttempt, got, "2")
		}
		if got := msg.Metadata.Get(MetadataStreamID); got != "1-0" {
			t.Errorf("%s = %q, want %q", MetadataStreamID, got, "1-0")
		}
		msg.Ack()
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for a reclaimed message")
	}
}

func TestSubscriber_ClaimLoopSkippedWithoutClaimInterval(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reader := newFakeReader()
	reader.pending = []internalredis.PendingEntry{
		{ID: "1-0", RetryCount: 1, Idle: time.Second},
	}
	reader.claimed["1-0"] = entry("1-0", "event-a", "{}")

	sub, err := NewSubscriber(SubscriberOptions{
		Reader:      reader,
		Concurrency: 1,
		BlockTime:   10 * time.Millisecond,
		// ClaimInterval left zero: reclaim disabled.
	})
	if err != nil {
		t.Fatalf("NewSubscriber: %v", err)
	}
	defer func() { _ = sub.Close() }()

	out, err := sub.Subscribe(ctx, "stream")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	select {
	case msg := <-out:
		t.Fatalf("unexpected reclaim with ClaimInterval unset: %q", msg.UUID)
	case <-time.After(300 * time.Millisecond):
	}
}

// multiStreamReader serves a different behaviour per stream so one
// subscription's read latency can be observed against another's throughput.
type multiStreamReader struct {
	mu sync.Mutex
	// busyStream has an unlimited supply of entries, served immediately.
	busyStream string
	busyNext   int
	// Every other stream never has an entry and always burns the full
	// blocking read, exactly as an empty XREADGROUP does.
}

func (m *multiStreamReader) EnsureGroup(context.Context, string) error { return nil }

func (m *multiStreamReader) ReadNew(ctx context.Context, stream string, _ int64, block time.Duration) ([]internalredis.Entry, error) {
	if stream == m.busyStream {
		m.mu.Lock()
		m.busyNext++
		id := strconv.Itoa(m.busyNext) + "-0"
		m.mu.Unlock()
		return []internalredis.Entry{entry(id, "busy-"+id, "{}")}, nil
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(block):
		return nil, nil
	}
}

func (m *multiStreamReader) PendingOverIdle(context.Context, string, time.Duration, int64) ([]internalredis.PendingEntry, error) {
	return nil, nil
}

func (m *multiStreamReader) Claim(context.Context, string, time.Duration, []string) ([]internalredis.Entry, error) {
	return nil, nil
}

func (m *multiStreamReader) Ack(context.Context, string, ...string) error { return nil }

// TestSubscriber_TopicsDoNotStarveEachOther pins the concurrency bound to a
// single subscription. Watermill calls Subscribe once per handler, so a
// Subscriber-wide semaphore would be shared by every topic's read loop — and
// because a slot is deliberately held across the whole blocking read, an
// idle topic's empty XREADGROUP would hold the only slot for BlockTime at a
// time while a busy topic's backlog waited on it.
//
// With a per-subscription semaphore the busy stream is limited only by how
// fast its messages are acked; with a shared one it is limited to roughly
// one message per BlockTime.
func TestSubscriber_TopicsDoNotStarveEachOther(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const (
		blockTime = 500 * time.Millisecond
		want      = 20
		budget    = 2 * time.Second
	)

	reader := &multiStreamReader{busyStream: "busy"}
	sub, err := NewSubscriber(SubscriberOptions{
		Reader: reader,
		// The documented default, and the value at which the bug bites
		// hardest.
		Concurrency: 1,
		BlockTime:   blockTime,
	})
	if err != nil {
		t.Fatalf("NewSubscriber: %v", err)
	}
	defer func() { _ = sub.Close() }()

	// Subscribed in the order that hurts: the idle topic takes a slot first.
	idleOut, err := sub.Subscribe(ctx, "idle")
	if err != nil {
		t.Fatalf("Subscribe(idle): %v", err)
	}
	busyOut, err := sub.Subscribe(ctx, "busy")
	if err != nil {
		t.Fatalf("Subscribe(busy): %v", err)
	}

	// Drain the idle channel so its subscription behaves normally; it never
	// actually produces anything.
	go func() {
		for msg := range idleOut {
			msg.Ack()
		}
	}()

	deadline := time.After(budget)
	got := 0
	for got < want {
		select {
		case msg, open := <-busyOut:
			if !open {
				t.Fatalf("busy channel closed after %d of %d messages", got, want)
			}
			msg.Ack()
			got++
		case <-deadline:
			t.Fatalf("busy stream delivered %d of %d messages in %v; an idle "+
				"co-subscribed topic must not throttle it (a shared semaphore "+
				"caps it at about one message per %v)", got, want, budget, blockTime)
		}
	}
}
