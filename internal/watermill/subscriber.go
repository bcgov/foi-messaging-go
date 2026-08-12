package watermill

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/ThreeDotsLabs/watermill-redisstream/pkg/redisstream"
	"github.com/ThreeDotsLabs/watermill/message"

	internalredis "github.com/bcgov/foi-messaging-go/internal/redis"
)

// Metadata keys stamped on messages at consume time. They live on the
// in-process message only, are never written to Redis, and never enter the
// event envelope.
const (
	MetadataStreamID        = "_foi_stream_id"
	MetadataDeliveryAttempt = "_foi_delivery_attempt"
)

// ErrNoReader is returned by NewSubscriber when no StreamReader is supplied.
var ErrNoReader = errors.New("subscriber: Reader is required")

const (
	defaultBlockTime = time.Second
	// claimBatchSize bounds one reclaim sweep, not the total pending set.
	claimBatchSize = int64(100)
	// readErrorBackoff throttles a failing read loop so a broken connection
	// does not spin.
	readErrorBackoff = 500 * time.Millisecond
	// ackTimeout bounds the XACK issued after a message is acked. It is
	// deliberately independent of the subscription context so a message
	// completing during a drain is still acknowledged.
	ackTimeout = 5 * time.Second
)

// StreamReader is the Redis Streams surface the Subscriber needs. It is
// satisfied by *internal/redis.StreamReader.
type StreamReader interface {
	EnsureGroup(ctx context.Context, stream string) error
	ReadNew(ctx context.Context, stream string, count int64, block time.Duration) ([]internalredis.Entry, error)
	PendingOverIdle(ctx context.Context, stream string, minIdle time.Duration, count int64) ([]internalredis.PendingEntry, error)
	Claim(ctx context.Context, stream string, minIdle time.Duration, ids []string) ([]internalredis.Entry, error)
	Ack(ctx context.Context, stream string, ids ...string) error
}

// SubscriberOptions configures a Subscriber. BlockTime is an internal knob
// (defaulted to 1s) so tests can shorten the blocking read; it deliberately
// has no equivalent in the public messaging.Config.
//
// Concurrency bounds in-flight messages per subscribed stream, not across
// the Subscriber as a whole — see Subscribe.
type SubscriberOptions struct {
	Reader        StreamReader
	Concurrency   int
	ClaimInterval time.Duration
	ClaimMinIdle  time.Duration
	BlockTime     time.Duration
	// Logger receives the read loop's, claim loop's and ack path's
	// failures. Left nil it falls back to slog's default; it is never
	// discarded, because a consume path that fails silently looks exactly
	// like a healthy idle one.
	Logger *slog.Logger

	// OnUndecodable is called for a stream entry that cannot be
	// unmarshalled at all. Returning nil acks the entry; returning an error
	// leaves it pending for the next reclaim sweep.
	//
	// The hook exists because this failure happens before a message is
	// produced, so the entry never reaches the consumer's dispatch — the
	// delivery-attempt cap and the DLQ both live there and neither can see
	// it. Left nil the subscriber logs and leaves the entry pending, which
	// re-loops every ClaimMinIdle forever.
	//
	// It takes only plain types, so the caller can dead-letter without any
	// watermill value crossing back over the package boundary.
	OnUndecodable func(stream, entryID string, fields map[string]any) error
}

// Subscriber implements watermill's message.Subscriber over Redis Streams,
// bounding in-flight messages so the router's goroutine-per-message
// behaviour cannot exceed the configured concurrency.
type Subscriber struct {
	reader        StreamReader
	concurrency   int
	claimInterval time.Duration
	claimMinIdle  time.Duration
	blockTime     time.Duration
	logger        *slog.Logger
	onUndecodable func(stream, entryID string, fields map[string]any) error

	closing   chan struct{}
	wg        sync.WaitGroup
	closeOnce sync.Once
}

// subscription is the state of one Subscribe call: one stream, one output
// channel, and its own concurrency semaphore.
//
// The semaphore is per-subscription rather than per-Subscriber because
// watermill calls Subscribe once per handler. A Subscriber-wide semaphore
// let one topic's read loop hold a slot for the whole blocking read while
// another topic's backlog waited on it: at the default Concurrency of 1, an
// idle topic could throttle a busy one to roughly one message per BlockTime.
type subscription struct {
	sub    *Subscriber
	stream string
	sem    chan struct{}
	out    chan *message.Message
}

// NewSubscriber builds a Subscriber. Concurrency below 1 is treated as 1.
func NewSubscriber(opts SubscriberOptions) (*Subscriber, error) {
	if opts.Reader == nil {
		return nil, ErrNoReader
	}
	if opts.Concurrency < 1 {
		opts.Concurrency = 1
	}
	if opts.BlockTime <= 0 {
		opts.BlockTime = defaultBlockTime
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}

	return &Subscriber{
		reader:        opts.Reader,
		concurrency:   opts.Concurrency,
		claimInterval: opts.ClaimInterval,
		claimMinIdle:  opts.ClaimMinIdle,
		blockTime:     opts.BlockTime,
		logger:        opts.Logger,
		onUndecodable: opts.OnUndecodable,
		closing:       make(chan struct{}),
	}, nil
}

// Subscribe consumes stream — the full Redis stream name, not the logical
// topic. Cancelling ctx stops the read loop and closes the returned channel,
// but only Close guarantees full teardown: it is what releases any in-flight
// message's ack-wait goroutine and its concurrency slot. Callers must always
// call Close, even after cancelling ctx.
//
// Each call gets its own semaphore of capacity Concurrency, so the bound is
// per subscribed stream. Subscribing to several streams therefore allows
// Concurrency in-flight messages on each, and no stream can starve another.
//
// The ack-wait deliberately does not observe ctx: a message that completes
// during a graceful drain (ctx already cancelled, Close not yet called) must
// still be acked rather than left to be redelivered.
func (s *Subscriber) Subscribe(ctx context.Context, stream string) (<-chan *message.Message, error) {
	if err := s.reader.EnsureGroup(ctx, stream); err != nil {
		return nil, fmt.Errorf("ensuring consumer group on %q: %w", stream, err)
	}

	sub := &subscription{
		sub:    s,
		stream: stream,
		sem:    make(chan struct{}, s.concurrency),
		out:    make(chan *message.Message),
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer close(sub.out)

		var loops sync.WaitGroup

		loops.Add(1)
		go func() {
			defer loops.Done()
			sub.readLoop(ctx)
		}()

		if s.claimInterval > 0 {
			loops.Add(1)
			go func() {
				defer loops.Done()
				sub.claimLoop(ctx)
			}()
		}

		loops.Wait()
	}()

	return sub.out, nil
}

// readLoop fetches never-delivered entries one slot at a time.
func (sc *subscription) readLoop(ctx context.Context) {
	s := sc.sub

	for {
		if !sc.acquire(ctx) {
			return
		}

		entries, err := s.reader.ReadNew(ctx, sc.stream, 1, s.blockTime)
		if err != nil || len(entries) == 0 {
			sc.release()
			if s.stopped(ctx) {
				return
			}
			if err != nil {
				// Unlogged, a read that keeps failing — an
				// unreachable Redis, say — loops here forever in
				// silence: Run never returns, nothing is consumed,
				// and the consumer still looks connected.
				s.logger.Error("messaging: reading from stream failed",
					"stream", sc.stream, "error", err)
				s.pause(ctx, readErrorBackoff)
			}
			continue
		}

		// One slot was acquired, so exactly one entry is emitted; any
		// surplus would have nowhere to run.
		if !sc.emit(ctx, entries[0], 1) {
			return
		}
	}
}

// claimLoop implements PRD §13 Layer 2. Every ClaimInterval it looks for
// entries pending longer than ClaimMinIdle and reclaims them, which is how
// nacked messages are redelivered and how messages survive a crashed
// consumer. Reclaimed messages arrive out of order relative to the live
// stream, which is inherent to reclaim.
func (sc *subscription) claimLoop(ctx context.Context) {
	s := sc.sub

	ticker := time.NewTicker(s.claimInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.closing:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		pending, err := s.reader.PendingOverIdle(ctx, sc.stream, s.claimMinIdle, claimBatchSize)
		if err != nil {
			if s.stopped(ctx) {
				return
			}
			// A reclaim sweep that keeps failing means nacked
			// messages are never redelivered. Say so.
			s.logger.Error("messaging: scanning pending entries failed",
				"stream", sc.stream, "error", err)
			s.pause(ctx, readErrorBackoff)
			continue
		}

		for _, p := range pending {
			if !sc.acquire(ctx) {
				return
			}

			entries, err := s.reader.Claim(ctx, sc.stream, s.claimMinIdle, []string{p.ID})
			if err != nil || len(entries) == 0 {
				// Lost the race to another instance, or the entry is gone.
				sc.release()
				if s.stopped(ctx) {
					return
				}
				if err != nil {
					s.logger.Error("messaging: claiming pending entry failed",
						"stream", sc.stream, "entry_id", p.ID, "error", err)
				}
				continue
			}

			// XPENDING reports deliveries that already happened; the XCLAIM
			// just issued is the next one.
			if !sc.emit(ctx, entries[0], p.RetryCount+1) {
				return
			}
		}
	}
}

// emit decodes an entry, hands it to the output channel, and arranges for
// its ack or nack to be honoured. The caller must already hold a semaphore
// slot; emit takes responsibility for releasing it.
func (sc *subscription) emit(ctx context.Context, e internalredis.Entry, attempt int64) bool {
	s := sc.sub

	msg, err := decodeEntry(e, attempt)
	if err != nil {
		// Spec §6 requires an ERROR for an undecodable envelope, and only
		// the JSON layer's version of that failure was being logged.
		s.logger.Error("messaging: undecodable stream entry",
			"stream", sc.stream, "entry_id", e.ID, "error", err)

		if s.onUndecodable != nil {
			if hookErr := s.onUndecodable(sc.stream, e.ID, e.Fields); hookErr != nil {
				// The hook owns recording the entry elsewhere. If it
				// failed, leave the entry pending rather than acking an
				// event nothing is holding — the same rule the dispatch
				// path's DLQ writes follow.
				s.logger.Error("messaging: dead-lettering undecodable entry failed",
					"stream", sc.stream, "entry_id", e.ID, "error", hookErr)
			} else {
				sc.ack(ctx, e.ID)
			}
		}

		sc.release()
		return !s.stopped(ctx)
	}

	// msgCtx is cancelled once this message's ack/nack settles, or when
	// Close tears the subscription down — so msg.Context() is Done() after
	// Ack, the standard Watermill contract, mirroring
	// watermill-redisstream's processMessage.
	//
	// It is deliberately detached from ctx. Deriving it from ctx directly
	// made every in-flight handler's context Done() the instant the caller
	// cancelled, *before* the router's CloseTimeout drain even began: a
	// handler doing the canonical `return db.ExecContext(ctx, ...)` failed
	// immediately with context.Canceled and its entry was redelivered
	// ClaimMinIdle later, so ShutdownTimeout bought nothing. Handlers now
	// keep a live context for the whole drain, which is what the drain is
	// documented to give them.
	msgCtx, cancelMsgCtx := context.WithCancel(context.WithoutCancel(ctx))
	msg.SetContext(msgCtx)

	select {
	case sc.out <- msg:
	case <-s.closing:
		cancelMsgCtx()
		sc.release()
		return false
	case <-ctx.Done():
		cancelMsgCtx()
		sc.release()
		return false
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer sc.release()
		// Deferred, so it runs after the ack attempt below has completed
		// and can never race-abort it.
		defer cancelMsgCtx()

		select {
		case <-msg.Acked():
			sc.ack(ctx, e.ID)
		case <-msg.Nacked():
			// Leave the entry pending; the claim loop redelivers it.
		case <-s.closing:
			// Close can race a handler that has just acked: both
			// channels are then ready and select picks between them at
			// random. Honour an ack that has already landed rather than
			// leaving completed work to be redelivered.
			select {
			case <-msg.Acked():
				sc.ack(ctx, e.ID)
			default:
			}
		}
	}()

	return true
}

// ack acknowledges an entry on a context detached from the subscription (not
// the message context) so a shutdown in progress still records completed
// work, and so emit's cancelMsgCtx cannot race-abort the very XACK it is
// meant to follow.
func (sc *subscription) ack(ctx context.Context, id string) {
	ackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), ackTimeout)
	defer cancel()
	if err := sc.sub.reader.Ack(ackCtx, sc.stream, id); err != nil {
		sc.sub.logger.Error("messaging: acking stream entry failed",
			"stream", sc.stream, "entry_id", id, "error", err)
	}
}

// decodeEntry converts a stream entry into a watermill message using the
// same marshaller the Phase 1 publisher writes with, so the wire format
// cannot drift between the two halves of the library.
func decodeEntry(e internalredis.Entry, attempt int64) (*message.Message, error) {
	msg, err := redisstream.DefaultMarshallerUnmarshaller{}.Unmarshal(e.Fields)
	if err != nil {
		return nil, fmt.Errorf("unmarshalling entry %q: %w", e.ID, err)
	}
	msg.Metadata.Set(MetadataStreamID, e.ID)
	msg.Metadata.Set(MetadataDeliveryAttempt, strconv.FormatInt(attempt, 10))
	return msg, nil
}

// acquire takes a concurrency slot on this subscription, reporting false if
// the subscriber is shutting down instead.
func (sc *subscription) acquire(ctx context.Context) bool {
	select {
	case sc.sem <- struct{}{}:
		return true
	case <-sc.sub.closing:
		return false
	case <-ctx.Done():
		return false
	}
}

func (sc *subscription) release() {
	<-sc.sem
}

// stopped reports whether the subscriber should stop looping.
func (s *Subscriber) stopped(ctx context.Context) bool {
	select {
	case <-s.closing:
		return true
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

// pause waits for d unless the subscriber is shutting down.
func (s *Subscriber) pause(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
	case <-s.closing:
	case <-ctx.Done():
	}
}

// Close stops every subscription's loops and waits for all in-flight
// messages to settle. It is idempotent.
func (s *Subscriber) Close() error {
	s.closeOnce.Do(func() { close(s.closing) })
	s.wg.Wait()
	return nil
}
