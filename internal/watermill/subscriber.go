package watermill

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	wm "github.com/ThreeDotsLabs/watermill"
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
type SubscriberOptions struct {
	Reader        StreamReader
	Concurrency   int
	ClaimInterval time.Duration
	ClaimMinIdle  time.Duration
	BlockTime     time.Duration
	Logger        wm.LoggerAdapter
}

// Subscriber implements watermill's message.Subscriber over Redis Streams,
// bounding in-flight messages so the router's goroutine-per-message
// behaviour cannot exceed the configured concurrency.
type Subscriber struct {
	reader        StreamReader
	claimInterval time.Duration
	claimMinIdle  time.Duration
	blockTime     time.Duration
	logger        wm.LoggerAdapter

	sem       chan struct{}
	closing   chan struct{}
	wg        sync.WaitGroup
	closeOnce sync.Once
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
		opts.Logger = &wm.NopLogger{}
	}

	return &Subscriber{
		reader:        opts.Reader,
		claimInterval: opts.ClaimInterval,
		claimMinIdle:  opts.ClaimMinIdle,
		blockTime:     opts.BlockTime,
		logger:        opts.Logger,
		sem:           make(chan struct{}, opts.Concurrency),
		closing:       make(chan struct{}),
	}, nil
}

// Subscribe consumes stream — the full Redis stream name, not the logical
// topic. Cancelling ctx stops the read loop and closes the returned channel,
// but only Close guarantees full teardown: it is what releases any in-flight
// message's ack-wait goroutine and its concurrency slot. Callers must always
// call Close, even after cancelling ctx.
//
// The ack-wait deliberately does not observe ctx: a message that completes
// during a graceful drain (ctx already cancelled, Close not yet called) must
// still be acked rather than left to be redelivered.
func (s *Subscriber) Subscribe(ctx context.Context, stream string) (<-chan *message.Message, error) {
	if err := s.reader.EnsureGroup(ctx, stream); err != nil {
		return nil, fmt.Errorf("ensuring consumer group on %q: %w", stream, err)
	}

	out := make(chan *message.Message)

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer close(out)

		var loops sync.WaitGroup

		loops.Add(1)
		go func() {
			defer loops.Done()
			s.readLoop(ctx, stream, out)
		}()

		if s.claimInterval > 0 {
			loops.Add(1)
			go func() {
				defer loops.Done()
				s.claimLoop(ctx, stream, out)
			}()
		}

		loops.Wait()
	}()

	return out, nil
}

// readLoop fetches never-delivered entries one slot at a time.
func (s *Subscriber) readLoop(ctx context.Context, stream string, out chan<- *message.Message) {
	for {
		if !s.acquire(ctx) {
			return
		}

		entries, err := s.reader.ReadNew(ctx, stream, 1, s.blockTime)
		if err != nil || len(entries) == 0 {
			s.release()
			if s.stopped(ctx) {
				return
			}
			if err != nil {
				s.pause(ctx, readErrorBackoff)
			}
			continue
		}

		// One slot was acquired, so exactly one entry is emitted; any
		// surplus would have nowhere to run.
		if !s.emit(ctx, stream, out, entries[0], 1) {
			return
		}
	}
}

// claimLoop implements PRD §13 Layer 2. Every ClaimInterval it looks for
// entries pending longer than ClaimMinIdle and reclaims them, which is how
// nacked messages are redelivered and how messages survive a crashed
// consumer. Reclaimed messages arrive out of order relative to the live
// stream, which is inherent to reclaim.
func (s *Subscriber) claimLoop(ctx context.Context, stream string, out chan<- *message.Message) {
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

		pending, err := s.reader.PendingOverIdle(ctx, stream, s.claimMinIdle, claimBatchSize)
		if err != nil {
			s.pause(ctx, readErrorBackoff)
			continue
		}

		for _, p := range pending {
			if !s.acquire(ctx) {
				return
			}

			entries, err := s.reader.Claim(ctx, stream, s.claimMinIdle, []string{p.ID})
			if err != nil || len(entries) == 0 {
				// Lost the race to another instance, or the entry is gone.
				s.release()
				if s.stopped(ctx) {
					return
				}
				continue
			}

			// XPENDING reports deliveries that already happened; the XCLAIM
			// just issued is the next one.
			if !s.emit(ctx, stream, out, entries[0], p.RetryCount+1) {
				return
			}
		}
	}
}

// emit decodes an entry, hands it to out, and arranges for its ack or nack
// to be honoured. The caller must already hold a semaphore slot; emit takes
// responsibility for releasing it.
func (s *Subscriber) emit(ctx context.Context, stream string, out chan<- *message.Message, e internalredis.Entry, attempt int64) bool {
	msg, err := decodeEntry(e, attempt)
	if err != nil {
		// An entry we cannot even decode is left pending rather than
		// dropped; Phase 2b routes it to the DLQ.
		s.release()
		return !s.stopped(ctx)
	}
	msg.SetContext(ctx)

	select {
	case out <- msg:
	case <-s.closing:
		s.release()
		return false
	case <-ctx.Done():
		s.release()
		return false
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer s.release()

		select {
		case <-msg.Acked():
			// Ack with a context detached from the subscription so a
			// shutdown in progress still records completed work.
			ackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			if err := s.reader.Ack(ackCtx, stream, e.ID); err != nil {
				s.logger.Error("failed to ack stream entry", err, wm.LogFields{
					"stream":   stream,
					"entry_id": e.ID,
				})
			}
		case <-msg.Nacked():
			// Leave the entry pending; the claim loop redelivers it.
		case <-s.closing:
		}
	}()

	return true
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

// acquire takes a concurrency slot, reporting false if the subscriber is
// shutting down instead.
func (s *Subscriber) acquire(ctx context.Context) bool {
	select {
	case s.sem <- struct{}{}:
		return true
	case <-s.closing:
		return false
	case <-ctx.Done():
		return false
	}
}

func (s *Subscriber) release() {
	<-s.sem
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

// Close stops all loops and waits for in-flight messages to settle. It is
// idempotent.
func (s *Subscriber) Close() error {
	s.closeOnce.Do(func() { close(s.closing) })
	s.wg.Wait()
	return nil
}
