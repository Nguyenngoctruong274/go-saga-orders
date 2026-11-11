package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Nguyenngoctruong274/go-saga-orders/internal/message"
)

// Handler processes one record. Returning nil marks it done. An error
// wrapped with Permanent sends it to the topic's dead-letter topic; any
// other error is transient and the record is handed over again after a
// backoff, for as long as it takes. Handlers must be idempotent: a record
// handled but not yet committed when the process stops is handed over
// again, which is what at-least-once means (the inbox of day 5 makes the
// effects exactly once).
type Handler func(ctx context.Context, rec message.Record) error

type permanent struct{ err error }

func (p permanent) Error() string { return p.err.Error() }
func (p permanent) Unwrap() error { return p.err }

// Permanent marks err as one no retry will fix: a record that does not
// decode, a command this service does not know. The consumer
// dead-letters the record and moves on rather than blocking its
// partition forever.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return permanent{err}
}

// IsPermanent reports whether err was marked with Permanent.
func IsPermanent(err error) bool {
	var p permanent
	return errors.As(err, &p)
}

// ConsumerOptions configures a Consumer.
type ConsumerOptions struct {
	Brokers []string
	// Group is the consumer group: the partitions of Topics are shared
	// among the processes of one group, and its committed offsets are
	// where a restarted process resumes.
	Group  string
	Topics []string
	// DeadLetters publishes dead-lettered records; required.
	DeadLetters *Client
	Logger      *slog.Logger
	// RetryBase and RetryMax bound the backoff between attempts at a
	// record that failed transiently: it doubles from RetryBase up to
	// RetryMax. Defaults 100ms and 5s.
	RetryBase, RetryMax time.Duration
	// MaxPollRecords caps one batch. Default 500.
	MaxPollRecords int
}

// Consumer reads Topics as a member of Group and feeds a Handler.
//
// Records of one partition are handled one at a time, in offset order, so
// the messages of one saga (one key) are seen in the order they were
// written; partitions are handled in parallel. A record that keeps failing
// transiently holds back its partition and nothing else.
//
// Offsets are committed after the records they cover were handled, once
// per batch, and rebalances are held until the batch is done, so a
// partition never moves to another process with handled records
// uncommitted or unhandled records committed.
type Consumer struct {
	cl      *kgo.Client
	handler Handler
	opts    ConsumerOptions
}

// NewConsumer prepares a consumer; Run starts it. A group seen for the
// first time starts from the earliest offsets, so a command written before
// its consumer ever ran is not skipped.
func NewConsumer(opts ConsumerOptions, h Handler) (*Consumer, error) {
	var errs []error
	if len(opts.Brokers) == 0 {
		errs = append(errs, errors.New("at least one broker is required"))
	}
	if opts.Group == "" {
		errs = append(errs, errors.New("group is required"))
	}
	if len(opts.Topics) == 0 {
		errs = append(errs, errors.New("at least one topic is required"))
	}
	if opts.DeadLetters == nil {
		errs = append(errs, errors.New("a client for dead letters is required"))
	}
	if h == nil {
		errs = append(errs, errors.New("handler is required"))
	}
	if err := errors.Join(errs...); err != nil {
		return nil, fmt.Errorf("kafka consumer: %w", err)
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.RetryBase <= 0 {
		opts.RetryBase = 100 * time.Millisecond
	}
	if opts.RetryMax < opts.RetryBase {
		opts.RetryMax = max(5*time.Second, opts.RetryBase)
	}
	if opts.MaxPollRecords <= 0 {
		opts.MaxPollRecords = 500
	}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(opts.Brokers...),
		kgo.ConsumerGroup(opts.Group),
		kgo.ConsumeTopics(opts.Topics...),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka consumer: %w", err)
	}
	opts.Logger = opts.Logger.With(slog.String("group", opts.Group))
	return &Consumer{cl: cl, handler: h, opts: opts}, nil
}

// Run consumes until ctx ends, then commits what was handled, leaves the
// group and returns nil. It returns an error only if the client fails for
// good.
func (c *Consumer) Run(ctx context.Context) error {
	defer c.cl.Close() // leaves the group, so its partitions move at once
	for {
		fetches := c.cl.PollRecords(ctx, c.opts.MaxPollRecords)
		if fetches.IsClientClosed() {
			return errors.New("kafka consumer: client closed")
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			if !errors.Is(err, context.Canceled) {
				c.opts.Logger.Warn("fetch failed", slog.String("topic", topic), slog.Int("partition", int(partition)), slog.Any("error", err))
			}
		})

		var (
			mu      sync.Mutex
			handled []*kgo.Record
			wg      sync.WaitGroup
		)
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for _, r := range p.Records {
					if !c.handle(ctx, r) {
						return // stopping: leave the rest for whoever resumes
					}
					mu.Lock()
					handled = append(handled, r)
					mu.Unlock()
				}
			}()
		})
		wg.Wait()

		if len(handled) > 0 {
			// Committed even when ctx is done: what was handled must not be
			// handed over again just because we are stopping.
			commitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			err := c.cl.CommitRecords(commitCtx, handled...)
			cancel()
			if err != nil {
				// Not fatal: the records will be handed over again, and
				// handlers are idempotent.
				c.opts.Logger.Warn("commit failed", slog.Int("records", len(handled)), slog.Any("error", err))
			}
		}
		c.cl.AllowRebalance()
		if ctx.Err() != nil {
			return nil
		}
	}
}

// handle runs the handler on r until it succeeds or is dead-lettered. It
// returns false only when ctx ended first.
func (c *Consumer) handle(ctx context.Context, kr *kgo.Record) bool {
	rec := fromKgo(kr)
	log := c.opts.Logger.With(slog.String("topic", kr.Topic), slog.Int("partition", int(kr.Partition)), slog.Int64("offset", kr.Offset))
	delay := c.opts.RetryBase
	for attempt := 1; ; attempt++ {
		if ctx.Err() != nil {
			return false
		}
		err := c.handler(ctx, rec)
		switch {
		case err == nil:
			return true
		case IsPermanent(err):
			if dlqErr := c.deadLetter(ctx, rec, err); dlqErr != nil {
				log.Error("dead-lettering failed; will retry", slog.Any("error", dlqErr))
				break
			}
			log.Error("record dead-lettered", slog.String("dead_letter_topic", message.DeadLetter(kr.Topic)), slog.Any("error", err))
			return true
		case ctx.Err() != nil:
			return false
		default:
			log.Warn("handler failed; retrying", slog.Int("attempt", attempt), slog.Duration("backoff", delay), slog.Any("error", err))
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(delay):
		}
		delay = min(2*delay, c.opts.RetryMax)
	}
}

// deadLetter copies rec, with why and where it came from, to the
// dead-letter topic of its topic. The key is kept, so a dead letter can be
// replayed into the same partition once the cause is fixed.
func (c *Consumer) deadLetter(ctx context.Context, rec message.Record, cause error) error {
	dl := message.Record{Topic: message.DeadLetter(rec.Topic), Key: rec.Key, Value: rec.Value}
	dl.Headers = append(dl.Headers, rec.Headers...)
	dl.Headers = append(dl.Headers,
		message.Header{Key: message.HeaderDeadLetterError, Value: []byte(cause.Error())},
		message.Header{Key: message.HeaderDeadLetterTopic, Value: []byte(rec.Topic)},
		message.Header{Key: message.HeaderDeadLetterPartition, Value: []byte(strconv.Itoa(int(rec.Partition)))},
		message.Header{Key: message.HeaderDeadLetterOffset, Value: []byte(strconv.FormatInt(rec.Offset, 10))},
	)
	return c.opts.DeadLetters.Publish(ctx, dl)
}
