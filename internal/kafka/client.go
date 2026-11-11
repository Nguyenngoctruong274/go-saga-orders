// Package kafka moves message.Records through Kafka with franz-go: a
// Client that publishes them (idempotent producer, every in-sync replica
// acknowledging) and creates topics, and a Consumer that hands them to a
// handler in order per partition, retries what failed transiently,
// dead-letters what can never succeed, and commits only what was handled.
package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/Nguyenngoctruong274/go-saga-orders/internal/message"
)

// Options configures a Client.
type Options struct {
	// Brokers are the bootstrap brokers, host:port.
	Brokers []string
	// ClientID names this process to the brokers, for their logs.
	ClientID string
	Logger   *slog.Logger
}

// Client publishes records and administers topics.
type Client struct {
	cl     *kgo.Client
	logger *slog.Logger
}

// NewClient connects lazily: no request is made until the first use, so a
// broker that is down at start is reported by Ping (readiness), not by a
// crash.
func NewClient(opts Options) (*Client, error) {
	if len(opts.Brokers) == 0 {
		return nil, errors.New("kafka: at least one broker is required")
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	kopts := []kgo.Opt{
		kgo.SeedBrokers(opts.Brokers...),
		// Idempotent production is franz-go's default; with acks from every
		// in-sync replica a published record survives a broker loss, and a
		// retried send is not written twice.
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(2 * time.Millisecond),
	}
	if opts.ClientID != "" {
		kopts = append(kopts, kgo.ClientID(opts.ClientID))
	}
	cl, err := kgo.NewClient(kopts...)
	if err != nil {
		return nil, fmt.Errorf("kafka: %w", err)
	}
	return &Client{cl: cl, logger: opts.Logger}, nil
}

// Publish writes records and waits until every one is acknowledged. They
// are sent together; records with the same key keep their relative order,
// since they go to one partition through one idempotent producer. The
// error, if any, joins the failure of every record that failed.
func (c *Client) Publish(ctx context.Context, recs ...message.Record) error {
	if len(recs) == 0 {
		return nil
	}
	krecs := make([]*kgo.Record, len(recs))
	for i, r := range recs {
		krecs[i] = toKgo(r)
	}
	var errs []error
	for _, res := range c.cl.ProduceSync(ctx, krecs...) {
		if res.Err != nil {
			errs = append(errs, fmt.Errorf("publish to %s: %w", res.Record.Topic, res.Err))
		}
	}
	return errors.Join(errs...)
}

// Ping checks that a broker answers, for readiness probes.
func (c *Client) Ping(ctx context.Context) error {
	if err := c.cl.Ping(ctx); err != nil {
		return fmt.Errorf("kafka: %w", err)
	}
	return nil
}

// EnsureTopics creates the topics that do not exist yet. Existing topics
// are left alone, partition count included: changing it would move keys,
// and so sagas, between partitions.
func (c *Client) EnsureTopics(ctx context.Context, specs []message.TopicSpec, replication int16) error {
	var bad []error
	if replication < 1 {
		bad = append(bad, fmt.Errorf("replication factor must be at least 1, got %d", replication))
	}
	for _, s := range specs {
		if s.Name == "" || s.Partitions < 1 {
			bad = append(bad, fmt.Errorf("topic %q: a name and at least one partition are required, got %d", s.Name, s.Partitions))
		}
	}
	if err := errors.Join(bad...); err != nil {
		return fmt.Errorf("kafka: create topics: %w", err)
	}
	req := kmsg.NewPtrCreateTopicsRequest()
	req.TimeoutMillis = 10_000
	for _, s := range specs {
		t := kmsg.NewCreateTopicsRequestTopic()
		t.Topic, t.NumPartitions, t.ReplicationFactor = s.Name, s.Partitions, replication
		req.Topics = append(req.Topics, t)
	}
	resp, err := req.RequestWith(ctx, c.cl)
	if err != nil {
		return fmt.Errorf("kafka: create topics: %w", err)
	}
	var errs []error
	for _, t := range resp.Topics {
		err := kerr.ErrorForCode(t.ErrorCode)
		switch {
		case err == nil:
			c.logger.Info("topic created", slog.String("topic", t.Topic))
		case errors.Is(err, kerr.TopicAlreadyExists):
		default:
			errs = append(errs, fmt.Errorf("create topic %s: %w", t.Topic, err))
		}
	}
	return errors.Join(errs...)
}

// EnsureTopicsRetrying calls EnsureTopics until it succeeds or ctx ends,
// waiting every between attempts: a daemon started before its broker
// keeps trying instead of exiting, and readiness reports the broker as
// down in the meantime.
func (c *Client) EnsureTopicsRetrying(ctx context.Context, specs []message.TopicSpec, replication int16, every time.Duration) error {
	for {
		attemptCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err := c.EnsureTopics(attemptCtx, specs, replication)
		cancel()
		if err == nil {
			return nil
		}
		c.logger.Warn("creating topics failed; retrying", slog.Duration("in", every), slog.Any("error", err))
		select {
		case <-ctx.Done():
			return fmt.Errorf("kafka: giving up on topics: %w", errors.Join(err, ctx.Err()))
		case <-time.After(every):
		}
	}
}

// Close flushes nothing (Publish already waited) and closes connections.
func (c *Client) Close() { c.cl.Close() }

func toKgo(r message.Record) *kgo.Record {
	kr := &kgo.Record{Topic: r.Topic, Key: r.Key, Value: r.Value}
	for _, h := range r.Headers {
		kr.Headers = append(kr.Headers, kgo.RecordHeader{Key: h.Key, Value: h.Value})
	}
	return kr
}

func fromKgo(kr *kgo.Record) message.Record {
	r := message.Record{Topic: kr.Topic, Key: kr.Key, Value: kr.Value, Partition: kr.Partition, Offset: kr.Offset}
	for _, h := range kr.Headers {
		r.Headers = append(r.Headers, message.Header{Key: h.Key, Value: h.Value})
	}
	return r
}
