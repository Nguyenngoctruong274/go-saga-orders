package kafka

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Nguyenngoctruong274/go-saga-orders/internal/message"
	"github.com/Nguyenngoctruong274/go-saga-orders/internal/saga"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

const topic = message.TopicInventoryCommands

// cluster starts an in-memory Kafka (franz-go's kfake, which speaks the
// real protocol over TCP) with the saga topics, and a client to it.
func cluster(t *testing.T) (*kfake.Cluster, *Client) {
	t.Helper()
	c, err := kfake.NewCluster(kfake.NumBrokers(1))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	cl, err := NewClient(Options{Brokers: c.ListenAddrs(), ClientID: "test", Logger: quiet})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := cl.EnsureTopics(ctx, message.Topics(), 1); err != nil {
		t.Fatal(err)
	}
	return c, cl
}

// command builds the record of attempt 1 of a reserve for saga sagaID; n
// goes into the subject so tests can tell records apart.
func command(t *testing.T, sagaID string, n int) message.Record {
	t.Helper()
	env, err := message.FromCommand(saga.Command{
		SagaID: sagaID, Subject: "order-" + strconv.Itoa(n), Step: "reserve", Attempt: 1,
		Kind: saga.KindAction, Name: message.InventoryReserve, Deadline: time.Now().Add(time.Minute),
	}, "ordersd", "", message.ReservePayload{OrderID: "order-" + strconv.Itoa(n), Items: []message.Item{{SKU: "A", Quantity: 1}}}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	rec, err := message.Encode(topic, env)
	if err != nil {
		t.Fatal(err)
	}
	return rec
}

// collector is a handler that records what it was given, in order.
type collector struct {
	mu   sync.Mutex
	seen []message.Envelope
	// fail decides, per call, whether to fail; nil never fails.
	fail func(e message.Envelope, calls int) error
	// calls counts handler calls per message id.
	calls map[string]int
	got   chan struct{}
}

func newCollector() *collector {
	return &collector{calls: map[string]int{}, got: make(chan struct{}, 1000)}
}

func (c *collector) handle(_ context.Context, rec message.Record) error {
	e, err := message.Decode(rec)
	if err != nil {
		return Permanent(err)
	}
	c.mu.Lock()
	c.calls[e.ID]++
	n := c.calls[e.ID]
	c.mu.Unlock()
	if c.fail != nil {
		if err := c.fail(e, n); err != nil {
			return err
		}
	}
	c.mu.Lock()
	c.seen = append(c.seen, e)
	c.mu.Unlock()
	c.got <- struct{}{}
	return nil
}

func (c *collector) wait(t *testing.T, n int) {
	t.Helper()
	timeout := time.After(10 * time.Second)
	for range n {
		select {
		case <-c.got:
		case <-timeout:
			t.Fatalf("timed out waiting for %d records", n)
		}
	}
}

func (c *collector) snapshot() []message.Envelope {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]message.Envelope(nil), c.seen...)
}

// run starts a consumer of group and returns a function that stops it and
// waits for Run to return.
func run(t *testing.T, c *kfake.Cluster, cl *Client, group string, h Handler, opts ...func(*ConsumerOptions)) (stop func()) {
	t.Helper()
	o := ConsumerOptions{
		Brokers: c.ListenAddrs(), Group: group, Topics: []string{topic}, DeadLetters: cl, Logger: quiet,
		RetryBase: 5 * time.Millisecond, RetryMax: 20 * time.Millisecond,
	}
	for _, f := range opts {
		f(&o)
	}
	cons, err := NewConsumer(o, h)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- cons.Run(ctx) }()
	var once sync.Once
	stop = func() {
		once.Do(func() {
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Errorf("Run: %v", err)
				}
			case <-time.After(15 * time.Second):
				t.Fatal("consumer did not stop")
			}
		})
	}
	t.Cleanup(stop)
	return stop
}

func TestPublishAndConsumeKeepOrderPerSaga(t *testing.T) {
	c, cl := cluster(t)
	ctx := context.Background()
	const sagas, perSaga = 8, 10
	var recs []message.Record
	for i := range perSaga {
		for s := range sagas {
			recs = append(recs, command(t, fmt.Sprintf("saga-%d", s), i))
		}
	}
	if err := cl.Publish(ctx, recs...); err != nil {
		t.Fatal(err)
	}

	col := newCollector()
	run(t, c, cl, "inventoryd", col.handle)
	col.wait(t, sagas*perSaga)

	// Within each saga the records arrive in the order they were written.
	next := map[string]int{}
	for _, e := range col.snapshot() {
		if want := "order-" + strconv.Itoa(next[e.SagaID]); e.Subject != want {
			t.Fatalf("saga %s: got %s, want %s: per-saga order broken", e.SagaID, e.Subject, want)
		}
		next[e.SagaID]++
	}
	if len(next) != sagas {
		t.Fatalf("saw %d sagas, want %d", len(next), sagas)
	}
}

func TestHeadersAndKeySurviveTheBroker(t *testing.T) {
	c, cl := cluster(t)
	rec := command(t, "saga-h", 1)
	if err := cl.Publish(context.Background(), rec); err != nil {
		t.Fatal(err)
	}
	var got message.Record
	done := make(chan struct{})
	run(t, c, cl, "g", func(_ context.Context, r message.Record) error {
		got = r
		close(done)
		return nil
	})
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("nothing consumed")
	}
	if string(got.Key) != "saga-h" || got.Topic != topic || got.Offset != 0 {
		t.Fatalf("record = topic %s key %s offset %d", got.Topic, got.Key, got.Offset)
	}
	for _, h := range rec.Headers {
		if v, ok := got.Header(h.Key); !ok || v != string(h.Value) {
			t.Errorf("header %s = %q, want %q", h.Key, v, h.Value)
		}
	}
	if _, err := message.Decode(got); err != nil {
		t.Fatal(err)
	}
}

func TestTransientErrorsAreRetriedInPlace(t *testing.T) {
	c, cl := cluster(t)
	// One saga, so one partition: a record that fails must hold back the
	// ones after it, or a release could overtake its reserve.
	var recs []message.Record
	for i := range 5 {
		recs = append(recs, command(t, "saga-r", i))
	}
	if err := cl.Publish(context.Background(), recs...); err != nil {
		t.Fatal(err)
	}
	col := newCollector()
	col.fail = func(e message.Envelope, calls int) error {
		if e.Subject == "order-2" && calls <= 3 {
			return errors.New("database unavailable")
		}
		return nil
	}
	run(t, c, cl, "g", col.handle)
	col.wait(t, 5)
	var order []string
	for _, e := range col.snapshot() {
		order = append(order, e.Subject)
	}
	if fmt.Sprint(order) != "[order-0 order-1 order-2 order-3 order-4]" {
		t.Fatalf("handled %v", order)
	}
	col.mu.Lock()
	defer col.mu.Unlock()
	for _, e := range col.seen {
		want := 1
		if e.Subject == "order-2" {
			want = 4
		}
		if col.calls[e.ID] != want {
			t.Errorf("%s handled %d times, want %d", e.Subject, col.calls[e.ID], want)
		}
	}
}

func TestPermanentErrorsAreDeadLetteredAndSkipped(t *testing.T) {
	c, cl := cluster(t)
	poison := message.Record{Topic: topic, Key: []byte("saga-p"), Value: []byte(`{"not":"an envelope"}`)}
	good := command(t, "saga-p", 1)
	if err := cl.Publish(context.Background(), poison, good); err != nil {
		t.Fatal(err)
	}
	col := newCollector()
	run(t, c, cl, "g", col.handle)
	col.wait(t, 1)
	if got := col.snapshot(); len(got) != 1 || got[0].Subject != "order-1" {
		t.Fatalf("handled %+v", got)
	}

	// The poison record is in the dead-letter topic, with why and where from.
	dl := readAll(t, c, message.DeadLetter(topic), 1)
	r := dl[0]
	if string(r.Value) != string(poison.Value) || string(r.Key) != "saga-p" {
		t.Fatalf("dead letter = %s %s", r.Key, r.Value)
	}
	if v, _ := header(r, message.HeaderDeadLetterTopic); v != topic {
		t.Errorf("original topic header = %q", v)
	}
	if v, _ := header(r, message.HeaderDeadLetterOffset); v != "0" {
		t.Errorf("original offset header = %q, want 0 (the first record of its partition)", v)
	}
	if v, _ := header(r, message.HeaderDeadLetterPartition); v == "" {
		t.Error("original partition header missing")
	} else if _, err := strconv.Atoi(v); err != nil {
		t.Errorf("original partition header = %q", v)
	}
	if v, _ := header(r, message.HeaderDeadLetterError); v == "" {
		t.Error("dead letter lacks the error")
	}
}

func TestCommittedRecordsAreNotHandledAgain(t *testing.T) {
	c, cl := cluster(t)
	ctx := context.Background()
	for i := range 3 {
		if err := cl.Publish(ctx, command(t, fmt.Sprintf("saga-%d", i), i)); err != nil {
			t.Fatal(err)
		}
	}
	first := newCollector()
	stop := run(t, c, cl, "g", first.handle)
	first.wait(t, 3)
	stop()

	// A new process in the same group resumes after the commits: it sees
	// only what was written since.
	if err := cl.Publish(ctx, command(t, "saga-new", 3)); err != nil {
		t.Fatal(err)
	}
	second := newCollector()
	run(t, c, cl, "g", second.handle)
	second.wait(t, 1)
	time.Sleep(200 * time.Millisecond)
	if got := second.snapshot(); len(got) != 1 || got[0].Subject != "order-3" {
		t.Fatalf("after restart handled %d records: %+v", len(got), got)
	}
	// Another group has its own position and reads everything.
	other := newCollector()
	run(t, c, cl, "audit", other.handle)
	other.wait(t, 4)
}

func TestStoppingMidRecordHandsItOverAgain(t *testing.T) {
	c, cl := cluster(t)
	ctx := context.Background()
	if err := cl.Publish(ctx, command(t, "saga-s", 0), command(t, "saga-s", 1)); err != nil {
		t.Fatal(err)
	}
	// The first process handles order-0, then is stopped while order-1 keeps
	// failing: order-0 is committed, order-1 is not.
	first := newCollector()
	first.fail = func(e message.Envelope, _ int) error {
		if e.Subject == "order-1" {
			return errors.New("stuck")
		}
		return nil
	}
	stop := run(t, c, cl, "g", first.handle)
	first.wait(t, 1)
	time.Sleep(100 * time.Millisecond) // let it retry order-1 a few times
	stop()

	second := newCollector()
	run(t, c, cl, "g", second.handle)
	second.wait(t, 1)
	time.Sleep(200 * time.Millisecond)
	if got := second.snapshot(); len(got) != 1 || got[0].Subject != "order-1" {
		t.Fatalf("the second process handled %+v; want only order-1", got)
	}
}

func TestEnsureTopicsIsIdempotentAndValidation(t *testing.T) {
	_, cl := cluster(t)
	ctx := context.Background()
	if err := cl.EnsureTopics(ctx, message.Topics(), 1); err != nil {
		t.Fatalf("second EnsureTopics: %v", err)
	}
	if err := cl.EnsureTopics(ctx, []message.TopicSpec{{Name: "bad", Partitions: 0}, {Name: "", Partitions: 1}}, 0); err == nil ||
		!strings.Contains(err.Error(), "replication factor") || !strings.Contains(err.Error(), `topic "bad"`) || !strings.Contains(err.Error(), `topic ""`) {
		t.Fatalf("invalid specs: %v", err)
	}
	if err := cl.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	if err := cl.Publish(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := NewClient(Options{}); err == nil {
		t.Fatal("client without brokers")
	}
	if _, err := NewConsumer(ConsumerOptions{}, nil); err == nil {
		t.Fatal("consumer without anything")
	}
	if Permanent(nil) != nil || IsPermanent(errors.New("x")) || !IsPermanent(fmt.Errorf("wrapped: %w", Permanent(errors.New("x")))) {
		t.Fatal("Permanent / IsPermanent")
	}
}

func TestPingAndTopicsWhenTheBrokerIsGone(t *testing.T) {
	c, cl := cluster(t)
	if err := cl.EnsureTopicsRetrying(context.Background(), message.Topics(), 1, time.Millisecond); err != nil {
		t.Fatalf("with the broker up: %v", err)
	}
	c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := cl.Ping(ctx); err == nil {
		t.Fatal("ping succeeded with the broker down")
	}
	// Retrying stops when its context does, and says why.
	ctx, cancel = context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := cl.EnsureTopicsRetrying(ctx, message.Topics(), 1, 50*time.Millisecond)
	if err == nil || !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > 5*time.Second {
		t.Fatalf("EnsureTopicsRetrying with the broker down = %v after %v", err, time.Since(start))
	}
}

// readAll reads n records of topic from the start, outside any group.
func readAll(t *testing.T, c *kfake.Cluster, topic string, n int) []*kgo.Record {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...), kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var out []*kgo.Record
	for len(out) < n {
		fs := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("read %d of %d records from %s", len(out), n, topic)
		}
		out = append(out, fs.Records()...)
	}
	return out
}

func header(r *kgo.Record, key string) (string, bool) {
	for _, h := range r.Headers {
		if h.Key == key {
			return string(h.Value), true
		}
	}
	return "", false
}
