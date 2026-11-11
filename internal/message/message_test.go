package message

import (
	"encoding/json"
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/Nguyenngoctruong274/go-saga-orders/internal/saga"
)

var t0 = time.Date(2026, 11, 11, 9, 0, 0, 0, time.UTC)

var orderSaga = saga.Definition{Name: "place_order", Steps: []saga.StepDef{
	{Name: "reserve", Action: InventoryReserve, Compensation: InventoryRelease, Timeout: 10 * time.Second, MaxAttempts: 3},
	{Name: "pay", Action: PaymentAuthorize, Compensation: PaymentVoid, Timeout: 10 * time.Second, MaxAttempts: 3},
}}

var reserve = ReservePayload{OrderID: "order-42", Items: []Item{{SKU: "TEA-1", Quantity: 2}}}

func startSaga(t *testing.T) (*saga.Saga, saga.Command) {
	t.Helper()
	s, err := saga.New(orderSaga, "saga-1", "order-42", t0)
	if err != nil {
		t.Fatal(err)
	}
	cmd, err := s.Start(t0)
	if err != nil {
		t.Fatal(err)
	}
	return s, cmd
}

// TestCommandAndReplyRoundTrip walks one step of the conversation as the
// services will: the orchestrator wraps a command, the participant
// decodes it and answers, the orchestrator decodes the answer and applies
// it to the saga.
func TestCommandAndReplyRoundTrip(t *testing.T) {
	s, cmd := startSaga(t)

	env, err := FromCommand(cmd, "ordersd", "", reserve, t0)
	if err != nil {
		t.Fatal(err)
	}
	topic, err := CommandTopic(cmd.Name)
	if err != nil || topic != TopicInventoryCommands {
		t.Fatalf("CommandTopic(%s) = %q, %v", cmd.Name, topic, err)
	}
	rec, err := Encode(topic, env)
	if err != nil {
		t.Fatal(err)
	}
	if string(rec.Key) != "saga-1" {
		t.Fatalf("key = %q, want the saga id so a saga's messages share a partition", rec.Key)
	}
	for h, want := range map[string]string{
		HeaderID: env.ID, HeaderType: InventoryReserve, HeaderVersion: "1", HeaderSagaID: "saga-1",
		HeaderCorrelationID: "saga-1", HeaderContentType: "application/json",
	} {
		if got, _ := rec.Header(h); got != want {
			t.Errorf("header %s = %q, want %q", h, got, want)
		}
	}
	if _, ok := rec.Header(HeaderCausationID); ok {
		t.Error("the first command of a saga has no cause, so no causation header")
	}

	// Participant side.
	got, err := Decode(rec)
	if err != nil {
		t.Fatal(err)
	}
	back, err := got.Command()
	if err != nil {
		t.Fatal(err)
	}
	if back != (saga.Command{SagaID: cmd.SagaID, Subject: cmd.Subject, Step: cmd.Step, Attempt: cmd.Attempt,
		Kind: cmd.Kind, Name: cmd.Name, Deadline: cmd.Deadline.UTC()}) {
		t.Fatalf("command round trip:\n got %+v\nwant %+v", back, cmd)
	}
	var p ReservePayload
	if err := got.DecodePayload(&p); err != nil || p.OrderID != "order-42" || p.Items[0] != (Item{"TEA-1", 2}) {
		t.Fatalf("payload = %+v, %v", p, err)
	}
	replyEnv, err := NewReply(got, "inventoryd", saga.OutcomeSucceeded, "", ResultPayload{Reference: "res-7"}, t0.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if replyEnv.Type != "inventory.reserve.reply" || replyEnv.CausationID != env.ID || replyEnv.CorrelationID != "saga-1" {
		t.Fatalf("reply ids: type %s causation %s correlation %s", replyEnv.Type, replyEnv.CausationID, replyEnv.CorrelationID)
	}
	replyRec, err := Encode(TopicSagaReplies, replyEnv)
	if err != nil {
		t.Fatal(err)
	}
	if c, _ := replyRec.Header(HeaderCausationID); c != env.ID {
		t.Fatalf("causation header = %q", c)
	}

	// Orchestrator side: the reply advances the saga to the next step.
	decoded, err := Decode(replyRec)
	if err != nil {
		t.Fatal(err)
	}
	r, err := decoded.Reply()
	if err != nil {
		t.Fatal(err)
	}
	next, err := s.Apply(r, t0.Add(time.Second))
	if err != nil || next == nil || next.Step != "pay" || next.Name != PaymentAuthorize {
		t.Fatalf("after the reply: next = %+v, %v", next, err)
	}
	// The same reply again is now stale: the fencing survives the trip.
	if _, err := s.Apply(r, t0.Add(2*time.Second)); !errors.Is(err, saga.ErrStaleReply) {
		t.Fatalf("duplicate reply: %v, want ErrStaleReply", err)
	}
	var res ResultPayload
	if err := decoded.DecodePayload(&res); err != nil || res.Reference != "res-7" {
		t.Fatalf("result = %+v, %v", res, err)
	}
}

func TestCompensationCommandsCarryTheirKind(t *testing.T) {
	s, cmd := startSaga(t)
	next, err := s.Apply(saga.Reply{SagaID: cmd.SagaID, Step: cmd.Step, Attempt: cmd.Attempt, Outcome: saga.OutcomeSucceeded}, t0)
	if err != nil {
		t.Fatal(err)
	}
	comp, err := s.Apply(saga.Reply{SagaID: next.SagaID, Step: next.Step, Attempt: next.Attempt, Outcome: saga.OutcomeFailed, Reason: "card declined"}, t0)
	if err != nil || comp == nil || comp.Kind != saga.KindCompensate {
		t.Fatalf("compensation = %+v, %v", comp, err)
	}
	env, err := FromCommand(*comp, "ordersd", "some-reply-id", ReleasePayload{OrderID: "order-42"}, t0)
	if err != nil {
		t.Fatal(err)
	}
	if !env.Compensate || env.Type != InventoryRelease || env.CausationID != "some-reply-id" {
		t.Fatalf("envelope = %+v", env)
	}
	back, err := env.Command()
	if err != nil || back.Kind != saga.KindCompensate {
		t.Fatalf("command = %+v, %v", back, err)
	}
	failed, err := NewReply(env, "inventoryd", saga.OutcomeFailed, "nothing to release", nil, t0)
	if err != nil {
		t.Fatal(err)
	}
	if r, err := failed.Reply(); err != nil || r.Outcome != saga.OutcomeFailed || r.Reason != "nothing to release" {
		t.Fatalf("reply = %+v, %v", r, err)
	}
}

func validEnvelope() Envelope {
	return Envelope{
		Version: 1, ID: NewID(), Kind: KindCommand, Type: InventoryReserve, Source: "ordersd",
		SagaID: "saga-1", Step: "reserve", Attempt: 1, Subject: "order-42",
		CorrelationID: "saga-1", OccurredAt: t0, Payload: json.RawMessage(`{"order_id":"order-42"}`),
	}
}

func encoded(t *testing.T, e Envelope) []byte {
	t.Helper()
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestUnmarshal(t *testing.T) {
	good := encoded(t, validEnvelope())
	withExtra := strings.Replace(string(good), `"version":1`, `"version":1,"priority":"high"`, 1)
	future := strings.Replace(string(good), `"version":1`, `"version":2`, 1)
	reply := validEnvelope()
	reply.Kind, reply.Outcome = KindReply, "succeeded"

	tests := []struct {
		name string
		in   string
		want error
		msg  string
	}{
		{"valid", string(good), nil, ""},
		{"unknown fields are ignored", withExtra, nil, ""},
		{"newer version", future, ErrUnsupportedVersion, "this reader knows up to 1"},
		{"not JSON", `{"version":1,`, ErrInvalid, ""},
		{"trailing data", string(good) + `{}`, ErrInvalid, "after top-level value"},
		{"empty object", `{}`, ErrInvalid, "id is required"},
		{"too big", `{"version":1,"payload":"` + strings.Repeat("x", MaxSize) + `"}`, ErrInvalid, "at most"},
		{"reply without causation", string(encoded(t, reply)), ErrInvalid, "causation_id"},
		{"bad kind", strings.Replace(string(good), `"kind":"command"`, `"kind":"event"`, 1), ErrInvalid, `kind "event"`},
		{"attempt zero", strings.Replace(string(good), `"attempt":1`, `"attempt":0`, 1), ErrInvalid, "attempt"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Unmarshal([]byte(tt.in))
			if tt.want == nil {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if !errors.Is(err, tt.want) || !strings.Contains(err.Error(), tt.msg) {
				t.Fatalf("error = %v, want %v containing %q", err, tt.want, tt.msg)
			}
		})
	}
	// Every problem is reported at once.
	_, err := Unmarshal([]byte(`{"version":1,"kind":"command"}`))
	for _, want := range []string{"id is required", "type is required", "saga_id is required", "occurred_at"} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("error %v lacks %q", err, want)
		}
	}
}

func TestDecodeChecksKeyAndHeaders(t *testing.T) {
	rec, err := Encode(TopicInventoryCommands, validEnvelope())
	if err != nil {
		t.Fatal(err)
	}
	wrongKey := rec
	wrongKey.Key = []byte("saga-2")
	if _, err := Decode(wrongKey); !errors.Is(err, ErrInvalid) || !strings.Contains(err.Error(), "key") {
		t.Fatalf("wrong key: %v", err)
	}
	lying := rec
	lying.Headers = append([]Header{{HeaderType, []byte(PaymentVoid)}}, rec.Headers...)
	if _, err := Decode(lying); !errors.Is(err, ErrInvalid) || !strings.Contains(err.Error(), HeaderType) {
		t.Fatalf("header disagreeing with the body: %v", err)
	}
	bare := rec
	bare.Headers = nil // headers are optional; the body is the truth
	if _, err := Decode(bare); err != nil {
		t.Fatalf("record without headers: %v", err)
	}
	if _, err := Encode("", validEnvelope()); !errors.Is(err, ErrInvalid) {
		t.Fatal("encode without a topic accepted")
	}
	bad := validEnvelope()
	bad.SagaID = ""
	if _, err := Encode(TopicInventoryCommands, bad); !errors.Is(err, ErrInvalid) {
		t.Fatal("encode of an invalid envelope accepted")
	}
}

func TestRepliesAndCommandsDoNotMix(t *testing.T) {
	cmd := validEnvelope()
	if _, err := cmd.Reply(); !errors.Is(err, ErrInvalid) {
		t.Fatal("a command read as a reply")
	}
	r, err := NewReply(cmd, "inventoryd", saga.OutcomeRetry, "database busy", nil, t0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Command(); !errors.Is(err, ErrInvalid) {
		t.Fatal("a reply read as a command")
	}
	if _, err := NewReply(r, "x", saga.OutcomeSucceeded, "", nil, t0); !errors.Is(err, ErrInvalid) {
		t.Fatal("replied to a reply")
	}
	if _, err := NewReply(cmd, "x", saga.Outcome("maybe"), "", nil, t0); !errors.Is(err, ErrInvalid) {
		t.Fatal("unknown outcome accepted when replying")
	}
	r.Outcome = "maybe"
	if _, err := r.Reply(); !errors.Is(err, ErrInvalid) {
		t.Fatal("unknown outcome accepted when reading: guessing would advance or compensate wrongly")
	}
	if _, err := FromCommand(saga.Command{SagaID: "s", Step: "x", Attempt: 1, Name: InventoryReserve, Subject: "o"}, "ordersd", "", nil, t0); !errors.Is(err, ErrInvalid) {
		t.Fatal("command without payload accepted")
	}
	if _, err := FromCommand(saga.Command{SagaID: "s", Step: "x", Attempt: 1, Name: InventoryReserve, Subject: "o"}, "ordersd", "", ReservePayload{}, t0); !errors.Is(err, ErrInvalid) {
		t.Fatal("invalid payload accepted")
	}
}

func TestPayloads(t *testing.T) {
	tests := []struct {
		name string
		p    Payload
		want string
	}{
		{"reserve ok", reserve, ""},
		{"reserve empty", ReservePayload{}, "order_id"},
		{"reserve bad item", ReservePayload{OrderID: "o", Items: []Item{{SKU: "", Quantity: 0}}}, "quantity must be at least 1"},
		{"authorize ok", AuthorizePayload{OrderID: "o", AmountMinor: 125000, Currency: "VND"}, ""},
		{"authorize zero", AuthorizePayload{OrderID: "o", AmountMinor: 0, Currency: "VND"}, "amount_minor"},
		{"authorize currency", AuthorizePayload{OrderID: "o", AmountMinor: 1, Currency: "vnd"}, "ISO 4217"},
		{"release", ReleasePayload{}, "order_id"},
		{"void", VoidPayload{OrderID: "o"}, ""},
		{"result", ResultPayload{}, "reference"},
	}
	for _, tt := range tests {
		err := tt.p.Validate()
		if (tt.want == "") != (err == nil) || (err != nil && !strings.Contains(err.Error(), tt.want)) {
			t.Errorf("%s: %v, want %q", tt.name, err, tt.want)
		}
	}
	// A payload field this reader does not know is ignored; a wrong type is not.
	e := validEnvelope()
	e.Payload = json.RawMessage(`{"order_id":"o","items":[{"sku":"A","quantity":1}],"gift_wrap":true}`)
	var p ReservePayload
	if err := e.DecodePayload(&p); err != nil {
		t.Fatalf("additive field: %v", err)
	}
	e.Payload = json.RawMessage(`{"order_id":"o","items":"many"}`)
	if err := e.DecodePayload(&p); !errors.Is(err, ErrInvalid) {
		t.Fatalf("wrong type: %v", err)
	}
	e.Payload = nil
	if err := e.DecodePayload(&p); !errors.Is(err, ErrInvalid) {
		t.Fatal("missing payload accepted")
	}
}

func TestTopics(t *testing.T) {
	for cmd, want := range map[string]string{
		InventoryReserve: TopicInventoryCommands, InventoryRelease: TopicInventoryCommands,
		PaymentAuthorize: TopicPaymentCommands, PaymentVoid: TopicPaymentCommands,
	} {
		if got, err := CommandTopic(cmd); err != nil || got != want {
			t.Errorf("CommandTopic(%s) = %q, %v", cmd, got, err)
		}
	}
	for _, bad := range []string{"order.confirm", "inventory", "shipping.book"} {
		if _, err := CommandTopic(bad); !errors.Is(err, ErrInvalid) {
			t.Errorf("CommandTopic(%s) accepted", bad)
		}
	}
	specs := Topics()
	if len(specs) != 6 || specs[0].Name != TopicInventoryCommands || specs[1].Name != "inventory.commands.dlq" || specs[0].Partitions != Partitions {
		t.Fatalf("topics = %+v", specs)
	}
}

var uuidV7 = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func TestNewIDIsVersion7AndIncreasing(t *testing.T) {
	prev := ""
	seen := map[string]bool{}
	for range 20000 {
		id := NewID()
		if !uuidV7.MatchString(id) {
			t.Fatalf("%q is not a UUIDv7", id)
		}
		if id <= prev || seen[id] {
			t.Fatalf("%q after %q: ids must increase", id, prev)
		}
		seen[id] = true
		prev = id
	}
	// The embedded time is the creation time.
	var g idGen
	id := g.next(t0)
	if id[:13] != "01a1c9d5-e280" {
		t.Fatalf("time prefix of %s; want 01a1c9d5-e280 (ms since the epoch, hex) for %s", id, t0)
	}
	// A clock that steps back does not make ids go back.
	later := g.next(t0.Add(time.Second))
	earlier := g.next(t0)
	if earlier <= later {
		t.Fatalf("after the clock stepped back: %s <= %s", earlier, later)
	}
}

func FuzzUnmarshal(f *testing.F) {
	f.Add(string(encoded(&testing.T{}, validEnvelope())))
	f.Add(`{"version":2}`)
	f.Add(`{"version":1,"kind":"reply","outcome":"succeeded"}`)
	f.Fuzz(func(t *testing.T, in string) {
		e, err := Unmarshal([]byte(in))
		if err != nil {
			if !errors.Is(err, ErrInvalid) && !errors.Is(err, ErrUnsupportedVersion) {
				t.Fatalf("unclassified error %v", err)
			}
			return
		}
		// What is accepted re-encodes and reads back the same.
		b, err := e.Marshal()
		if err != nil {
			t.Fatalf("accepted envelope does not marshal: %v", err)
		}
		again, err := Unmarshal(b)
		if err != nil {
			t.Fatalf("re-encoded envelope refused: %v", err)
		}
		if again.ID != e.ID || again.Kind != e.Kind || again.Attempt != e.Attempt || !again.OccurredAt.Equal(e.OccurredAt) {
			t.Fatalf("round trip changed the envelope: %+v != %+v", again, e)
		}
	})
}
