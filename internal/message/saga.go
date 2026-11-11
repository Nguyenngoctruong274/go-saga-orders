package message

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/Nguyenngoctruong274/go-saga-orders/internal/saga"
)

// FromCommand wraps a command the saga decided to send. causationID is the
// id of the message that led to it (the reply that advanced the saga),
// empty for the first command. The saga id is the correlation id: every
// message of one saga shares it.
func FromCommand(cmd saga.Command, source, causationID string, payload Payload, now time.Time) (Envelope, error) {
	if payload == nil {
		return Envelope{}, fmt.Errorf("%w: %s needs a payload", ErrInvalid, cmd.Name)
	}
	if err := payload.Validate(); err != nil {
		return Envelope{}, fmt.Errorf("%w: %s payload: %w", ErrInvalid, cmd.Name, err)
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return Envelope{}, fmt.Errorf("%w: %s payload: %w", ErrInvalid, cmd.Name, err)
	}
	e := Envelope{
		Version: Version, ID: NewID(), Kind: KindCommand, Type: cmd.Name, Source: source,
		SagaID: cmd.SagaID, Step: cmd.Step, Attempt: cmd.Attempt, Subject: cmd.Subject,
		CorrelationID: cmd.SagaID, CausationID: causationID, OccurredAt: now.UTC(),
		Compensate: cmd.Kind == saga.KindCompensate, Deadline: cmd.Deadline.UTC(),
		Payload: body,
	}
	return e, e.Validate()
}

// Command reads a command envelope back into the domain's terms, for
// participants.
func (e Envelope) Command() (saga.Command, error) {
	if e.Kind != KindCommand {
		return saga.Command{}, fmt.Errorf("%w: %s is a %s, not a command", ErrInvalid, e.ID, e.Kind)
	}
	kind := saga.KindAction
	if e.Compensate {
		kind = saga.KindCompensate
	}
	return saga.Command{
		SagaID: e.SagaID, Subject: e.Subject, Step: e.Step, Attempt: e.Attempt,
		Kind: kind, Name: e.Type, Deadline: e.Deadline,
	}, nil
}

// NewReply answers cmd. The reply quotes the saga, step and attempt of the
// command, so the orchestrator can tell it from a reply to an earlier
// attempt, and names the command as its cause. result may be nil.
func NewReply(cmd Envelope, source string, outcome saga.Outcome, reason string, result Payload, now time.Time) (Envelope, error) {
	if cmd.Kind != KindCommand {
		return Envelope{}, fmt.Errorf("%w: cannot reply to a %s", ErrInvalid, cmd.Kind)
	}
	switch outcome {
	case saga.OutcomeSucceeded, saga.OutcomeFailed, saga.OutcomeRetry:
	default:
		return Envelope{}, fmt.Errorf("%w: outcome %q", ErrInvalid, outcome)
	}
	var body json.RawMessage
	if result != nil {
		if err := result.Validate(); err != nil {
			return Envelope{}, fmt.Errorf("%w: reply payload: %w", ErrInvalid, err)
		}
		b, err := json.Marshal(result)
		if err != nil {
			return Envelope{}, fmt.Errorf("%w: reply payload: %w", ErrInvalid, err)
		}
		body = b
	}
	e := Envelope{
		Version: Version, ID: NewID(), Kind: KindReply, Type: ReplyType(cmd.Type), Source: source,
		SagaID: cmd.SagaID, Step: cmd.Step, Attempt: cmd.Attempt, Subject: cmd.Subject,
		CorrelationID: cmd.CorrelationID, CausationID: cmd.ID, OccurredAt: now.UTC(),
		Outcome: string(outcome), Reason: reason, Payload: body,
	}
	return e, e.Validate()
}

// Reply reads a reply envelope into the domain's terms, for the
// orchestrator. An outcome this reader does not know is invalid, not a
// failure: guessing would either compensate or advance a saga wrongly.
func (e Envelope) Reply() (saga.Reply, error) {
	if e.Kind != KindReply {
		return saga.Reply{}, fmt.Errorf("%w: %s is a %s, not a reply", ErrInvalid, e.ID, e.Kind)
	}
	if !strings.HasSuffix(e.Type, ".reply") {
		return saga.Reply{}, fmt.Errorf("%w: reply type %q lacks the .reply suffix", ErrInvalid, e.Type)
	}
	out := saga.Outcome(e.Outcome)
	switch out {
	case saga.OutcomeSucceeded, saga.OutcomeFailed, saga.OutcomeRetry:
	default:
		return saga.Reply{}, fmt.Errorf("%w: outcome %q", ErrInvalid, e.Outcome)
	}
	return saga.Reply{SagaID: e.SagaID, Step: e.Step, Attempt: e.Attempt, Outcome: out, Reason: e.Reason}, nil
}
