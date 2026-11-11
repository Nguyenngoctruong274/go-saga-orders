// Package message defines what travels between the services: a versioned
// JSON envelope around a typed payload, the topics it goes to, the Kafka
// headers that mirror it, and the conversion between saga commands and
// replies and envelopes. It knows nothing about Kafka clients; a Record
// is plain bytes that internal/kafka produces and consumes.
package message

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Version is the envelope schema version this code writes. Readers accept
// every version up to it; a newer one is refused with ErrUnsupportedVersion
// rather than half understood. Adding an optional field does not bump it,
// since readers ignore fields they do not know; renaming, removing or
// changing the meaning of a field does.
const Version = 1

// Kind says what an envelope is for.
type Kind string

// Envelope kinds.
const (
	// KindCommand asks a participant to do or undo something.
	KindCommand Kind = "command"
	// KindReply is a participant's answer to one command.
	KindReply Kind = "reply"
)

// Errors returned by Decode and Validate; match them with errors.Is.
var (
	// ErrInvalid: the bytes are not a well-formed envelope. Retrying will
	// not fix it; the consumer dead-letters it.
	ErrInvalid = errors.New("invalid message")
	// ErrUnsupportedVersion: written by a newer producer than this reader.
	ErrUnsupportedVersion = errors.New("unsupported message version")
)

// Envelope is the unit of communication. Commands and replies share it so
// that every message carries the same tracing fields.
type Envelope struct {
	Version int    `json:"version"`
	ID      string `json:"id"`
	Kind    Kind   `json:"kind"`
	// Type names the command ("inventory.reserve") or, for a reply, the
	// command answered with a ".reply" suffix ("inventory.reserve.reply").
	Type string `json:"type"`
	// Source is the service that wrote the message.
	Source string `json:"source"`

	// SagaID, Step and Attempt fence the conversation: a reply must quote
	// the attempt of the command it answers, or the orchestrator ignores it.
	SagaID  string `json:"saga_id"`
	Step    string `json:"step"`
	Attempt int    `json:"attempt"`
	// Subject is what the saga is about: the order id.
	Subject string `json:"subject"`

	// CorrelationID ties together every message of one business
	// transaction (the saga); CausationID is the id of the message that
	// caused this one, empty for the first command of a saga. Together
	// they rebuild the conversation from the logs alone.
	CorrelationID string `json:"correlation_id"`
	CausationID   string `json:"causation_id,omitempty"`

	OccurredAt time.Time `json:"occurred_at"`

	// Command fields.
	Compensate bool      `json:"compensate,omitempty"`
	Deadline   time.Time `json:"deadline,omitzero"`

	// Reply fields.
	Outcome string `json:"outcome,omitempty"`
	Reason  string `json:"reason,omitempty"`

	// Payload is the typed body (see payloads.go), kept raw so the
	// envelope can be routed and checked without knowing its type.
	Payload json.RawMessage `json:"payload,omitempty"`
}

// MaxSize bounds an encoded envelope. Kafka's default limit is 1 MiB; a
// saga message is a few hundred bytes, so anything near the limit is a
// bug or an attack, refused before it reaches the broker.
const MaxSize = 256 << 10

// Validate reports every missing or inconsistent field at once.
func (e Envelope) Validate() error {
	var errs []error
	req := func(name, v string) {
		if v == "" {
			errs = append(errs, fmt.Errorf("%s is required", name))
		}
	}
	if e.Version < 1 {
		errs = append(errs, fmt.Errorf("version %d", e.Version))
	}
	req("id", e.ID)
	req("type", e.Type)
	req("source", e.Source)
	req("saga_id", e.SagaID)
	req("step", e.Step)
	req("subject", e.Subject)
	req("correlation_id", e.CorrelationID)
	if e.Attempt < 1 {
		errs = append(errs, fmt.Errorf("attempt must be at least 1, got %d", e.Attempt))
	}
	if e.OccurredAt.IsZero() {
		errs = append(errs, errors.New("occurred_at is required"))
	}
	switch e.Kind {
	case KindCommand:
		if e.Outcome != "" {
			errs = append(errs, errors.New("a command has no outcome"))
		}
	case KindReply:
		if e.Outcome == "" {
			errs = append(errs, errors.New("a reply needs an outcome"))
		}
		if e.CausationID == "" {
			errs = append(errs, errors.New("a reply needs the id of the command it answers as causation_id"))
		}
		if e.Compensate || !e.Deadline.IsZero() {
			errs = append(errs, errors.New("compensate and deadline belong to commands"))
		}
	default:
		errs = append(errs, fmt.Errorf("kind %q", e.Kind))
	}
	if len(e.Payload) > 0 && !json.Valid(e.Payload) {
		errs = append(errs, errors.New("payload is not JSON"))
	}
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalid, err)
	}
	return nil
}

// Marshal validates e and encodes it.
func (e Envelope) Marshal() ([]byte, error) {
	if err := e.Validate(); err != nil {
		return nil, err
	}
	b, err := json.Marshal(e)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalid, err)
	}
	if len(b) > MaxSize {
		return nil, fmt.Errorf("%w: %d bytes, at most %d", ErrInvalid, len(b), MaxSize)
	}
	return b, nil
}

// Unmarshal decodes and validates an envelope. Unknown fields are
// ignored, so an older reader keeps working when a newer writer adds one;
// a newer version, or a body that is not a valid envelope, is refused.
func Unmarshal(b []byte) (Envelope, error) {
	var e Envelope
	if len(b) > MaxSize {
		return e, fmt.Errorf("%w: %d bytes, at most %d", ErrInvalid, len(b), MaxSize)
	}
	// Read the version first, so that a future envelope whose fields no
	// longer fit this struct is reported as what it is.
	var probe struct {
		Version int `json:"version"`
	}
	if err := json.Unmarshal(b, &probe); err != nil {
		return e, fmt.Errorf("%w: %w", ErrInvalid, err)
	}
	if probe.Version > Version {
		return e, fmt.Errorf("%w: %d, this reader knows up to %d", ErrUnsupportedVersion, probe.Version, Version)
	}
	// json.Unmarshal (unlike a Decoder) also refuses trailing data, so
	// two envelopes glued together are not read as the first one.
	if err := json.Unmarshal(b, &e); err != nil {
		return e, fmt.Errorf("%w: %w", ErrInvalid, err)
	}
	if err := e.Validate(); err != nil {
		return e, err
	}
	return e, nil
}

// Payload is implemented by every payload type: it checks the fields its
// command needs.
type Payload interface {
	Validate() error
}

// DecodePayload decodes the payload into v and validates it. Unknown
// fields are ignored, like in the envelope, so a producer may add an
// optional field before its consumers know it.
func (e Envelope) DecodePayload(v Payload) error {
	if len(e.Payload) == 0 {
		return fmt.Errorf("%w: %s has no payload", ErrInvalid, e.Type)
	}
	if err := json.Unmarshal(e.Payload, v); err != nil {
		return fmt.Errorf("%w: %s payload: %w", ErrInvalid, e.Type, err)
	}
	if err := v.Validate(); err != nil {
		return fmt.Errorf("%w: %s payload: %w", ErrInvalid, e.Type, err)
	}
	return nil
}
