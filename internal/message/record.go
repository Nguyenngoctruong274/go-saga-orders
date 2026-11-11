package message

import (
	"fmt"
	"strconv"
)

// Record is an encoded envelope ready for, or read from, the broker. It is
// the boundary between this package and internal/kafka.
type Record struct {
	Topic   string
	Key     []byte
	Value   []byte
	Headers []Header

	// Set on records read from the broker, for logs and dead letters.
	Partition int32
	Offset    int64
}

// Header is one Kafka record header.
type Header struct {
	Key   string
	Value []byte
}

// Header names. They mirror fields of the envelope so that tools and
// consumers can route, filter and trace without parsing the body; the
// body stays the source of truth and Decode checks the two agree.
const (
	HeaderID            = "message-id"
	HeaderType          = "message-type"
	HeaderVersion       = "message-version"
	HeaderSagaID        = "saga-id"
	HeaderCorrelationID = "correlation-id"
	HeaderCausationID   = "causation-id"
	HeaderContentType   = "content-type"

	// Dead-letter headers, added by the consumer that gave up on a record.
	HeaderDeadLetterError     = "dlq-error"
	HeaderDeadLetterTopic     = "dlq-original-topic"
	HeaderDeadLetterPartition = "dlq-original-partition"
	HeaderDeadLetterOffset    = "dlq-original-offset"
)

const contentType = "application/json"

// Header returns the value of the first header named key.
func (r Record) Header(key string) (string, bool) {
	for _, h := range r.Headers {
		if h.Key == key {
			return string(h.Value), true
		}
	}
	return "", false
}

// Encode turns e into a record for topic, keyed by saga id.
func Encode(topic string, e Envelope) (Record, error) {
	if topic == "" {
		return Record{}, fmt.Errorf("%w: topic is required", ErrInvalid)
	}
	b, err := e.Marshal()
	if err != nil {
		return Record{}, err
	}
	hs := []Header{
		{HeaderID, []byte(e.ID)},
		{HeaderType, []byte(e.Type)},
		{HeaderVersion, []byte(strconv.Itoa(e.Version))},
		{HeaderSagaID, []byte(e.SagaID)},
		{HeaderCorrelationID, []byte(e.CorrelationID)},
		{HeaderContentType, []byte(contentType)},
	}
	if e.CausationID != "" {
		hs = append(hs, Header{HeaderCausationID, []byte(e.CausationID)})
	}
	return Record{Topic: topic, Key: []byte(e.SagaID), Value: b, Headers: hs}, nil
}

// Decode reads the envelope of a record and checks that the key and the
// headers present agree with it: a record whose key is not its saga id
// would land in the wrong partition and break per-saga ordering.
func Decode(r Record) (Envelope, error) {
	e, err := Unmarshal(r.Value)
	if err != nil {
		return e, err
	}
	if string(r.Key) != e.SagaID {
		return e, fmt.Errorf("%w: record key %q is not the saga id %q", ErrInvalid, r.Key, e.SagaID)
	}
	for _, c := range []struct{ header, want string }{
		{HeaderID, e.ID},
		{HeaderType, e.Type},
		{HeaderSagaID, e.SagaID},
		{HeaderVersion, strconv.Itoa(e.Version)},
	} {
		if got, ok := r.Header(c.header); ok && got != c.want {
			return e, fmt.Errorf("%w: header %s is %q, the body says %q", ErrInvalid, c.header, got, c.want)
		}
	}
	return e, nil
}
