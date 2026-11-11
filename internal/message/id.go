package message

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"sync"
	"time"
)

// NewID returns a UUID version 7: 48 bits of Unix milliseconds, then
// random bits. Ids sort by creation time, which keeps the inbox tables
// that store them (day 5) appending to the end of their index rather than
// writing all over it, and makes a log readable in order.
//
// Within one millisecond the ids of this process still increase: the 12
// bits after the timestamp count up from a random start (RFC 9562,
// method 1).
func NewID() string { return ids.next(time.Now()) }

var ids idGen

type idGen struct {
	mu     sync.Mutex
	lastMS int64
	seq    uint16
}

func (g *idGen) next(now time.Time) string {
	var b [16]byte
	_, _ = rand.Read(b[:]) // never fails on supported platforms

	g.mu.Lock()
	ms := now.UnixMilli()
	if ms <= g.lastMS {
		ms = g.lastMS // the clock went back or stood still: stay monotonic
		g.seq++
		if g.seq > 0x0fff { // counter spent: borrow the next millisecond
			ms++
			g.seq = uint16(binary.BigEndian.Uint16(b[6:8])) & 0x07ff
		}
	} else {
		g.seq = uint16(binary.BigEndian.Uint16(b[6:8])) & 0x07ff // leave room to count up
	}
	g.lastMS = ms
	seq := g.seq
	g.mu.Unlock()

	var ts [8]byte
	binary.BigEndian.PutUint64(ts[:], uint64(ms)) //nolint:gosec // Unix milliseconds: positive, below 2^48 until the year 10889
	copy(b[0:6], ts[2:8])
	b[6] = 0x70 | byte(seq>>8)&0x0f // version 7
	b[7] = byte(seq)
	b[8] = b[8]&0x3f | 0x80 // RFC 9562 variant

	var out [36]byte
	hex.Encode(out[0:8], b[0:4])
	out[8] = '-'
	hex.Encode(out[9:13], b[4:6])
	out[13] = '-'
	hex.Encode(out[14:18], b[6:8])
	out[18] = '-'
	hex.Encode(out[19:23], b[8:10])
	out[23] = '-'
	hex.Encode(out[24:36], b[10:16])
	return string(out[:])
}
