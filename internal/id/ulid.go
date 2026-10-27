// Package id generates ULID-style identifiers: 26-character Crockford
// base32 strings whose first 10 characters encode a millisecond timestamp
// and whose remaining 16 encode 80 bits of entropy. IDs generated within the
// same millisecond are made strictly monotonic by incrementing the entropy,
// so lexical order equals creation order.
package id

import (
	"crypto/rand"
	"errors"
	"sync"
	"time"
)

const (
	// Len is the length of an encoded id.
	Len      = 26
	alphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
)

var decode = func() [256]byte {
	var t [256]byte
	for i := range t {
		t[i] = 0xFF
	}
	for i := 0; i < len(alphabet); i++ {
		t[alphabet[i]] = byte(i)
		// Accept lowercase on input.
		if alphabet[i] >= 'A' && alphabet[i] <= 'Z' {
			t[alphabet[i]+('a'-'A')] = byte(i)
		}
	}
	return t
}()

// Generator produces monotonic ids. The zero value is ready to use.
type Generator struct {
	mu      sync.Mutex
	lastMS  uint64
	entropy [10]byte
	// readRand lets tests inject deterministic entropy.
	readRand func([]byte) (int, error)
}

var global Generator

// New returns a new id using the global generator and the current time.
func New() string { return global.New(time.Now()) }

// New returns a new id for the given timestamp.
func (g *Generator) New(now time.Time) string {
	g.mu.Lock()
	defer g.mu.Unlock()

	ms := uint64(now.UnixMilli())
	if ms > g.lastMS || g.lastMS == 0 {
		g.lastMS = ms
		g.fill()
	} else {
		// Same (or earlier, clock went backwards) millisecond: keep the last
		// timestamp and increment entropy so ordering stays strict.
		ms = g.lastMS
		if !g.increment() {
			// Entropy overflowed (astronomically unlikely); bump the clock.
			g.lastMS++
			ms = g.lastMS
			g.fill()
		}
	}

	var raw [16]byte
	raw[0] = byte(ms >> 40)
	raw[1] = byte(ms >> 32)
	raw[2] = byte(ms >> 24)
	raw[3] = byte(ms >> 16)
	raw[4] = byte(ms >> 8)
	raw[5] = byte(ms)
	copy(raw[6:], g.entropy[:])
	return encode(raw)
}

func (g *Generator) fill() {
	read := g.readRand
	if read == nil {
		read = rand.Read
	}
	if _, err := read(g.entropy[:]); err != nil {
		// crypto/rand failing is unrecoverable on supported platforms;
		// fall back to a time-derived pattern rather than panicking.
		n := uint64(time.Now().UnixNano())
		for i := range g.entropy {
			g.entropy[i] = byte(n >> (8 * uint(i%8)))
		}
	}
}

// increment adds one to the big-endian entropy; it reports false on overflow.
func (g *Generator) increment() bool {
	for i := len(g.entropy) - 1; i >= 0; i-- {
		g.entropy[i]++
		if g.entropy[i] != 0 {
			return true
		}
	}
	return false
}

// encode converts 128 bits into 26 base32 characters (the first character
// only carries 3 bits, as in the ULID spec).
func encode(b [16]byte) string {
	var out [Len]byte
	out[0] = alphabet[(b[0]&224)>>5]
	out[1] = alphabet[b[0]&31]
	out[2] = alphabet[(b[1]&248)>>3]
	out[3] = alphabet[((b[1]&7)<<2)|((b[2]&192)>>6)]
	out[4] = alphabet[(b[2]&62)>>1]
	out[5] = alphabet[((b[2]&1)<<4)|((b[3]&240)>>4)]
	out[6] = alphabet[((b[3]&15)<<1)|((b[4]&128)>>7)]
	out[7] = alphabet[(b[4]&124)>>2]
	out[8] = alphabet[((b[4]&3)<<3)|((b[5]&224)>>5)]
	out[9] = alphabet[b[5]&31]
	out[10] = alphabet[(b[6]&248)>>3]
	out[11] = alphabet[((b[6]&7)<<2)|((b[7]&192)>>6)]
	out[12] = alphabet[(b[7]&62)>>1]
	out[13] = alphabet[((b[7]&1)<<4)|((b[8]&240)>>4)]
	out[14] = alphabet[((b[8]&15)<<1)|((b[9]&128)>>7)]
	out[15] = alphabet[(b[9]&124)>>2]
	out[16] = alphabet[((b[9]&3)<<3)|((b[10]&224)>>5)]
	out[17] = alphabet[b[10]&31]
	out[18] = alphabet[(b[11]&248)>>3]
	out[19] = alphabet[((b[11]&7)<<2)|((b[12]&192)>>6)]
	out[20] = alphabet[(b[12]&62)>>1]
	out[21] = alphabet[((b[12]&1)<<4)|((b[13]&240)>>4)]
	out[22] = alphabet[((b[13]&15)<<1)|((b[14]&128)>>7)]
	out[23] = alphabet[(b[14]&124)>>2]
	out[24] = alphabet[((b[14]&3)<<3)|((b[15]&224)>>5)]
	out[25] = alphabet[b[15]&31]
	return string(out[:])
}

// ErrInvalid is returned by Time for malformed ids.
var ErrInvalid = errors.New("id: invalid identifier")

// Time extracts the millisecond timestamp embedded in an id.
func Time(s string) (time.Time, error) {
	if !Valid(s) {
		return time.Time{}, ErrInvalid
	}
	var ms uint64
	for i := 0; i < 10; i++ {
		ms = ms<<5 | uint64(decode[s[i]])
	}
	return time.UnixMilli(int64(ms)).UTC(), nil
}

// Valid reports whether s is a well-formed id.
func Valid(s string) bool {
	if len(s) != Len || decode[s[0]] > 7 {
		return false
	}
	for i := 0; i < Len; i++ {
		if decode[s[i]] == 0xFF {
			return false
		}
	}
	return true
}
