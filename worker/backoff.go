package worker

import (
	"math"
	"math/rand/v2"
	"time"
)

// Backoff computes the delay before a failed job is retried:
//
//	delay = min(Max, Base * Factor^(attempt-1)) * (1 ± Jitter*U)
//
// where U is uniform in [0,1). The zero value is usable and equals
// DefaultBackoff.
type Backoff struct {
	Base   time.Duration // first retry delay (default 1s)
	Max    time.Duration // cap on the un-jittered delay (default 10m)
	Factor float64       // exponential growth factor (default 2)
	Jitter float64       // fraction of the delay to randomise, 0..1 (default 0.2)

	// Rand returns a float in [0,1). Nil selects math/rand/v2. Tests inject a
	// deterministic source.
	Rand func() float64
}

// DefaultBackoff is used when a queue does not override it.
var DefaultBackoff = Backoff{Base: time.Second, Max: 10 * time.Minute, Factor: 2, Jitter: 0.2}

func (b Backoff) withDefaults() Backoff {
	if b.Base <= 0 {
		b.Base = DefaultBackoff.Base
	}
	if b.Max <= 0 {
		b.Max = DefaultBackoff.Max
	}
	if b.Factor < 1 {
		b.Factor = DefaultBackoff.Factor
	}
	if b.Jitter < 0 || b.Jitter > 1 {
		b.Jitter = DefaultBackoff.Jitter
	}
	return b
}

// Delay returns the retry delay after the given 1-based attempt number has
// failed. It is always >= 0.
func (b Backoff) Delay(attempt int) time.Duration {
	b = b.withDefaults()
	if attempt < 1 {
		attempt = 1
	}
	d := float64(b.Base) * math.Pow(b.Factor, float64(attempt-1))
	if d > float64(b.Max) || math.IsInf(d, 0) || math.IsNaN(d) {
		d = float64(b.Max)
	}
	if b.Jitter > 0 {
		r := b.Rand
		if r == nil {
			r = rand.Float64
		}
		// Spread symmetrically: factor in [1-Jitter, 1+Jitter).
		d *= 1 + b.Jitter*(2*r()-1)
	}
	if d < 0 {
		d = 0
	}
	return time.Duration(d)
}
