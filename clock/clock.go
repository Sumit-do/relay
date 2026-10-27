// Package clock abstracts time.Now so that lease expiry, backoff and
// scheduling logic can be tested deterministically.
package clock

import (
	"sync"
	"time"
)

// Clock returns the current time.
type Clock interface {
	Now() time.Time
}

// Real is a Clock backed by time.Now.
type Real struct{}

// Now implements Clock.
func (Real) Now() time.Time { return time.Now() }

// Fake is a manually controlled Clock for tests. It is safe for concurrent use.
type Fake struct {
	mu  sync.Mutex
	now time.Time
}

// NewFake returns a Fake clock frozen at t.
func NewFake(t time.Time) *Fake { return &Fake{now: t} }

// Now implements Clock.
func (f *Fake) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

// Advance moves the clock forward by d and returns the new time.
func (f *Fake) Advance(d time.Duration) time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
	return f.now
}

// Set moves the clock to t.
func (f *Fake) Set(t time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = t
}
