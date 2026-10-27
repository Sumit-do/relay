package worker

import (
	"testing"
	"time"
)

func TestBackoffScheduleNoJitter(t *testing.T) {
	b := Backoff{Base: time.Second, Max: 10 * time.Second, Factor: 2, Jitter: 0, Rand: func() float64 { return 0.5 }}
	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 10 * time.Second, 10 * time.Second}
	for i, w := range want {
		if got := b.Delay(i + 1); got != w {
			t.Errorf("Delay(%d) = %v, want %v", i+1, got, w)
		}
	}
	if got := b.Delay(0); got != time.Second {
		t.Errorf("Delay(0) = %v, want clamp to attempt 1", got)
	}
	if got := b.Delay(1000); got != 10*time.Second {
		t.Errorf("Delay(1000) = %v, want cap (overflow guard)", got)
	}
}

func TestBackoffJitterBounds(t *testing.T) {
	b := Backoff{Base: time.Second, Max: time.Hour, Factor: 2, Jitter: 0.5}
	for _, r := range []float64{0, 0.25, 0.5, 0.999} {
		b.Rand = func() float64 { return r }
		got := b.Delay(3) // un-jittered 4s, jitter range [2s, 6s)
		if got < 2*time.Second || got >= 6*time.Second {
			t.Errorf("rand=%v: Delay = %v outside [2s,6s)", r, got)
		}
	}
	b.Rand = func() float64 { return 0 }
	if b.Delay(3) != 2*time.Second {
		t.Errorf("rand=0 should yield lower bound, got %v", b.Delay(3))
	}
	b.Rand = func() float64 { return 0.5 }
	if b.Delay(3) != 4*time.Second {
		t.Errorf("rand=0.5 should yield the un-jittered delay, got %v", b.Delay(3))
	}
}

func TestBackoffZeroValueUsesDefaults(t *testing.T) {
	var b Backoff
	b.Rand = func() float64 { return 0.5 }
	if got := b.Delay(1); got != DefaultBackoff.Base {
		t.Errorf("zero Backoff Delay(1) = %v, want %v", got, DefaultBackoff.Base)
	}
	if got := b.Delay(100); got != DefaultBackoff.Max {
		t.Errorf("zero Backoff Delay(100) = %v, want cap %v", got, DefaultBackoff.Max)
	}
	for i := 0; i < 100; i++ {
		if d := (Backoff{}).Delay(i); d < 0 {
			t.Fatalf("negative delay %v", d)
		}
	}
}
