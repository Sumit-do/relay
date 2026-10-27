package id

import (
	"sort"
	"sync"
	"testing"
	"time"
)

func TestNewIsValidAndEncodesTime(t *testing.T) {
	now := time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC)
	var g Generator
	s := g.New(now)
	if len(s) != Len {
		t.Fatalf("len = %d, want %d", len(s), Len)
	}
	if !Valid(s) {
		t.Fatalf("Valid(%q) = false", s)
	}
	got, err := Time(s)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Equal(now) {
		t.Fatalf("Time = %v, want %v", got, now)
	}
}

func TestMonotonicWithinSameMillisecond(t *testing.T) {
	var g Generator
	now := time.UnixMilli(1_700_000_000_000)
	prev := g.New(now)
	for i := 0; i < 1000; i++ {
		next := g.New(now)
		if next <= prev {
			t.Fatalf("id %d not monotonic: %s <= %s", i, next, prev)
		}
		prev = next
	}
}

func TestOrderingAcrossTime(t *testing.T) {
	var g Generator
	base := time.UnixMilli(1_700_000_000_000)
	var ids []string
	for i := 0; i < 50; i++ {
		ids = append(ids, g.New(base.Add(time.Duration(i)*time.Millisecond)))
	}
	if !sort.StringsAreSorted(ids) {
		t.Fatal("ids generated at increasing times are not lexically sorted")
	}
}

func TestEntropyOverflowBumpsClock(t *testing.T) {
	g := Generator{readRand: func(b []byte) (int, error) {
		for i := range b {
			b[i] = 0xFF
		}
		return len(b), nil
	}}
	now := time.UnixMilli(1_700_000_000_000)
	a := g.New(now)
	b := g.New(now)
	if b <= a {
		t.Fatalf("overflow did not keep ordering: %s <= %s", b, a)
	}
	ta, _ := Time(a)
	tb, _ := Time(b)
	if tb.Sub(ta) != time.Millisecond {
		t.Fatalf("expected clock bump of 1ms, got %v", tb.Sub(ta))
	}
}

func TestConcurrentUniqueness(t *testing.T) {
	var g Generator
	var mu sync.Mutex
	seen := make(map[string]struct{})
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				s := g.New(time.Now())
				mu.Lock()
				if _, dup := seen[s]; dup {
					t.Errorf("duplicate id %s", s)
				}
				seen[s] = struct{}{}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
}

func TestInvalid(t *testing.T) {
	for _, s := range []string{"", "short", "01ARZ3NDEKTSV4RRFFQ69G5FA!", "ZZZZZZZZZZZZZZZZZZZZZZZZZZ"} {
		if Valid(s) {
			t.Errorf("Valid(%q) = true", s)
		}
		if _, err := Time(s); err == nil {
			t.Errorf("Time(%q) succeeded", s)
		}
	}
	if !Valid("01arz3ndektsv4rrffq69g5fav") {
		t.Error("lowercase should be accepted")
	}
}
