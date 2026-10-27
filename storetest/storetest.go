// Package storetest is a contract test suite that every relay.Store
// implementation must pass. Backends call Run from their own test package:
//
//	func TestContract(t *testing.T) {
//		storetest.Run(t, func(t *testing.T) relay.Store { return memory.New() })
//	}
package storetest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sumit-do/relay"
	"github.com/sumit-do/relay/internal/id"
)

// Factory returns a fresh, empty store for each test.
type Factory func(t *testing.T) relay.Store

var t0 = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

// Run executes the contract suite against stores produced by f.
func Run(t *testing.T, f Factory) {
	tests := []struct {
		name string
		fn   func(t *testing.T, s relay.Store)
	}{
		{"EnqueueAndGet", testEnqueueAndGet},
		{"GetNotFound", testGetNotFound},
		{"Idempotency", testIdempotency},
		{"ListFiltersAndOrder", testList},
		{"LeaseOrdering", testLeaseOrdering},
		{"LeaseRespectsRunAt", testLeaseRespectsRunAt},
		{"LeaseIsExclusive", testLeaseExclusive},
		{"LeaseConcurrent", testLeaseConcurrent},
		{"HeartbeatExtendsLease", testHeartbeat},
		{"Succeed", testSucceed},
		{"FailSchedulesRetry", testFail},
		{"Dead", testDead},
		{"LeaseTokenMismatch", testLeaseTokenMismatch},
		{"ReapExpired", testReapExpired},
		{"ReapExpiredDeadLetters", testReapDead},
		{"RetryTransitions", testRetry},
		{"CancelTransitions", testCancel},
		{"Stats", testStats},
		{"RoundTripFields", testRoundTrip},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := f(t)
			t.Cleanup(func() { _ = s.Close() })
			tc.fn(t, s)
		})
	}
}

var gen id.Generator

func newJob(queue, typ string, prio int, runAt time.Time) *relay.Job {
	return &relay.Job{
		ID:          gen.New(runAt),
		Queue:       queue,
		Type:        typ,
		Payload:     json.RawMessage(`{"n":1}`),
		Status:      relay.StatusPending,
		Priority:    prio,
		MaxAttempts: 3,
		RunAt:       runAt,
		CreatedAt:   runAt,
		UpdatedAt:   runAt,
	}
}

func mustEnqueue(t *testing.T, s relay.Store, j *relay.Job) *relay.Job {
	t.Helper()
	got, created, err := s.Enqueue(context.Background(), j)
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if !created {
		t.Fatalf("Enqueue: expected created=true for %s", j.ID)
	}
	return got
}

func mustGet(t *testing.T, s relay.Store, id string) *relay.Job {
	t.Helper()
	j, err := s.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("Get(%s): %v", id, err)
	}
	return j
}

func mustLease(t *testing.T, s relay.Store, queue, token string, now time.Time) *relay.Job {
	t.Helper()
	j, err := s.Lease(context.Background(), queue, token, now, time.Minute)
	if err != nil {
		t.Fatalf("Lease: %v", err)
	}
	return j
}

func wantStatus(t *testing.T, j *relay.Job, want relay.Status) {
	t.Helper()
	if j.Status != want {
		t.Fatalf("job %s status = %s, want %s", j.ID, j.Status, want)
	}
}

func testEnqueueAndGet(t *testing.T, s relay.Store) {
	j := newJob("q", "t", 0, t0)
	stored := mustEnqueue(t, s, j)
	if stored.ID != j.ID {
		t.Fatalf("stored id = %s, want %s", stored.ID, j.ID)
	}
	got := mustGet(t, s, j.ID)
	wantStatus(t, got, relay.StatusPending)
	if got.Queue != "q" || got.Type != "t" || got.MaxAttempts != 3 || string(got.Payload) != `{"n":1}` {
		t.Fatalf("round trip mismatch: %+v", got)
	}
	if !got.RunAt.Equal(t0) {
		t.Fatalf("run_at = %v, want %v", got.RunAt, t0)
	}
	// Mutating the returned copy must not affect the store.
	got.Status = relay.StatusDead
	wantStatus(t, mustGet(t, s, j.ID), relay.StatusPending)
}

func testGetNotFound(t *testing.T, s relay.Store) {
	if _, err := s.Get(context.Background(), "nope"); !errors.Is(err, relay.ErrNotFound) {
		t.Fatalf("Get missing = %v, want ErrNotFound", err)
	}
}

func testIdempotency(t *testing.T, s relay.Store) {
	a := newJob("q", "t", 0, t0)
	a.IdempotencyKey = "order-42"
	first := mustEnqueue(t, s, a)

	b := newJob("q", "t", 5, t0.Add(time.Hour))
	b.IdempotencyKey = "order-42"
	second, created, err := s.Enqueue(context.Background(), b)
	if err != nil {
		t.Fatal(err)
	}
	if created {
		t.Fatal("duplicate key should not create a job")
	}
	if second.ID != first.ID || second.Priority != 0 {
		t.Fatalf("expected existing job %s returned, got %+v", first.ID, second)
	}
	if _, err := s.Get(context.Background(), b.ID); !errors.Is(err, relay.ErrNotFound) {
		t.Fatalf("duplicate job was persisted: %v", err)
	}
	// Empty keys never collide.
	c := newJob("q", "t", 0, t0)
	d := newJob("q", "t", 0, t0)
	mustEnqueue(t, s, c)
	mustEnqueue(t, s, d)
}

func testList(t *testing.T, s relay.Store) {
	var ids []string
	for i := 0; i < 5; i++ {
		q := "a"
		if i%2 == 1 {
			q = "b"
		}
		j := newJob(q, fmt.Sprintf("type%d", i%2), 0, t0.Add(time.Duration(i)*time.Second))
		mustEnqueue(t, s, j)
		ids = append(ids, j.ID)
	}
	ctx := context.Background()
	all, err := s.List(ctx, relay.ListFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 5 {
		t.Fatalf("List all = %d, want 5", len(all))
	}
	for i := 1; i < len(all); i++ {
		if all[i-1].ID < all[i].ID {
			t.Fatalf("List not ordered newest-first: %s before %s", all[i-1].ID, all[i].ID)
		}
	}
	bs, _ := s.List(ctx, relay.ListFilter{Queue: "b"})
	if len(bs) != 2 {
		t.Fatalf("List queue b = %d, want 2", len(bs))
	}
	lim, _ := s.List(ctx, relay.ListFilter{Limit: 2})
	if len(lim) != 2 || lim[0].ID != ids[4] {
		t.Fatalf("List limit 2 = %v", lim)
	}
	byType, _ := s.List(ctx, relay.ListFilter{Type: "type0"})
	if len(byType) != 3 {
		t.Fatalf("List type0 = %d, want 3", len(byType))
	}
	mustLease(t, s, "a", "w", t0.Add(time.Hour))
	leased, _ := s.List(ctx, relay.ListFilter{Status: relay.StatusLeased})
	if len(leased) != 1 {
		t.Fatalf("List leased = %d, want 1", len(leased))
	}
	pending, _ := s.List(ctx, relay.ListFilter{Queue: "a", Status: relay.StatusPending})
	if len(pending) != 2 {
		t.Fatalf("List a/pending = %d, want 2", len(pending))
	}
}

func testLeaseOrdering(t *testing.T, s relay.Store) {
	low := newJob("q", "t", 0, t0)
	high := newJob("q", "t", 10, t0.Add(time.Second)) // later run_at but higher priority
	early := newJob("q", "t", 0, t0.Add(-time.Second))
	other := newJob("other", "t", 100, t0)
	for _, j := range []*relay.Job{low, high, early, other} {
		mustEnqueue(t, s, j)
	}
	now := t0.Add(time.Minute)
	got := mustLease(t, s, "q", "w1", now)
	if got.ID != high.ID {
		t.Fatalf("first lease = %s, want highest priority %s", got.ID, high.ID)
	}
	wantStatus(t, got, relay.StatusLeased)
	if got.Attempts != 1 || got.LeaseToken != "w1" || got.LeaseExpiresAt == nil || !got.LeaseExpiresAt.Equal(now.Add(time.Minute)) {
		t.Fatalf("lease fields wrong: %+v", got)
	}
	if mustLease(t, s, "q", "w1", now).ID != early.ID {
		t.Fatal("second lease should be earliest run_at among equal priority")
	}
	if mustLease(t, s, "q", "w1", now).ID != low.ID {
		t.Fatal("third lease should be remaining job")
	}
	if _, err := s.Lease(context.Background(), "q", "w1", now, time.Minute); !errors.Is(err, relay.ErrNoJob) {
		t.Fatalf("empty queue lease = %v, want ErrNoJob", err)
	}
	if mustLease(t, s, "other", "w1", now).ID != other.ID {
		t.Fatal("queues must be isolated")
	}
}

func testLeaseRespectsRunAt(t *testing.T, s relay.Store) {
	j := newJob("q", "t", 0, t0.Add(time.Hour))
	mustEnqueue(t, s, j)
	if _, err := s.Lease(context.Background(), "q", "w", t0, time.Minute); !errors.Is(err, relay.ErrNoJob) {
		t.Fatalf("delayed job leased early: %v", err)
	}
	if _, err := s.Lease(context.Background(), "q", "w", t0.Add(time.Hour), time.Minute); err != nil {
		t.Fatalf("job not leasable at run_at: %v", err)
	}
}

func testLeaseExclusive(t *testing.T, s relay.Store) {
	mustEnqueue(t, s, newJob("q", "t", 0, t0))
	mustLease(t, s, "q", "w1", t0)
	if _, err := s.Lease(context.Background(), "q", "w2", t0, time.Minute); !errors.Is(err, relay.ErrNoJob) {
		t.Fatalf("leased job re-leased: %v", err)
	}
}

func testLeaseConcurrent(t *testing.T, s relay.Store) {
	const n = 20
	for i := 0; i < n; i++ {
		mustEnqueue(t, s, newJob("q", "t", 0, t0))
	}
	var mu sync.Mutex
	seen := make(map[string]int)
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for {
				j, err := s.Lease(context.Background(), "q", fmt.Sprintf("w%d", w), t0, time.Minute)
				if errors.Is(err, relay.ErrNoJob) {
					return
				}
				if err != nil {
					t.Errorf("Lease: %v", err)
					return
				}
				mu.Lock()
				seen[j.ID]++
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()
	if len(seen) != n {
		t.Fatalf("leased %d distinct jobs, want %d", len(seen), n)
	}
	for id, c := range seen {
		if c != 1 {
			t.Fatalf("job %s leased %d times", id, c)
		}
	}
}

func testHeartbeat(t *testing.T, s relay.Store) {
	mustEnqueue(t, s, newJob("q", "t", 0, t0))
	j := mustLease(t, s, "q", "w", t0)
	until := t0.Add(10 * time.Minute)
	if err := s.Heartbeat(context.Background(), j.ID, "w", until); err != nil {
		t.Fatal(err)
	}
	got := mustGet(t, s, j.ID)
	if got.LeaseExpiresAt == nil || !got.LeaseExpiresAt.Equal(until) {
		t.Fatalf("lease not extended: %v", got.LeaseExpiresAt)
	}
	// Reaping at the old expiry must not touch the job.
	if n, _ := s.ReapExpired(context.Background(), t0.Add(2*time.Minute)); n != 0 {
		t.Fatalf("reaped %d jobs after heartbeat", n)
	}
	if err := s.Heartbeat(context.Background(), j.ID, "other", until); !errors.Is(err, relay.ErrLeaseLost) {
		t.Fatalf("heartbeat with wrong token = %v", err)
	}
	if err := s.Heartbeat(context.Background(), "missing", "w", until); !errors.Is(err, relay.ErrNotFound) {
		t.Fatalf("heartbeat missing = %v", err)
	}
}

func testSucceed(t *testing.T, s relay.Store) {
	mustEnqueue(t, s, newJob("q", "t", 0, t0))
	j := mustLease(t, s, "q", "w", t0)
	done := t0.Add(time.Second)
	if err := s.Succeed(context.Background(), j.ID, "w", done); err != nil {
		t.Fatal(err)
	}
	got := mustGet(t, s, j.ID)
	wantStatus(t, got, relay.StatusSucceeded)
	if got.LeaseToken != "" || got.LeaseExpiresAt != nil || got.CompletedAt == nil || !got.CompletedAt.Equal(done) {
		t.Fatalf("succeeded job fields: %+v", got)
	}
	if err := s.Succeed(context.Background(), j.ID, "w", done); !errors.Is(err, relay.ErrLeaseLost) {
		t.Fatalf("double succeed = %v, want ErrLeaseLost", err)
	}
}

func testFail(t *testing.T, s relay.Store) {
	mustEnqueue(t, s, newJob("q", "t", 0, t0))
	j := mustLease(t, s, "q", "w", t0)
	retryAt := t0.Add(30 * time.Second)
	if err := s.Fail(context.Background(), j.ID, "w", "boom", retryAt, t0.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	got := mustGet(t, s, j.ID)
	wantStatus(t, got, relay.StatusFailed)
	if got.LastError != "boom" || !got.RunAt.Equal(retryAt) || got.Attempts != 1 || got.LeaseToken != "" {
		t.Fatalf("failed job fields: %+v", got)
	}
	if _, err := s.Lease(context.Background(), "q", "w", t0.Add(10*time.Second), time.Minute); !errors.Is(err, relay.ErrNoJob) {
		t.Fatalf("failed job leased before retry time: %v", err)
	}
	again := mustLease(t, s, "q", "w", retryAt)
	if again.ID != j.ID || again.Attempts != 2 {
		t.Fatalf("retry lease: %+v", again)
	}
}

func testDead(t *testing.T, s relay.Store) {
	mustEnqueue(t, s, newJob("q", "t", 0, t0))
	j := mustLease(t, s, "q", "w", t0)
	if err := s.Dead(context.Background(), j.ID, "w", "gave up", t0.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	got := mustGet(t, s, j.ID)
	wantStatus(t, got, relay.StatusDead)
	if got.LastError != "gave up" || got.CompletedAt == nil {
		t.Fatalf("dead job fields: %+v", got)
	}
	if _, err := s.Lease(context.Background(), "q", "w", t0.Add(time.Hour), time.Minute); !errors.Is(err, relay.ErrNoJob) {
		t.Fatalf("dead job leased: %v", err)
	}
}

func testLeaseTokenMismatch(t *testing.T, s relay.Store) {
	mustEnqueue(t, s, newJob("q", "t", 0, t0))
	j := mustLease(t, s, "q", "w", t0)
	ctx := context.Background()
	if err := s.Succeed(ctx, j.ID, "x", t0); !errors.Is(err, relay.ErrLeaseLost) {
		t.Fatalf("Succeed wrong token = %v", err)
	}
	if err := s.Fail(ctx, j.ID, "x", "e", t0, t0); !errors.Is(err, relay.ErrLeaseLost) {
		t.Fatalf("Fail wrong token = %v", err)
	}
	if err := s.Dead(ctx, j.ID, "x", "e", t0); !errors.Is(err, relay.ErrLeaseLost) {
		t.Fatalf("Dead wrong token = %v", err)
	}
	wantStatus(t, mustGet(t, s, j.ID), relay.StatusLeased)
}

func testReapExpired(t *testing.T, s relay.Store) {
	mustEnqueue(t, s, newJob("q", "t", 0, t0))
	j := mustLease(t, s, "q", "w", t0) // expires t0+1m
	ctx := context.Background()
	if n, err := s.ReapExpired(ctx, t0.Add(59*time.Second)); err != nil || n != 0 {
		t.Fatalf("reap before expiry = %d, %v", n, err)
	}
	n, err := s.ReapExpired(ctx, t0.Add(time.Minute))
	if err != nil || n != 1 {
		t.Fatalf("reap at expiry = %d, %v", n, err)
	}
	got := mustGet(t, s, j.ID)
	wantStatus(t, got, relay.StatusPending)
	if got.LeaseToken != "" || got.LeaseExpiresAt != nil || got.Attempts != 1 {
		t.Fatalf("reaped job fields: %+v", got)
	}
	// The stale worker can no longer complete it.
	if err := s.Succeed(ctx, j.ID, "w", t0); !errors.Is(err, relay.ErrLeaseLost) {
		t.Fatalf("stale Succeed = %v", err)
	}
	// And it can be leased again with attempts incremented.
	again := mustLease(t, s, "q", "w2", t0.Add(time.Minute))
	if again.ID != j.ID || again.Attempts != 2 {
		t.Fatalf("re-lease after reap: %+v", again)
	}
}

func testReapDead(t *testing.T, s relay.Store) {
	j := newJob("q", "t", 0, t0)
	j.MaxAttempts = 1
	mustEnqueue(t, s, j)
	mustLease(t, s, "q", "w", t0)
	if n, _ := s.ReapExpired(context.Background(), t0.Add(time.Hour)); n != 1 {
		t.Fatalf("reaped %d", n)
	}
	got := mustGet(t, s, j.ID)
	wantStatus(t, got, relay.StatusDead)
	if got.LastError == "" {
		t.Fatal("dead-lettered job should record why")
	}
}

func testRetry(t *testing.T, s relay.Store) {
	ctx := context.Background()
	j := newJob("q", "t", 0, t0)
	j.MaxAttempts = 1
	mustEnqueue(t, s, j)
	leased := mustLease(t, s, "q", "w", t0)
	if err := s.Dead(ctx, leased.ID, "w", "x", t0); err != nil {
		t.Fatal(err)
	}
	later := t0.Add(time.Hour)
	got, err := s.Retry(ctx, j.ID, later)
	if err != nil {
		t.Fatal(err)
	}
	wantStatus(t, got, relay.StatusPending)
	if got.Attempts != 0 || got.LastError != "" || !got.RunAt.Equal(later) || got.CompletedAt != nil {
		t.Fatalf("retried job fields: %+v", got)
	}
	if _, err := s.Retry(ctx, j.ID, later); !errors.Is(err, relay.ErrInvalidTransition) {
		t.Fatalf("retry pending = %v, want ErrInvalidTransition", err)
	}
	mustLease(t, s, "q", "w", later)
	if _, err := s.Retry(ctx, j.ID, later); !errors.Is(err, relay.ErrInvalidTransition) {
		t.Fatalf("retry leased = %v", err)
	}
	if err := s.Succeed(ctx, j.ID, "w", later); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Retry(ctx, j.ID, later); !errors.Is(err, relay.ErrInvalidTransition) {
		t.Fatalf("retry succeeded = %v", err)
	}
	if _, err := s.Retry(ctx, "missing", later); !errors.Is(err, relay.ErrNotFound) {
		t.Fatalf("retry missing = %v", err)
	}
	// Failed and cancelled jobs are redrivable too.
	f := mustEnqueue(t, s, newJob("q", "t", 0, t0))
	mustLease(t, s, "q", "w", later)
	if err := s.Fail(ctx, f.ID, "w", "e", later.Add(time.Hour), later); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Retry(ctx, f.ID, later); err != nil {
		t.Fatalf("retry failed job: %v", err)
	}
	c := mustEnqueue(t, s, newJob("q", "t", 0, t0))
	if _, err := s.Cancel(ctx, c.ID, later); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Retry(ctx, c.ID, later); err != nil {
		t.Fatalf("retry cancelled job: %v", err)
	}
}

func testCancel(t *testing.T, s relay.Store) {
	ctx := context.Background()
	p := mustEnqueue(t, s, newJob("q", "t", 0, t0))
	got, err := s.Cancel(ctx, p.ID, t0)
	if err != nil {
		t.Fatal(err)
	}
	wantStatus(t, got, relay.StatusCancelled)
	if got.CompletedAt == nil {
		t.Fatal("cancelled job should have completed_at")
	}
	if _, err := s.Cancel(ctx, p.ID, t0); !errors.Is(err, relay.ErrInvalidTransition) {
		t.Fatalf("double cancel = %v", err)
	}
	if _, err := s.Lease(ctx, "q", "w", t0.Add(time.Hour), time.Minute); !errors.Is(err, relay.ErrNoJob) {
		t.Fatalf("cancelled job leased: %v", err)
	}
	l := mustEnqueue(t, s, newJob("q", "t", 0, t0))
	mustLease(t, s, "q", "w", t0)
	if _, err := s.Cancel(ctx, l.ID, t0); !errors.Is(err, relay.ErrInvalidTransition) {
		t.Fatalf("cancel leased = %v", err)
	}
	if _, err := s.Cancel(ctx, "missing", t0); !errors.Is(err, relay.ErrNotFound) {
		t.Fatalf("cancel missing = %v", err)
	}
}

func testStats(t *testing.T, s relay.Store) {
	ctx := context.Background()
	empty, err := s.Stats(ctx)
	if err != nil || len(empty) != 0 {
		t.Fatalf("empty stats = %v, %v", empty, err)
	}
	for i := 0; i < 3; i++ {
		mustEnqueue(t, s, newJob("a", "t", 0, t0))
	}
	mustEnqueue(t, s, newJob("b", "t", 0, t0))
	a := mustLease(t, s, "a", "w", t0)
	_ = s.Succeed(ctx, a.ID, "w", t0)
	mustLease(t, s, "a", "w", t0)
	st, err := s.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if st["a"][relay.StatusPending] != 1 || st["a"][relay.StatusLeased] != 1 || st["a"][relay.StatusSucceeded] != 1 {
		t.Fatalf("stats a = %v", st["a"])
	}
	if st["b"][relay.StatusPending] != 1 {
		t.Fatalf("stats b = %v", st["b"])
	}
}

func testRoundTrip(t *testing.T, s relay.Store) {
	j := newJob("q", "t", -7, t0.Add(1500*time.Millisecond))
	j.Payload = json.RawMessage(`{"nested":{"a":[1,2,3]},"s":"café \"quoted\""}`)
	j.IdempotencyKey = "k"
	j.MaxAttempts = 42
	mustEnqueue(t, s, j)
	got := mustGet(t, s, j.ID)
	if got.Priority != -7 || got.MaxAttempts != 42 || got.IdempotencyKey != "k" {
		t.Fatalf("fields: %+v", got)
	}
	if !got.RunAt.Equal(j.RunAt) || !got.CreatedAt.Equal(j.CreatedAt) {
		t.Fatalf("times: run_at %v created %v", got.RunAt, got.CreatedAt)
	}
	var a, b any
	if err := json.Unmarshal(got.Payload, &a); err != nil {
		t.Fatal(err)
	}
	_ = json.Unmarshal(j.Payload, &b)
	if fmt.Sprint(a) != fmt.Sprint(b) {
		t.Fatalf("payload mismatch: %s", got.Payload)
	}
	// Nil payload survives.
	n := newJob("q", "t", 0, t0)
	n.Payload = nil
	mustEnqueue(t, s, n)
	if got := mustGet(t, s, n.ID); len(got.Payload) != 0 {
		t.Fatalf("nil payload became %q", got.Payload)
	}
}
