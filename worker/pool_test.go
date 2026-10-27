package worker

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sumit-do/relay"
	"github.com/sumit-do/relay/clock"
	"github.com/sumit-do/relay/metrics"
	"github.com/sumit-do/relay/store/memory"
)

var t0 = time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

type harness struct {
	t      *testing.T
	store  *memory.Store
	clk    *clock.Fake
	client *relay.Client
	m      *relay.Metrics
	pool   *Pool
	cancel context.CancelFunc
	done   chan error
}

// newHarness builds a pool over an in-memory store with a fake clock and fast
// polling. cfg lets tests override pool settings.
func newHarness(t *testing.T, cfg Config) *harness {
	t.Helper()
	h := &harness{t: t, store: memory.New(), clk: clock.NewFake(t0)}
	h.m = relay.NewMetrics(metrics.NewRegistry())
	h.client = relay.NewClient(h.store, relay.WithClock(h.clk), relay.WithMetrics(h.m))
	if cfg.Clock == nil {
		cfg.Clock = h.clk
	}
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 5 * time.Millisecond
	}
	if cfg.ReapInterval == 0 {
		cfg.ReapInterval = 5 * time.Millisecond
	}
	if cfg.Concurrency == 0 {
		cfg.Concurrency = 2
	}
	if cfg.Backoff.Base == 0 {
		cfg.Backoff = Backoff{Base: time.Second, Max: time.Hour, Factor: 2, Jitter: 0}
	}
	cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg.Metrics = h.m
	h.pool = New(h.store, cfg)
	return h
}

// start runs the pool in the background; stop cancels and waits for Run.
func (h *harness) start() {
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	h.done = make(chan error, 1)
	go func() { h.done <- h.pool.Run(ctx) }()
	h.t.Cleanup(func() { h.stop() })
}

func (h *harness) stop() error {
	if h.cancel == nil {
		return nil
	}
	h.cancel()
	h.cancel = nil
	select {
	case err := <-h.done:
		return err
	case <-time.After(10 * time.Second):
		h.t.Fatal("pool did not stop")
		return nil
	}
}

func (h *harness) enqueue(req relay.EnqueueRequest) *relay.Job {
	h.t.Helper()
	j, _, err := h.client.Enqueue(context.Background(), req)
	if err != nil {
		h.t.Fatalf("Enqueue: %v", err)
	}
	return j
}

func (h *harness) get(id string) *relay.Job {
	h.t.Helper()
	j, err := h.store.Get(context.Background(), id)
	if err != nil {
		h.t.Fatalf("Get: %v", err)
	}
	return j
}

// waitFor polls cond until it is true or the deadline passes.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func (h *harness) waitStatus(id string, want relay.Status) *relay.Job {
	h.t.Helper()
	var j *relay.Job
	waitFor(h.t, "job "+id+" to be "+string(want), func() bool {
		j = h.get(id)
		return j.Status == want
	})
	return j
}

func TestSuccessFlow(t *testing.T) {
	h := newHarness(t, Config{})
	var got atomic.Value
	h.pool.Handle("echo", func(_ context.Context, j *relay.Job) error {
		got.Store(string(j.Payload))
		return nil
	})
	h.start()
	j := h.enqueue(relay.EnqueueRequest{Type: "echo", Payload: json.RawMessage(`{"hello":"world"}`)})
	done := h.waitStatus(j.ID, relay.StatusSucceeded)
	if got.Load() != `{"hello":"world"}` {
		t.Fatalf("handler saw payload %v", got.Load())
	}
	if done.Attempts != 1 || done.CompletedAt == nil || done.LeaseToken != "" {
		t.Fatalf("succeeded job: %+v", done)
	}
	if h.m.Succeeded.Value("default") != 1 || h.m.Enqueued.Value("default") != 1 || h.m.Duration.Count("default", "echo") != 1 {
		t.Fatal("metrics not recorded")
	}
}

func TestRetryBackoffScheduleThenDeadLetter(t *testing.T) {
	h := newHarness(t, Config{Concurrency: 1})
	var calls atomic.Int32
	h.pool.Handle("flaky", func(context.Context, *relay.Job) error {
		calls.Add(1)
		return errors.New("boom")
	})
	h.start()
	j := h.enqueue(relay.EnqueueRequest{Type: "flaky", MaxAttempts: 3})

	// Attempt 1 fails -> retry in 1s.
	f1 := h.waitStatus(j.ID, relay.StatusFailed)
	if f1.Attempts != 1 || f1.LastError != "boom" || !f1.RunAt.Equal(t0.Add(time.Second)) {
		t.Fatalf("after attempt 1: %+v", f1)
	}
	// Not retried before run_at.
	time.Sleep(30 * time.Millisecond)
	if calls.Load() != 1 {
		t.Fatalf("job retried before run_at (calls=%d)", calls.Load())
	}

	// Attempt 2 fails -> retry in 2s.
	h.clk.Set(t0.Add(time.Second))
	waitFor(t, "attempt 2", func() bool { return h.get(j.ID).Attempts == 2 && h.get(j.ID).Status == relay.StatusFailed })
	f2 := h.get(j.ID)
	if !f2.RunAt.Equal(t0.Add(3 * time.Second)) {
		t.Fatalf("after attempt 2 run_at = %v, want %v", f2.RunAt, t0.Add(3*time.Second))
	}

	// Attempt 3 fails -> dead.
	h.clk.Set(t0.Add(3 * time.Second))
	d := h.waitStatus(j.ID, relay.StatusDead)
	if d.Attempts != 3 || d.LastError != "boom" || d.CompletedAt == nil {
		t.Fatalf("dead job: %+v", d)
	}
	if h.m.Failed.Value("default") != 3 || h.m.Dead.Value("default") != 1 {
		t.Fatalf("metrics failed=%v dead=%v", h.m.Failed.Value("default"), h.m.Dead.Value("default"))
	}
	time.Sleep(20 * time.Millisecond)
	if calls.Load() != 3 {
		t.Fatalf("dead job kept running (calls=%d)", calls.Load())
	}

	// Redrive: the job runs again with a fresh attempt counter.
	if _, err := h.store.Retry(context.Background(), j.ID, h.clk.Now()); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "redriven attempt", func() bool { return calls.Load() == 4 })
	r := h.waitStatus(j.ID, relay.StatusFailed)
	if r.Attempts != 1 {
		t.Fatalf("redriven attempts = %d, want 1", r.Attempts)
	}
}

func TestPerQueueBackoffOverride(t *testing.T) {
	h := newHarness(t, Config{Concurrency: 1})
	h.pool.AddQueue(QueueConfig{Name: "slow", Backoff: &Backoff{Base: time.Minute, Max: time.Hour, Factor: 3, Jitter: 0}})
	h.pool.AddQueue(QueueConfig{Name: relay.DefaultQueue})
	h.pool.Handle("fail", func(context.Context, *relay.Job) error { return errors.New("x") })
	h.start()
	a := h.enqueue(relay.EnqueueRequest{Type: "fail", Queue: "slow"})
	b := h.enqueue(relay.EnqueueRequest{Type: "fail"})
	fa := h.waitStatus(a.ID, relay.StatusFailed)
	fb := h.waitStatus(b.ID, relay.StatusFailed)
	if !fa.RunAt.Equal(t0.Add(time.Minute)) {
		t.Fatalf("slow queue run_at = %v, want +1m", fa.RunAt)
	}
	if !fb.RunAt.Equal(t0.Add(time.Second)) {
		t.Fatalf("default queue run_at = %v, want +1s", fb.RunAt)
	}
}

func TestLeaseExpiryReturnsJobToPending(t *testing.T) {
	// Lease is 1 minute of fake time; heartbeats run every 20s of real time
	// so none fire during the test. Advancing the fake clock past the lease
	// lets the reaper hand the job to a second worker.
	h := newHarness(t, Config{Concurrency: 2, LeaseDuration: time.Minute})
	release := make(chan struct{})
	var first, second atomic.Int32
	h.pool.Handle("stuck", func(_ context.Context, j *relay.Job) error {
		if j.Attempts == 1 {
			first.Add(1)
			<-release
			return nil
		}
		second.Add(1)
		return nil
	})
	h.start()
	j := h.enqueue(relay.EnqueueRequest{Type: "stuck", MaxAttempts: 5})
	waitFor(t, "first attempt to start", func() bool { return first.Load() == 1 })
	if h.get(j.ID).Status != relay.StatusLeased {
		t.Fatal("job should be leased")
	}

	h.clk.Advance(2 * time.Minute)
	done := h.waitStatus(j.ID, relay.StatusSucceeded)
	if done.Attempts != 2 || second.Load() != 1 {
		t.Fatalf("expected second attempt after reap: %+v (second=%d)", done, second.Load())
	}
	// The stale worker finishing must not corrupt the record (ErrLeaseLost).
	close(release)
	time.Sleep(20 * time.Millisecond)
	final := h.get(j.ID)
	if final.Status != relay.StatusSucceeded || final.Attempts != 2 {
		t.Fatalf("stale worker altered job: %+v", final)
	}
	if h.m.Succeeded.Value("default") != 1 {
		t.Fatalf("succeeded counter = %v, want 1", h.m.Succeeded.Value("default"))
	}
}

func TestHeartbeatKeepsLongJobLeased(t *testing.T) {
	// Real clock: lease 60ms, heartbeat every 20ms, handler runs 250ms. The
	// reaper (every 5ms) must never reclaim the job.
	h := newHarness(t, Config{Concurrency: 1, LeaseDuration: 60 * time.Millisecond, Clock: clock.Real{}})
	var calls atomic.Int32
	h.pool.Handle("long", func(ctx context.Context, _ *relay.Job) error {
		calls.Add(1)
		select {
		case <-time.After(250 * time.Millisecond):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	h.start()
	j := h.enqueue(relay.EnqueueRequest{Type: "long"})
	done := h.waitStatus(j.ID, relay.StatusSucceeded)
	if done.Attempts != 1 || calls.Load() != 1 {
		t.Fatalf("long job was re-leased: attempts=%d calls=%d", done.Attempts, calls.Load())
	}
}

func TestLostLeaseCancelsHandlerContext(t *testing.T) {
	h := newHarness(t, Config{Concurrency: 1, LeaseDuration: 30 * time.Millisecond, ReapInterval: time.Hour})
	cancelled := make(chan struct{}, 1)
	var calls atomic.Int32
	h.pool.Handle("watch", func(ctx context.Context, j *relay.Job) error {
		if calls.Add(1) > 1 {
			return nil
		}
		<-ctx.Done()
		cancelled <- struct{}{}
		return ctx.Err()
	})
	h.start()
	j := h.enqueue(relay.EnqueueRequest{Type: "watch"})
	waitFor(t, "handler start", func() bool { return calls.Load() == 1 })

	// Simulate an operator or another node reclaiming the lease.
	h.clk.Advance(time.Hour)
	if n, _ := h.store.ReapExpired(context.Background(), h.clk.Now()); n != 1 {
		t.Fatalf("reaped %d", n)
	}
	select {
	case <-cancelled:
	case <-time.After(5 * time.Second):
		t.Fatal("handler context was not cancelled after lease loss")
	}
	done := h.waitStatus(j.ID, relay.StatusSucceeded)
	if done.Attempts != 2 {
		t.Fatalf("attempts = %d, want 2", done.Attempts)
	}
}

func TestPanicRecovery(t *testing.T) {
	h := newHarness(t, Config{Concurrency: 1})
	var calls atomic.Int32
	h.pool.Handle("panicky", func(context.Context, *relay.Job) error {
		if calls.Add(1) == 1 {
			panic("kaboom")
		}
		return nil
	})
	h.start()
	j := h.enqueue(relay.EnqueueRequest{Type: "panicky", MaxAttempts: 2})
	f := h.waitStatus(j.ID, relay.StatusFailed)
	if !strings.Contains(f.LastError, "panic: kaboom") || !strings.Contains(f.LastError, "pool_test.go") {
		t.Fatalf("last_error should contain panic message and stack, got %q", f.LastError)
	}
	h.clk.Advance(time.Second)
	done := h.waitStatus(j.ID, relay.StatusSucceeded)
	if done.Attempts != 2 {
		t.Fatalf("attempts = %d", done.Attempts)
	}
	// The pool is still healthy after the panic.
	k := h.enqueue(relay.EnqueueRequest{Type: "panicky"})
	h.waitStatus(k.ID, relay.StatusSucceeded)
}

func TestPermanentErrorDeadLettersImmediately(t *testing.T) {
	h := newHarness(t, Config{})
	h.pool.Handle("bad", func(context.Context, *relay.Job) error {
		return Permanent(errors.New("malformed input"))
	})
	h.start()
	j := h.enqueue(relay.EnqueueRequest{Type: "bad", MaxAttempts: 10})
	d := h.waitStatus(j.ID, relay.StatusDead)
	if d.Attempts != 1 || d.LastError != "malformed input" {
		t.Fatalf("dead job: %+v", d)
	}
	if !IsPermanent(Permanent(errors.New("x"))) || IsPermanent(errors.New("x")) || Permanent(nil) != nil {
		t.Fatal("Permanent/IsPermanent helpers misbehave")
	}
}

func TestUnknownTypeDeadLetters(t *testing.T) {
	h := newHarness(t, Config{})
	h.start()
	j := h.enqueue(relay.EnqueueRequest{Type: "nobody_handles_this"})
	d := h.waitStatus(j.ID, relay.StatusDead)
	if !strings.Contains(d.LastError, ErrNoHandler.Error()) {
		t.Fatalf("last_error = %q", d.LastError)
	}
}

func TestDelayedJobWaitsForRunAt(t *testing.T) {
	h := newHarness(t, Config{})
	var calls atomic.Int32
	h.pool.Handle("later", func(context.Context, *relay.Job) error { calls.Add(1); return nil })
	h.start()
	j := h.enqueue(relay.EnqueueRequest{Type: "later", Delay: "1h"})
	time.Sleep(30 * time.Millisecond)
	if calls.Load() != 0 || h.get(j.ID).Status != relay.StatusPending {
		t.Fatal("delayed job ran early")
	}
	h.clk.Advance(time.Hour)
	h.waitStatus(j.ID, relay.StatusSucceeded)
}

func TestPriorityOrdering(t *testing.T) {
	h := newHarness(t, Config{Concurrency: 1})
	var mu sync.Mutex
	var order []int
	h.pool.Handle("p", func(_ context.Context, j *relay.Job) error {
		mu.Lock()
		order = append(order, j.Priority)
		mu.Unlock()
		return nil
	})
	var ids []string
	for _, p := range []int{1, 5, 3, 5} {
		ids = append(ids, h.enqueue(relay.EnqueueRequest{Type: "p", Priority: p}).ID)
	}
	h.start()
	for _, id := range ids {
		h.waitStatus(id, relay.StatusSucceeded)
	}
	mu.Lock()
	defer mu.Unlock()
	want := []int{5, 5, 3, 1}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("execution order = %v, want %v", order, want)
		}
	}
}

func TestPerQueueConcurrencyLimit(t *testing.T) {
	h := newHarness(t, Config{Concurrency: 8})
	h.pool.AddQueue(QueueConfig{Name: "narrow", Concurrency: 2})
	release := make(chan struct{})
	var running, peak atomic.Int32
	h.pool.Handle("block", func(context.Context, *relay.Job) error {
		n := running.Add(1)
		for {
			old := peak.Load()
			if n <= old || peak.CompareAndSwap(old, n) {
				break
			}
		}
		<-release
		running.Add(-1)
		return nil
	})
	h.start()
	var ids []string
	for i := 0; i < 5; i++ {
		ids = append(ids, h.enqueue(relay.EnqueueRequest{Type: "block", Queue: "narrow"}).ID)
	}
	waitFor(t, "two handlers running", func() bool { return running.Load() == 2 })
	time.Sleep(30 * time.Millisecond)
	if running.Load() != 2 {
		t.Fatalf("running = %d, want 2", running.Load())
	}
	close(release)
	for _, id := range ids {
		h.waitStatus(id, relay.StatusSucceeded)
	}
	if peak.Load() != 2 {
		t.Fatalf("peak concurrency = %d, want 2", peak.Load())
	}
}

func TestGracefulDrainWaitsForInflight(t *testing.T) {
	h := newHarness(t, Config{Concurrency: 1, DrainTimeout: 5 * time.Second})
	release := make(chan struct{})
	started := make(chan struct{})
	h.pool.Handle("slow", func(context.Context, *relay.Job) error {
		close(started)
		<-release
		return nil
	})
	h.start()
	j := h.enqueue(relay.EnqueueRequest{Type: "slow"})
	<-started
	// A second job enqueued now must not be picked up once draining starts.
	k := h.enqueue(relay.EnqueueRequest{Type: "slow"})

	h.cancel()
	h.cancel = nil
	select {
	case err := <-h.done:
		t.Fatalf("Run returned %v before in-flight job finished", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-h.done:
		if err != nil {
			t.Fatalf("Run = %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after handler finished")
	}
	if h.get(j.ID).Status != relay.StatusSucceeded {
		t.Fatalf("in-flight job status = %s", h.get(j.ID).Status)
	}
	if h.get(k.ID).Status != relay.StatusPending {
		t.Fatalf("job enqueued during drain was leased: %s", h.get(k.ID).Status)
	}
}

func TestDrainTimeoutCancelsHandlers(t *testing.T) {
	h := newHarness(t, Config{Concurrency: 1, DrainTimeout: 50 * time.Millisecond})
	started := make(chan struct{})
	h.pool.Handle("hang", func(ctx context.Context, _ *relay.Job) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	})
	h.start()
	j := h.enqueue(relay.EnqueueRequest{Type: "hang"})
	<-started
	if err := h.stop(); !errors.Is(err, ErrDrainTimeout) {
		t.Fatalf("Run = %v, want ErrDrainTimeout", err)
	}
	// The interrupted attempt is recorded as a failure and will be retried.
	f := h.get(j.ID)
	if f.Status != relay.StatusFailed || f.Attempts != 1 || !strings.Contains(f.LastError, "context canceled") {
		t.Fatalf("interrupted job: %+v", f)
	}
}

func TestShutdownIdempotentAndBeforeStart(t *testing.T) {
	h := newHarness(t, Config{})
	if err := h.pool.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown before Start = %v", err)
	}
	h.pool.Start()
	h.pool.Start() // no-op
	if err := h.pool.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := h.pool.Shutdown(context.Background()); err != nil {
		t.Fatalf("second Shutdown = %v", err)
	}
}

func TestConfigDefaults(t *testing.T) {
	c := Config{}.withDefaults()
	if c.Concurrency != 8 || c.LeaseDuration != 30*time.Second || c.DrainTimeout != 30*time.Second || c.WorkerID == "" || c.Logger == nil {
		t.Fatalf("defaults: %+v", c)
	}
	p := New(memory.New(), Config{Concurrency: 3})
	p.AddQueue(QueueConfig{Name: "q"})
	p.AddQueue(QueueConfig{Name: "q", Concurrency: 5}) // replaces
	if len(p.queues) != 1 || p.queues[0].Concurrency != 5 || p.queues[0].Backoff == nil {
		t.Fatalf("queues: %+v", p.queues)
	}
}
