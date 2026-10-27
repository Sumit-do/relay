// Package worker runs job handlers against a relay.Store with per-queue
// concurrency, lease heartbeats, retries with exponential backoff,
// dead-lettering, panic recovery and graceful draining.
package worker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"runtime/debug"
	"sync"
	"time"

	"github.com/sumit-do/relay"
	"github.com/sumit-do/relay/clock"
)

// Handler processes one job. Returning nil marks the job succeeded; any
// error schedules a retry (or dead-letters the job once max_attempts is
// reached). Wrap an error with Permanent to dead-letter immediately.
//
// The context is cancelled when the pool loses the job's lease (so another
// worker may already be running it) or when a shutdown's drain timeout
// expires. Long-running handlers should watch ctx.Done().
type Handler func(ctx context.Context, job *relay.Job) error

// Errors returned by the pool.
var (
	// ErrDrainTimeout is returned by Shutdown/Run when in-flight jobs did not
	// finish within the drain timeout. Their contexts were cancelled.
	ErrDrainTimeout = errors.New("worker: drain timeout exceeded")
	// ErrNoHandler is recorded as last_error when a job's type has no handler.
	ErrNoHandler = errors.New("worker: no handler registered for job type")
)

type permanentError struct{ err error }

func (p *permanentError) Error() string { return p.err.Error() }
func (p *permanentError) Unwrap() error { return p.err }

// Permanent wraps err so that the job is dead-lettered without further
// retries.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

// IsPermanent reports whether err was wrapped with Permanent.
func IsPermanent(err error) bool {
	var p *permanentError
	return errors.As(err, &p)
}

// QueueConfig tunes one queue. Zero fields inherit from Config.
type QueueConfig struct {
	Name          string
	Concurrency   int
	LeaseDuration time.Duration
	Backoff       *Backoff
}

// Config tunes a Pool. Zero fields take the documented defaults.
type Config struct {
	// Concurrency is the default number of concurrent handlers per queue (8).
	Concurrency int
	// LeaseDuration is the default visibility timeout (30s). Heartbeats run
	// every LeaseDuration/3.
	LeaseDuration time.Duration
	// PollInterval is how long an idle worker sleeps before polling again (500ms).
	PollInterval time.Duration
	// ReapInterval is how often expired leases are returned to pending (5s).
	ReapInterval time.Duration
	// DrainTimeout bounds how long Run waits for in-flight jobs after its
	// context is cancelled (30s).
	DrainTimeout time.Duration
	// Backoff is the default retry policy.
	Backoff Backoff
	// Logger defaults to slog.Default().
	Logger *slog.Logger
	// Clock defaults to the wall clock.
	Clock clock.Clock
	// Metrics is optional.
	Metrics *relay.Metrics
	// WorkerID identifies this process in lease tokens (default: hostname-pid).
	WorkerID string
}

func (c Config) withDefaults() Config {
	if c.Concurrency <= 0 {
		c.Concurrency = 8
	}
	if c.LeaseDuration <= 0 {
		c.LeaseDuration = 30 * time.Second
	}
	if c.PollInterval <= 0 {
		c.PollInterval = 500 * time.Millisecond
	}
	if c.ReapInterval <= 0 {
		c.ReapInterval = 5 * time.Second
	}
	if c.DrainTimeout <= 0 {
		c.DrainTimeout = 30 * time.Second
	}
	c.Backoff = c.Backoff.withDefaults()
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	if c.Clock == nil {
		c.Clock = clock.Real{}
	}
	if c.WorkerID == "" {
		host, _ := os.Hostname()
		c.WorkerID = fmt.Sprintf("%s-%d", host, os.Getpid())
	}
	return c
}

// Pool leases jobs from a Store and dispatches them to handlers.
type Pool struct {
	store relay.Store
	cfg   Config

	mu       sync.Mutex
	handlers map[string]Handler
	queues   []QueueConfig
	started  bool

	stop     chan struct{}      // closed by Shutdown: stop leasing new work
	hardCtx  context.Context    // parent of every handler ctx
	hardStop context.CancelFunc // cancelled when the drain timeout expires
	wg       sync.WaitGroup
}

// New creates a Pool. Register handlers with Handle and queues with AddQueue
// before calling Start or Run.
func New(store relay.Store, cfg Config) *Pool {
	return &Pool{
		store:    store,
		cfg:      cfg.withDefaults(),
		handlers: make(map[string]Handler),
		stop:     make(chan struct{}),
	}
}

// Handle registers h for jobs of the given type. Registering the same type
// twice replaces the handler.
func (p *Pool) Handle(jobType string, h Handler) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.handlers[jobType] = h
}

// AddQueue configures a queue to poll. If no queue is added before Start, the
// pool polls relay.DefaultQueue with the pool defaults.
func (p *Pool) AddQueue(q QueueConfig) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.addQueueLocked(q)
}

func (p *Pool) addQueueLocked(q QueueConfig) {
	if q.Concurrency <= 0 {
		q.Concurrency = p.cfg.Concurrency
	}
	if q.LeaseDuration <= 0 {
		q.LeaseDuration = p.cfg.LeaseDuration
	}
	if q.Backoff == nil {
		b := p.cfg.Backoff
		q.Backoff = &b
	} else {
		b := q.Backoff.withDefaults()
		q.Backoff = &b
	}
	for i, existing := range p.queues {
		if existing.Name == q.Name {
			p.queues[i] = q
			return
		}
	}
	p.queues = append(p.queues, q)
}

// Start launches the worker goroutines and returns immediately.
func (p *Pool) Start() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.started {
		return
	}
	p.started = true
	if len(p.queues) == 0 {
		p.addQueueLocked(QueueConfig{Name: relay.DefaultQueue})
	}
	p.hardCtx, p.hardStop = context.WithCancel(context.Background())

	p.wg.Add(1)
	go p.reaper()
	for _, q := range p.queues {
		for i := 0; i < q.Concurrency; i++ {
			p.wg.Add(1)
			go p.worker(q, i)
		}
	}
	p.cfg.Logger.Info("worker pool started", "worker_id", p.cfg.WorkerID, "queues", len(p.queues))
}

// Shutdown stops leasing new jobs and waits for in-flight handlers until ctx
// is done. If ctx expires first, handler contexts are cancelled, the pool
// waits for them to return, and ErrDrainTimeout is reported.
func (p *Pool) Shutdown(ctx context.Context) error {
	p.mu.Lock()
	if !p.started {
		p.mu.Unlock()
		return nil
	}
	select {
	case <-p.stop:
	default:
		close(p.stop)
	}
	p.mu.Unlock()

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()
	var err error
	select {
	case <-done:
	case <-ctx.Done():
		p.cfg.Logger.Warn("drain timeout exceeded; cancelling in-flight jobs")
		p.hardStop()
		<-done
		err = ErrDrainTimeout
	}
	p.hardStop()
	p.cfg.Logger.Info("worker pool stopped")
	return err
}

// Run starts the pool, blocks until ctx is cancelled, then drains with the
// configured DrainTimeout.
func (p *Pool) Run(ctx context.Context) error {
	p.Start()
	<-ctx.Done()
	drainCtx, cancel := context.WithTimeout(context.Background(), p.cfg.DrainTimeout)
	defer cancel()
	return p.Shutdown(drainCtx)
}

// sleep waits for d or until the pool is stopping; it reports false when
// stopping.
func (p *Pool) sleep(d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-p.stop:
		return false
	}
}

func (p *Pool) reaper() {
	defer p.wg.Done()
	for {
		if !p.sleep(p.cfg.ReapInterval) {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		n, err := p.store.ReapExpired(ctx, p.cfg.Clock.Now())
		cancel()
		if err != nil {
			p.cfg.Logger.Error("reap expired leases", "err", err)
		} else if n > 0 {
			p.cfg.Logger.Warn("returned expired leases to the queue", "count", n)
		}
	}
}

func (p *Pool) worker(q QueueConfig, slot int) {
	defer p.wg.Done()
	log := p.cfg.Logger.With("queue", q.Name, "slot", slot)
	for {
		select {
		case <-p.stop:
			return
		default:
		}
		token := p.cfg.WorkerID + "/" + randomToken()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		job, err := p.store.Lease(ctx, q.Name, token, p.cfg.Clock.Now(), q.LeaseDuration)
		cancel()
		switch {
		case errors.Is(err, relay.ErrNoJob):
			if !p.sleep(p.cfg.PollInterval) {
				return
			}
			continue
		case err != nil:
			log.Error("lease job", "err", err)
			if !p.sleep(p.cfg.PollInterval) {
				return
			}
			continue
		}
		p.process(log, q, job)
	}
}

func (p *Pool) process(log *slog.Logger, q QueueConfig, job *relay.Job) {

	log = log.With("job_id", job.ID, "type", job.Type, "attempt", job.Attempts, "max_attempts", job.MaxAttempts)
	p.mu.Lock()
	h, ok := p.handlers[job.Type]
	p.mu.Unlock()

	ctx, cancel := context.WithCancel(p.hardCtx)
	defer cancel()

	// Heartbeat until the handler returns.
	hbDone := make(chan struct{})
	hbStopped := make(chan struct{})
	go func() {
		defer close(hbStopped)
		p.heartbeat(ctx, cancel, log, job, q.LeaseDuration, hbDone)
	}()

	start := time.Now()
	var err error
	if !ok {
		err = Permanent(fmt.Errorf("%w: %q", ErrNoHandler, job.Type))
	} else {
		err = runHandler(ctx, h, job)
	}
	elapsed := time.Since(start)
	close(hbDone)
	<-hbStopped

	if p.cfg.Metrics != nil {
		p.cfg.Metrics.Duration.Observe(elapsed.Seconds(), job.Queue, job.Type)
	}

	now := p.cfg.Clock.Now()
	sctx, scancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer scancel()

	if err == nil {
		if serr := p.store.Succeed(sctx, job.ID, job.LeaseToken, now); serr != nil {
			log.Warn("mark succeeded", "err", serr, "duration", elapsed)
			return
		}
		if p.cfg.Metrics != nil {
			p.cfg.Metrics.Succeeded.Inc(job.Queue)
		}
		log.Info("job succeeded", "duration", elapsed)
		return
	}

	if p.cfg.Metrics != nil {
		p.cfg.Metrics.Failed.Inc(job.Queue)
	}
	msg := truncate(err.Error(), 4096)
	if IsPermanent(err) || job.Attempts >= job.MaxAttempts {
		if derr := p.store.Dead(sctx, job.ID, job.LeaseToken, msg, now); derr != nil {
			log.Warn("mark dead", "err", derr, "handler_err", err)
			return
		}
		if p.cfg.Metrics != nil {
			p.cfg.Metrics.Dead.Inc(job.Queue)
		}
		log.Error("job dead-lettered", "err", err, "duration", elapsed, "permanent", IsPermanent(err))
		return
	}
	delay := q.Backoff.Delay(job.Attempts)
	retryAt := now.Add(delay)
	if ferr := p.store.Fail(sctx, job.ID, job.LeaseToken, msg, retryAt, now); ferr != nil {
		log.Warn("mark failed", "err", ferr, "handler_err", err)
		return
	}
	log.Warn("job failed; retry scheduled", "err", err, "retry_in", delay, "duration", elapsed)
}

// heartbeat extends the lease every leaseFor/3 until done is closed. If the
// store reports the lease lost, the handler context is cancelled.
func (p *Pool) heartbeat(ctx context.Context, cancel context.CancelFunc, log *slog.Logger, job *relay.Job, leaseFor time.Duration, done <-chan struct{}) {
	interval := leaseFor / 3
	if interval <= 0 {
		interval = time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-done:
			return
		case <-t.C:
			hctx, hcancel := context.WithTimeout(ctx, 5*time.Second)
			err := p.store.Heartbeat(hctx, job.ID, job.LeaseToken, p.cfg.Clock.Now().Add(leaseFor))
			hcancel()
			switch {
			case err == nil:
			case errors.Is(err, relay.ErrLeaseLost), errors.Is(err, relay.ErrNotFound):
				log.Warn("lease lost; cancelling handler", "err", err)
				cancel()
				return
			case ctx.Err() != nil:
				return
			default:
				log.Error("heartbeat", "err", err)
			}
		}
	}
}

// runHandler invokes h, converting panics into errors.
func runHandler(ctx context.Context, h Handler, job *relay.Job) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v\n%s", r, debug.Stack())
		}
	}()
	return h(ctx, job)
}

func randomToken() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%x", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
