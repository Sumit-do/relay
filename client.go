package relay

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"unicode"

	"github.com/sumit-do/relay/clock"
	"github.com/sumit-do/relay/internal/id"
	"github.com/sumit-do/relay/metrics"
)

// Defaults applied by Client.Enqueue.
const (
	DefaultQueue       = "default"
	DefaultMaxAttempts = 10
	MaxPayloadBytes    = 1 << 20 // 1 MiB
)

// EnqueueRequest describes a job to enqueue. It doubles as the JSON body of
// POST /jobs.
type EnqueueRequest struct {
	Queue          string          `json:"queue,omitempty"`
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload,omitempty"`
	Priority       int             `json:"priority,omitempty"`
	MaxAttempts    int             `json:"max_attempts,omitempty"`
	RunAt          *time.Time      `json:"run_at,omitempty"`
	Delay          string          `json:"delay,omitempty"` // Go duration, e.g. "30s", "5m"
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
}

// Metrics bundles the counters and histogram exported by relay.
type Metrics struct {
	Enqueued  *metrics.Counter   // jobs_enqueued_total{queue}
	Succeeded *metrics.Counter   // jobs_succeeded_total{queue}
	Failed    *metrics.Counter   // jobs_failed_total{queue}  (failed attempts)
	Dead      *metrics.Counter   // jobs_dead_total{queue}
	Duration  *metrics.Histogram // job_duration_seconds{queue,type}
}

// NewMetrics registers relay's metrics on r (or on a fresh registry if r is
// nil) and returns the bundle.
func NewMetrics(r *metrics.Registry) *Metrics {
	if r == nil {
		r = metrics.NewRegistry()
	}
	return &Metrics{
		Enqueued:  r.Counter("jobs_enqueued_total", "Jobs accepted by Enqueue (idempotent duplicates excluded).", "queue"),
		Succeeded: r.Counter("jobs_succeeded_total", "Job attempts that completed successfully.", "queue"),
		Failed:    r.Counter("jobs_failed_total", "Job attempts that failed (including those later retried).", "queue"),
		Dead:      r.Counter("jobs_dead_total", "Jobs moved to the dead-letter state.", "queue"),
		Duration:  r.Histogram("job_duration_seconds", "Handler execution time in seconds.", nil, "queue", "type"),
	}
}

// Client is the producer-side API: it validates requests, assigns ids and
// writes jobs to a Store.
type Client struct {
	store   Store
	clock   clock.Clock
	ids     *id.Generator
	metrics *Metrics
	// DefaultMaxAttempts is applied when a request omits max_attempts.
	DefaultMaxAttempts int
}

// ClientOption configures a Client.
type ClientOption func(*Client)

// WithClock sets the clock used for run_at defaults and ids.
func WithClock(c clock.Clock) ClientOption { return func(cl *Client) { cl.clock = c } }

// WithMetrics sets the metrics bundle to update on enqueue.
func WithMetrics(m *Metrics) ClientOption { return func(cl *Client) { cl.metrics = m } }

// WithDefaultMaxAttempts overrides DefaultMaxAttempts.
func WithDefaultMaxAttempts(n int) ClientOption {
	return func(cl *Client) { cl.DefaultMaxAttempts = n }
}

// NewClient returns a Client writing to store.
func NewClient(store Store, opts ...ClientOption) *Client {
	c := &Client{
		store:              store,
		clock:              clock.Real{},
		ids:                &id.Generator{},
		DefaultMaxAttempts: DefaultMaxAttempts,
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

// Store returns the underlying store.
func (c *Client) Store() Store { return c.store }

// Enqueue validates req, builds a Job and persists it. created is false when
// an idempotency key matched an existing job, in which case that job is
// returned unchanged.
func (c *Client) Enqueue(ctx context.Context, req EnqueueRequest) (job *Job, created bool, err error) {
	now := c.clock.Now().UTC()
	j, err := c.build(req, now)
	if err != nil {
		return nil, false, err
	}
	stored, created, err := c.store.Enqueue(ctx, j)
	if err != nil {
		return nil, false, err
	}
	if created && c.metrics != nil {
		c.metrics.Enqueued.Inc(stored.Queue)
	}
	return stored, created, nil
}

func (c *Client) build(req EnqueueRequest, now time.Time) (*Job, error) {
	if strings.TrimSpace(req.Type) == "" {
		return nil, fmt.Errorf("%w: type is required", ErrInvalidJob)
	}
	if len(req.Type) > 128 || !isName(req.Type) {
		return nil, fmt.Errorf("%w: type must be 1-128 chars of letters, digits, '_', '-', '.', ':' or '/'", ErrInvalidJob)
	}
	queue := req.Queue
	if queue == "" {
		queue = DefaultQueue
	}
	if len(queue) > 64 || !isName(queue) {
		return nil, fmt.Errorf("%w: queue must be 1-64 chars of letters, digits, '_', '-', '.', ':' or '/'", ErrInvalidJob)
	}
	if len(req.IdempotencyKey) > 255 {
		return nil, fmt.Errorf("%w: idempotency_key must be at most 255 bytes", ErrInvalidJob)
	}
	if len(req.Payload) > MaxPayloadBytes {
		return nil, fmt.Errorf("%w: payload exceeds %d bytes", ErrInvalidJob, MaxPayloadBytes)
	}
	payload := json.RawMessage(nil)
	if len(req.Payload) > 0 {
		if !json.Valid(req.Payload) {
			return nil, fmt.Errorf("%w: payload is not valid JSON", ErrInvalidJob)
		}
		payload = append(json.RawMessage(nil), req.Payload...)
	}
	maxAttempts := req.MaxAttempts
	if maxAttempts == 0 {
		maxAttempts = c.DefaultMaxAttempts
	}
	if maxAttempts < 1 || maxAttempts > 1000 {
		return nil, fmt.Errorf("%w: max_attempts must be between 1 and 1000", ErrInvalidJob)
	}
	runAt := now
	if req.RunAt != nil && req.Delay != "" {
		return nil, fmt.Errorf("%w: run_at and delay are mutually exclusive", ErrInvalidJob)
	}
	if req.RunAt != nil {
		runAt = req.RunAt.UTC()
	}
	if req.Delay != "" {
		d, err := time.ParseDuration(req.Delay)
		if err != nil || d < 0 {
			return nil, fmt.Errorf("%w: delay must be a non-negative Go duration such as \"30s\"", ErrInvalidJob)
		}
		runAt = now.Add(d)
	}
	return &Job{
		ID:             c.ids.New(now),
		Queue:          queue,
		Type:           req.Type,
		Payload:        payload,
		Status:         StatusPending,
		Priority:       req.Priority,
		MaxAttempts:    maxAttempts,
		RunAt:          runAt,
		IdempotencyKey: req.IdempotencyKey,
		CreatedAt:      now,
		UpdatedAt:      now,
	}, nil
}

func isName(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r > unicode.MaxASCII {
			return false
		}
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			continue
		}
		switch r {
		case '_', '-', '.', ':', '/':
			continue
		}
		return false
	}
	return true
}
