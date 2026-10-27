// Package memory implements relay.Store in process memory. It is the
// reference implementation used by tests and is suitable for development or
// for embedding when durability is not required.
package memory

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/sumit-do/relay"
)

// Store is an in-memory relay.Store. It is safe for concurrent use.
type Store struct {
	mu     sync.RWMutex
	jobs   map[string]*relay.Job
	byKey  map[string]string // idempotency key -> job id
	closed bool
}

// New returns an empty store.
func New() *Store {
	return &Store{
		jobs:  make(map[string]*relay.Job),
		byKey: make(map[string]string),
	}
}

var _ relay.Store = (*Store)(nil)

// Enqueue implements relay.Store.
func (s *Store) Enqueue(_ context.Context, job *relay.Job) (*relay.Job, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if job.IdempotencyKey != "" {
		if id, ok := s.byKey[job.IdempotencyKey]; ok {
			return s.jobs[id].Clone(), false, nil
		}
	}
	j := job.Clone()
	s.jobs[j.ID] = j
	if j.IdempotencyKey != "" {
		s.byKey[j.IdempotencyKey] = j.ID
	}
	return j.Clone(), true, nil
}

// Get implements relay.Store.
func (s *Store) Get(_ context.Context, id string) (*relay.Job, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	j, ok := s.jobs[id]
	if !ok {
		return nil, relay.ErrNotFound
	}
	return j.Clone(), nil
}

// List implements relay.Store.
func (s *Store) List(_ context.Context, f relay.ListFilter) ([]*relay.Job, error) {
	limit := f.Limit
	if limit <= 0 {
		limit = relay.DefaultListLimit
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*relay.Job, 0, 32)
	for _, j := range s.jobs {
		if f.Queue != "" && j.Queue != f.Queue {
			continue
		}
		if f.Status != "" && j.Status != f.Status {
			continue
		}
		if f.Type != "" && j.Type != f.Type {
			continue
		}
		out = append(out, j)
	}
	sort.Slice(out, func(a, b int) bool { return out[a].ID > out[b].ID })
	if len(out) > limit {
		out = out[:limit]
	}
	res := make([]*relay.Job, len(out))
	for i, j := range out {
		res[i] = j.Clone()
	}
	return res, nil
}

// Lease implements relay.Store.
func (s *Store) Lease(_ context.Context, queue, token string, now time.Time, leaseFor time.Duration) (*relay.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var best *relay.Job
	for _, j := range s.jobs {
		if j.Queue != queue || !(j.Status == relay.StatusPending || j.Status == relay.StatusFailed) || j.RunAt.After(now) {
			continue
		}
		if best == nil || less(j, best) {
			best = j
		}
	}
	if best == nil {
		return nil, relay.ErrNoJob
	}
	exp := now.Add(leaseFor)
	best.Status = relay.StatusLeased
	best.Attempts++
	best.LeaseToken = token
	best.LeaseExpiresAt = &exp
	best.UpdatedAt = now
	return best.Clone(), nil
}

// less orders ready jobs: priority desc, run_at asc, id asc.
func less(a, b *relay.Job) bool {
	if a.Priority != b.Priority {
		return a.Priority > b.Priority
	}
	if !a.RunAt.Equal(b.RunAt) {
		return a.RunAt.Before(b.RunAt)
	}
	return a.ID < b.ID
}

// leased returns the job if it is leased with token, else ErrLeaseLost /
// ErrNotFound. Caller must hold s.mu.
func (s *Store) leased(id, token string) (*relay.Job, error) {
	j, ok := s.jobs[id]
	if !ok {
		return nil, relay.ErrNotFound
	}
	if j.Status != relay.StatusLeased || j.LeaseToken != token {
		return nil, relay.ErrLeaseLost
	}
	return j, nil
}

// Heartbeat implements relay.Store.
func (s *Store) Heartbeat(_ context.Context, id, token string, expiresAt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, err := s.leased(id, token)
	if err != nil {
		return err
	}
	j.LeaseExpiresAt = &expiresAt
	return nil
}

// Succeed implements relay.Store.
func (s *Store) Succeed(_ context.Context, id, token string, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, err := s.leased(id, token)
	if err != nil {
		return err
	}
	j.Status = relay.StatusSucceeded
	j.LeaseToken = ""
	j.LeaseExpiresAt = nil
	j.LastError = ""
	j.CompletedAt = &now
	j.UpdatedAt = now
	return nil
}

// Fail implements relay.Store.
func (s *Store) Fail(_ context.Context, id, token, lastError string, retryAt, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, err := s.leased(id, token)
	if err != nil {
		return err
	}
	j.Status = relay.StatusFailed
	j.LeaseToken = ""
	j.LeaseExpiresAt = nil
	j.LastError = lastError
	j.RunAt = retryAt
	j.UpdatedAt = now
	return nil
}

// Dead implements relay.Store.
func (s *Store) Dead(_ context.Context, id, token, lastError string, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, err := s.leased(id, token)
	if err != nil {
		return err
	}
	j.Status = relay.StatusDead
	j.LeaseToken = ""
	j.LeaseExpiresAt = nil
	j.LastError = lastError
	j.CompletedAt = &now
	j.UpdatedAt = now
	return nil
}

// ReapExpired implements relay.Store.
func (s *Store) ReapExpired(_ context.Context, now time.Time) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, j := range s.jobs {
		if j.Status != relay.StatusLeased || j.LeaseExpiresAt == nil || j.LeaseExpiresAt.After(now) {
			continue
		}
		j.LeaseToken = ""
		j.LeaseExpiresAt = nil
		j.UpdatedAt = now
		j.LastError = "lease expired"
		if j.Attempts >= j.MaxAttempts {
			j.Status = relay.StatusDead
			j.CompletedAt = &now
		} else {
			j.Status = relay.StatusPending
		}
		n++
	}
	return n, nil
}

// Retry implements relay.Store.
func (s *Store) Retry(_ context.Context, id string, now time.Time) (*relay.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[id]
	if !ok {
		return nil, relay.ErrNotFound
	}
	switch j.Status {
	case relay.StatusDead, relay.StatusFailed, relay.StatusCancelled:
	default:
		return nil, relay.ErrInvalidTransition
	}
	j.Status = relay.StatusPending
	j.Attempts = 0
	j.RunAt = now
	j.LastError = ""
	j.CompletedAt = nil
	j.UpdatedAt = now
	return j.Clone(), nil
}

// Cancel implements relay.Store.
func (s *Store) Cancel(_ context.Context, id string, now time.Time) (*relay.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[id]
	if !ok {
		return nil, relay.ErrNotFound
	}
	switch j.Status {
	case relay.StatusPending, relay.StatusFailed:
	default:
		return nil, relay.ErrInvalidTransition
	}
	j.Status = relay.StatusCancelled
	j.CompletedAt = &now
	j.UpdatedAt = now
	return j.Clone(), nil
}

// Stats implements relay.Store.
func (s *Store) Stats(_ context.Context) (relay.QueueStats, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	st := make(relay.QueueStats)
	for _, j := range s.jobs {
		q, ok := st[j.Queue]
		if !ok {
			q = make(map[relay.Status]int)
			st[j.Queue] = q
		}
		q[j.Status]++
	}
	return st, nil
}

// Close implements relay.Store.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}
