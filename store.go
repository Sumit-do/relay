package relay

import (
	"context"
	"time"
)

// ListFilter narrows a Store.List call. Zero values mean "no filter".
type ListFilter struct {
	Queue  string
	Status Status
	Type   string
	Limit  int // <= 0 means the backend default (100)
}

// DefaultListLimit is used when ListFilter.Limit is not positive.
const DefaultListLimit = 100

// QueueStats is a per-queue breakdown of job counts by status.
type QueueStats map[string]map[Status]int

// Store is the persistence contract. All backends must satisfy the contract
// test suite in package storetest. Every method must be safe for concurrent
// use; mutating methods must be atomic with respect to each other so that a
// job can only ever be leased by one worker at a time.
//
// Time is always passed in explicitly (never read from time.Now inside the
// store) so that behaviour is deterministic under test.
type Store interface {
	// Enqueue persists a new job. If job.IdempotencyKey is non-empty and a
	// job with the same key already exists, the existing job is returned and
	// created is false; nothing is written.
	Enqueue(ctx context.Context, job *Job) (stored *Job, created bool, err error)

	// Get returns the job with the given id or ErrNotFound.
	Get(ctx context.Context, id string) (*Job, error)

	// List returns jobs matching the filter, ordered by id descending (newest
	// first, since ids are time-sortable).
	List(ctx context.Context, f ListFilter) ([]*Job, error)

	// Lease atomically claims the highest-priority ready job (status pending
	// or failed, run_at <= now) in queue, ordered by priority desc, run_at
	// asc, id asc. It sets status=leased, increments attempts, records the
	// lease token and lease_expires_at = now + leaseFor. Returns ErrNoJob when
	// nothing is ready.
	Lease(ctx context.Context, queue, leaseToken string, now time.Time, leaseFor time.Duration) (*Job, error)

	// Heartbeat extends the lease of a leased job to expiresAt. Returns
	// ErrLeaseLost if the job is not leased with the given token.
	Heartbeat(ctx context.Context, id, leaseToken string, expiresAt time.Time) error

	// Succeed marks a leased job as succeeded. Returns ErrLeaseLost if the
	// token does not match.
	Succeed(ctx context.Context, id, leaseToken string, now time.Time) error

	// Fail records a failed attempt and schedules a retry at retryAt
	// (status=failed). Returns ErrLeaseLost if the token does not match.
	Fail(ctx context.Context, id, leaseToken, lastError string, retryAt, now time.Time) error

	// Dead moves a leased job to the dead-letter state (status=dead).
	// Returns ErrLeaseLost if the token does not match.
	Dead(ctx context.Context, id, leaseToken, lastError string, now time.Time) error

	// ReapExpired returns every leased job whose lease_expires_at <= now back
	// to pending (or dead, if attempts >= max_attempts) and reports how many
	// jobs were touched.
	ReapExpired(ctx context.Context, now time.Time) (int, error)

	// Retry redrives a dead, failed or cancelled job: status=pending,
	// attempts=0, run_at=now, last_error cleared. Other statuses return
	// ErrInvalidTransition.
	Retry(ctx context.Context, id string, now time.Time) (*Job, error)

	// Cancel moves a pending or failed job to cancelled. Leased and terminal
	// jobs return ErrInvalidTransition.
	Cancel(ctx context.Context, id string, now time.Time) (*Job, error)

	// Stats returns per-queue job counts by status.
	Stats(ctx context.Context) (QueueStats, error)

	// Close releases resources held by the store.
	Close() error
}
