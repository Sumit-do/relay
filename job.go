// Package relay is a durable background job queue with a worker pool,
// retries with exponential backoff, dead-lettering and a REST API.
//
// The package defines the Job model and the Store interface that persistence
// backends implement. Workers live in package worker, HTTP handlers in
// internal/api, and backends in store/memory and store/sqlite.
package relay

import (
	"encoding/json"
	"errors"
	"time"
)

// Status is the lifecycle state of a Job.
type Status string

// Job statuses. See the state machine in the README.
const (
	StatusPending   Status = "pending"   // waiting to run (run_at may be in the future)
	StatusLeased    Status = "leased"    // claimed by a worker, lease_expires_at set
	StatusSucceeded Status = "succeeded" // handler returned nil
	StatusFailed    Status = "failed"    // attempt failed; scheduled for retry at run_at
	StatusDead      Status = "dead"      // exhausted max_attempts; needs manual redrive
	StatusCancelled Status = "cancelled" // cancelled via the API before it ran
)

// AllStatuses lists every status in display order.
var AllStatuses = []Status{
	StatusPending, StatusLeased, StatusSucceeded, StatusFailed, StatusDead, StatusCancelled,
}

// Valid reports whether s is a known status.
func (s Status) Valid() bool {
	for _, k := range AllStatuses {
		if k == s {
			return true
		}
	}
	return false
}

// Terminal reports whether no further automatic transitions happen from s.
func (s Status) Terminal() bool {
	return s == StatusSucceeded || s == StatusDead || s == StatusCancelled
}

// Job is a unit of background work.
type Job struct {
	ID             string          `json:"id"`
	Queue          string          `json:"queue"`
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload,omitempty"`
	Status         Status          `json:"status"`
	Priority       int             `json:"priority"`
	Attempts       int             `json:"attempts"`
	MaxAttempts    int             `json:"max_attempts"`
	RunAt          time.Time       `json:"run_at"`
	LeaseExpiresAt *time.Time      `json:"lease_expires_at,omitempty"`
	LeaseToken     string          `json:"lease_token,omitempty"`
	LastError      string          `json:"last_error,omitempty"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
	CompletedAt    *time.Time      `json:"completed_at,omitempty"`
}

// Clone returns a deep copy of j.
func (j *Job) Clone() *Job {
	if j == nil {
		return nil
	}
	c := *j
	if j.Payload != nil {
		c.Payload = append(json.RawMessage(nil), j.Payload...)
	}
	if j.LeaseExpiresAt != nil {
		t := *j.LeaseExpiresAt
		c.LeaseExpiresAt = &t
	}
	if j.CompletedAt != nil {
		t := *j.CompletedAt
		c.CompletedAt = &t
	}
	return &c
}

// Sentinel errors returned by Store implementations.
var (
	// ErrNotFound is returned when a job id does not exist.
	ErrNotFound = errors.New("relay: job not found")
	// ErrNoJob is returned by Lease when no job is ready in the queue.
	ErrNoJob = errors.New("relay: no job available")
	// ErrLeaseLost is returned when a worker acts on a job whose lease it no
	// longer holds (expired and reaped, or re-leased by another worker).
	ErrLeaseLost = errors.New("relay: lease lost")
	// ErrInvalidTransition is returned when an operation is not allowed from
	// the job's current status (e.g. cancelling a succeeded job).
	ErrInvalidTransition = errors.New("relay: invalid status transition")
	// ErrInvalidJob is returned when an enqueue request fails validation.
	ErrInvalidJob = errors.New("relay: invalid job")
)
