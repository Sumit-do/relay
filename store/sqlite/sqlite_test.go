package sqlite

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/sumit-do/relay"
	"github.com/sumit-do/relay/storetest"
)

func open(t *testing.T) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "relay.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return s
}

func TestContract(t *testing.T) {
	storetest.Run(t, func(t *testing.T) relay.Store { return open(t) })
}

func TestInMemory(t *testing.T) {
	storetest.Run(t, func(t *testing.T) relay.Store {
		s, err := Open(":memory:")
		if err != nil {
			t.Fatal(err)
		}
		return s
	})
}

// TestDurability verifies that jobs survive closing and reopening the file.
func TestDurability(t *testing.T) {
	path := filepath.Join(t.TempDir(), "durable.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	job := &relay.Job{
		ID: "01ARZ3NDEKTSV4RRFFQ69G5FAV", Queue: "q", Type: "t", Status: relay.StatusPending,
		Payload: json.RawMessage(`{"k":"v"}`), MaxAttempts: 3, RunAt: now, CreatedAt: now, UpdatedAt: now,
	}
	if _, _, err := s.Enqueue(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Lease(context.Background(), "q", "w", now, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	got, err := s2.Get(context.Background(), job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != relay.StatusLeased || got.Attempts != 1 || string(got.Payload) != `{"k":"v"}` {
		t.Fatalf("job after reopen: %+v", got)
	}
	// Migration is idempotent: a second Open on the same file must succeed and
	// a lease that expired while "down" is reaped as usual.
	n, err := s2.ReapExpired(context.Background(), now.Add(time.Hour))
	if err != nil || n != 1 {
		t.Fatalf("reap after reopen = %d, %v", n, err)
	}
}
