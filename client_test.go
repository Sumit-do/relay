package relay_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/sumit-do/relay"
	"github.com/sumit-do/relay/clock"
	"github.com/sumit-do/relay/internal/id"
	"github.com/sumit-do/relay/metrics"
	"github.com/sumit-do/relay/store/memory"
)

var t0 = time.Date(2026, 5, 5, 5, 5, 5, 0, time.UTC)

func newClient(t *testing.T) (*relay.Client, *clock.Fake, *relay.Metrics) {
	t.Helper()
	clk := clock.NewFake(t0)
	m := relay.NewMetrics(metrics.NewRegistry())
	return relay.NewClient(memory.New(), relay.WithClock(clk), relay.WithMetrics(m)), clk, m
}

func TestEnqueueDefaults(t *testing.T) {
	c, _, m := newClient(t)
	j, created, err := c.Enqueue(context.Background(), relay.EnqueueRequest{Type: "send_email", Payload: json.RawMessage(`{"to":"x"}`)})
	if err != nil || !created {
		t.Fatalf("Enqueue = %v, created=%v", err, created)
	}
	if !id.Valid(j.ID) {
		t.Fatalf("id %q is not a valid ULID", j.ID)
	}
	ts, _ := id.Time(j.ID)
	if !ts.Equal(t0) {
		t.Fatalf("id timestamp = %v, want %v", ts, t0)
	}
	if j.Queue != relay.DefaultQueue || j.MaxAttempts != relay.DefaultMaxAttempts || j.Status != relay.StatusPending {
		t.Fatalf("defaults: %+v", j)
	}
	if !j.RunAt.Equal(t0) || !j.CreatedAt.Equal(t0) {
		t.Fatalf("timestamps: run_at=%v created=%v", j.RunAt, j.CreatedAt)
	}
	if m.Enqueued.Value(relay.DefaultQueue) != 1 {
		t.Fatal("enqueued counter not incremented")
	}
	// Stored copy must be independent of the request payload buffer.
	got, _ := c.Store().Get(context.Background(), j.ID)
	if string(got.Payload) != `{"to":"x"}` {
		t.Fatalf("payload = %s", got.Payload)
	}
}

func TestEnqueueDelayAndRunAt(t *testing.T) {
	c, _, _ := newClient(t)
	j, _, err := c.Enqueue(context.Background(), relay.EnqueueRequest{Type: "t", Delay: "90s"})
	if err != nil || !j.RunAt.Equal(t0.Add(90*time.Second)) {
		t.Fatalf("delay: %v %v", err, j)
	}
	at := time.Date(2030, 1, 1, 0, 0, 0, 0, time.FixedZone("X", 3600))
	j, _, err = c.Enqueue(context.Background(), relay.EnqueueRequest{Type: "t", RunAt: &at})
	if err != nil || !j.RunAt.Equal(at) || j.RunAt.Location() != time.UTC {
		t.Fatalf("run_at: %v %v", err, j.RunAt)
	}
}

func TestEnqueueIdempotent(t *testing.T) {
	c, clk, m := newClient(t)
	ctx := context.Background()
	a, created, err := c.Enqueue(ctx, relay.EnqueueRequest{Type: "t", IdempotencyKey: "k1", Priority: 1})
	if err != nil || !created {
		t.Fatal(err)
	}
	clk.Advance(time.Minute)
	b, created, err := c.Enqueue(ctx, relay.EnqueueRequest{Type: "t", IdempotencyKey: "k1", Priority: 9})
	if err != nil {
		t.Fatal(err)
	}
	if created || b.ID != a.ID || b.Priority != 1 {
		t.Fatalf("expected existing job back: created=%v %+v", created, b)
	}
	if m.Enqueued.Value(relay.DefaultQueue) != 1 {
		t.Fatalf("enqueued = %v, want 1 (duplicates do not count)", m.Enqueued.Value(relay.DefaultQueue))
	}
}

func TestEnqueueValidation(t *testing.T) {
	c, _, _ := newClient(t)
	c.DefaultMaxAttempts = 3
	cases := []relay.EnqueueRequest{
		{},
		{Type: " "},
		{Type: "has space"},
		{Type: "ok", Queue: "bad queue"},
		{Type: "ok", MaxAttempts: -1},
		{Type: "ok", MaxAttempts: 1001},
		{Type: "ok", Delay: "nope"},
		{Type: "ok", Delay: "-5s"},
		{Type: "ok", Delay: "1s", RunAt: &t0},
		{Type: "ok", Payload: json.RawMessage(`{bad`)},
		{Type: "ok", IdempotencyKey: string(make([]byte, 256))},
		{Type: "ok", Payload: json.RawMessage(make([]byte, relay.MaxPayloadBytes+1))},
		{Type: "ünïcode"},
	}
	for i, req := range cases {
		if _, _, err := c.Enqueue(context.Background(), req); !errors.Is(err, relay.ErrInvalidJob) {
			t.Errorf("case %d (%+v): err = %v, want ErrInvalidJob", i, req, err)
		}
	}
	j, _, err := c.Enqueue(context.Background(), relay.EnqueueRequest{Type: "ns:resize-image.v2/large", Queue: "img_01"})
	if err != nil || j.MaxAttempts != 3 {
		t.Fatalf("valid names rejected or default not applied: %v %+v", err, j)
	}
}

func TestStatusHelpers(t *testing.T) {
	for _, s := range relay.AllStatuses {
		if !s.Valid() {
			t.Errorf("%s should be valid", s)
		}
	}
	if relay.Status("nope").Valid() {
		t.Error("unknown status valid")
	}
	if !relay.StatusDead.Terminal() || relay.StatusFailed.Terminal() || relay.StatusLeased.Terminal() {
		t.Error("Terminal misclassifies statuses")
	}
}

func TestJobClone(t *testing.T) {
	exp := t0
	j := &relay.Job{ID: "x", Payload: json.RawMessage(`{"a":1}`), LeaseExpiresAt: &exp}
	c := j.Clone()
	c.Payload[2] = 'b'
	*c.LeaseExpiresAt = t0.Add(time.Hour)
	if string(j.Payload) != `{"a":1}` || !j.LeaseExpiresAt.Equal(t0) {
		t.Fatal("Clone shares memory with the original")
	}
	var nilJob *relay.Job
	if nilJob.Clone() != nil {
		t.Fatal("Clone of nil should be nil")
	}
}
