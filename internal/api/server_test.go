package api

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/sumit-do/relay"
	"github.com/sumit-do/relay/clock"
	"github.com/sumit-do/relay/metrics"
	"github.com/sumit-do/relay/store/memory"
)

var t0 = time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)

type env struct {
	t     *testing.T
	store *memory.Store
	clk   *clock.Fake
	srv   *httptest.Server
	reg   *metrics.Registry
}

func newEnv(t *testing.T) *env {
	t.Helper()
	e := &env{t: t, store: memory.New(), clk: clock.NewFake(t0), reg: metrics.NewRegistry()}
	m := relay.NewMetrics(e.reg)
	client := relay.NewClient(e.store, relay.WithClock(e.clk), relay.WithMetrics(m))
	s := New(Config{
		Client:   client,
		Registry: e.reg,
		Clock:    e.clk,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	e.srv = httptest.NewServer(s.Handler())
	t.Cleanup(e.srv.Close)
	return e
}

func (e *env) do(method, path, body string) (*http.Response, []byte) {
	e.t.Helper()
	var rdr io.Reader
	if body != "" {
		rdr = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, e.srv.URL+path, rdr)
	if err != nil {
		e.t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		e.t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp, b
}

func (e *env) decodeJob(b []byte) relay.Job {
	e.t.Helper()
	var j relay.Job
	if err := json.Unmarshal(b, &j); err != nil {
		e.t.Fatalf("decode job: %v (%s)", err, b)
	}
	return j
}

func (e *env) enqueue(body string) relay.Job {
	e.t.Helper()
	resp, b := e.do("POST", "/jobs", body)
	if resp.StatusCode != http.StatusCreated {
		e.t.Fatalf("enqueue status = %d: %s", resp.StatusCode, b)
	}
	return e.decodeJob(b)
}

func TestEnqueueAndGet(t *testing.T) {
	e := newEnv(t)
	resp, b := e.do("POST", "/jobs", `{"queue":"mail","type":"send_email","payload":{"to":"a@b.c"},"priority":3,"max_attempts":4,"delay":"5m"}`)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d: %s", resp.StatusCode, b)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Fatalf("content-type = %q", ct)
	}
	j := e.decodeJob(b)
	if resp.Header.Get("Location") != "/jobs/"+j.ID {
		t.Fatalf("Location = %q", resp.Header.Get("Location"))
	}
	if j.Queue != "mail" || j.Type != "send_email" || j.Priority != 3 || j.MaxAttempts != 4 || j.Status != relay.StatusPending {
		t.Fatalf("job = %+v", j)
	}
	if !j.RunAt.Equal(t0.Add(5 * time.Minute)) {
		t.Fatalf("run_at = %v, want +5m", j.RunAt)
	}
	if string(j.Payload) != `{"to":"a@b.c"}` {
		t.Fatalf("payload = %s", j.Payload)
	}

	resp, b = e.do("GET", "/jobs/"+j.ID, "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("get status = %d", resp.StatusCode)
	}
	if got := e.decodeJob(b); got.ID != j.ID {
		t.Fatalf("get returned %+v", got)
	}
	if v := e.reg.Counter("jobs_enqueued_total", "", "queue").Value("mail"); v != 1 {
		t.Fatalf("jobs_enqueued_total = %v", v)
	}
}

func TestEnqueueDefaultsAndValidation(t *testing.T) {
	e := newEnv(t)
	j := e.enqueue(`{"type":"t"}`)
	if j.Queue != relay.DefaultQueue || j.MaxAttempts != relay.DefaultMaxAttempts || !j.RunAt.Equal(t0) {
		t.Fatalf("defaults: %+v", j)
	}
	cases := map[string]string{
		"missing type":      `{"queue":"q"}`,
		"bad json":          `{"type":`,
		"unknown field":     `{"type":"t","bogus":1}`,
		"bad delay":         `{"type":"t","delay":"soon"}`,
		"negative delay":    `{"type":"t","delay":"-1s"}`,
		"delay and run_at":  `{"type":"t","delay":"1s","run_at":"2026-01-01T00:00:00Z"}`,
		"bad queue name":    `{"type":"t","queue":"has space"}`,
		"bad max_attempts":  `{"type":"t","max_attempts":-1}`,
		"non-object body":   `[1,2]`,
		"invalid payload":   `{"type":"t","payload":"not closed`,
		"type with spaces":  `{"type":"send email"}`,
		"huge max_attempts": `{"type":"t","max_attempts":5000}`,
	}
	for name, body := range cases {
		resp, b := e.do("POST", "/jobs", body)
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("%s: status = %d, want 400 (%s)", name, resp.StatusCode, b)
		}
		var er errorResponse
		if json.Unmarshal(b, &er) != nil || er.Error == "" {
			t.Errorf("%s: error body = %s", name, b)
		}
	}
	resp, _ := e.do("POST", "/jobs", `{"type":"t","payload":"`+strings.Repeat("x", relay.MaxPayloadBytes+100)+`"}`)
	if resp.StatusCode != http.StatusBadRequest && resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversize payload status = %d", resp.StatusCode)
	}
}

func TestIdempotentEnqueue(t *testing.T) {
	e := newEnv(t)
	first := e.enqueue(`{"type":"charge","idempotency_key":"order-1","payload":{"amount":10}}`)
	resp, b := e.do("POST", "/jobs", `{"type":"charge","idempotency_key":"order-1","payload":{"amount":999}}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("duplicate status = %d, want 200", resp.StatusCode)
	}
	dup := e.decodeJob(b)
	if dup.ID != first.ID || string(dup.Payload) != `{"amount":10}` {
		t.Fatalf("duplicate returned %+v", dup)
	}
	if v := e.reg.Counter("jobs_enqueued_total", "", "queue").Value("default"); v != 1 {
		t.Fatalf("jobs_enqueued_total = %v, want 1", v)
	}
	_, b = e.do("GET", "/jobs", "")
	var lr listResponse
	_ = json.Unmarshal(b, &lr)
	if len(lr.Jobs) != 1 {
		t.Fatalf("expected 1 job, got %d", len(lr.Jobs))
	}
}

func TestGetNotFoundAndMethodNotAllowed(t *testing.T) {
	e := newEnv(t)
	resp, b := e.do("GET", "/jobs/01ARZ3NDEKTSV4RRFFQ69G5FAV", "")
	if resp.StatusCode != http.StatusNotFound || !strings.Contains(string(b), "not found") {
		t.Fatalf("status = %d body = %s", resp.StatusCode, b)
	}
	resp, _ = e.do("DELETE", "/jobs/x", "")
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("DELETE status = %d", resp.StatusCode)
	}
	resp, _ = e.do("GET", "/nope", "")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown route status = %d", resp.StatusCode)
	}
}

func TestListFilters(t *testing.T) {
	e := newEnv(t)
	for i := 0; i < 3; i++ {
		e.enqueue(`{"type":"a","queue":"q1"}`)
	}
	e.enqueue(`{"type":"b","queue":"q2"}`)
	leased, err := e.store.Lease(context.Background(), "q1", "w", t0, time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	list := func(query string) []*relay.Job {
		t.Helper()
		resp, b := e.do("GET", "/jobs"+query, "")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s: status = %d: %s", query, resp.StatusCode, b)
		}
		var lr listResponse
		if err := json.Unmarshal(b, &lr); err != nil {
			t.Fatal(err)
		}
		return lr.Jobs
	}
	if got := list(""); len(got) != 4 {
		t.Fatalf("all = %d", len(got))
	}
	if got := list("?queue=q1"); len(got) != 3 {
		t.Fatalf("q1 = %d", len(got))
	}
	if got := list("?queue=q1&status=pending"); len(got) != 2 {
		t.Fatalf("q1 pending = %d", len(got))
	}
	if got := list("?status=leased"); len(got) != 1 || got[0].ID != leased.ID {
		t.Fatalf("leased = %v", got)
	}
	if got := list("?type=b"); len(got) != 1 {
		t.Fatalf("type b = %d", len(got))
	}
	if got := list("?limit=2"); len(got) != 2 {
		t.Fatalf("limit 2 = %d", len(got))
	}
	if got := list("?queue=empty"); got == nil || len(got) != 0 {
		t.Fatalf("empty queue should return [] not null: %v", got)
	}
	for _, q := range []string{"?status=bogus", "?limit=0", "?limit=abc", "?limit=5000"} {
		if resp, _ := e.do("GET", "/jobs"+q, ""); resp.StatusCode != http.StatusBadRequest {
			t.Errorf("%s: status = %d, want 400", q, resp.StatusCode)
		}
	}
}

func TestRetryAndCancel(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	j := e.enqueue(`{"type":"t","max_attempts":1}`)
	leased, _ := e.store.Lease(ctx, "default", "w", t0, time.Minute)
	if err := e.store.Dead(ctx, leased.ID, "w", "boom", t0); err != nil {
		t.Fatal(err)
	}

	// Retry a dead job.
	e.clk.Advance(time.Hour)
	resp, b := e.do("POST", "/jobs/"+j.ID+"/retry", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("retry status = %d: %s", resp.StatusCode, b)
	}
	r := e.decodeJob(b)
	if r.Status != relay.StatusPending || r.Attempts != 0 || !r.RunAt.Equal(t0.Add(time.Hour)) || r.LastError != "" {
		t.Fatalf("retried job = %+v", r)
	}
	// Retrying a pending job is a conflict.
	if resp, _ := e.do("POST", "/jobs/"+j.ID+"/retry", ""); resp.StatusCode != http.StatusConflict {
		t.Fatalf("retry pending status = %d", resp.StatusCode)
	}

	// Cancel the pending job.
	resp, b = e.do("POST", "/jobs/"+j.ID+"/cancel", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("cancel status = %d: %s", resp.StatusCode, b)
	}
	if c := e.decodeJob(b); c.Status != relay.StatusCancelled {
		t.Fatalf("cancelled job = %+v", c)
	}
	if resp, _ := e.do("POST", "/jobs/"+j.ID+"/cancel", ""); resp.StatusCode != http.StatusConflict {
		t.Fatalf("double cancel status = %d", resp.StatusCode)
	}
	// Cancelling a leased job is a conflict; unknown ids are 404.
	k := e.enqueue(`{"type":"t"}`)
	if _, err := e.store.Lease(ctx, "default", "w", e.clk.Now(), time.Minute); err != nil {
		t.Fatal(err)
	}
	if resp, _ := e.do("POST", "/jobs/"+k.ID+"/cancel", ""); resp.StatusCode != http.StatusConflict {
		t.Fatalf("cancel leased status = %d", resp.StatusCode)
	}
	if resp, _ := e.do("POST", "/jobs/missing/retry", ""); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("retry missing status = %d", resp.StatusCode)
	}
	if resp, _ := e.do("POST", "/jobs/missing/cancel", ""); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("cancel missing status = %d", resp.StatusCode)
	}
}

func TestQueues(t *testing.T) {
	e := newEnv(t)
	e.enqueue(`{"type":"t","queue":"b"}`)
	e.enqueue(`{"type":"t","queue":"a"}`)
	e.enqueue(`{"type":"t","queue":"a"}`)
	if _, err := e.store.Lease(context.Background(), "a", "w", t0, time.Minute); err != nil {
		t.Fatal(err)
	}
	resp, b := e.do("GET", "/queues", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	var qr queuesResponse
	if err := json.Unmarshal(b, &qr); err != nil {
		t.Fatal(err)
	}
	if len(qr.Queues) != 2 || qr.Queues[0].Name != "a" || qr.Queues[1].Name != "b" {
		t.Fatalf("queues = %+v", qr.Queues)
	}
	a := qr.Queues[0]
	if a.Total != 2 || a.Counts[relay.StatusPending] != 1 || a.Counts[relay.StatusLeased] != 1 || a.Counts[relay.StatusDead] != 0 {
		t.Fatalf("queue a = %+v", a)
	}
	if _, ok := a.Counts[relay.StatusSucceeded]; !ok {
		t.Fatal("every status should be present in counts")
	}
}

func TestHealthAndMetrics(t *testing.T) {
	e := newEnv(t)
	resp, b := e.do("GET", "/healthz", "")
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(b), `"ok"`) {
		t.Fatalf("healthz = %d %s", resp.StatusCode, b)
	}
	e.enqueue(`{"type":"t"}`)
	resp, b = e.do("GET", "/metrics", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("metrics status = %d", resp.StatusCode)
	}
	if !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/plain") {
		t.Fatalf("metrics content-type = %q", resp.Header.Get("Content-Type"))
	}
	body := string(b)
	for _, want := range []string{
		"# TYPE jobs_enqueued_total counter",
		`jobs_enqueued_total{queue="default"} 1`,
		"# TYPE jobs_succeeded_total counter",
		"# TYPE jobs_failed_total counter",
		"# TYPE jobs_dead_total counter",
		"# TYPE job_duration_seconds histogram",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics missing %q:\n%s", want, body)
		}
	}
}

func TestMetricsDisabledWithoutRegistry(t *testing.T) {
	client := relay.NewClient(memory.New())
	s := New(Config{Client: client, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("metrics without registry = %d", rec.Code)
	}
}

type panickyStore struct{ relay.Store }

func (panickyStore) Get(context.Context, string) (*relay.Job, error) { panic("db exploded") }

func TestPanicRecovery(t *testing.T) {
	client := relay.NewClient(panickyStore{memory.New()})
	s := New(Config{Client: client, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, httptest.NewRequest("GET", "/jobs/abc", nil))
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "internal server error") {
		t.Fatalf("body = %s", rec.Body.String())
	}
}
