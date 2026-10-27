package metrics

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCounterRender(t *testing.T) {
	r := NewRegistry()
	c := r.Counter("jobs_total", "Jobs.", "queue")
	c.Inc("default")
	c.Add(2, "default")
	c.Inc("mail")
	if got := c.Value("default"); got != 3 {
		t.Fatalf("Value = %v, want 3", got)
	}
	// Re-registering returns the same counter.
	if r.Counter("jobs_total", "x", "queue") != c {
		t.Fatal("expected same counter instance")
	}

	var sb strings.Builder
	if _, err := r.WriteTo(&sb); err != nil {
		t.Fatal(err)
	}
	want := "# HELP jobs_total Jobs.\n# TYPE jobs_total counter\njobs_total{queue=\"default\"} 3\njobs_total{queue=\"mail\"} 1\n"
	if sb.String() != want {
		t.Fatalf("render mismatch:\n%s\nwant:\n%s", sb.String(), want)
	}
}

func TestHistogramRender(t *testing.T) {
	r := NewRegistry()
	h := r.Histogram("dur_seconds", "Duration.", []float64{1, 0.1}, "queue")
	h.Observe(0.05, "q")
	h.Observe(0.5, "q")
	h.Observe(5, "q")
	if h.Count("q") != 3 {
		t.Fatalf("Count = %d", h.Count("q"))
	}
	var sb strings.Builder
	_, _ = r.WriteTo(&sb)
	out := sb.String()
	for _, line := range []string{
		"# TYPE dur_seconds histogram",
		`dur_seconds_bucket{queue="q",le="0.1"} 1`,
		`dur_seconds_bucket{queue="q",le="1"} 2`,
		`dur_seconds_bucket{queue="q",le="+Inf"} 3`,
		`dur_seconds_sum{queue="q"} 5.55`,
		`dur_seconds_count{queue="q"} 3`,
	} {
		if !strings.Contains(out, line) {
			t.Errorf("missing %q in:\n%s", line, out)
		}
	}
}

func TestUnlabelledAndEscaping(t *testing.T) {
	r := NewRegistry()
	r.Counter("plain", "no labels").Inc()
	r.Counter("esc", "l", "t").Inc(`a"b\c`)
	var sb strings.Builder
	_, _ = r.WriteTo(&sb)
	if !strings.Contains(sb.String(), "plain 1\n") {
		t.Errorf("unlabelled counter not rendered:\n%s", sb.String())
	}
	if !strings.Contains(sb.String(), `esc{t="a\"b\\c"} 1`) {
		t.Errorf("label not escaped:\n%s", sb.String())
	}
}

func TestLabelArityPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on wrong label count")
		}
	}()
	NewRegistry().Counter("c", "h", "a").Inc()
}

func TestHandler(t *testing.T) {
	r := NewRegistry()
	r.Counter("up", "h").Inc()
	rec := httptest.NewRecorder()
	r.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Fatalf("content type = %q", ct)
	}
	if !strings.Contains(rec.Body.String(), "up 1") {
		t.Fatalf("body = %q", rec.Body.String())
	}
}
