// Package metrics is a tiny, dependency-free implementation of labelled
// counters and histograms rendered in the Prometheus text exposition format.
package metrics

import (
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// DefaultBuckets mirror the Prometheus client defaults (seconds).
var DefaultBuckets = []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10}

const labelSep = "\x00"

// Registry owns a set of metrics and renders them.
type Registry struct {
	mu         sync.Mutex
	counters   map[string]*Counter
	histograms map[string]*Histogram
	order      []string
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{
		counters:   make(map[string]*Counter),
		histograms: make(map[string]*Histogram),
	}
}

// Counter returns the counter with the given name, creating it on first use.
// Calling it again with the same name returns the same counter; the help and
// label names of the first call win.
func (r *Registry) Counter(name, help string, labelNames ...string) *Counter {
	r.mu.Lock()
	defer r.mu.Unlock()
	if c, ok := r.counters[name]; ok {
		return c
	}
	c := &Counter{name: name, help: help, labelNames: labelNames, series: make(map[string]*float64)}
	r.counters[name] = c
	r.order = append(r.order, name)
	return c
}

// Histogram returns the histogram with the given name, creating it on first
// use. A nil buckets slice selects DefaultBuckets.
func (r *Registry) Histogram(name, help string, buckets []float64, labelNames ...string) *Histogram {
	r.mu.Lock()
	defer r.mu.Unlock()
	if h, ok := r.histograms[name]; ok {
		return h
	}
	if buckets == nil {
		buckets = DefaultBuckets
	}
	b := append([]float64(nil), buckets...)
	sort.Float64s(b)
	h := &Histogram{name: name, help: help, labelNames: labelNames, buckets: b, series: make(map[string]*histSeries)}
	r.histograms[name] = h
	r.order = append(r.order, name)
	return h
}

// Handler serves the registry in Prometheus text format.
func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = r.WriteTo(w)
	})
}

// WriteTo renders every metric in registration order.
func (r *Registry) WriteTo(w io.Writer) (int64, error) {
	r.mu.Lock()
	order := append([]string(nil), r.order...)
	r.mu.Unlock()

	var sb strings.Builder
	for _, name := range order {
		r.mu.Lock()
		c, isCounter := r.counters[name]
		h, isHist := r.histograms[name]
		r.mu.Unlock()
		switch {
		case isCounter:
			c.render(&sb)
		case isHist:
			h.render(&sb)
		}
	}
	n, err := io.WriteString(w, sb.String())
	return int64(n), err
}

// Counter is a monotonically increasing labelled value.
type Counter struct {
	name, help string
	labelNames []string
	mu         sync.Mutex
	series     map[string]*float64
}

// Inc adds one to the series identified by labelValues.
func (c *Counter) Inc(labelValues ...string) { c.Add(1, labelValues...) }

// Add adds delta (which must be >= 0) to the series identified by labelValues.
func (c *Counter) Add(delta float64, labelValues ...string) {
	if delta < 0 {
		panic("metrics: counter cannot decrease")
	}
	key := seriesKey(c.labelNames, labelValues)
	c.mu.Lock()
	defer c.mu.Unlock()
	v, ok := c.series[key]
	if !ok {
		v = new(float64)
		c.series[key] = v
	}
	*v += delta
}

// Value returns the current value of a series (0 if never touched).
func (c *Counter) Value(labelValues ...string) float64 {
	key := seriesKey(c.labelNames, labelValues)
	c.mu.Lock()
	defer c.mu.Unlock()
	if v, ok := c.series[key]; ok {
		return *v
	}
	return 0
}

func (c *Counter) render(sb *strings.Builder) {
	fmt.Fprintf(sb, "# HELP %s %s\n# TYPE %s counter\n", c.name, escapeHelp(c.help), c.name)
	c.mu.Lock()
	keys := sortedKeys(c.series)
	for _, k := range keys {
		fmt.Fprintf(sb, "%s%s %s\n", c.name, formatLabels(c.labelNames, k, "", ""), formatFloat(*c.series[k]))
	}
	c.mu.Unlock()
}

// Histogram records observations into cumulative buckets.
type Histogram struct {
	name, help string
	labelNames []string
	buckets    []float64
	mu         sync.Mutex
	series     map[string]*histSeries
}

type histSeries struct {
	counts []uint64 // per bucket, non-cumulative
	sum    float64
	count  uint64
}

// Observe records v in the series identified by labelValues.
func (h *Histogram) Observe(v float64, labelValues ...string) {
	key := seriesKey(h.labelNames, labelValues)
	h.mu.Lock()
	defer h.mu.Unlock()
	s, ok := h.series[key]
	if !ok {
		s = &histSeries{counts: make([]uint64, len(h.buckets))}
		h.series[key] = s
	}
	for i, b := range h.buckets {
		if v <= b {
			s.counts[i]++
			break
		}
	}
	s.sum += v
	s.count++
}

// Count returns the number of observations in a series.
func (h *Histogram) Count(labelValues ...string) uint64 {
	key := seriesKey(h.labelNames, labelValues)
	h.mu.Lock()
	defer h.mu.Unlock()
	if s, ok := h.series[key]; ok {
		return s.count
	}
	return 0
}

func (h *Histogram) render(sb *strings.Builder) {
	fmt.Fprintf(sb, "# HELP %s %s\n# TYPE %s histogram\n", h.name, escapeHelp(h.help), h.name)
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, k := range sortedKeys(h.series) {
		s := h.series[k]
		var cum uint64
		for i, b := range h.buckets {
			cum += s.counts[i]
			fmt.Fprintf(sb, "%s_bucket%s %d\n", h.name, formatLabels(h.labelNames, k, "le", formatFloat(b)), cum)
		}
		fmt.Fprintf(sb, "%s_bucket%s %d\n", h.name, formatLabels(h.labelNames, k, "le", "+Inf"), s.count)
		fmt.Fprintf(sb, "%s_sum%s %s\n", h.name, formatLabels(h.labelNames, k, "", ""), formatFloat(s.sum))
		fmt.Fprintf(sb, "%s_count%s %d\n", h.name, formatLabels(h.labelNames, k, "", ""), s.count)
	}
}

func seriesKey(names, values []string) string {
	if len(names) != len(values) {
		panic(fmt.Sprintf("metrics: expected %d label values, got %d", len(names), len(values)))
	}
	return strings.Join(values, labelSep)
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// formatLabels renders {a="1",b="2"} for a series key, optionally appending
// one extra label (used for histogram "le").
func formatLabels(names []string, key, extraName, extraValue string) string {
	var values []string
	if len(names) > 0 {
		values = strings.Split(key, labelSep)
	}
	parts := make([]string, 0, len(names)+1)
	for i, n := range names {
		parts = append(parts, n+`="`+escapeLabel(values[i])+`"`)
	}
	if extraName != "" {
		parts = append(parts, extraName+`="`+extraValue+`"`)
	}
	if len(parts) == 0 {
		return ""
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func formatFloat(f float64) string {
	switch {
	case math.IsInf(f, 1):
		return "+Inf"
	case math.IsInf(f, -1):
		return "-Inf"
	case math.IsNaN(f):
		return "NaN"
	}
	return strconv.FormatFloat(f, 'g', -1, 64)
}

func escapeLabel(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, "\n", `\n`)
	return strings.ReplaceAll(s, `"`, `\"`)
}

func escapeHelp(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	return strings.ReplaceAll(s, "\n", `\n`)
}
