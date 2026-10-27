// Package api exposes relay over HTTP/JSON.
package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"runtime/debug"
	"sort"
	"strconv"
	"time"

	"github.com/sumit-do/relay"
	"github.com/sumit-do/relay/clock"
	"github.com/sumit-do/relay/metrics"
)

// MaxListLimit caps the ?limit= query parameter.
const MaxListLimit = 1000

// Config wires a Server.
type Config struct {
	Client   *relay.Client     // required
	Registry *metrics.Registry // optional; enables GET /metrics
	Logger   *slog.Logger      // defaults to slog.Default()
	Clock    clock.Clock       // defaults to the wall clock
}

// Server is the HTTP API.
type Server struct {
	client   *relay.Client
	store    relay.Store
	registry *metrics.Registry
	log      *slog.Logger
	clock    clock.Clock
	mux      *http.ServeMux
}

// New builds a Server and its routes.
func New(cfg Config) *Server {
	if cfg.Client == nil {
		panic("api: Config.Client is required")
	}
	s := &Server{
		client:   cfg.Client,
		store:    cfg.Client.Store(),
		registry: cfg.Registry,
		log:      cfg.Logger,
		clock:    cfg.Clock,
		mux:      http.NewServeMux(),
	}
	if s.log == nil {
		s.log = slog.Default()
	}
	if s.clock == nil {
		s.clock = clock.Real{}
	}
	s.routes()
	return s
}

func (s *Server) routes() {
	s.mux.HandleFunc("POST /jobs", s.handleEnqueue)
	s.mux.HandleFunc("GET /jobs", s.handleList)
	s.mux.HandleFunc("GET /jobs/{id}", s.handleGet)
	s.mux.HandleFunc("POST /jobs/{id}/retry", s.handleRetry)
	s.mux.HandleFunc("POST /jobs/{id}/cancel", s.handleCancel)
	s.mux.HandleFunc("GET /queues", s.handleQueues)
	s.mux.HandleFunc("GET /healthz", s.handleHealth)
	if s.registry != nil {
		s.mux.Handle("GET /metrics", s.registry.Handler())
	}
}

// Handler returns the root handler with logging and panic recovery applied.
func (s *Server) Handler() http.Handler {
	return s.recoverer(s.logger(s.mux))
}

// ServeHTTP implements http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.Handler().ServeHTTP(w, r) }

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func (s *Server) logger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		s.log.Info("http request",
			"method", r.Method, "path", r.URL.Path, "status", rec.status,
			"duration", time.Since(start), "remote", r.RemoteAddr)
	})
}

func (s *Server) recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				s.log.Error("http handler panic", "panic", rec, "stack", string(debug.Stack()))
				writeError(w, http.StatusInternalServerError, "internal server error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

type errorResponse struct {
	Error string `json:"error"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, errorResponse{Error: msg})
}

// writeStoreError maps store errors to HTTP statuses.
func (s *Server) writeStoreError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, relay.ErrNotFound):
		writeError(w, http.StatusNotFound, "job not found")
	case errors.Is(err, relay.ErrInvalidTransition):
		writeError(w, http.StatusConflict, err.Error())
	case errors.Is(err, relay.ErrInvalidJob):
		writeError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		writeError(w, http.StatusServiceUnavailable, "request cancelled")
	default:
		s.log.Error("store error", "err", err)
		writeError(w, http.StatusInternalServerError, "internal server error")
	}
}

func (s *Server) handleEnqueue(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, relay.MaxPayloadBytes+64*1024))
	if err != nil {
		writeError(w, http.StatusRequestEntityTooLarge, "request body too large")
		return
	}
	var req relay.EnqueueRequest
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	job, created, err := s.client.Enqueue(r.Context(), req)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	w.Header().Set("Location", "/jobs/"+job.ID)
	writeJSON(w, status, job)
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	job, err := s.store.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, job)
}

type listResponse struct {
	Jobs []*relay.Job `json:"jobs"`
}

func (s *Server) handleList(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f := relay.ListFilter{Queue: q.Get("queue"), Type: q.Get("type")}
	if st := q.Get("status"); st != "" {
		f.Status = relay.Status(st)
		if !f.Status.Valid() {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("unknown status %q", st))
			return
		}
	}
	if l := q.Get("limit"); l != "" {
		n, err := strconv.Atoi(l)
		if err != nil || n < 1 || n > MaxListLimit {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("limit must be an integer between 1 and %d", MaxListLimit))
			return
		}
		f.Limit = n
	}
	jobs, err := s.store.List(r.Context(), f)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if jobs == nil {
		jobs = []*relay.Job{}
	}
	writeJSON(w, http.StatusOK, listResponse{Jobs: jobs})
}

func (s *Server) handleRetry(w http.ResponseWriter, r *http.Request) {
	job, err := s.store.Retry(r.Context(), r.PathValue("id"), s.clock.Now().UTC())
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func (s *Server) handleCancel(w http.ResponseWriter, r *http.Request) {
	job, err := s.store.Cancel(r.Context(), r.PathValue("id"), s.clock.Now().UTC())
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, job)
}

// QueueSummary is one entry of GET /queues.
type QueueSummary struct {
	Name   string               `json:"name"`
	Counts map[relay.Status]int `json:"counts"`
	Total  int                  `json:"total"`
}

type queuesResponse struct {
	Queues []QueueSummary `json:"queues"`
}

func (s *Server) handleQueues(w http.ResponseWriter, r *http.Request) {
	stats, err := s.store.Stats(r.Context())
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	out := make([]QueueSummary, 0, len(stats))
	for name, counts := range stats {
		qs := QueueSummary{Name: name, Counts: make(map[relay.Status]int, len(relay.AllStatuses))}
		for _, st := range relay.AllStatuses {
			qs.Counts[st] = counts[st]
			qs.Total += counts[st]
		}
		out = append(out, qs)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	writeJSON(w, http.StatusOK, queuesResponse{Queues: out})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if _, err := s.store.Stats(ctx); err != nil {
		s.log.Error("health check failed", "err", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unavailable", "error": "store unreachable"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}
