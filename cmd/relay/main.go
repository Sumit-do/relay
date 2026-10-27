// Command relay runs the relay HTTP API and worker pool.
//
//	relay serve --db relay.db --addr :8080 --concurrency 8
//
// The serve command ships with a few built-in demo handlers (noop, sleep,
// fail) so the queue can be exercised end to end. Real deployments embed the
// library and register their own handlers; see examples/worker.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/sumit-do/relay"
	"github.com/sumit-do/relay/internal/api"
	"github.com/sumit-do/relay/metrics"
	"github.com/sumit-do/relay/store/sqlite"
	"github.com/sumit-do/relay/worker"
)

// version is overridden at build time via -ldflags "-X main.version=...".
var version = "dev"

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "serve":
		err = serve(os.Args[2:])
	case "version":
		fmt.Println("relay", version)
	case "help", "-h", "--help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "relay: unknown command %q\n\n", os.Args[1])
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "relay:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `relay - durable background job queue

Usage:
  relay serve [flags]   run the HTTP API and worker pool
  relay version         print the version

Run "relay serve -h" for flags.
`)
}

func serve(args []string) error {
	fs := flag.NewFlagSet("relay serve", flag.ContinueOnError)
	var (
		dbPath      = fs.String("db", envOr("RELAY_DB", "relay.db"), "SQLite database path (\":memory:\" for ephemeral)")
		addr        = fs.String("addr", envOr("RELAY_ADDR", ":8080"), "HTTP listen address")
		concurrency = fs.Int("concurrency", envInt("RELAY_CONCURRENCY", 8), "default handlers per queue")
		queues      = fs.String("queues", envOr("RELAY_QUEUES", "default"), "comma-separated queues to work, optionally name=concurrency")
		lease       = fs.Duration("lease", envDuration("RELAY_LEASE", 30*time.Second), "lease (visibility timeout) per attempt")
		poll        = fs.Duration("poll", envDuration("RELAY_POLL", 500*time.Millisecond), "idle poll interval")
		drain       = fs.Duration("drain", envDuration("RELAY_DRAIN", 30*time.Second), "graceful shutdown timeout")
		backoffBase = fs.Duration("backoff-base", envDuration("RELAY_BACKOFF_BASE", time.Second), "first retry delay")
		backoffMax  = fs.Duration("backoff-max", envDuration("RELAY_BACKOFF_MAX", 10*time.Minute), "maximum retry delay")
		workers     = fs.Bool("workers", envBool("RELAY_WORKERS", true), "run the worker pool (false = API only)")
		logFormat   = fs.String("log-format", envOr("RELAY_LOG_FORMAT", "text"), "log format: text or json")
		logLevel    = fs.String("log-level", envOr("RELAY_LOG_LEVEL", "info"), "log level: debug, info, warn, error")
	)
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}

	log, err := newLogger(*logFormat, *logLevel)
	if err != nil {
		return err
	}
	slog.SetDefault(log)

	store, err := sqlite.Open(*dbPath)
	if err != nil {
		return err
	}
	defer store.Close()
	log.Info("store opened", "backend", "sqlite", "path", *dbPath)

	reg := metrics.NewRegistry()
	m := relay.NewMetrics(reg)
	client := relay.NewClient(store, relay.WithMetrics(m))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Worker pool.
	var pool *worker.Pool
	if *workers {
		pool = worker.New(store, worker.Config{
			Concurrency:   *concurrency,
			LeaseDuration: *lease,
			PollInterval:  *poll,
			DrainTimeout:  *drain,
			Backoff:       worker.Backoff{Base: *backoffBase, Max: *backoffMax, Factor: 2, Jitter: 0.2},
			Logger:        log.With("component", "worker"),
			Metrics:       m,
		})
		qcfgs, err := parseQueues(*queues, *concurrency)
		if err != nil {
			return err
		}
		for _, q := range qcfgs {
			pool.AddQueue(q)
		}
		registerDemoHandlers(pool)
		pool.Start()
	}

	// HTTP API.
	srv := &http.Server{
		Addr:              *addr,
		Handler:           api.New(api.Config{Client: client, Registry: reg, Logger: log.With("component", "api")}).Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	httpErr := make(chan error, 1)
	go func() {
		log.Info("http api listening", "addr", *addr, "version", version)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			httpErr <- err
		}
		close(httpErr)
	}()

	select {
	case <-ctx.Done():
		log.Info("shutdown signal received")
	case err := <-httpErr:
		if err != nil {
			return fmt.Errorf("http server: %w", err)
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), *drain)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("http shutdown", "err", err)
	}
	if pool != nil {
		if err := pool.Shutdown(shutdownCtx); err != nil {
			log.Error("worker shutdown", "err", err)
		}
	}
	log.Info("bye")
	return nil
}

// parseQueues parses "default,mail=4,images=2".
func parseQueues(spec string, defaultConcurrency int) ([]worker.QueueConfig, error) {
	var out []worker.QueueConfig
	for _, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		q := worker.QueueConfig{Name: part, Concurrency: defaultConcurrency}
		if name, c, ok := strings.Cut(part, "="); ok {
			n, err := strconv.Atoi(c)
			if err != nil || n < 1 {
				return nil, fmt.Errorf("invalid concurrency in --queues %q", part)
			}
			q.Name, q.Concurrency = name, n
		}
		out = append(out, q)
	}
	if len(out) == 0 {
		return nil, errors.New("--queues must name at least one queue")
	}
	return out, nil
}

// registerDemoHandlers installs handlers that make the server useful out of
// the box for smoke tests and demos.
func registerDemoHandlers(p *worker.Pool) {
	// noop: succeeds immediately.
	p.Handle("noop", func(context.Context, *relay.Job) error { return nil })
	// sleep: {"ms": 1500} sleeps for the given duration, honouring cancellation.
	p.Handle("sleep", func(ctx context.Context, j *relay.Job) error {
		var p struct {
			MS int `json:"ms"`
		}
		if len(j.Payload) > 0 {
			if err := json.Unmarshal(j.Payload, &p); err != nil {
				return worker.Permanent(fmt.Errorf("bad payload: %w", err))
			}
		}
		select {
		case <-time.After(time.Duration(p.MS) * time.Millisecond):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	// fail: always fails, to demonstrate backoff and dead-lettering.
	p.Handle("fail", func(_ context.Context, j *relay.Job) error {
		return fmt.Errorf("demo failure on attempt %d", j.Attempts)
	})
}

func newLogger(format, level string) (*slog.Logger, error) {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		return nil, fmt.Errorf("invalid --log-level %q", level)
	}
	opts := &slog.HandlerOptions{Level: lvl}
	switch format {
	case "json":
		return slog.New(slog.NewJSONHandler(os.Stdout, opts)), nil
	case "text":
		return slog.New(slog.NewTextHandler(os.Stdout, opts)), nil
	}
	return nil, fmt.Errorf("invalid --log-format %q (want text or json)", format)
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envBool(key string, def bool) bool {
	if v := os.Getenv(key); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
