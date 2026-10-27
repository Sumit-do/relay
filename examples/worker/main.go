// Command worker shows how to embed relay in your own service: open a store,
// register typed handlers, run the pool and (optionally) enqueue work from
// the same process.
//
//	go run ./examples/worker --db relay.db
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sumit-do/relay"
	"github.com/sumit-do/relay/store/sqlite"
	"github.com/sumit-do/relay/worker"
)

type sendEmailPayload struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
}

type resizeImagePayload struct {
	URL    string `json:"url"`
	Width  int    `json:"width"`
	Height int    `json:"height"`
}

func main() {
	dbPath := flag.String("db", "relay.db", "SQLite database path")
	seed := flag.Bool("seed", true, "enqueue a few demo jobs at startup")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stdout, nil))

	store, err := sqlite.Open(*dbPath)
	if err != nil {
		log.Error("open store", "err", err)
		os.Exit(1)
	}
	defer store.Close()

	pool := worker.New(store, worker.Config{
		Concurrency:   4,
		LeaseDuration: 30 * time.Second,
		DrainTimeout:  20 * time.Second,
		Logger:        log,
	})
	// Different queues get different concurrency and retry policies.
	pool.AddQueue(worker.QueueConfig{Name: "email", Concurrency: 8,
		Backoff: &worker.Backoff{Base: 2 * time.Second, Max: 5 * time.Minute, Factor: 2, Jitter: 0.3}})
	pool.AddQueue(worker.QueueConfig{Name: "images", Concurrency: 2, LeaseDuration: 2 * time.Minute})

	pool.Handle("send_email", sendEmail)
	pool.Handle("resize_image", resizeImage)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if *seed {
		client := relay.NewClient(store)
		must := func(req relay.EnqueueRequest) {
			j, created, err := client.Enqueue(ctx, req)
			if err != nil {
				log.Error("enqueue", "err", err)
				return
			}
			log.Info("enqueued", "id", j.ID, "type", j.Type, "queue", j.Queue, "created", created)
		}
		must(relay.EnqueueRequest{Queue: "email", Type: "send_email",
			Payload:        mustJSON(sendEmailPayload{To: "ada@example.com", Subject: "Welcome!"}),
			IdempotencyKey: "welcome-ada"})
		must(relay.EnqueueRequest{Queue: "email", Type: "send_email",
			Payload:        mustJSON(sendEmailPayload{To: "ada@example.com", Subject: "Welcome!"}),
			IdempotencyKey: "welcome-ada"}) // duplicate: returns the existing job
		must(relay.EnqueueRequest{Queue: "images", Type: "resize_image", Priority: 10,
			Payload: mustJSON(resizeImagePayload{URL: "https://example.com/cat.jpg", Width: 320, Height: 240})})
		must(relay.EnqueueRequest{Queue: "images", Type: "resize_image", Delay: "10s",
			Payload: mustJSON(resizeImagePayload{URL: "https://example.com/bad.gif", Width: 0, Height: 0})})
	}

	log.Info("worker running; press Ctrl-C to drain and exit")
	if err := pool.Run(ctx); err != nil {
		log.Error("pool stopped with error", "err", err)
		os.Exit(1)
	}
}

func sendEmail(ctx context.Context, job *relay.Job) error {
	var p sendEmailPayload
	if err := json.Unmarshal(job.Payload, &p); err != nil {
		// Malformed payloads will never succeed; skip the retry schedule.
		return worker.Permanent(fmt.Errorf("decode payload: %w", err))
	}
	if p.To == "" {
		return worker.Permanent(errors.New("recipient is required"))
	}
	// Simulate a transient SMTP hiccup on the first attempt.
	if job.Attempts == 1 {
		return errors.New("smtp: 421 try again later")
	}
	select {
	case <-time.After(200 * time.Millisecond):
	case <-ctx.Done():
		return ctx.Err()
	}
	slog.Info("email sent", "to", p.To, "subject", p.Subject, "attempt", job.Attempts)
	return nil
}

func resizeImage(ctx context.Context, job *relay.Job) error {
	var p resizeImagePayload
	if err := json.Unmarshal(job.Payload, &p); err != nil {
		return worker.Permanent(fmt.Errorf("decode payload: %w", err))
	}
	if p.Width <= 0 || p.Height <= 0 {
		return worker.Permanent(fmt.Errorf("invalid dimensions %dx%d", p.Width, p.Height))
	}
	// Long-running work: the pool heartbeats the lease in the background, so
	// the handler only needs to honour ctx for cancellation.
	select {
	case <-time.After(1500 * time.Millisecond):
	case <-ctx.Done():
		return ctx.Err()
	}
	slog.Info("image resized", "url", p.URL, "size", fmt.Sprintf("%dx%d", p.Width, p.Height))
	return nil
}

func mustJSON(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}
