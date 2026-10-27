// Package sqlite implements relay.Store on top of SQLite using the pure-Go
// driver modernc.org/sqlite (no cgo required).
//
// Design notes:
//   - The pool is limited to a single connection. SQLite serialises writers
//     anyway, and a single connection makes every multi-statement operation
//     (lease, idempotent enqueue) trivially atomic without SQLITE_BUSY
//     retries. WAL mode keeps readers from blocking on the writer within
//     the same process group.
//   - Timestamps are stored as integer Unix microseconds so comparisons are
//     cheap and timezone-free.
package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	_ "modernc.org/sqlite" // registers the "sqlite" driver

	"github.com/sumit-do/relay"
)

const schema = `
CREATE TABLE IF NOT EXISTS jobs (
	id               TEXT PRIMARY KEY,
	queue            TEXT    NOT NULL,
	type             TEXT    NOT NULL,
	payload          BLOB,
	status           TEXT    NOT NULL,
	priority         INTEGER NOT NULL DEFAULT 0,
	attempts         INTEGER NOT NULL DEFAULT 0,
	max_attempts     INTEGER NOT NULL,
	run_at           INTEGER NOT NULL,
	lease_expires_at INTEGER,
	lease_token      TEXT    NOT NULL DEFAULT '',
	last_error       TEXT    NOT NULL DEFAULT '',
	idempotency_key  TEXT,
	created_at       INTEGER NOT NULL,
	updated_at       INTEGER NOT NULL,
	completed_at     INTEGER
);
CREATE UNIQUE INDEX IF NOT EXISTS jobs_idempotency_key ON jobs(idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS jobs_ready ON jobs(queue, status, run_at, priority);
CREATE INDEX IF NOT EXISTS jobs_lease ON jobs(status, lease_expires_at);
CREATE INDEX IF NOT EXISTS jobs_queue_status ON jobs(queue, status);
`

const columns = `id, queue, type, payload, status, priority, attempts, max_attempts, run_at,
	lease_expires_at, lease_token, last_error, idempotency_key, created_at, updated_at, completed_at`

// Store is a SQLite-backed relay.Store.
type Store struct {
	db *sql.DB
}

var _ relay.Store = (*Store)(nil)

// Open opens (creating if necessary) the database at path and migrates the
// schema. Use ":memory:" for an in-process database.
func Open(path string) (*Store, error) {
	dsn := path
	if path == ":memory:" {
		dsn = "file::memory:"
	} else if !strings.HasPrefix(path, "file:") {
		dsn = "file:" + path
	}
	q := url.Values{}
	q.Add("_pragma", "busy_timeout(5000)")
	q.Add("_pragma", "journal_mode(WAL)")
	q.Add("_pragma", "synchronous(NORMAL)")
	q.Add("_pragma", "foreign_keys(1)")
	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	db, err := sql.Open("sqlite", dsn+sep+q.Encode())
	if err != nil {
		return nil, fmt.Errorf("sqlite: open: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)
	db.SetConnMaxIdleTime(0)
	if _, err := db.Exec(schema); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("sqlite: migrate: %w", err)
	}
	return &Store{db: db}, nil
}

// DB exposes the underlying handle (for diagnostics and tests).
func (s *Store) DB() *sql.DB { return s.db }

// Close implements relay.Store.
func (s *Store) Close() error { return s.db.Close() }

func micros(t time.Time) int64 { return t.UnixMicro() }

func fromMicros(us int64) time.Time { return time.UnixMicro(us).UTC() }

func nullMicros(t *time.Time) sql.NullInt64 {
	if t == nil {
		return sql.NullInt64{}
	}
	return sql.NullInt64{Int64: micros(*t), Valid: true}
}

func nullString(s string) sql.NullString {
	return sql.NullString{String: s, Valid: s != ""}
}

type scanner interface {
	Scan(dest ...any) error
}

func scanJob(sc scanner) (*relay.Job, error) {
	var (
		j        relay.Job
		payload  []byte
		runAt    int64
		leaseExp sql.NullInt64
		idem     sql.NullString
		created  int64
		updated  int64
		complete sql.NullInt64
	)
	err := sc.Scan(&j.ID, &j.Queue, &j.Type, &payload, &j.Status, &j.Priority, &j.Attempts, &j.MaxAttempts,
		&runAt, &leaseExp, &j.LeaseToken, &j.LastError, &idem, &created, &updated, &complete)
	if err != nil {
		return nil, err
	}
	if len(payload) > 0 {
		j.Payload = json.RawMessage(payload)
	}
	j.RunAt = fromMicros(runAt)
	j.CreatedAt = fromMicros(created)
	j.UpdatedAt = fromMicros(updated)
	if leaseExp.Valid {
		t := fromMicros(leaseExp.Int64)
		j.LeaseExpiresAt = &t
	}
	if complete.Valid {
		t := fromMicros(complete.Int64)
		j.CompletedAt = &t
	}
	j.IdempotencyKey = idem.String
	return &j, nil
}

func (s *Store) getTx(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, id string) (*relay.Job, error) {
	j, err := scanJob(q.QueryRowContext(ctx, `SELECT `+columns+` FROM jobs WHERE id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, relay.ErrNotFound
	}
	return j, err
}

// withTx runs fn inside a transaction, committing on nil error.
func (s *Store) withTx(ctx context.Context, fn func(tx *sql.Tx) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// Enqueue implements relay.Store.
func (s *Store) Enqueue(ctx context.Context, job *relay.Job) (*relay.Job, bool, error) {
	var (
		out     *relay.Job
		created bool
	)
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		if job.IdempotencyKey != "" {
			var existing string
			err := tx.QueryRowContext(ctx, `SELECT id FROM jobs WHERE idempotency_key = ?`, job.IdempotencyKey).Scan(&existing)
			switch {
			case err == nil:
				j, err := s.getTx(ctx, tx, existing)
				if err != nil {
					return err
				}
				out = j
				return nil
			case !errors.Is(err, sql.ErrNoRows):
				return err
			}
		}
		var payload []byte
		if len(job.Payload) > 0 {
			payload = []byte(job.Payload)
		}
		_, err := tx.ExecContext(ctx, `INSERT INTO jobs (`+columns+`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			job.ID, job.Queue, job.Type, payload, string(job.Status), job.Priority, job.Attempts, job.MaxAttempts,
			micros(job.RunAt), nullMicros(job.LeaseExpiresAt), job.LeaseToken, job.LastError,
			nullString(job.IdempotencyKey), micros(job.CreatedAt), micros(job.UpdatedAt), nullMicros(job.CompletedAt))
		if err != nil {
			return err
		}
		j, err := s.getTx(ctx, tx, job.ID)
		if err != nil {
			return err
		}
		out, created = j, true
		return nil
	})
	if err != nil {
		return nil, false, fmt.Errorf("sqlite: enqueue: %w", err)
	}
	return out, created, nil
}

// Get implements relay.Store.
func (s *Store) Get(ctx context.Context, id string) (*relay.Job, error) {
	j, err := s.getTx(ctx, s.db, id)
	if err != nil && !errors.Is(err, relay.ErrNotFound) {
		return nil, fmt.Errorf("sqlite: get: %w", err)
	}
	return j, err
}

// List implements relay.Store.
func (s *Store) List(ctx context.Context, f relay.ListFilter) ([]*relay.Job, error) {
	limit := f.Limit
	if limit <= 0 {
		limit = relay.DefaultListLimit
	}
	var (
		where []string
		args  []any
	)
	if f.Queue != "" {
		where = append(where, "queue = ?")
		args = append(args, f.Queue)
	}
	if f.Status != "" {
		where = append(where, "status = ?")
		args = append(args, string(f.Status))
	}
	if f.Type != "" {
		where = append(where, "type = ?")
		args = append(args, f.Type)
	}
	q := `SELECT ` + columns + ` FROM jobs`
	if len(where) > 0 {
		q += " WHERE " + strings.Join(where, " AND ")
	}
	q += " ORDER BY id DESC LIMIT ?"
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("sqlite: list: %w", err)
	}
	defer rows.Close()
	out := make([]*relay.Job, 0, limit)
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, fmt.Errorf("sqlite: list: %w", err)
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

// Lease implements relay.Store.
func (s *Store) Lease(ctx context.Context, queue, token string, now time.Time, leaseFor time.Duration) (*relay.Job, error) {
	var out *relay.Job
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		var id string
		err := tx.QueryRowContext(ctx, `
			SELECT id FROM jobs
			WHERE queue = ? AND status IN ('pending','failed') AND run_at <= ?
			ORDER BY priority DESC, run_at ASC, id ASC
			LIMIT 1`, queue, micros(now)).Scan(&id)
		if errors.Is(err, sql.ErrNoRows) {
			return relay.ErrNoJob
		}
		if err != nil {
			return err
		}
		res, err := tx.ExecContext(ctx, `
			UPDATE jobs SET status = 'leased', attempts = attempts + 1, lease_token = ?,
				lease_expires_at = ?, updated_at = ?
			WHERE id = ? AND status IN ('pending','failed')`,
			token, micros(now.Add(leaseFor)), micros(now), id)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n != 1 {
			// Cannot happen with a single connection, but keep the invariant
			// explicit in case the pool size is ever raised.
			return relay.ErrNoJob
		}
		out, err = s.getTx(ctx, tx, id)
		return err
	})
	if err != nil {
		if errors.Is(err, relay.ErrNoJob) {
			return nil, relay.ErrNoJob
		}
		return nil, fmt.Errorf("sqlite: lease: %w", err)
	}
	return out, nil
}

// updateLeased runs an UPDATE that must match exactly one leased row holding
// token, translating "no rows" into ErrLeaseLost / ErrNotFound.
func (s *Store) updateLeased(ctx context.Context, op, id, token, set string, args ...any) error {
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		args = append(args, id, token)
		res, err := tx.ExecContext(ctx, `UPDATE jobs SET `+set+` WHERE id = ? AND status = 'leased' AND lease_token = ?`, args...)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n == 1 {
			return nil
		}
		var exists int
		if err := tx.QueryRowContext(ctx, `SELECT 1 FROM jobs WHERE id = ?`, id).Scan(&exists); errors.Is(err, sql.ErrNoRows) {
			return relay.ErrNotFound
		} else if err != nil {
			return err
		}
		return relay.ErrLeaseLost
	})
	if err != nil && !errors.Is(err, relay.ErrLeaseLost) && !errors.Is(err, relay.ErrNotFound) {
		return fmt.Errorf("sqlite: %s: %w", op, err)
	}
	return err
}

// Heartbeat implements relay.Store.
func (s *Store) Heartbeat(ctx context.Context, id, token string, expiresAt time.Time) error {
	return s.updateLeased(ctx, "heartbeat", id, token, `lease_expires_at = ?`, micros(expiresAt))
}

// Succeed implements relay.Store.
func (s *Store) Succeed(ctx context.Context, id, token string, now time.Time) error {
	return s.updateLeased(ctx, "succeed", id, token,
		`status = 'succeeded', lease_token = '', lease_expires_at = NULL, last_error = '', completed_at = ?, updated_at = ?`,
		micros(now), micros(now))
}

// Fail implements relay.Store.
func (s *Store) Fail(ctx context.Context, id, token, lastError string, retryAt, now time.Time) error {
	return s.updateLeased(ctx, "fail", id, token,
		`status = 'failed', lease_token = '', lease_expires_at = NULL, last_error = ?, run_at = ?, updated_at = ?`,
		lastError, micros(retryAt), micros(now))
}

// Dead implements relay.Store.
func (s *Store) Dead(ctx context.Context, id, token, lastError string, now time.Time) error {
	return s.updateLeased(ctx, "dead", id, token,
		`status = 'dead', lease_token = '', lease_expires_at = NULL, last_error = ?, completed_at = ?, updated_at = ?`,
		lastError, micros(now), micros(now))
}

// ReapExpired implements relay.Store.
func (s *Store) ReapExpired(ctx context.Context, now time.Time) (int, error) {
	var total int64
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		ts := micros(now)
		dead, err := tx.ExecContext(ctx, `
			UPDATE jobs SET status = 'dead', lease_token = '', lease_expires_at = NULL,
				last_error = 'lease expired', completed_at = ?, updated_at = ?
			WHERE status = 'leased' AND lease_expires_at <= ? AND attempts >= max_attempts`, ts, ts, ts)
		if err != nil {
			return err
		}
		pending, err := tx.ExecContext(ctx, `
			UPDATE jobs SET status = 'pending', lease_token = '', lease_expires_at = NULL,
				last_error = 'lease expired', updated_at = ?
			WHERE status = 'leased' AND lease_expires_at <= ?`, ts, ts)
		if err != nil {
			return err
		}
		d, _ := dead.RowsAffected()
		p, _ := pending.RowsAffected()
		total = d + p
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("sqlite: reap: %w", err)
	}
	return int(total), nil
}

// transition runs an UPDATE guarded by a set of allowed source statuses.
func (s *Store) transition(ctx context.Context, op, id string, from []relay.Status, set string, args ...any) (*relay.Job, error) {
	var out *relay.Job
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		cur, err := s.getTx(ctx, tx, id)
		if err != nil {
			return err
		}
		allowed := false
		for _, f := range from {
			if cur.Status == f {
				allowed = true
				break
			}
		}
		if !allowed {
			return relay.ErrInvalidTransition
		}
		args = append(args, id)
		if _, err := tx.ExecContext(ctx, `UPDATE jobs SET `+set+` WHERE id = ?`, args...); err != nil {
			return err
		}
		out, err = s.getTx(ctx, tx, id)
		return err
	})
	if err != nil {
		if errors.Is(err, relay.ErrNotFound) || errors.Is(err, relay.ErrInvalidTransition) {
			return nil, err
		}
		return nil, fmt.Errorf("sqlite: %s: %w", op, err)
	}
	return out, nil
}

// Retry implements relay.Store.
func (s *Store) Retry(ctx context.Context, id string, now time.Time) (*relay.Job, error) {
	return s.transition(ctx, "retry", id,
		[]relay.Status{relay.StatusDead, relay.StatusFailed, relay.StatusCancelled},
		`status = 'pending', attempts = 0, run_at = ?, last_error = '', completed_at = NULL, updated_at = ?`,
		micros(now), micros(now))
}

// Cancel implements relay.Store.
func (s *Store) Cancel(ctx context.Context, id string, now time.Time) (*relay.Job, error) {
	return s.transition(ctx, "cancel", id,
		[]relay.Status{relay.StatusPending, relay.StatusFailed},
		`status = 'cancelled', completed_at = ?, updated_at = ?`,
		micros(now), micros(now))
}

// Stats implements relay.Store.
func (s *Store) Stats(ctx context.Context) (relay.QueueStats, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT queue, status, COUNT(*) FROM jobs GROUP BY queue, status`)
	if err != nil {
		return nil, fmt.Errorf("sqlite: stats: %w", err)
	}
	defer rows.Close()
	st := make(relay.QueueStats)
	for rows.Next() {
		var (
			queue, status string
			n             int
		)
		if err := rows.Scan(&queue, &status, &n); err != nil {
			return nil, fmt.Errorf("sqlite: stats: %w", err)
		}
		if st[queue] == nil {
			st[queue] = make(map[relay.Status]int)
		}
		st[queue][relay.Status(status)] = n
	}
	return st, rows.Err()
}
