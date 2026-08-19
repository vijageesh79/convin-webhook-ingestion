// Package store persists webhook events, calls, and per-account aggregates.
package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Event is one call-completion webhook delivery.
type Event struct {
	EventID      string
	CallID       string
	AccountID    string
	Status       string
	DurationSec  int
	RecordingURL string
	OccurredAt   time.Time
	Payload      []byte
}

// Stats is the durable per-account aggregate.
type Stats struct {
	CallCount        int64
	TotalDurationSec int64
}

// Store is a Postgres-backed repository.
type Store struct {
	pool *pgxpool.Pool
}

// New opens a connection pool bounded to maxConns.
func New(ctx context.Context, dsn string, maxConns int32) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = maxConns

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &Store{pool: pool}, nil
}

// Pool exposes the underlying pool for tests and ad-hoc queries.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// Close releases all pooled connections.
func (s *Store) Close() { s.pool.Close() }

// EventExists reports whether an event with this ID has already been stored.
func (s *Store) EventExists(ctx context.Context, eventID string) (bool, error) {
	var one int
	err := s.pool.QueryRow(ctx,
		`SELECT 1 FROM events WHERE event_id = $1 LIMIT 1`, eventID).Scan(&one)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// InsertEvent stores the raw delivery.
func (s *Store) InsertEvent(ctx context.Context, e Event) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO events (event_id, call_id, account_id, payload)
		 VALUES ($1, $2, $3, $4)`,
		e.EventID, e.CallID, e.AccountID, e.Payload)
	return err
}

// UpsertCall creates or refreshes the call record for this event.
func (s *Store) UpsertCall(ctx context.Context, e Event) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO calls (call_id, account_id, status, duration_sec, recording_url, updated_at)
		 VALUES ($1, $2, $3, $4, $5, now())
		 ON CONFLICT (call_id) DO UPDATE SET
		     status        = EXCLUDED.status,
		     duration_sec  = EXCLUDED.duration_sec,
		     recording_url = EXCLUDED.recording_url,
		     updated_at    = now()`,
		e.CallID, e.AccountID, e.Status, e.DurationSec, e.RecordingURL)
	return err
}

// MarkRecordingProcessed flags the call's recording as handled.
func (s *Store) MarkRecordingProcessed(ctx context.Context, callID string) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE calls SET recording_processed = TRUE, updated_at = now()
		 WHERE call_id = $1`, callID)
	return err
}

// ApplyDelivery writes the event, call, and stats in one transaction.
// inserted=false means we already had this event_id — don't count it again.
func (s *Store) ApplyDelivery(ctx context.Context, e Event) (inserted bool, isNewCall bool, oldDuration int, isProcessed bool, err error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, false, 0, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	tag, err := tx.Exec(ctx,
		`INSERT INTO events (event_id, call_id, account_id, payload)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (event_id) DO NOTHING`,
		e.EventID, e.CallID, e.AccountID, e.Payload)
	if err != nil {
		return false, false, 0, false, err
	}
	if tag.RowsAffected() == 0 {
		var processed bool
		err = tx.QueryRow(ctx,
			`SELECT recording_processed FROM calls WHERE call_id = $1`, e.CallID).Scan(&processed)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return false, false, 0, false, nil
			}
			return false, false, 0, false, err
		}
		if err := tx.Commit(ctx); err != nil {
			return false, false, 0, false, err
		}
		return false, false, 0, processed, nil
	}

	var oldDur int
	var processed bool
	var callExists bool
	err = tx.QueryRow(ctx,
		`SELECT duration_sec, recording_processed FROM calls WHERE call_id = $1`, e.CallID).Scan(&oldDur, &processed)
	if err == nil {
		callExists = true
	} else if errors.Is(err, pgx.ErrNoRows) {
		callExists = false
	} else {
		return false, false, 0, false, err
	}

	_, err = tx.Exec(ctx,
		`INSERT INTO calls (call_id, account_id, status, duration_sec, recording_url, updated_at)
		 VALUES ($1, $2, $3, $4, $5, now())
		 ON CONFLICT (call_id) DO UPDATE SET
		     status        = EXCLUDED.status,
		     duration_sec  = EXCLUDED.duration_sec,
		     recording_url = EXCLUDED.recording_url,
		     updated_at    = now()`,
		e.CallID, e.AccountID, e.Status, e.DurationSec, e.RecordingURL)
	if err != nil {
		return false, false, 0, false, err
	}

	if callExists {
		_, err = tx.Exec(ctx,
			`INSERT INTO account_stats (account_id, call_count, total_duration_sec)
			 VALUES ($1, 0, $2)
			 ON CONFLICT (account_id) DO UPDATE SET
			     total_duration_sec = account_stats.total_duration_sec + EXCLUDED.total_duration_sec`,
			e.AccountID, int64(e.DurationSec - oldDur))
	} else {
		_, err = tx.Exec(ctx,
			`INSERT INTO account_stats (account_id, call_count, total_duration_sec)
			 VALUES ($1, 1, $2)
			 ON CONFLICT (account_id) DO UPDATE SET
			     call_count         = account_stats.call_count + 1,
			     total_duration_sec = account_stats.total_duration_sec + EXCLUDED.total_duration_sec`,
			e.AccountID, int64(e.DurationSec))
	}
	if err != nil {
		return false, false, 0, false, err
	}

	if err := tx.Commit(ctx); err != nil {
		return false, false, 0, false, err
	}
	return true, !callExists, oldDur, processed, nil
}

// UnprocessedRecordings is the work we still owe after a restart.
func (s *Store) UnprocessedRecordings(ctx context.Context) ([]Event, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT call_id, account_id, status, duration_sec, COALESCE(recording_url, '')
		FROM calls
		WHERE recording_processed = FALSE
		  AND recording_url IS NOT NULL
		  AND recording_url <> ''`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Event
	for rows.Next() {
		var e Event
		if err := rows.Scan(&e.CallID, &e.AccountID, &e.Status, &e.DurationSec, &e.RecordingURL); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// AllAccountStats is used to fill the in-memory cache on boot.
func (s *Store) AllAccountStats(ctx context.Context) (map[string]Stats, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT account_id, call_count, total_duration_sec FROM account_stats`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string]Stats)
	for rows.Next() {
		var accountID string
		var st Stats
		if err := rows.Scan(&accountID, &st.CallCount, &st.TotalDurationSec); err != nil {
			return nil, err
		}
		out[accountID] = st
	}
	return out, rows.Err()
}

// IncrementAccountStats folds one completed call into the durable aggregate.
func (s *Store) IncrementAccountStats(ctx context.Context, accountID string, durationSec int) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO account_stats (account_id, call_count, total_duration_sec)
		 VALUES ($1, 1, $2)
		 ON CONFLICT (account_id) DO UPDATE SET
		     call_count         = account_stats.call_count + 1,
		     total_duration_sec = account_stats.total_duration_sec + EXCLUDED.total_duration_sec`,
		accountID, durationSec)
	return err
}

// AccountStats reads the durable aggregate. A missing account reads as zero.
func (s *Store) AccountStats(ctx context.Context, accountID string) (Stats, error) {
	var st Stats
	err := s.pool.QueryRow(ctx,
		`SELECT call_count, total_duration_sec FROM account_stats WHERE account_id = $1`,
		accountID).Scan(&st.CallCount, &st.TotalDurationSec)
	if errors.Is(err, pgx.ErrNoRows) {
		return Stats{}, nil
	}
	if err != nil {
		return Stats{}, err
	}
	return st, nil
}
