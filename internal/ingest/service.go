// Package ingest accepts call-completion webhooks and processes them.
package ingest

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/convin/webhook-ingest/internal/stats"
	"github.com/convin/webhook-ingest/internal/store"
)

// recordingWork stands in for downloading and transcoding a recording.
const recordingWork = 50 * time.Millisecond

// Service ingests webhook deliveries.
type Service struct {
	store *store.Store
	cache *stats.Cache
	rdb   *redis.Client
	log   *slog.Logger

	wg sync.WaitGroup
}

// New builds a Service.
func New(s *store.Store, c *stats.Cache, rdb *redis.Client, log *slog.Logger) *Service {
	return &Service{store: s, cache: c, rdb: rdb, log: log}
}

// Start warms the in-memory stats cache from Postgres and resumes recording
// work that was still pending when the previous process exited.
func (s *Service) Start(ctx context.Context) error {
	totals, err := s.store.AllAccountStats(ctx)
	if err != nil {
		return err
	}
	for accountID, st := range totals {
		s.cache.Seed(accountID, stats.AccountStats{
			CallCount:        st.CallCount,
			TotalDurationSec: st.TotalDurationSec,
		})
	}

	pending, err := s.store.UnprocessedRecordings(ctx)
	if err != nil {
		return err
	}
	for _, rec := range pending {
		s.enqueueRecording(rec)
	}
	return nil
}

// Shutdown waits for in-flight recording work to finish, or until ctx ends.
func (s *Service) Shutdown(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Stats returns the cached totals for an account.
func (s *Service) Stats(accountID string) stats.AccountStats {
	return s.cache.Get(accountID)
}

// Ingest stores a delivery and kicks off processing. Processing runs
// asynchronously so the provider gets a fast acknowledgement.
func (s *Service) Ingest(ctx context.Context, evt Event) error {
	payload, err := json.Marshal(evt)
	if err != nil {
		return err
	}

	rec := store.Event{
		EventID:      evt.EventID,
		CallID:       evt.CallID,
		AccountID:    evt.AccountID,
		Status:       evt.Status,
		DurationSec:  evt.DurationSec,
		RecordingURL: evt.RecordingURL,
		OccurredAt:   evt.OccurredAt,
		Payload:      payload,
	}

	inserted, err := s.store.ApplyDelivery(ctx, rec)
	if err != nil {
		return err
	}
	if !inserted {
		s.log.Info("duplicate delivery ignored", "event_id", evt.EventID)
		return nil
	}

	s.cache.Record(rec.AccountID, rec.DurationSec)

	// Recordings are slow to fetch, so that part does not block the provider.
	if rec.RecordingURL != "" {
		s.enqueueRecording(rec)
	}

	return nil
}

func (s *Service) enqueueRecording(rec store.Event) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		// Detached from the HTTP request: r.Context() is cancelled as soon as
		// we write 200, which is exactly when this work still needs to run.
		if err := s.processRecording(context.Background(), rec); err != nil {
			s.log.Error("process recording failed", "call_id", rec.CallID, "err", err)
		}
	}()
}

// processRecording downloads and transcodes the call recording, then marks
// the call as done.
func (s *Service) processRecording(ctx context.Context, rec store.Event) error {
	time.Sleep(recordingWork)
	return s.store.MarkRecordingProcessed(ctx, rec.CallID)
}
