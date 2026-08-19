-- Without this, two retries of the same webhook can both insert.
DROP INDEX IF EXISTS idx_events_event_id;
CREATE UNIQUE INDEX IF NOT EXISTS events_event_id_key ON events (event_id);
