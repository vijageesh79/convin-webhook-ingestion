-- event_id is the provider's stable delivery key. Uniqueness is what makes
-- concurrent redeliveries collapse to a single insert instead of racing
-- through the old SELECT-then-INSERT check.
DROP INDEX IF EXISTS idx_events_event_id;
CREATE UNIQUE INDEX IF NOT EXISTS events_event_id_key ON events (event_id);
