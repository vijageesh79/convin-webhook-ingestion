# Solution

## What was broken, and why

Four defects produced the ops report, and the existing tests never hit them.

**Duplicate rows and drifting call-counts.** Dedup was a non-atomic `SELECT` then `INSERT`, and `events.event_id` was not unique. Concurrent redeliveries of the same `event_id` both passed the check, both wrote an event, and both incremented `account_stats` and the in-memory cache. Sequential retries happened to look fine, which is why `TestDuplicateDeliveryIsIgnored` stayed green.

**Recordings never marked processed, with no logs.** Processing ran in a goroutine that reused the HTTP request context. That context is cancelled as soon as we return 200, so `MarkRecordingProcessed` failed with `context canceled`. The error was swallowed by `// TODO: handle`.

**In-flight work vanished on deploy.** `http.Server.Shutdown` waits for handlers, not for those goroutines. Pending work lived only in process memory, so a restart dropped it. The stats endpoint had the same shape of bug: it read an empty in-memory cache after boot even though `account_stats` still had the durable totals.

**The cache raced.** `Cache.Get` took `RLock`, but `Record` mutated the map with no lock, so concurrent ingests could lose updates or corrupt the map.

## Deduplication strategy

Postgres `UNIQUE (event_id)` plus `INSERT … ON CONFLICT DO NOTHING` inside a single transaction that also upserts the call and increments `account_stats`. The unique index is the claim; the transaction makes the side effects atomic with that claim. A duplicate is a no-op and still returns 200, which is what at-least-once delivery needs.

I considered Redis `SET NX` as the lock. It is a poor source of truth here: the counters live in Postgres, Redis in this compose file is not durable, and a Redis/Postgres disagreement would either drop a real event or double-count. A unique constraint is one round-trip, transactional, and correct under concurrency. Redis stays connected for a later job queue or hot cache; it is the wrong place to commit “this event happened.”

## If this had to handle 10,000 webhooks/second

Keep the unique insert as the commit, but stop doing a goroutine per recording. Use a bounded worker pool (Redis Streams or `SKIP LOCKED` in Postgres) so deploy drain is finite. Put the hot stats read path on Redis with Postgres as write-ahead truth, and ingest through a batcher so 10k/s becomes fewer, larger transactions instead of 10k single-row commits.
