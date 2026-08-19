# SOLUTION

I investigated the system behavior and resolved the bugs reported by the operations team.

## 1. What Was Broken and Why

- **Database Stats & Cache Drift**: 
  While `event_id` uniqueness prevented duplicate event records, if a retry or update for an existing call was sent, the service was unconditionally incrementing the `call_count` in both `account_stats` and the memory cache. This caused the call statistics to drift higher than the actual count of unique calls.
- **Recordings Never Getting Marked Processed**:
  When a webhook was retried, the duplicate delivery check returned `inserted = false` and aborted early. If the first attempt's recording worker was interrupted (e.g., due to a deploy) or failed, subsequent retries did not enqueue the recording again, leaving it permanently unprocessed without logging any errors.
- **Connection Pool Starvation (Hanging Background Tasks)**:
  Resuming a large batch of unprocessed recordings on startup or under high concurrency spawned an unbounded number of goroutines. Since each goroutine attempted to run database updates concurrently, they exhausted the 20-connection database pool. This caused database connections to starve and hang indefinitely without logging errors.
- **Loss of In-Flight Tasks on Deploy**:
  The HTTP server and background service shared the same shutdown context. If the HTTP server shutdown took most of the allotted grace period, the background service was terminated prematurely before its active recordings could finish.

---

## 2. Our Implementation & Deduplication Strategy

- **Call-Level Idempotency & Delta Adjustments**:
  We updated `ApplyDelivery` to inspect the existence of the call in the `calls` table during event ingestion. If a call already exists, we do not increment the `call_count` in the database stats or in-memory cache, and we only update the duration difference (`DurationSec - oldDuration`) to prevent statistics drift.
- **Redis Distributed Locking**:
  We implemented a Redis-based distributed lock (`SetNX` with a TTL) inside the recording processor. If multiple webhooks or retries for the same call land concurrently, they will serialize on the Redis lock key (`lock:recording:<call_id>`), ensuring we do not process the same recording concurrently.
- **Safe Retries for Incomplete Work**:
  Even if an event is a duplicate, we query the `calls` table to see if the recording has been processed. If `recording_processed` is still false, we re-enqueue it.
- **Concurrency Limiting (Semaphore)**:
  We introduced a semaphore channel in the service (default capacity 5) to throttle active recording workers. This guarantees that background workers never starve the 20-connection DB pool.

---

## 3. Designing for 10,000 Webhooks / Second

If we need to scale this system to 10k webhooks/sec, we would make the following architectural changes:

1. **Decouple Ingestion from DB Writes**:
   Instead of writing to Postgres inside the HTTP request handler, we would publish the raw webhook payload to a message queue or log-structured stream (e.g., **Kafka** or **Redis Streams**) and immediately respond with `202 Accepted`.
2. **Batch Database Operations**:
   Committing 10,000 single-row transactions per second to a relational database creates massive disk I/O and lock contention. We would run consumer workers that read from the queue and write to Postgres in micro-batches (e.g., bulk inserting 500 events at a time).
3. **Move Stats Cache to Redis**:
   Instead of keeping an in-memory map per service instance (which does not scale horizontally and drifts across replicas), we would store per-account stats in Redis using `HINCRBY`. The hot-path reads would query Redis directly, while Postgres remains the durable archive of record.
4. **Optimistic Locking/SKIP LOCKED**:
   To scale background recording processing, we would use a queue or a database worker queue with `SELECT ... FOR UPDATE SKIP LOCKED` to allow multiple horizontal worker instances to pull unprocessed jobs concurrently without conflicts.
