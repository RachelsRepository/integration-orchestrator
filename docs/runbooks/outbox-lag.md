# Runbook: Outbox lag

## Symptoms

- `outbox_pending_total` gauge is rising.
- `outbox_dead_lettered_total` is non-zero or climbing.
- Downstream consumers stop receiving lifecycle events even though requests
  complete in the API.
- Outbox rows show increasing `attempt_count` and `last_error`.

## Immediate checks

1. Confirm the outbox publisher worker is running.
2. Confirm Kafka connectivity from the worker process.
3. Inspect failing (still retrying) rows:

```sql
SELECT id, event_type, aggregate_id, attempt_count, last_error, next_attempt_at
FROM outbox_events
WHERE published_at IS NULL
  AND dead_lettered_at IS NULL
ORDER BY created_at ASC
LIMIT 50;
```

4. Inspect dead-lettered rows (exhausted `WORKERS__OUTBOX_MAX_ATTEMPTS`):

```sql
SELECT id, event_type, aggregate_id, attempt_count, last_error, dead_lettered_at
FROM outbox_events
WHERE dead_lettered_at IS NOT NULL
  AND published_at IS NULL
ORDER BY dead_lettered_at DESC
LIMIT 50;
```

## Response

1. If Kafka is down, restore the broker. Non-DLQ rows will republish
   automatically; do not delete them.
2. If a poison payload is repeatedly failing, fix the consumer or the mapping —
   do not mark the row published by hand. Manual publication skips the audit of
   what actually left the system.
3. After max attempts, rows move to the dead-letter state and stop automatic
   retry. After fixing the root cause, re-arm selected rows with the CLI
   (arguments are **outbox row** UUIDs, `outbox_events.id`, not domain
   `event_id`):

```bash
integration-orchestrator outbox-redrive <uuid> [<uuid>...]
```

   The command prints `{"redriven": N, "requested": M}` and clears
   `dead_lettered_at` so the publisher can claim them again. Prefer redriving
   known-good IDs over bulk selection.
4. After recovery, confirm `outbox_pending_total` returns toward zero,
   `outbox_dead_lettered_total` drops for redriven rows, and consumers did not
   observe gaps they cannot tolerate. Remember delivery is at-least-once;
   duplicates are expected after a broker outage or redrive.

## Anti-patterns

- Truncating `outbox_events` to "clear the backlog".
- Publishing from a one-off script without updating `published_at` through the
  worker path.
- Redriving DLQ rows before understanding why they failed.
- Disabling Kafka in a production-like environment. Settings validation refuses
  that configuration because the in-memory publisher would silently lose events.
