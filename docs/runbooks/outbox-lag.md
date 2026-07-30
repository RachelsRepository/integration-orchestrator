# Runbook: Outbox lag

## Symptoms

- `outbox_pending` gauge is rising.
- Downstream consumers stop receiving lifecycle events even though requests
  complete in the API.
- Outbox rows show increasing `attempt_count` and `last_error`.

## Immediate checks

1. Confirm the outbox publisher worker is running.
2. Confirm Kafka connectivity from the worker process.
3. Inspect failing rows:

```sql
SELECT id, event_type, aggregate_id, attempt_count, last_error, next_attempt_at
FROM outbox_events
WHERE published_at IS NULL
ORDER BY created_at ASC
LIMIT 50;
```

## Response

1. If Kafka is down, restore the broker. Rows will republish automatically; do
   not delete them.
2. If a poison payload is repeatedly failing, fix the consumer or the mapping —
   do not mark the row published by hand. Manual publication skips the audit of
   what actually left the system.
3. After recovery, confirm lag returns to near zero and that consumers did not
   observe gaps they cannot tolerate. Remember delivery is at-least-once;
   duplicates are expected after a broker outage.

## Anti-patterns

- Truncating `outbox_events` to "clear the backlog".
- Publishing from a one-off script without updating `published_at` through the
  worker path.
- Disabling Kafka in a production-like environment. Settings validation refuses
  that configuration because the in-memory publisher would silently lose events.
