# Runbook: Stuck integration requests

## Symptoms

- Requests remain in `pending`, `dispatching`, or `retry_scheduled` past the
  expected completion window.
- `reconciliation_mismatches_total` is rising.
- Operators report missing completions downstream.

## Immediate checks

1. Confirm API and workers are healthy: `GET /readyz`, worker process logs.
2. Confirm the outbox is draining: `outbox_pending` gauge and Kafka producer
   errors.
3. Identify the stuck population:

```sql
SELECT id, provider, status, provider_reference, attempt_count,
       last_error_code, updated_at, next_retry_at
FROM integration_requests
WHERE status IN ('pending', 'dispatching', 'retry_scheduled', 'manual_review')
  AND updated_at < now() - interval '15 minutes'
ORDER BY updated_at ASC
LIMIT 100;
```

4. Check deferred webhooks that never correlated:

```sql
SELECT id, provider, event_id, provider_reference, failure_reason,
       attempt_count, next_attempt_at
FROM webhook_receipts
WHERE processing_status = 'deferred'
ORDER BY received_at ASC
LIMIT 100;
```

## Decision tree

| Local state | Provider reference | Provider supports status | Action |
|---|---|---|---|
| `retry_scheduled`, due | any | n/a | Confirm retry worker is running; check circuit breaker |
| `pending`, stale | present | yes | Wait for reconciliation, or call status manually |
| `pending`, stale | present | no | Look for missing webhook; escalate if lost |
| any in-flight | absent | n/a | Do not retry blindly; escalate after grace period |
| `manual_review` | any | n/a | Human decides; use operator retry only when safe |

## Safe interventions

- Operator retry (`POST .../retry`) is safe when the provider supports
  idempotency keys, or when you have confirmed no provider-side operation
  exists.
- Cancellation is only offered for providers that declare
  `supports_cancellation`.
- Never delete outbox or audit rows to "unstick" a request.

## Escalation

If more than a handful of requests enter `manual_review` for the same provider
in a short window, treat it as a provider incident and open the circuit
deliberately while investigating.
