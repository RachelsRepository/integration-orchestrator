# Failure scenarios

The provider sandbox selects behaviour from the `external_reference` (and, for
some paths, from control endpoints). Failures are deterministic so demos and
tests are reproducible.

## Scenario prefixes

| Prefix | Effect | Expected platform outcome |
|---|---|---|
| `scenario-unavailable-` | Provider returns 503 | Retry scheduled (while attempts remain) |
| `scenario-unavailable-once-` | First two calls 503, then success | In-process and/or durable retry, then success |
| `scenario-timeout-` | Provider stalls past the timeout budget | Retryable timeout, then retry or fail |
| `scenario-rate-limit-` | Provider returns 429 with Retry-After | Retry scheduled honouring the cap |
| `scenario-reject-` | Provider returns a non-retryable 4xx | Immediate `failed` |
| `scenario-auth-` | Provider rejects credentials | Non-retryable auth failure |
| `scenario-no-reference-` | 2xx without an operation id | `manual_review` (do not retry) |
| `scenario-unknown-status-` | Status the adapter cannot map | `manual_review` |
| `scenario-webhook-fail-` | Completion webhook reports failure | Request moves to `failed` |
| *(no prefix)* | Happy path | Accept → webhook/status → `succeeded` |

Exact prefix constants live in
`integration_orchestrator.infrastructure.providers.sandbox.scenarios`.

## Races the suite covers

1. **Webhook before create response.** The receipt is stored as deferred and
   applied once the request becomes correlatable.
2. **Idempotent create race.** Two concurrent inserts with the same key; the
   loser replays the winner's result.
3. **Duplicate webhook delivery.** Second delivery is acknowledged and marked
   duplicate without re-applying.
4. **Lost webhook with status lookup.** Reconciliation corrects local state from
   the provider (Meridian, Cobalt).
5. **Lost webhook without status lookup.** Reconciliation escalates to manual
   review after the grace period (Northstar).
6. **Open circuit.** Calls are refused locally without contacting the provider;
   a retry is scheduled.
7. **Retry exhaustion.** After `max_attempts`, the request fails and is audited.

## How to trigger locally

```bash
make token
# Transient failure that schedules a retry:
curl -X POST http://localhost:8000/api/v1/integration-requests \
  -H "Authorization: Bearer $TOKEN" \
  -H "Idempotency-Key: fail-1" \
  -H "Content-Type: application/json" \
  -d '{
    "provider": "meridian",
    "operation_type": "resource_provision",
    "external_reference": "scenario-unavailable-demo-1",
    "payload": {"sku": "widget"}
  }'
```

Inspect the audit trail with `GET /api/v1/integration-requests/{id}/audit`.
