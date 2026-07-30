# Runbook: Webhook backlog

## Symptoms

- Deferred webhook count is rising.
- Requests stay `pending` even though the provider reports completion.
- Provider dashboards show repeated delivery attempts.

## Immediate checks

1. Confirm the webhook processor worker is running.
2. Sample deferred receipts (see stuck-requests runbook).
3. Verify signature material has not rotated without a config update.
4. Confirm body size limits are not rejecting legitimate oversized payloads
   (`413` in access logs).

## Common causes

| Cause | Signal | Fix |
|---|---|---|
| Create response slower than webhook | Deferred receipts that later process | None needed; design handles this |
| Wrong webhook secret / public key | `401` on ingest, no receipts | Rotate config, redeploy |
| Provider reference never stored | Deferred forever, then abandoned | Escalate; do not invent a reference |
| Worker down | `next_attempt_at` in the past, no progress | Restart workers |
| Replay / duplicate storm | Receipts marked duplicate | Acknowledge; provider will stop |

## Interventions

- Do not manually insert a forged receipt. Re-deliver from the provider if they
  support it, or reconcile from status lookup.
- Abandoned deferred receipts (`webhook_deferred_abandon_after_seconds`) need
  human review; they indicate a create path that never produced a correlatable
  request.
