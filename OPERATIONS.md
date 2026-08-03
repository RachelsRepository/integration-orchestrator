# Operations

Day-2 operations for a single-tenant deployment unit. Compose remains local-only;
treat cloud topology, TLS, and on-call as outside this repository (see
[PRODUCTION_READINESS.md](PRODUCTION_READINESS.md)).

## Health and readiness

| Endpoint | Purpose |
|---|---|
| `GET /health/live` | Process liveness. Does **not** probe dependencies (avoids cascading restarts on shared DB blips). |
| `GET /health/ready` | Readiness. Probes PostgreSQL, Redis, and Kafka (when enabled). Returns `503` if any required dependency is unhealthy. |
| `GET /metrics` | Prometheus exposition (when metrics are enabled). |

Workers write a heartbeat file (Compose healthchecks use freshness). Ensure the
API and worker processes are both running: outbox, retry, webhook processor,
reconciliation, and workflow workers share `integration-orchestrator worker`
unless `--only` selects a subset.

## Outbox dead letter and redrive

After `WORKERS__OUTBOX_MAX_ATTEMPTS` failed publications, a row is marked
dead-lettered (`dead_lettered_at`) and stops automatic retry. Gauges:

- `outbox_pending_total` — unpublished, non-DLQ rows
- `outbox_dead_lettered_total` — current DLQ depth

Redrive is an operator CLI (requires database connectivity), not an HTTP API:

```bash
integration-orchestrator outbox-redrive <outbox-row-uuid> [<uuid>...]
```

Arguments are **outbox row** UUIDs (`outbox_events.id`), not domain `event_id`
values. Successful redrive clears `dead_lettered_at` and re-arms publication;
the publisher worker must be running. Always fix the root cause (broker,
payload, consumer) before redriving. See
[docs/runbooks/outbox-lag.md](docs/runbooks/outbox-lag.md).

## Reconciliation worker

The reconciliation worker periodically selects stale in-flight requests and
asks providers that support status lookup for ground truth. It **never guesses**:
ambiguous or unsupported cases escalate to `manual_review` (ADR 0004). Watch
`reconciliation_mismatches_total` and follow
[docs/runbooks/stuck-requests.md](docs/runbooks/stuck-requests.md).

## Key and secret rotation expectations

| Material | Expectation |
|---|---|
| JWT verification (HS256 secret or RS256 public key) | Rotate in the IdP / secrets store, roll API tasks, invalidate old tokens via expiry. |
| `TOKEN_ENCRYPTION__SECRET` | Rotating invalidates cached provider tokens in Redis; expect a burst of OAuth refreshes after rollout. Coordinate a dual-read window if you require zero disruption — not built into the app today. |
| Provider OAuth client secrets / API keys | Update secrets manager → redeploy → confirm provider calls succeed; circuits may open during misconfiguration. |
| Webhook HMAC secrets | Update config and provider console together; mismatches yield `401` and no receipts ([docs/runbooks/webhook-backlog.md](docs/runbooks/webhook-backlog.md)). |
| Cobalt Ed25519 public key | Config holds a single verification key; `key_id` is part of signed material and metadata. Coordinate provider key cutover with config update. |

Local `make token` / CLI minting must not be used in production-like environments.

## Incident response

Start with the runbooks under [docs/runbooks/](docs/runbooks/):

| Situation | Runbook |
|---|---|
| Outbox lag / DLQ | [outbox-lag.md](docs/runbooks/outbox-lag.md) |
| Stuck requests / manual review | [stuck-requests.md](docs/runbooks/stuck-requests.md) |
| Provider circuit open | [circuit-open.md](docs/runbooks/circuit-open.md) |
| Webhook backlog / signature failures | [webhook-backlog.md](docs/runbooks/webhook-backlog.md) |

Also useful: [docs/failure-scenarios.md](docs/failure-scenarios.md),
[SECURITY.md](SECURITY.md), [THREAT_MODEL.md](THREAT_MODEL.md).

Inspect effective config without leaking secrets:

```bash
integration-orchestrator config
```

## Alert catalog (metrics)

Suggested conditions; thresholds are environment-specific.

| Metric | Signal | Suggested response |
|---|---|---|
| `outbox_pending_total` | Sustained rise or above lag SLO | [outbox-lag.md](docs/runbooks/outbox-lag.md) — broker / publisher health |
| `outbox_dead_lettered_total` | Non-zero or increasing | Inspect DLQ rows; fix cause; CLI redrive selected UUIDs |
| `provider_circuit_state` | Gauge `open` for a provider | [circuit-open.md](docs/runbooks/circuit-open.md) |
| `reconciliation_mismatches_total` | Rising counter | [stuck-requests.md](docs/runbooks/stuck-requests.md); provider vs local drift |
| `inbound_rate_limit_total` | Spike / `fail_closed` outcomes | Abuse or Redis issues; see ADR 0005 — inbound mutations fail closed when Redis is unavailable |

Complement with readiness failures, worker heartbeat staleness, and provider error
logs (`provider_unavailable`, `provider_timeout`).
