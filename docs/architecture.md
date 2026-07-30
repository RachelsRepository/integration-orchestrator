# Architecture

## Intent

The platform exists to absorb the inconsistency of external APIs so callers can
treat every provider the same. That means:

1. A normalized inbound contract for creating and tracking work.
2. A provider-neutral domain model with an explicit state machine.
3. Adapters that translate to and from each provider's quirks.
4. Resilience and recovery that assume the network, the provider, and this
   process will all fail in ways that leave work half-done.

## Layers

| Layer | Responsibility | May depend on |
|---|---|---|
| `domain` | Entities, value objects, state machine, policies, errors | Standard library only |
| `application` | Use cases, ports, DTOs, orchestration services | `domain` |
| `infrastructure` | SQLAlchemy, Redis, Kafka, HTTP adapters, sandbox | `application`, `domain`, `config` |
| `api` | HTTP transport, auth, schemas | Composition root, application ports |
| `workers` | Polling loops that drive recovery | Composition root, application services |
| `observability` | Logging, metrics, tracing, redaction | Standard library + OTel/Prometheus |
| `composition` | Constructs and wires everything | All of the above |

Import boundaries are enforced by `import-linter` and by
`tests/unit/test_architecture.py`.

## Core flows

### Create request

1. The API authenticates the caller (JWT + RBAC) and validates the body.
2. `CreateIntegrationRequestUseCase` looks up any existing idempotency record.
3. On a miss it opens a unit of work, inserts the request and the idempotency
   row, and flushes so a concurrent duplicate loses at the unique constraint.
4. `RequestDispatcher` asks the registry for the adapter, checks capability,
   and calls the provider through the resilience decorator.
5. The resulting status transition, audit event, and outbox event are committed
   together.
6. The outbox publisher later emits the Kafka event outside that transaction.

### Webhook

1. The raw body is size-limited, then handed to the matching adapter for
   signature verification and normalization.
2. A receipt is persisted before correlation is attempted. That is what makes
   webhook-before-response races recoverable rather than lost.
3. If the matching request is not yet known, the receipt is deferred and the
   webhook processor retries later.
4. On a match, the request is transitioned, audited, and outboxed in one
   transaction.

### Retry and reconciliation

- The retry worker claims due `retry_scheduled` rows with
  `FOR UPDATE SKIP LOCKED` and re-enters the dispatcher.
- Reconciliation asks the provider for status when a request has been
  in-flight past a threshold. It only rewrites local state when the provider
  answers unambiguously. Everything else escalates to `manual_review`.

## Provider abstraction

Adapters implement `ProviderGateway`. The registry wraps each adapter with:

1. Client-side rate limiting (Redis token bucket)
2. Circuit breaker (Redis, fail-open on Redis errors)
3. Bulkhead (in-process semaphore)
4. In-process retry for immediately retryable transport failures
5. Explicit timeout budget

Capability differences (`supports_status_lookup`, `supports_cancellation`,
`supports_idempotency_key`) live on the descriptor. Use cases consult the
descriptor; they never branch on provider slug.

## Consistency model

- Aggregate updates, audit rows, and outbox rows share one database transaction.
- Kafka publication is at-least-once. Consumers must deduplicate on `event_id`.
- Idempotency and provider-reference uniqueness are database constraints, not
  application checks.
- Optimistic concurrency on `integration_requests.version` catches stale writes
  that somehow bypassed the row lock.

## Security model

- Internal API: bearer JWT, HS256 locally / RS256 in deployed environments,
  role-derived scopes.
- Provider auth: OAuth2 client credentials with a shared Redis token cache and a
  distributed lock around refresh; API key for Meridian.
- Webhooks: HMAC-SHA256 (Northstar, Meridian) or Ed25519 (Cobalt), timestamp
  window, signature-digest replay protection.
- Secrets and sensitive payload keys are redacted before they enter logs, audit
  metadata, or outbox payloads.

## Observability

Every request carries an `X-Correlation-ID`. It is stored on the aggregate,
propagated into provider calls, written onto audit and outbox rows, and
returned on the response. Metrics cover request outcomes, provider latency,
circuit state, outbox lag, and worker batch sizes. Traces are exported over OTLP
when enabled.
