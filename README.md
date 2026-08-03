# Integration Orchestrator

A production-style integration platform for enterprise API orchestration.

Callers submit a normalized request naming a provider and an operation. The
platform selects an adapter, authenticates to the provider, applies timeout,
retry, circuit breaker and concurrency controls, tracks the workflow through an
explicit state machine, ingests the provider's webhooks, publishes normalized
domain events, and reconciles anything that ends up ambiguous.

This repository is an independent portfolio project. It is intentionally not a
wallet, ledger, or payments system. The focus is provider abstraction, OAuth2
and API-key authentication, webhook security, durable workflows, resilience, and
operational recovery.

## What it demonstrates

- Clean Architecture with Ports and Adapters
- Three fictional provider adapters behind a shared `ProviderGateway`
- OAuth2 client-credentials lifecycle with proactive refresh
- API-key authentication
- HMAC and Ed25519 webhook signature verification with replay protection
- Canonical request fingerprints and client-supplied idempotency keys
- Transactional outbox with Kafka publication outside the database transaction
- Retries with exponential backoff and jitter
- Distributed circuit breakers, bulkheads, and client-side rate limits
- Retry, webhook-processor, outbox, and reconciliation workers
- Structured logging, correlation IDs, Prometheus metrics, and OpenTelemetry traces
- Alembic migrations, Docker Compose local stack, GitHub Actions CI, and illustrative Terraform

## Architecture at a glance

```
Client ──JWT──► API ──► Application use cases ──► Domain
                              │
                              ▼
                     ProviderGateway port
                              │
              ┌───────────────┼───────────────┐
              ▼               ▼               ▼
          Northstar       Meridian         Cobalt
         (OAuth2)        (API key)        (OAuth2)
              │               │               │
              └────── webhooks / status ──────┘
                              │
                     PostgreSQL + Redis + Kafka
```

See [docs/architecture.md](docs/architecture.md) for the layering rules,
[docs/diagrams/](docs/diagrams/) for Mermaid diagrams, and
[docs/adrs/](docs/adrs/) for the decisions behind the design.

## Requirements

- Python 3.12+
- [uv](https://docs.astral.sh/uv/)
- Docker and Docker Compose (for the local stack and integration tests)
- Make (optional, wraps the common commands)

## Quick start

```bash
cp .env.example .env
make install
make up                 # PostgreSQL, Redis, Kafka-compatible broker, API, workers
make migrate
make token              # prints a local bearer token
make demo               # walks a request through create → webhook → event
make compose-e2e        # live Compose probe (API on host port 18100)
make chaos-subset       # worker/Redis/API recovery probe
```

The Compose API is published on `http://localhost:18100` (container listens on
`8000`). Interactive docs are at `/docs`. The deterministic provider sandbox is
mounted at `/__sandbox__` in local and test environments only; production
configuration refuses to start with it enabled.

### Manual request

```bash
TOKEN=$(make token)
curl -sS -X POST http://localhost:8000/api/v1/integration-requests \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: demo-1" \
  -H "X-Correlation-ID: demo-corr-1" \
  -d '{
    "provider": "northstar",
    "operation_type": "resource_provision",
    "external_reference": "order-1001",
    "payload": {"sku": "widget", "quantity": 2}
  }'
```

## Project layout

```
src/integration_orchestrator/
  domain/            # entities, state machine, policies, errors
  application/       # use cases, ports, DTOs
  infrastructure/    # SQLAlchemy, Redis, Kafka, provider adapters
  api/               # FastAPI routers, schemas, middleware
  workers/           # outbox, retry, webhook processor, reconciliation
  observability/     # logging, metrics, tracing, redaction
  config/            # typed settings
  composition.py     # dependency wiring
tests/
  unit/ contract/ e2e/ integration/
docs/
  architecture.md adrs/ diagrams/ runbooks/ failure-scenarios.md
terraform/           # illustrative AWS deployment
docker/ docker-compose.yml
migrations/
```

## Development

```bash
make verify            # format check, lint, mypy, import boundaries, unit/contract/e2e
make test              # full suite (integration tests skip without Docker)
make test-integration  # PostgreSQL + Redis via testcontainers
make boundaries        # import-linter contracts
make format lint typecheck
```

Architecture boundaries are enforced two ways: `import-linter` contracts in
`pyproject.toml`, and AST-based unit tests in
`tests/unit/test_architecture.py`. The domain layer has no framework imports.
The application layer talks only to ports.

## Providers

| Provider | Auth | Create | Status | Cancel | Webhooks |
|---|---|---|---|---|---|
| Northstar Connect | OAuth2 client credentials | Sync accept | No | No | HMAC-SHA256 |
| Meridian Services | API key | Sync accept | Yes | No | HMAC-SHA256 |
| Cobalt Network | OAuth2 client credentials | Async accept | Yes | Yes | Ed25519 |

Failure behaviour is deterministic and selected by the external reference
prefix. See [docs/failure-scenarios.md](docs/failure-scenarios.md).

## Operations

- [OPERATIONS.md](OPERATIONS.md) — health, DLQ redrive, reconciliation, alerts
- [Runbooks](docs/runbooks/) — outage response, stuck requests, circuit open, webhook backlog
- [Failure scenarios](docs/failure-scenarios.md) — what each sandbox scenario exercises
- [Deployment notes](docs/deployment.md) — Compose (local-only), CI, and illustrative Terraform
- Health: `GET /health/live`, readiness: `GET /health/ready`, metrics: `GET /metrics`

## Configuration

Settings are typed with Pydantic and loaded from the environment. Nested values
use a double-underscore delimiter:

```bash
PROVIDERS__NORTHSTAR__TOTAL_TIMEOUT_SECONDS=6.0
PROVIDERS__MERIDIAN__RATE_LIMIT_PER_SECOND=10
```

Production-like environments refuse to start with development placeholders, the
provider sandbox enabled, memory persistence, console log rendering, database
statement echoing, Kafka disabled, or a placeholder token-encryption secret.
See `.env.example` for the full surface.

## Verified scope and limitations

This is a **verified reference implementation** of durable single-request and
multi-step orchestration with **at-least-once** outbox delivery and idempotent
provider effects. It is **production-inspired**, not a certified or turnkey
production deployment. See the
[Production Readiness Boundary](PRODUCTION_READINESS.md#production-readiness-boundary)
in [PRODUCTION_READINESS.md](PRODUCTION_READINESS.md). Independent security and
operational review is required before real third-party traffic.

Honest limitations:

- Each deployment is a **single-tenant** unit (one org / one database). Optional
  subject isolation via `owner_subject` is controlled by
  `SECURITY__ENFORCE_SUBJECT_ISOLATION` (required in production-like settings).
  See [SECURITY.md](SECURITY.md) and [THREAT_MODEL.md](THREAT_MODEL.md).
- Multi-step workflows use one `IntegrationRequest` per step; compensation is
  reverse-order deprovision/revoke where declared. Workflow (and outbox) claim
  leases reduce duplicate work under concurrent workers. Dependency-ready
  promotion supports parallel independent steps; Compose proves sequential
  `customer_onboarding` and live `parallel_provisioning` fan-out/fan-in.
  Workflow cancel (`POST /api/v1/workflows/executions/{id}/cancel`) and hard
  deadlines (`deadline_seconds` on start) are durable and worker-enforced.
- Compose runs two API replicas (`api` :18100, `api-b` :18101) sharing Postgres,
  Redis, and Redpanda; `scripts/dual_api_rate_limit.py` proves shared inbound RL.
- Delivery is at-least-once; exactly-once is not claimed. Consumers must
  deduplicate on `event_id`.
- Redis outage policies are risk-based (see ADR 0005): inbound mutations fail
  closed; provider breakers/rate limits fail open.
- Compose is local-only (and CI probes); it uses a Kafka-protocol broker
  (Redpanda). ECS Terraform under `terraform/` is illustrative, not a default
  production topology.
- Chaos coverage is an expanded automated suite in `scripts/chaos_subset.py`
  (worker/API/Redis/provider/stack, cancel, deadline, parallel restart).
- Live reconciliation matrix: `scripts/recon_matrix.py`.
- Day-2 ops: [OPERATIONS.md](OPERATIONS.md).
- Remote GitHub Actions runtime is defined but must be green after commit/push
  before claiming CI.
## License

See [LICENSE](LICENSE).
