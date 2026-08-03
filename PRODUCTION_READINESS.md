# Production readiness

This repository is a **verified reference implementation** of durable
single-request and multi-step orchestration. It is production-**inspired**, not
a certified or turnkey production product.

## Production Readiness Boundary

| In repository (engineering readiness) | Outside repository (operator / platform responsibility) |
|---|---|
| Clean Architecture, migrations, typed settings with production-safety guards | Cloud account design, IAM, networking, private subnets |
| Compose for **local** development and CI probes only | TLS termination, certificates, DNS, load balancers, WAF/CDN |
| Unit, contract, e2e, and integration tests; illustrative GitHub Actions workflow | Remote CI green status after your commit/push; release gating |
| Prometheus metrics, structured logs, OTLP hooks, health/readiness endpoints | Alert routing, on-call, SLO/error budgets, log retention |
| Transactional outbox, DLQ + CLI redrive, reconciliation, runbooks | Capacity sizing, Kafka/Postgres/Redis SLAs, multi-AZ ops |
| Secrets typed/masked; Terraform secrets **module shape** | KMS, Secrets Manager values, credential issuance and rotation |
| Single-tenant deployment unit + optional subject isolation | Multi-organization isolation (separate deployments), tenancy contracts |
| Illustrative ECS/VPC/RDS Terraform sketch | Actual `terraform apply`, hardening, backup/DR drills |
| Documented at-least-once delivery and idempotent effects | Exactly-once claims, compliance certification, pen-test sign-off |

Crossing this boundary requires independent security review, operational
ownership, and environment-specific validation. This document does **not**
certify production readiness.

## Delivery semantics

- Kafka/outbox publication is **at-least-once**. Stable `event_id` values let
  consumers deduplicate. Exactly-once delivery is **not** claimed.
- Provider-facing effects rely on **idempotent** create/retry behaviour where
  the provider and platform support it (idempotency keys, fingerprints).
  Callers and consumers must tolerate duplicates after retries and broker
  recovery.
- Workflow claim leases and outbox claim leases reduce duplicate work under
  concurrent workers; they do not upgrade the delivery guarantee to exactly-once.

## What the repo does enforce

Production-like `ENVIRONMENT` values refuse to start when unsafe defaults remain
(sandbox enabled, placeholder secrets, Kafka disabled, console log renderer,
statement echo, localhost provider URLs, subject isolation off). See
[docs/deployment.md](docs/deployment.md) and `.env.example`.

## What remains unverified in-repo

- Remote GitHub Actions green status (workflow YAML is present; a real run after
  commit/push is required).
- Every exotic chaos row in [docs/failure-scenarios.md](docs/failure-scenarios.md)
  (Compose automates an expanded suite, not an exhaustive universe of faults).

## Related documents

- [SECURITY.md](SECURITY.md)
- [THREAT_MODEL.md](THREAT_MODEL.md)
- [OPERATIONS.md](OPERATIONS.md)
- [docs/deployment.md](docs/deployment.md)
