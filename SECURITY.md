# Security

## Vulnerability reporting

This is an independent portfolio / reference repository, not a commercial SaaS
with a dedicated security inbox. If you believe you have found a vulnerability
in this codebase:

1. Open a private GitHub security advisory on the repository when available, or
   open a GitHub issue marked clearly as a security concern without including
   exploit details that would enable abuse.
2. Include affected component (API, webhook ingest, workers, adapters), impact,
   and steps to reproduce against a local Compose stack.
3. Do not open public issues that include live credentials, signed webhook
   payloads against third-party systems, or working exploit code.

There is no bug bounty program and no guaranteed response SLA.

## Single-tenant deployment model

Each deployment unit is **single-tenant**: one organization, one PostgreSQL
database, one Redis namespace, one Kafka topic prefix. The platform is not a
multi-tenant SaaS control plane.

Within a deployment, optional **subject isolation** stores `owner_subject` on
integration requests and workflow executions (the JWT subject of the creating
client). When `SECURITY__ENFORCE_SUBJECT_ISOLATION=true`, non-admin API clients
may only read or mutate rows they own. Cross-subject access returns `404` (not
`403`) to avoid confirming another client's UUIDs. Principals with
`operations:admin` retain full visibility for incident response.

Production-like environments refuse to start unless subject isolation is
enabled. Isolation does not replace network controls, IdP hygiene, or separate
deployments for separate organizations.

## Authentication and authorization

| Surface | Mechanism |
|---|---|
| Internal HTTP API | Bearer JWT (HS256 locally; RS256 with a public key path in deployed environments). Issuer, audience, and expiry are verified. |
| Authorization | Role-derived scopes: `requests:read`, `requests:write`, `requests:retry`, `requests:cancel`, `providers:read`, `operations:admin`. Roles: `viewer`, `integration-client`, `operator`. |
| Provider outbound auth | OAuth2 client credentials (Northstar, Cobalt) with Redis-cached tokens and Fernet encryption at rest; API key (Meridian). |
| Inbound webhooks | Cryptographic signature verification only — not bearer JWT. See [docs/webhook-security.md](docs/webhook-security.md). |

Local token minting (`integration-orchestrator token` / `make token`) is refused
in production-like environments and is not exposed as an HTTP route.

## Webhook security (summary)

- Signatures are verified over the **raw request body** (HMAC-SHA256 for
  Northstar/Meridian; Ed25519 for Cobalt). Comparisons are constant-time.
- Replay controls: receipt uniqueness on `(provider, event_id)`, timestamp
  windows where the provider signs a timestamp, optional Redis signature-digest
  dedupe, and body size limits.
- Invalid signatures and stale timestamps produce no side effects on integration
  requests (`401` / `413` as appropriate).

Full detail: [docs/webhook-security.md](docs/webhook-security.md).

## Secrets handling

- Secrets are typed as Pydantic `SecretStr` and masked by
  `integration-orchestrator config` / `Settings.describe()`.
- Structured logs and audit/outbox metadata pass through redaction before emit.
- Cached OAuth tokens are encrypted with a key derived from
  `TOKEN_ENCRYPTION__SECRET`.
- Production-like startup rejects development placeholders, sandbox mounts,
  console log rendering, Kafka disabled, DB statement echoing, and localhost
  provider endpoints.
- Inject credentials from a secrets manager at deploy time. Values must never be
  committed. The Terraform secrets module is illustrative shape only.

## Related documents

- [THREAT_MODEL.md](THREAT_MODEL.md) — trust boundaries and attack surfaces
- [PRODUCTION_READINESS.md](PRODUCTION_READINESS.md) — repository vs cloud boundary
- [OPERATIONS.md](OPERATIONS.md) — day-2 operations and incident pointers
