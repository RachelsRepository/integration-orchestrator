# Threat model

Scope: one **single-tenant** deployment of the Integration Orchestrator
(PostgreSQL + Redis + Kafka/Redpanda, API + workers). Compose is local-only
tooling; illustrative ECS Terraform is not a production default.

## Trust boundaries

| Boundary | Inside | Outside |
|---|---|---|
| Deployment unit | API, workers, Postgres, Redis, broker for one organization | Other orgs, public internet, provider networks |
| Control plane API | Callers presenting valid JWTs from the configured IdP | Unauthenticated clients; forged tokens |
| Webhook ingress | Provider-signed HTTP deliveries | Arbitrary clients that can reach the URL |
| Provider edge | Configured `base_url` / OAuth token URLs | Untrusted redirect targets, internal metadata endpoints |
| Operator tooling | CLI on hosts with DB credentials (`outbox-redrive`, SQL) | Remote callers without host/DB access |
| Subject isolation (optional) | Rows owned by JWT `sub` | Other clients in the same DB when isolation is on |

## Assets

- Provider credentials (OAuth client secrets, API keys) and webhook secrets / public keys
- JWT verification material and `TOKEN_ENCRYPTION__SECRET`
- Integration request / workflow state, audit history, outbox payloads
- Correlation and operational metrics that could leak business activity
- Ability to create provider-side effects (provision, deprovision, cancel)

## Actors

- Legitimate API clients (`integration-client`, `viewer`)
- Operators (`operator` / `operations:admin`) performing remediation
- Provider webhook senders (and impostors who can reach the webhook URL)
- Compromised or malicious callers inside the same deployment (IDOR / privilege abuse)
- Insiders with database, Redis, broker, or secrets-manager access
- Automated scanners and opportunistic internet attackers

## Attack surfaces and mitigations

### Webhook forgery

**Risk:** Crafted POSTs create or advance request state without a real provider event.

**Mitigations present:** Per-provider signature verification over raw bytes;
constant-time compare; reject missing/invalid material with no request mutation.
See [docs/webhook-security.md](docs/webhook-security.md).

### Replay

**Risk:** Replaying a valid signed delivery re-applies side effects.

**Mitigations present:** Durable receipts on `(provider, event_id)`; timestamp
replay windows (Northstar, Cobalt); optional Redis signature-digest TTL;
Meridian relies primarily on event-id uniqueness (documented weaker freshness).

### IDOR / subject isolation bypass

**Risk:** Guessing UUIDs reads or mutates another client's requests/workflows.

**Mitigations present:** Optional `owner_subject` checks via
`SECURITY__ENFORCE_SUBJECT_ISOLATION`; cross-subject access returns `404`;
required `true` in production-like environments. Isolation is **within** one
tenant DB — not multi-tenant SaaS partitioning.

### Privilege escalation

**Risk:** A low-privilege token gains retry/cancel/admin or mints tokens.

**Mitigations present:** Scope checks on routes; role→scope mapping;
local minting refused in production-like environments and not HTTP-exposed.
Admin scope intentionally bypasses subject isolation for ops — treat
`operations:admin` issuance as high trust.

### SSRF via provider configuration

**Risk:** Pointing provider URLs at internal services exfiltrates or probes the VPC.

**Mitigations present:** Production-like settings reject localhost/loopback
provider and OAuth URLs; sandbox mount disabled outside local/test. There is
**no** general private-IP / metadata allowlist at the HTTP client — URL safety
depends on controlled configuration and network policy.

### Injection

**Risk:** Payload fields drive SQL/command injection or log forging.

**Mitigations present:** SQLAlchemy parameterized access; typed settings; no
shelling out on request paths. Treat provider and client JSON as untrusted data
in adapters and logs (redaction, size limits on webhooks).

### Secret exposure

**Risk:** Credentials in logs, metrics, config dumps, or images.

**Mitigations present:** `SecretStr`, redaction helpers, masked `config`
describe, encrypted cached OAuth tokens, non-root container user, placeholder
rejection at startup. Compose `.env` and example files are local placeholders
only.

### DLQ redrive abuse

**Risk:** Re-arming poison or malicious outbox rows republishes harmful events.

**Mitigations present:** Redrive is a **CLI** requiring database access
(`integration-orchestrator outbox-redrive <uuid>...`), not a public API.
Operators must select row UUIDs deliberately after root-cause analysis.

### Admin remediation abuse

**Risk:** Operator retry/cancel or SQL edits amplify outages or erase evidence.

**Mitigations present:** Separate retry/cancel scopes; reconciliation never
guesses (ADR 0004); runbooks discourage truncating outbox/audit; circuits fail
open on Redis errors for provider path (ADR 0005) while inbound mutations fail
closed. Process and access control remain operator responsibilities.

## Items requiring independent pen-test / review

Do not treat repository tests as a substitute for these:

- JWT algorithm confusion, key compromise, and IdP misconfiguration
- Webhook crypto edge cases under real provider SDKs and clock skew
- Subject-isolation completeness across every read/write/list path
- SSRF and egress controls in the target cloud account
- Secrets Manager / KMS / TLS termination and certificate lifecycle
- Authorization around operator hosts running redrive and DB access
- Load and abuse of webhook and API rate limits under production WAF/CDN
- Compliance mappings (SOC2, HIPAA, PCI, etc.) — none are claimed here
