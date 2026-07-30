# ADR 0004: Reconciliation Never Guesses

## Status

Accepted

## Context

Distributed systems lose messages. A provider may accept an operation whose
response never arrives, or a webhook may be dropped after the work finished. The
platform must notice and, where safe, correct local state.

Guessing an outcome — treating silence as failure, or treating an unknown
provider status as success — produces silently wrong data that operators trust.

## Decision

Reconciliation only rewrites local state when the provider can be asked directly
and answers with a status the adapter can map. Requests without a provider
reference, providers without a status endpoint, unmapped statuses, and unknown
references escalate to `manual_review` after a grace period.

## Consequences

- Some requests stop progressing until a human looks. That is preferred to
  inventing an outcome.
- Providers that do not expose status lookup (Northstar) cannot be
  auto-corrected; they rely on webhooks and escalate when those are lost.
- Metrics distinguish mismatch kinds so the operational response can be specific.
