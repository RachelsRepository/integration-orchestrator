# ADR 0002: Transactional Outbox

## Status

Accepted

## Context

State changes must be visible to downstream consumers. Publishing a Kafka
message inside the same database transaction that writes the aggregate is not
possible: Kafka has no 2PC with PostgreSQL. Publishing after commit risks losing
the event if the process dies between the two.

## Decision

Persist an outbox row in the same transaction as the aggregate update and the
audit event. A separate publisher worker claims unpublished rows with
`FOR UPDATE SKIP LOCKED`, publishes them, and marks them published.

Publication is at-least-once. Event ids are generated once, at write time, and
never regenerated on retry, so consumers can deduplicate.

## Consequences

- Downstream systems never observe a state change that did not commit, and never
  permanently miss one that did.
- The outbox is a queue, not an archive; published rows are pruned.
- Consumers must tolerate redelivery and out-of-order delivery across aggregates.
