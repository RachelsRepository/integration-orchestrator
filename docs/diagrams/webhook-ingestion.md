# Webhook ingestion

```mermaid
sequenceDiagram
    participant P as Provider
    participant API as Webhook endpoint
    participant A as Adapter
    participant DB as PostgreSQL
    participant W as Webhook processor

    P->>API: POST /webhooks/{provider}
    API->>A: verify signature + normalize
    alt invalid signature / replay / stale timestamp
        API-->>P: 401
    else valid
        API->>DB: INSERT webhook_receipts
        API->>DB: find request by provider_reference
        alt request known
            API->>DB: transition + audit + outbox (one txn)
            API-->>P: 202
        else request not yet known
            API->>DB: mark receipt deferred
            API-->>P: 202
            Note over W,DB: Later, when the create path lands
            W->>DB: claim deferred receipts
            W->>DB: correlate and apply
        end
    end
```

Persisting the receipt before correlation is what makes the webhook-before-response
race recoverable. Acknowledging 202 for duplicates prevents provider retry storms
without re-applying the event.
