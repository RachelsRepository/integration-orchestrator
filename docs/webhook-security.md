# Webhook security

Inbound webhooks are authenticated by signature verification, not by bearer
tokens. A provider that can reach the public webhook URL must still prove it
holds the shared secret or private key.

## Schemes

| Provider | Scheme | Signed material | Extra controls |
|---|---|---|---|
| Northstar | HMAC-SHA256 | `timestamp.body` | Timestamp must fall inside the replay window |
| Meridian | HMAC-SHA256 | raw body | Event-id deduplication only; no timestamp |
| Cobalt | Ed25519 | `key_id.timestamp.body` | Timestamp window + key id for rotation |

Comparisons are constant-time. Signatures are verified over the raw request
bytes, never over a re-serialised JSON body.

## Replay protection

1. Persist a receipt keyed by `(provider, event_id)` before applying it. A
   duplicate delivery finds the existing row and is acknowledged without
   re-application.
2. Where the provider includes a timestamp, reject deliveries outside
   `webhooks.replay_window_seconds`.
3. Optionally record a short-lived digest of the signature in Redis to catch
   replays that somehow reuse an event id (defence in depth).

Meridian's lack of a signed timestamp is a deliberate weaker case in the
portfolio: it shows why event-id uniqueness alone is not as strong as a signed
freshness claim, and why the platform still accepts the webhook but documents
the tradeoff.

## Rejection behaviour

Invalid signatures, stale timestamps, and oversized bodies never create side
effects on integration requests. The HTTP status is `401` or `413` as
appropriate, and nothing is written that an attacker could use to probe for
valid references.
