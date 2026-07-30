# Runbook: Provider circuit open

## Symptoms

- Requests for one provider return or schedule retries with
  `provider_circuit_open`.
- Metric `provider_circuit_state` reports `open` for that provider.
- Other providers continue normally.

## Immediate checks

1. Confirm the provider's own status page / sandbox control endpoint.
2. Inspect recent provider errors:

```text
provider=northstar error_code=provider_unavailable
provider=northstar error_code=provider_timeout
```

3. Check Redis connectivity. The breaker fails open on Redis errors, so an open
   circuit means Redis is reachable and the failure threshold was genuinely hit.

## Response

1. Stop manual retries against the open provider; they will only queue more work.
2. Let the cool-down elapse. The breaker moves to half-open and admits a limited
   number of probes.
3. If probes succeed, the circuit closes automatically.
4. If the provider remains down, keep the circuit open and communicate the
   degraded dependency. Downstream callers should see normalized retryable
   errors rather than timeouts.

## Forced recovery

There is no admin endpoint to force-close a circuit. Clearing the Redis key is
possible but resets failure history and should only be done when the provider is
confirmed healthy and the open state is preventing a validated recovery:

```bash
redis-cli DEL integration-orchestrator:circuit:northstar
```

Prefer waiting for half-open probes.
