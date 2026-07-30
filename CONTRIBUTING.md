# Contributing

This repository is maintained as an independent portfolio project. Contributions
are welcome as discussion of the design; there is no expectation of production
support or a published release cadence.

## Local setup

1. Install Python 3.12+ and [uv](https://docs.astral.sh/uv/).
2. Copy `.env.example` to `.env`.
3. Run `make install`.
4. Optionally start dependencies with `make up`.

## Before opening a change

```bash
make format
make verify
```

`make verify` runs formatting checks, Ruff, MyPy, import-boundary contracts, and
the unit/contract/e2e suites. Integration tests require a running Docker daemon
and are covered by `make test-integration`.

## Architecture rules

- Put business rules in `domain/`. It may not import FastAPI, SQLAlchemy, Redis,
  Kafka, httpx, or any outer package layer.
- Put orchestration in `application/`. It talks to ports, never to adapters.
- Put adapters in `infrastructure/`. Wire them only in `composition.py`.
- Do not put provider-specific branching in use cases. Capability differences
  belong on the provider descriptor and in the adapter.
- Do not publish Kafka events inside a database transaction. Persist to the
  outbox in the same transaction as the state change; publish afterwards.
- Every meaningful state change must atomically persist the integration request,
  an audit event, and an outbox event.
- Persist webhook receipts before attempting correlation.
- Prefer deterministic sandbox scenarios over random failure injection.

`make boundaries` and `tests/unit/test_architecture.py` enforce the layering.

## Tests

| Suite | What it covers | Needs |
|---|---|---|
| `tests/unit` | Domain, use cases, policies, workers with in-memory doubles | Nothing |
| `tests/contract` | Real adapters against the ASGI provider sandbox | Nothing |
| `tests/e2e` | Full API + workers with in-memory persistence | Nothing |
| `tests/integration` | PostgreSQL repositories, Alembic, Redis Lua scripts | Docker or `TEST_*_URL` |

Name tests as statements about behaviour:

```python
async def test_a_forged_webhook_is_rejected_without_side_effects(...):
    ...
```

## Commit messages

Write the commit message in the imperative mood, describing why the change
exists rather than restating the diff. Do not add co-author trailers or tool
attribution.

## Security

Never commit real credentials. Local placeholders in `.env.example` are
deliberate and are rejected by the production-safety validator. Report sensitive
findings privately rather than opening a public issue with exploit detail.
