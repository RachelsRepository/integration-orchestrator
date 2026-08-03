"""The console entry point.

One command with subcommands, so a container image can run either role by
changing its arguments rather than its entrypoint:

    integration-orchestrator serve
    integration-orchestrator worker --only outbox retry
    integration-orchestrator token --roles operator
    integration-orchestrator config

Logging is configured before anything else runs, so even a failure during
container construction is emitted in the same structured format as the rest of
the service.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from collections.abc import Sequence

from integration_orchestrator.config.settings import Settings, get_settings
from integration_orchestrator.observability.logging import configure_logging
from integration_orchestrator.workers.runner import WORKER_NAMES, run_workers


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="integration-orchestrator",
        description="Run and inspect the Integration Orchestrator.",
    )
    subcommands = parser.add_subparsers(dest="command", required=True)

    serve = subcommands.add_parser("serve", help="Run the HTTP API.")
    serve.add_argument("--host", default="0.0.0.0", help="Bind address.")  # noqa: S104
    serve.add_argument("--port", type=int, default=8000, help="Bind port.")
    serve.add_argument(
        "--reload",
        action="store_true",
        help="Reload on source changes. Local development only.",
    )

    worker = subcommands.add_parser("worker", help="Run the background workers.")
    worker.add_argument(
        "--only",
        nargs="+",
        choices=WORKER_NAMES,
        help="Run only the named workers. Defaults to all of them.",
    )

    token = subcommands.add_parser("token", help="Mint a local bearer token.")
    token.add_argument("--subject", default="local-operator")
    token.add_argument("--roles", nargs="+", default=["operator"])
    token.add_argument("--ttl", type=int, default=None, help="Lifetime in seconds.")

    redrive = subcommands.add_parser(
        "outbox-redrive",
        help="Re-arm dead-lettered outbox events for another publication attempt.",
    )
    redrive.add_argument(
        "outbox_ids",
        nargs="+",
        help="Outbox row UUIDs to redrive (not domain event_ids).",
    )

    subcommands.add_parser(
        "reconcile-once",
        help="Run one reconciliation worker pass (Compose/CI probes).",
    )

    subcommands.add_parser("config", help="Print the effective configuration with secrets masked.")

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    settings = get_settings()
    configure_logging(
        level=settings.log_level,
        service=settings.service_name,
        environment=settings.environment.value,
        version=settings.service_version,
        console=settings.log_console_renderer,
    )

    if args.command == "serve":
        return _serve(settings, host=args.host, port=args.port, reload=args.reload)
    if args.command == "worker":
        return _worker(args.only)
    if args.command == "token":
        return _token(settings, subject=args.subject, roles=args.roles, ttl=args.ttl)
    if args.command == "outbox-redrive":
        return _outbox_redrive(args.outbox_ids)
    if args.command == "reconcile-once":
        return _reconcile_once()
    return _config(settings)


def _serve(settings: Settings, *, host: str, port: int, reload: bool) -> int:
    import uvicorn

    if reload and settings.environment.is_production_like:
        print("refusing to enable reload in a production-like environment", file=sys.stderr)
        return 2

    uvicorn.run(
        "integration_orchestrator.api.app:create_app",
        factory=True,
        host=host,
        port=port,
        reload=reload,
        # The service emits its own structured access log, and uvicorn's would
        # duplicate it in a different format.
        access_log=False,
        log_config=None,
    )
    return 0


def _worker(only: Sequence[str] | None) -> int:
    try:
        asyncio.run(run_workers(only))
    except KeyboardInterrupt:  # pragma: no cover - interactive use only
        return 130
    return 0


def _token(settings: Settings, *, subject: str, roles: Sequence[str], ttl: int | None) -> int:
    from integration_orchestrator.infrastructure.security.tokens import (
        ROLE_SCOPES,
        issue_local_token,
    )

    if settings.environment.is_production_like:
        print("refusing to mint a token in a production-like environment", file=sys.stderr)
        return 2

    unknown = [role for role in roles if role not in ROLE_SCOPES]
    if unknown:
        print(
            f"unknown role(s): {', '.join(unknown)}. Choose from: {', '.join(sorted(ROLE_SCOPES))}",
            file=sys.stderr,
        )
        return 2

    print(issue_local_token(settings.jwt, subject=subject, roles=list(roles), ttl_seconds=ttl))
    return 0


def _outbox_redrive(outbox_ids: Sequence[str]) -> int:
    from datetime import UTC, datetime
    from uuid import UUID

    from integration_orchestrator.composition import build_container

    async def _run() -> int:
        ids = [UUID(value) for value in outbox_ids]
        container = await build_container()
        try:
            async with container.uow_factory() as uow:
                count = await uow.outbox.redrive_dead_lettered(ids, now=datetime.now(tz=UTC))
                await uow.commit()
            print(json.dumps({"redriven": count, "requested": len(ids)}))
            return 0
        finally:
            await container.shutdown()

    try:
        return asyncio.run(_run())
    except ValueError as exc:
        print(f"invalid outbox id: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:  # pragma: no cover - operator tooling
        print(f"outbox redrive failed: {exc}", file=sys.stderr)
        return 1


def _reconcile_once() -> int:
    from integration_orchestrator.composition import build_container
    from integration_orchestrator.workers.reconciliation_worker import ReconciliationWorker

    async def _run() -> int:
        container = await build_container()
        try:
            worker = ReconciliationWorker(
                reconciliation=container.reconciliation,
                metrics=container.metrics,
                settings=container.settings.workers,
            )
            processed = await worker.run_once()
            print(json.dumps({"reconciled": processed}))
            return 0
        finally:
            await container.shutdown()

    try:
        return asyncio.run(_run())
    except Exception as exc:  # pragma: no cover - operator tooling
        print(f"reconcile-once failed: {exc}", file=sys.stderr)
        return 1


def _config(settings: Settings) -> int:
    print(json.dumps(settings.describe(), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover - process entry point
    sys.exit(main())
