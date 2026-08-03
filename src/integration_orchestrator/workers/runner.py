"""The worker process entry point.

Runs all four background workers in one process. They are independent poll loops,
so a single event loop hosts them comfortably; splitting them across processes is
a scaling decision that can be made later by passing ``--only`` without changing
any code.

Shutdown is graceful and bounded. On SIGTERM every worker is asked to finish its
current batch, and the process waits up to the configured grace period before
cancelling. That ordering matters during a rolling deploy: a worker cancelled
mid-batch leaves claimed rows that nothing will look at until they age out,
whereas one allowed to finish leaves nothing behind.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys
from collections.abc import Sequence

from integration_orchestrator.composition import Container, build_container
from integration_orchestrator.config.settings import get_settings
from integration_orchestrator.workers.base import Worker
from integration_orchestrator.workers.heartbeat import WorkerHeartbeat
from integration_orchestrator.workers.outbox_publisher import OutboxPublisherWorker
from integration_orchestrator.workers.reconciliation_worker import ReconciliationWorker
from integration_orchestrator.workers.retry_worker import RetryWorker
from integration_orchestrator.workers.webhook_processor import WebhookProcessorWorker
from integration_orchestrator.workers.workflow_worker import WorkflowWorker

logger = logging.getLogger(__name__)

WORKER_NAMES = ("outbox", "retry", "webhooks", "reconciliation", "workflow")


def build_workers(container: Container, *, only: Sequence[str] | None = None) -> list[Worker]:
    """Construct the requested workers from an already-wired container."""
    settings = container.settings.workers
    available: dict[str, Worker] = {
        "outbox": OutboxPublisherWorker(
            uow_factory=container.uow_factory,
            publisher=container.publisher,
            clock=container.clock,
            metrics=container.metrics,
            settings=settings,
        ),
        "retry": RetryWorker(
            uow_factory=container.uow_factory,
            dispatcher=container.dispatcher,
            clock=container.clock,
            metrics=container.metrics,
            settings=settings,
        ),
        "webhooks": WebhookProcessorWorker(
            uow_factory=container.uow_factory,
            ingest=container.use_cases.ingest_webhook,
            journal=container.journal,
            clock=container.clock,
            metrics=container.metrics,
            settings=settings,
        ),
        "reconciliation": ReconciliationWorker(
            reconciliation=container.reconciliation,
            metrics=container.metrics,
            settings=settings,
        ),
        "workflow": WorkflowWorker(
            uow_factory=container.uow_factory,
            orchestrator=container.workflow_orchestrator,
            metrics=container.metrics,
            settings=settings,
        ),
    }

    selected = list(only) if only else list(WORKER_NAMES)
    unknown = [name for name in selected if name not in available]
    if unknown:
        raise SystemExit(
            f"unknown worker(s): {', '.join(unknown)}. Choose from: {', '.join(WORKER_NAMES)}"
        )
    return [available[name] for name in selected]


class WorkerRunner:
    """Supervises a set of workers and coordinates their shutdown."""

    def __init__(self, container: Container, workers: Sequence[Worker]) -> None:
        self._container = container
        self._workers = list(workers)
        self._tasks: list[asyncio.Task[None]] = []

    async def run(self) -> None:
        await self._container.startup()
        self._install_signal_handlers()
        heartbeat = WorkerHeartbeat(self._container.settings.workers)

        logger.info(
            "worker process started",
            extra={"workers": [worker.name for worker in self._workers]},
        )
        self._tasks = [
            asyncio.create_task(worker.run(), name=worker.name) for worker in self._workers
        ]
        self._tasks.append(asyncio.create_task(heartbeat.run(), name="heartbeat"))
        try:
            # A worker loop only returns when it is stopped, so a task finishing
            # early means something is wrong and the whole process should come
            # down rather than silently run degraded.
            done, _ = await asyncio.wait(self._tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                if task.get_name() == "heartbeat":
                    continue
                exc = task.exception()
                if exc is not None:
                    logger.error(
                        "a worker exited unexpectedly",
                        extra={"worker": task.get_name(), "error": type(exc).__name__},
                    )
        except asyncio.CancelledError:
            logger.info("worker process cancelled")
        finally:
            heartbeat.stop()
            await self.shutdown()

    async def shutdown(self) -> None:
        for worker in self._workers:
            worker.stop()

        grace = self._container.settings.workers.shutdown_grace_seconds
        pending = [task for task in self._tasks if not task.done()]
        if pending:
            _, still_running = await asyncio.wait(pending, timeout=grace)
            for task in still_running:
                logger.warning(
                    "cancelling a worker that did not stop within the grace period",
                    extra={"worker": task.get_name(), "grace_seconds": grace},
                )
                task.cancel()
            if still_running:
                await asyncio.gather(*still_running, return_exceptions=True)

        await self._container.shutdown()
        logger.info("worker process stopped")

    def _install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, self._request_stop, sig)
            except NotImplementedError:  # pragma: no cover - Windows has no add_signal_handler
                signal.signal(sig, lambda *_args, captured=sig: self._request_stop(captured))

    def _request_stop(self, sig: signal.Signals) -> None:
        logger.info("shutdown signal received", extra={"signal": sig.name})
        for worker in self._workers:
            worker.stop()


async def run_workers(only: Sequence[str] | None = None) -> None:
    container = await build_container(get_settings())
    runner = WorkerRunner(container, build_workers(container, only=only))
    await runner.run()


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the Integration Orchestrator workers.")
    parser.add_argument(
        "--only",
        nargs="+",
        choices=WORKER_NAMES,
        help="Run only the named workers. Defaults to all of them.",
    )
    args = parser.parse_args(argv)

    try:
        asyncio.run(run_workers(args.only))
    except KeyboardInterrupt:  # pragma: no cover - interactive use only
        return 130
    return 0


if __name__ == "__main__":  # pragma: no cover - process entry point
    sys.exit(main())
