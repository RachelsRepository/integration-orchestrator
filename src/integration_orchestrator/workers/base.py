"""Shared worker scaffolding.

Every background worker is a poll loop, and every poll loop needs the same four
properties: it stops promptly when asked, it never dies because one batch raised,
it does not spin when there is nothing to do, and it reports how long its batches
take. Putting that here means each worker only has to implement the interesting
part.

The loop sleeps only when a batch comes back empty or short. A full batch is
followed immediately by another, so a backlog drains at the speed of the
database rather than at the speed of the poll interval.
"""

from __future__ import annotations

import asyncio
import logging
import time
from abc import ABC, abstractmethod

from integration_orchestrator.application.ports.observability import MetricsSink
from integration_orchestrator.observability.correlation import correlation_scope

logger = logging.getLogger(__name__)

#: Multiplier applied to the poll interval after a batch fails. Keeps a worker
#: from hammering a database or broker that is already struggling.
BACKOFF_MULTIPLIER = 4.0
MAX_ERROR_SLEEP_SECONDS = 30.0


class Worker(ABC):
    """A cancellable poll loop with metrics and error isolation."""

    #: Name used in logs, metric labels and the runner's task table.
    name: str = "worker"

    def __init__(self, *, poll_interval_seconds: float, metrics: MetricsSink) -> None:
        self._poll_interval = poll_interval_seconds
        self._metrics = metrics
        self._stopping = asyncio.Event()
        self._consecutive_errors = 0

    @abstractmethod
    async def run_once(self) -> int:
        """Process one batch and return how many items were handled."""

    async def run(self) -> None:
        """Poll until stopped."""
        logger.info(
            "worker started",
            extra={"worker": self.name, "poll_interval_seconds": self._poll_interval},
        )
        while not self._stopping.is_set():
            processed = await self._run_batch()
            if self._stopping.is_set():
                break
            await self._pause(processed)
        logger.info("worker stopped", extra={"worker": self.name})

    async def _run_batch(self) -> int:
        started = time.perf_counter()
        try:
            with correlation_scope():
                processed = await self.run_once()
        except asyncio.CancelledError:
            raise
        except Exception:
            self._consecutive_errors += 1
            self._metrics.increment("worker_batch_failures_total", labels={"worker": self.name})
            logger.exception(
                "worker batch failed",
                extra={"worker": self.name, "consecutive_errors": self._consecutive_errors},
            )
            return 0
        else:
            self._consecutive_errors = 0
            self._metrics.observe(
                "worker_batch_duration_seconds",
                time.perf_counter() - started,
                labels={"worker": self.name},
            )
            return processed

    async def _pause(self, processed: int) -> None:
        """Wait before the next batch, unless the last one was full."""
        if self._consecutive_errors:
            delay = min(
                self._poll_interval * BACKOFF_MULTIPLIER * self._consecutive_errors,
                MAX_ERROR_SLEEP_SECONDS,
            )
        elif processed > 0:
            # Work is available; go straight round again but yield first so
            # cancellation and other tasks still get a turn.
            delay = 0.0
        else:
            delay = self._poll_interval

        try:
            # Waiting on the stop event rather than sleeping means shutdown is
            # immediate instead of taking up to one poll interval.
            await asyncio.wait_for(self._stopping.wait(), timeout=delay or 0.001)
        except TimeoutError:
            return

    def stop(self) -> None:
        """Ask the loop to finish after the current batch."""
        self._stopping.set()

    @property
    def stopping(self) -> bool:
        return self._stopping.is_set()
