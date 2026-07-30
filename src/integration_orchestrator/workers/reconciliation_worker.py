"""The reconciliation worker.

Finds requests that stopped moving and asks the provider what actually happened.
Something always ends up in this state: a webhook is lost, a dispatch times out
after the provider committed, a process dies between the call and the settle.

The worker itself is a thin loop. Every decision — what counts as stale, what a
provider status means, when to escalate rather than guess — lives in
:class:`~integration_orchestrator.application.services.reconciliation.ReconciliationService`,
which is where it can be unit tested without a clock or a database.
"""

from __future__ import annotations

import logging

from integration_orchestrator.application.ports.observability import MetricsSink
from integration_orchestrator.application.services.reconciliation import ReconciliationService
from integration_orchestrator.config.settings import WorkerSettings
from integration_orchestrator.domain.errors import DomainError
from integration_orchestrator.observability.correlation import correlation_scope
from integration_orchestrator.workers.base import Worker

logger = logging.getLogger(__name__)


class ReconciliationWorker(Worker):
    """Periodically reconciles stale in-flight requests against providers."""

    name = "reconciliation_worker"

    def __init__(
        self,
        *,
        reconciliation: ReconciliationService,
        metrics: MetricsSink,
        settings: WorkerSettings,
    ) -> None:
        super().__init__(
            poll_interval_seconds=settings.reconciliation_interval_seconds, metrics=metrics
        )
        self._reconciliation = reconciliation
        self._settings = settings

    async def run_once(self) -> int:
        candidates = await self._reconciliation.find_candidates(
            limit=self._settings.reconciliation_batch_size
        )
        if not candidates:
            return 0

        logger.info(
            "reconciling stale requests",
            extra={"worker": self.name, "count": len(candidates)},
        )

        changed = 0
        for request in candidates:
            # Sequential on purpose. Reconciliation is a background correctness
            # sweep, and hitting a provider that is already suspect with a burst
            # of concurrent status lookups is how a slow provider becomes a
            # failing one.
            with correlation_scope(
                correlation_id=request.correlation_id.value,
                integration_request_id=str(request.id),
            ):
                try:
                    outcome = await self._reconciliation.reconcile(request)
                except DomainError as exc:
                    logger.warning(
                        "could not reconcile a request",
                        extra={
                            "worker": self.name,
                            "integration_request_id": str(request.id),
                            "provider": request.provider.value,
                            "error_code": exc.code,
                        },
                    )
                    continue
                except Exception:
                    logger.exception(
                        "unexpected error while reconciling a request",
                        extra={
                            "worker": self.name,
                            "integration_request_id": str(request.id),
                        },
                    )
                    continue

            if outcome.changed:
                changed += 1
                logger.info(
                    "reconciliation changed a request's state",
                    extra={
                        "worker": self.name,
                        "integration_request_id": str(request.id),
                        "provider": request.provider.value,
                        "action": outcome.action,
                        "previous_status": outcome.previous_status.value,
                        "new_status": outcome.new_status.value,
                    },
                )

        logger.info(
            "reconciliation sweep complete",
            extra={"worker": self.name, "examined": len(candidates), "changed": changed},
        )
        return len(candidates)
