"""The retry worker.

Picks up requests whose scheduled retry time has arrived and dispatches them
again. The claim is atomic — the repository selects and locks in one statement —
so several replicas can poll simultaneously without two of them dispatching the
same request.

Retries run through the same dispatcher the API uses. A separate retry path would
be a second implementation of the same decisions, and the two would drift.

The provider call happens outside any transaction. Each request is processed
independently so that one provider timing out does not delay the rest of the
batch, and the concurrency is bounded so a large backlog cannot open more
provider connections than the bulkhead allows.
"""

from __future__ import annotations

import asyncio
import logging

from integration_orchestrator.application.dto.commands import Actor
from integration_orchestrator.application.ports.observability import MetricsSink
from integration_orchestrator.application.ports.system import Clock
from integration_orchestrator.application.ports.unit_of_work import UnitOfWorkFactory
from integration_orchestrator.application.services.dispatcher import RequestDispatcher
from integration_orchestrator.config.settings import WorkerSettings
from integration_orchestrator.domain.entities import IntegrationRequest
from integration_orchestrator.domain.enums import ActorType, RequestStatus
from integration_orchestrator.domain.errors import DomainError
from integration_orchestrator.observability.correlation import correlation_scope
from integration_orchestrator.workers.base import Worker

logger = logging.getLogger(__name__)

RETRY_ACTOR = Actor(type=ActorType.RETRY_WORKER)

#: Upper bound on requests dispatched in parallel within one batch. The provider
#: bulkhead is the real limit; this simply avoids creating hundreds of tasks that
#: would immediately queue behind it.
MAX_PARALLEL_DISPATCHES = 8


class RetryWorker(Worker):
    """Dispatches requests whose retry time has arrived."""

    name = "retry_worker"

    def __init__(
        self,
        *,
        uow_factory: UnitOfWorkFactory,
        dispatcher: RequestDispatcher,
        clock: Clock,
        metrics: MetricsSink,
        settings: WorkerSettings,
    ) -> None:
        super().__init__(
            poll_interval_seconds=settings.retry_poll_interval_seconds, metrics=metrics
        )
        self._uow_factory = uow_factory
        self._dispatcher = dispatcher
        self._clock = clock
        self._settings = settings
        self._semaphore = asyncio.Semaphore(MAX_PARALLEL_DISPATCHES)

    async def run_once(self) -> int:
        claimed = await self._claim()
        if not claimed:
            return 0

        logger.info(
            "claimed requests for retry",
            extra={"worker": self.name, "count": len(claimed)},
        )
        await asyncio.gather(*(self._dispatch(request) for request in claimed))
        return len(claimed)

    async def _claim(self) -> list[IntegrationRequest]:
        """Claim a batch and move each request into ``dispatching``.

        Claiming and transitioning share one transaction. If they did not, a
        crash between them would leave rows claimed but never dispatched, and
        nothing would pick them up again.
        """
        async with self._uow_factory() as uow:
            candidates = list(
                await uow.requests.claim_due_for_retry(
                    now=self._clock.now(), limit=self._settings.retry_batch_size
                )
            )
            claimed: list[IntegrationRequest] = []
            for request in candidates:
                if request.status is not RequestStatus.RETRY_SCHEDULED:
                    continue
                await self._dispatcher.claim_for_dispatch(uow, request, actor=RETRY_ACTOR)
                claimed.append(request)
            await uow.commit()
            return claimed

    async def _dispatch(self, request: IntegrationRequest) -> None:
        async with self._semaphore:
            with correlation_scope(
                correlation_id=request.correlation_id.value,
                integration_request_id=str(request.id),
            ):
                try:
                    await self._dispatcher.complete_attempt(request, actor=RETRY_ACTOR)
                except DomainError as exc:
                    # The dispatcher already converted provider failures into
                    # state changes, so reaching here means something structural
                    # went wrong. The request keeps its ``dispatching`` status and
                    # reconciliation will investigate it.
                    logger.error(
                        "a retry attempt could not be settled",
                        extra={
                            "worker": self.name,
                            "integration_request_id": str(request.id),
                            "provider": request.provider.value,
                            "error_code": exc.code,
                        },
                    )
                except Exception:
                    logger.exception(
                        "unexpected error while retrying a request",
                        extra={
                            "worker": self.name,
                            "integration_request_id": str(request.id),
                        },
                    )
