"""Worker that advances runnable multi-step workflow executions."""

from __future__ import annotations

import logging

from integration_orchestrator.application.ports.observability import MetricsSink
from integration_orchestrator.application.ports.unit_of_work import UnitOfWorkFactory
from integration_orchestrator.application.services.workflow_orchestrator import (
    WorkflowOrchestrator,
)
from integration_orchestrator.config.settings import WorkerSettings
from integration_orchestrator.domain.enums import WorkflowStatus
from integration_orchestrator.workers.base import Worker

logger = logging.getLogger(__name__)


class WorkflowWorker(Worker):
    name = "workflow"

    def __init__(
        self,
        *,
        uow_factory: UnitOfWorkFactory,
        orchestrator: WorkflowOrchestrator,
        metrics: MetricsSink,
        settings: WorkerSettings,
    ) -> None:
        super().__init__(
            poll_interval_seconds=settings.workflow_poll_interval_seconds,
            metrics=metrics,
        )
        self._uow_factory = uow_factory
        self._orchestrator = orchestrator
        self._batch_size = settings.workflow_batch_size
        self._lease_seconds = settings.workflow_claim_lease_seconds
        self._clock = None  # set via composition if needed

    async def run_once(self) -> int:
        from datetime import UTC, datetime, timedelta

        now = datetime.now(tz=UTC)
        lease_until = now + timedelta(seconds=self._lease_seconds)
        async with self._uow_factory() as uow:
            claimed = list(
                await uow.workflow_executions.claim_runnable(
                    limit=self._batch_size, now=now, lease_until=lease_until
                )
            )
            await uow.commit()
        for execution in claimed:
            try:
                if execution.status is WorkflowStatus.COMPENSATING:
                    await self._orchestrator._continue_compensation(execution.id)
                else:
                    await self._orchestrator.advance(execution.id)
            except Exception:
                logger.exception(
                    "workflow advance failed",
                    extra={"workflow_execution_id": str(execution.id)},
                )
        return len(claimed)
