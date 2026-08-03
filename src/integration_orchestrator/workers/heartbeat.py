"""File-based worker heartbeat for Compose healthchecks."""

from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path

from integration_orchestrator.config.settings import WorkerSettings

logger = logging.getLogger(__name__)


class WorkerHeartbeat:
    def __init__(self, settings: WorkerSettings) -> None:
        self._path = Path(settings.heartbeat_path)
        self._interval = settings.heartbeat_interval_seconds
        self._stopping = asyncio.Event()

    async def run(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        while not self._stopping.is_set():
            self.beat()
            try:
                await asyncio.wait_for(self._stopping.wait(), timeout=self._interval)
            except TimeoutError:
                continue

    def beat(self) -> None:
        self._path.write_text(str(time.time()), encoding="utf-8")

    def stop(self) -> None:
        self._stopping.set()


def heartbeat_is_fresh(settings: WorkerSettings, *, now: float | None = None) -> bool:
    path = Path(settings.heartbeat_path)
    if not path.exists():
        return False
    try:
        stamped = float(path.read_text(encoding="utf-8").strip())
    except ValueError:
        return False
    age = (now if now is not None else time.time()) - stamped
    return age <= settings.heartbeat_stale_after_seconds
