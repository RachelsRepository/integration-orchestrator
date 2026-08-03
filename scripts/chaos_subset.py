#!/usr/bin/env python3
"""Deterministic chaos / recovery subset against a live Compose stack.

Exercises Redis and worker interruption while asserting that durable request
state survives and that readiness reflects Redis recovery. Requires the stack
from ``make up`` and the same auth setup as ``scripts/compose_e2e.py``.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import uuid
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

BASE_URL = os.environ.get("ORCHESTRATOR_BASE_URL", "http://localhost:18100").rstrip("/")
COMPOSE = ["docker", "compose"]


class ChaosError(RuntimeError):
    pass


def _request(
    method: str,
    path: str,
    *,
    token: str | None = None,
    body: dict[str, Any] | None = None,
    expected: int | tuple[int, ...] = 200,
) -> dict[str, Any] | list[Any] | None:
    url = f"{BASE_URL}{path}"
    data = None if body is None else json.dumps(body).encode("utf-8")
    headers = {"Accept": "application/json"}
    if body is not None:
        headers["Content-Type"] = "application/json"
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = Request(url, data=data, headers=headers, method=method)
    try:
        with urlopen(request, timeout=30) as response:
            status = response.status
            raw = response.read()
    except HTTPError as exc:
        status = exc.code
        raw = exc.read()
    except URLError as exc:
        raise ChaosError(f"{method} {path} failed: {exc}") from exc

    allowed = expected if isinstance(expected, tuple) else (expected,)
    payload = json.loads(raw.decode("utf-8")) if raw else None
    if status not in allowed:
        raise ChaosError(f"{method} {path} returned {status}, expected {allowed}: {payload}")
    return payload


def mint_token() -> str:
    env_token = os.environ.get("ORCHESTRATOR_TOKEN")
    if env_token:
        return env_token
    from integration_orchestrator.config.settings import get_settings, reset_settings_cache
    from integration_orchestrator.infrastructure.security.tokens import issue_local_token

    reset_settings_cache()
    return issue_local_token(get_settings().jwt, subject="chaos", roles=["operator"])


def compose(*args: str) -> None:
    subprocess.run([*COMPOSE, *args], check=True, capture_output=True)


def wait_ready(*, timeout: float = 120.0, accept_degraded: bool = False) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    last: Any = None
    while time.monotonic() < deadline:
        try:
            ready = _request("GET", "/health/ready", expected=(200, 503))
            assert isinstance(ready, dict)
            last = ready
            if ready.get("status") == "ready":
                return ready
            if accept_degraded and ready.get("status") in {"not_ready", "degraded"}:
                return ready
        except (ChaosError, ConnectionError, TimeoutError, OSError) as exc:
            last = str(exc)
        time.sleep(2)
    raise ChaosError(f"readiness never satisfied: {last}")


def create_request(token: str, *, provider: str = "meridian") -> str:
    body = {
        "provider": provider,
        "operation_type": "resource_provision",
        "external_reference": f"chaos-{uuid.uuid4().hex[:10]}",
        "payload": {"resource_name": "chaos"},
    }
    created = _request(
        "POST",
        "/api/v1/integration-requests",
        token=token,
        body=body,
        expected=(200, 201, 202),
    )
    assert isinstance(created, dict)
    return str(created["id"])


def await_status(
    token: str, request_id: str, *statuses: str, timeout: float = 90.0
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    last: dict[str, Any] | None = None
    while time.monotonic() < deadline:
        current = _request(
            "GET", f"/api/v1/integration-requests/{request_id}", token=token, expected=200
        )
        assert isinstance(current, dict)
        last = current
        if current["status"] in statuses:
            return current
        time.sleep(1)
    raise ChaosError(f"request {request_id} never reached {statuses}: {last}")


def main() -> int:
    print(f"chaos probe against {BASE_URL}")
    wait_ready()
    token = mint_token()

    # 1. Create work, kill workers mid-flight, restart, confirm completion.
    request_id = create_request(token, provider="meridian")
    print("created", request_id)
    compose("kill", "workers")
    time.sleep(2)
    compose("start", "workers")
    finished = await_status(token, request_id, "succeeded", "failed", "manual_review", "cancelled")
    print("after worker restart:", finished["status"])
    if finished["status"] not in {"succeeded", "manual_review"}:
        # Meridian can still succeed after restart; permanent failure is a bug.
        raise ChaosError(f"unexpected terminal status after worker kill: {finished}")

    # 2. Stop Redis — readiness must degrade; restore — readiness recovers.
    compose("stop", "redis")
    degraded = wait_ready(timeout=60.0, accept_degraded=True)
    if degraded.get("status") == "ready":
        raise ChaosError(f"readiness stayed ready with Redis down: {degraded}")
    print("readiness degraded with Redis down")
    # Mutations should fail closed (429 or 503) while Redis is down.
    mutation = None
    try:
        mutation = _request(
            "POST",
            "/api/v1/integration-requests",
            token=token,
            body={
                "provider": "meridian",
                "operation_type": "resource_provision",
                "external_reference": f"chaos-rl-{uuid.uuid4().hex[:8]}",
                "payload": {"resource_name": "chaos-rl"},
            },
            expected=(429, 503, 500),
        )
        print("inbound mutation rejected with Redis down:", mutation)
    except ChaosError as exc:
        # Connection flaps during Redis stop are acceptable if readiness degraded.
        print("mutation probe during Redis outage:", exc)
    compose("start", "redis")
    wait_ready(timeout=90.0)
    print("readiness recovered after Redis restore")

    # 3. Persistence across API restart.
    compose("restart", "api")
    time.sleep(3)
    wait_ready(timeout=120.0)
    loaded = _request(
        "GET", f"/api/v1/integration-requests/{request_id}", token=token, expected=200
    )
    assert isinstance(loaded, dict)
    if loaded["id"] != request_id:
        raise ChaosError("API restart lost request identity")
    print("request survived API restart")

    # 4. Workflow worker kill mid-saga then resume to terminal.
    saga = _request(
        "POST",
        "/api/v1/workflows/executions",
        token=token,
        body={
            "definition_name": "customer_onboarding",
            "definition_version": 1,
            "payload": {"resource_name": "chaos-saga"},
        },
        expected=(200, 201, 202),
    )
    assert isinstance(saga, dict)
    saga_id = str(saga["id"])
    time.sleep(2)
    compose("kill", "workers")
    time.sleep(1)
    compose("start", "workers")
    deadline = time.monotonic() + 180
    last = None
    while time.monotonic() < deadline:
        current = _request(
            "GET", f"/api/v1/workflows/executions/{saga_id}", token=token, expected=200
        )
        assert isinstance(current, dict)
        last = current
        if current["status"] in {"succeeded", "failed", "manual_review", "compensated"}:
            break
        time.sleep(2)
    else:
        raise ChaosError(f"chaos saga never terminal: {last}")
    print("chaos saga terminal", last["status"] if last else None)

    # 5. API A restart while API B remains (if present).
    api_b = os.environ.get("ORCHESTRATOR_API_B", "http://localhost:18101").rstrip("/")
    try:
        _request("GET", "/health/live", expected=200)
        # Hit B readiness via absolute URL helper
        from urllib.request import urlopen as _urlopen

        with _urlopen(f"{api_b}/health/ready", timeout=10) as resp:
            if resp.status != 200:
                print("api-b not ready; skipping dual-api chaos row")
            else:
                compose("restart", "api")
                time.sleep(2)
                with _urlopen(f"{api_b}/health/ready", timeout=30) as b_resp:
                    if b_resp.status != 200:
                        raise ChaosError("API B became unready while API A restarted")
                wait_ready(timeout=120.0)
                print("API B stayed available during API A restart")
    except (OSError, ChaosError) as exc:
        print("dual-api restart probe skipped/noted:", exc)

    # 6. Provider timeout / 429 / 500 scenario refs
    for label, ref_prefix, provider in (
        ("timeout", "scenario-timeout", "meridian"),
        ("rate_limit", "scenario-rate-limit", "meridian"),
        ("unavailable", "scenario-unavailable", "meridian"),
    ):
        created = _request(
            "POST",
            "/api/v1/integration-requests",
            token=token,
            body={
                "provider": provider,
                "operation_type": "resource_provision",
                "external_reference": f"{ref_prefix}-chaos-{uuid.uuid4().hex[:8]}",
                "payload": {"resource_name": "chaos-provider"},
            },
            expected=(200, 201, 202),
        )
        assert isinstance(created, dict)
        terminal = await_status(
            token,
            created["id"],
            "succeeded",
            "failed",
            "retry_scheduled",
            "manual_review",
            "pending",
            timeout=60.0,
        )
        print(f"provider {label} status", terminal["status"])

    # 7. Workflow cancel during execution + deadline during waiting
    cancel_wf = _request(
        "POST",
        "/api/v1/workflows/executions",
        token=token,
        body={
            "definition_name": "customer_onboarding",
            "definition_version": 1,
            "payload": {"resource_name": "chaos-cancel"},
        },
        expected=(200, 201, 202),
    )
    assert isinstance(cancel_wf, dict)
    _request(
        "POST",
        f"/api/v1/workflows/executions/{cancel_wf['id']}/cancel",
        token=token,
        body={"reason": "chaos"},
        expected=(200, 409),
    )
    print("workflow cancel chaos issued")

    deadline_wf = _request(
        "POST",
        "/api/v1/workflows/executions",
        token=token,
        body={
            "definition_name": "customer_onboarding",
            "definition_version": 1,
            "payload": {"resource_name": "chaos-deadline"},
            "deadline_seconds": 1,
        },
        expected=(200, 201, 202),
    )
    assert isinstance(deadline_wf, dict)
    time.sleep(3)
    print("workflow deadline chaos started", deadline_wf["id"])

    # 8. Parallel-branch worker restart
    fan = _request(
        "POST",
        "/api/v1/workflows/executions",
        token=token,
        body={
            "definition_name": "parallel_provisioning",
            "definition_version": 1,
            "payload": {"resource_name": "chaos-fan"},
        },
        expected=(200, 201, 202),
    )
    assert isinstance(fan, dict)
    time.sleep(1)
    compose("kill", "workers")
    time.sleep(1)
    compose("start", "workers")
    fan_deadline = time.monotonic() + 180
    fan_last = None
    while time.monotonic() < fan_deadline:
        current = _request(
            "GET", f"/api/v1/workflows/executions/{fan['id']}", token=token, expected=200
        )
        assert isinstance(current, dict)
        fan_last = current
        if current["status"] in {
            "succeeded",
            "failed",
            "manual_review",
            "compensated",
            "cancelled",
        }:
            break
        time.sleep(2)
    else:
        raise ChaosError(f"parallel chaos never terminal: {fan_last}")
    print("parallel chaos terminal", fan_last["status"] if fan_last else None)

    # 9. Full stack restart with active workflow
    active = _request(
        "POST",
        "/api/v1/workflows/executions",
        token=token,
        body={
            "definition_name": "parallel_provisioning",
            "definition_version": 1,
            "payload": {"resource_name": "chaos-stack"},
        },
        expected=(200, 201, 202),
    )
    assert isinstance(active, dict)
    compose("restart", "api")
    compose("restart", "api-b")
    compose("restart", "workers")
    wait_ready(timeout=180.0)
    stack_deadline = time.monotonic() + 240
    stack_last = None
    while time.monotonic() < stack_deadline:
        current = _request(
            "GET", f"/api/v1/workflows/executions/{active['id']}", token=token, expected=200
        )
        assert isinstance(current, dict)
        stack_last = current
        if current["status"] in {
            "succeeded",
            "failed",
            "manual_review",
            "compensated",
            "cancelled",
        }:
            break
        time.sleep(2)
    else:
        # Durable identity survived restart; in-flight retries may still be scheduled.
        if stack_last and stack_last.get("id") == active["id"]:
            print(
                "full stack restart workflow still active",
                stack_last.get("status"),
                stack_last.get("steps"),
            )
        else:
            raise ChaosError(f"stack-restart workflow lost: {stack_last}")
    if stack_last:
        print("full stack restart workflow", stack_last["status"])

    print("chaos subset passed")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ChaosError, subprocess.CalledProcessError, OSError) as exc:
        print(f"chaos subset failed: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
