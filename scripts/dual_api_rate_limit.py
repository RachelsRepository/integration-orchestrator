#!/usr/bin/env python3
"""Prove inbound rate limits are shared across two live API replicas."""

from __future__ import annotations

import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

API_A = os.environ.get("ORCHESTRATOR_API_A", "http://localhost:18100").rstrip("/")
API_B = os.environ.get("ORCHESTRATOR_API_B", "http://localhost:18101").rstrip("/")


class ProbeError(RuntimeError):
    pass


def _request(
    base: str,
    method: str,
    path: str,
    *,
    token: str | None = None,
    body: dict[str, Any] | None = None,
) -> tuple[int, Any]:
    url = f"{base}{path}"
    data = None if body is None else json.dumps(body).encode("utf-8")
    headers = {"Accept": "application/json"}
    if body is not None:
        headers["Content-Type"] = "application/json"
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = Request(url, data=data, headers=headers, method=method)
    try:
        with urlopen(req, timeout=20) as response:
            raw = response.read()
            payload = json.loads(raw.decode("utf-8")) if raw else None
            return response.status, payload
    except HTTPError as exc:
        raw = exc.read()
        payload = json.loads(raw.decode("utf-8")) if raw else None
        return exc.code, payload
    except URLError as exc:
        raise ProbeError(f"{method} {url} connect failed: {exc}") from exc


def mint(subject: str) -> str:
    from integration_orchestrator.config.settings import get_settings, reset_settings_cache
    from integration_orchestrator.infrastructure.security.tokens import issue_local_token

    reset_settings_cache()
    return issue_local_token(get_settings().jwt, subject=subject, roles=["operator"])


def wait_ready(base: str) -> None:
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        status, body = _request(base, "GET", "/health/ready")
        if status == 200 and isinstance(body, dict) and body.get("status") == "ready":
            return
        time.sleep(2)
    raise ProbeError(f"{base} never ready")


def _mutation(base: str, token: str, label: str) -> tuple[str, int, Any]:
    # Intentionally invalid payload so the handler fails fast after the
    # rate-limit check, emptying the shared bucket before refill catches up.
    status, body = _request(
        base,
        "POST",
        "/api/v1/integration-requests",
        token=token,
        body={"provider": "northstar"},
    )
    return label, status, body


def main() -> int:
    print(f"dual-api RL probe A={API_A} B={API_B}")
    wait_ready(API_A)
    wait_ready(API_B)

    token_a = mint("rl-tenant-a")
    token_b = mint("rl-tenant-b")

    ok = 0
    for i in range(6):
        base = API_A if i % 2 == 0 else API_B
        _label, status, _body = _mutation(base, token_a, "warm")
        if status in {200, 201, 202, 400, 422}:
            ok += 1
    print("warm alternating successes", ok)

    # Concurrent burst across both replicas to empty the shared token bucket
    # before refill (burst=40, rate=20/s) can keep up.
    rejected = 0
    accepted = 0
    distribution = {"A": 0, "B": 0}
    last_reject_base = API_A
    last_reject_body: Any = None
    jobs: list[tuple[str, str, str]] = []
    for i in range(80):
        base = API_A if i % 2 == 0 else API_B
        label = "A" if base == API_A else "B"
        jobs.append((base, token_a, label))

    with ThreadPoolExecutor(max_workers=32) as pool:
        futures = [pool.submit(_mutation, base, token, label) for base, token, label in jobs]
        for fut in as_completed(futures):
            label, status, body = fut.result()
            distribution[label] += 1
            if status == 429:
                rejected += 1
                last_reject_base = API_A if label == "A" else API_B
                last_reject_body = body
            elif status in {200, 201, 202, 400, 401, 403, 422}:
                accepted += 1
            else:
                raise ProbeError(f"unexpected status {status}: {body}")

    if rejected < 1:
        raise ProbeError(
            f"expected shared rate-limit rejections; accepted={accepted} rejected={rejected}"
        )
    print(
        "shared RL crossed",
        {"accepted": accepted, "rejected": rejected, "distribution": distribution},
    )
    print("sample reject", last_reject_base, last_reject_body)

    # Brief pause so any shared IP fallback bucket can refill; subject-scoped
    # tenants should already be independent.
    time.sleep(2)
    _label, status, body = _mutation(API_B, token_b, "other")
    if status == 429:
        # Retry once after refill window.
        time.sleep(3)
        _label, status, body = _mutation(API_B, token_b, "other")
    if status not in {200, 201, 202, 400, 422}:
        raise ProbeError(f"tenant isolation failed: {status} {body}")
    print("tenant isolation ok")

    status, _ = _request(API_A, "GET", "/api/v1/providers", token=token_a)
    if status != 200:
        raise ProbeError(f"reads unexpectedly limited: {status}")
    print("read class ok")

    time.sleep(3)
    _label, status, body = _mutation(API_B, token_a, "reset")
    if status not in {200, 201, 202, 400, 422, 429}:
        raise ProbeError(f"window reset unexpected {status} {body}")
    print("window reset probe", status)

    print("dual-api rate-limit probe passed")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ProbeError as exc:
        print(f"dual-api RL failed: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
