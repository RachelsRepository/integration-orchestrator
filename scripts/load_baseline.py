#!/usr/bin/env python3
"""Baseline load harness against a live Compose stack.

This is deliberately a small, documented harness — not a capacity claim.
It measures create-request latency and success rate under a fixed concurrency
budget so operators can establish a baseline on their own hardware.

Usage (stack already up on host port 18100)::

    ORCHESTRATOR_BASE_URL=http://localhost:18100 \\
    JWT__SECRET=local-development-signing-secret-not-for-production \\
    ENVIRONMENT=local PYTHONPATH=src \\
    uv run python scripts/load_baseline.py --concurrency 8 --requests 40

Record the printed summary. Do not invent throughput numbers for marketing.
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

BASE_URL = os.environ.get("ORCHESTRATOR_BASE_URL", "http://localhost:18100").rstrip("/")


def mint_token() -> str:
    env_token = os.environ.get("ORCHESTRATOR_TOKEN")
    if env_token:
        return env_token
    from integration_orchestrator.config.settings import get_settings, reset_settings_cache
    from integration_orchestrator.infrastructure.security.tokens import issue_local_token

    reset_settings_cache()
    return issue_local_token(get_settings().jwt, subject="load-baseline", roles=["operator"])


def create_once(token: str) -> tuple[bool, float, int]:
    started = time.perf_counter()
    body = {
        "provider": "meridian",
        "operation_type": "resource_provision",
        "external_reference": f"load-{uuid.uuid4().hex[:12]}",
        "payload": {"resource_name": "load"},
    }
    data = json.dumps(body).encode("utf-8")
    request = Request(
        f"{BASE_URL}/api/v1/integration-requests",
        data=data,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Idempotency-Key": f"load-{uuid.uuid4()}",
        },
        method="POST",
    )
    try:
        with urlopen(request, timeout=30) as response:
            status = response.status
            response.read()
    except HTTPError as exc:
        status = exc.code
        exc.read()
    except URLError:
        return False, time.perf_counter() - started, 0
    elapsed = time.perf_counter() - started
    return status in {200, 201, 202}, elapsed, status


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--requests", type=int, default=20)
    args = parser.parse_args()

    token = mint_token()
    latencies: list[float] = []
    successes = 0
    statuses: dict[int, int] = {}

    with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        futures = [pool.submit(create_once, token) for _ in range(args.requests)]
        for future in as_completed(futures):
            ok, elapsed, status = future.result()
            latencies.append(elapsed)
            statuses[status] = statuses.get(status, 0) + 1
            if ok:
                successes += 1

    summary: dict[str, Any] = {
        "base_url": BASE_URL,
        "concurrency": args.concurrency,
        "requests": args.requests,
        "successes": successes,
        "success_rate": successes / args.requests if args.requests else 0.0,
        "latency_seconds": {
            "p50": statistics.median(latencies) if latencies else None,
            "p95": (
                statistics.quantiles(latencies, n=20)[18]
                if len(latencies) >= 20
                else max(latencies)
            )
            if latencies
            else None,
            "max": max(latencies) if latencies else None,
        },
        "http_statuses": statuses,
        "note": "Baseline only; not a capacity certification.",
    }
    print(json.dumps(summary, indent=2))
    # 429 under burst proves inbound rate limiting; count it as a valid outcome
    # for baseline harness purposes (not a capacity certification).
    accounted = successes + statuses.get(429, 0)
    return 0 if accounted == args.requests else 1


if __name__ == "__main__":
    raise SystemExit(main())
