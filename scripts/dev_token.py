"""Mint a bearer token for local development.

Exists so the demonstration script and manual `curl` sessions do not need an
identity provider. It refuses to run outside local and test environments and it
only works with HS256, so it cannot become a way of issuing production
credentials.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence

from integration_orchestrator.config.settings import get_settings
from integration_orchestrator.infrastructure.security.tokens import ROLE_SCOPES, issue_local_token


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Print a development bearer token.")
    parser.add_argument("--subject", default="local-operator", help="Token subject claim.")
    parser.add_argument(
        "--roles",
        nargs="+",
        default=["operator"],
        choices=sorted(ROLE_SCOPES),
        help="Roles to grant. Scopes are derived from them.",
    )
    parser.add_argument("--ttl", type=int, default=None, help="Lifetime in seconds.")
    args = parser.parse_args(argv)

    settings = get_settings()
    if settings.environment.is_production_like:
        print(
            "refusing to mint a token in a production-like environment",
            file=sys.stderr,
        )
        return 2

    print(
        issue_local_token(
            settings.jwt,
            subject=args.subject,
            roles=list(args.roles),
            ttl_seconds=args.ttl,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
