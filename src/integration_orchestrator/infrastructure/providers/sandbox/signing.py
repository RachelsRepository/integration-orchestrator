"""Deterministic signing material for the provider sandbox.

The keys here are generated from fixed seeds so that every developer, CI run and
test fixture produces byte-identical signatures. That is what makes the sandbox
useful for contract tests: a recorded webhook body has one correct signature, not
a different one on every run.

These are sandbox credentials. They authenticate nothing real, they are checked
into the repository on purpose, and the settings validator refuses to start a
production-like environment with the sandbox enabled.
"""

from __future__ import annotations

import base64
from functools import lru_cache

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

# Exactly 32 bytes, as required for an Ed25519 seed.
COBALT_SIGNING_SEED = b"cobalt-network-sandbox-seed-0001"
COBALT_KEY_ID = "cobalt-sandbox-2026-01"

NORTHSTAR_WEBHOOK_SECRET = "northstar-local-webhook-secret"
MERIDIAN_WEBHOOK_SECRET = "meridian-local-webhook-secret"

NORTHSTAR_CLIENT_ID = "northstar-local-client"
NORTHSTAR_CLIENT_SECRET = "northstar-local-secret"
MERIDIAN_API_KEY = "meridian-local-api-key"
COBALT_CLIENT_ID = "cobalt-local-client"
COBALT_CLIENT_SECRET = "cobalt-local-secret"


@lru_cache(maxsize=1)
def cobalt_private_key() -> Ed25519PrivateKey:
    """The sandbox's deterministic Ed25519 signing key."""
    return Ed25519PrivateKey.from_private_bytes(COBALT_SIGNING_SEED)


@lru_cache(maxsize=1)
def cobalt_public_key_b64() -> str:
    """Base64 of the raw 32-byte public key, matching what the adapter expects."""
    from cryptography.hazmat.primitives.serialization import (
        Encoding,
        PublicFormat,
    )

    raw = (
        cobalt_private_key()
        .public_key()
        .public_bytes(encoding=Encoding.Raw, format=PublicFormat.Raw)
    )
    return base64.b64encode(raw).decode("ascii")


def cobalt_sign(*, key_id: str, timestamp: str, body: bytes) -> str:
    """Produce the signature the Cobalt adapter verifies."""
    message = b".".join([key_id.encode("utf-8"), timestamp.encode("utf-8"), body])
    return base64.b64encode(cobalt_private_key().sign(message)).decode("ascii")
