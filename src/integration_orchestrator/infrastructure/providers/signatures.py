"""Webhook signature verification primitives.

Three properties matter and are easy to get subtly wrong:

*Constant-time comparison.* Comparing signatures with ``==`` leaks how many
leading bytes matched through timing, which is enough to forge a signature byte
by byte. Every comparison here uses :func:`hmac.compare_digest`.

*Signing over the exact bytes received.* The signature covers the raw request
body. Parsing the JSON and re-serialising it before verification changes
whitespace and key order, so a valid signature stops matching and — worse — an
implementation that "fixes" this by verifying the re-serialised form can be
tricked by a payload that parses differently than it verifies.

*A timestamp inside the signed material.* Signing the body alone means a captured
webhook is replayable forever. Binding a timestamp into the signed string and
rejecting old timestamps bounds the replay window; providers that omit a
timestamp are protected instead by event-id deduplication, which is weaker
because it depends on storage rather than cryptography.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
from datetime import UTC, datetime

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

MAX_TIMESTAMP_SKEW_SECONDS = 300


class SignatureFailure(Exception):
    """Raised internally when verification fails. Never surfaced to callers verbatim."""


def compute_hmac_sha256(*, secret: str, message: bytes) -> str:
    """Return the lowercase hex HMAC-SHA256 of ``message``."""
    return hmac.new(secret.encode("utf-8"), message, hashlib.sha256).hexdigest()


def verify_hmac_sha256(*, secret: str, message: bytes, signature: str) -> bool:
    """Verify a hex-encoded HMAC-SHA256 signature in constant time."""
    expected = compute_hmac_sha256(secret=secret, message=message)
    candidate = signature.strip()
    # Providers vary on whether they prefix the algorithm. Accept both forms.
    if "=" in candidate:
        _, _, candidate = candidate.partition("=")
    return hmac.compare_digest(expected, candidate.strip().lower())


def sign_ed25519(*, private_key_bytes: bytes, message: bytes) -> str:
    """Sign a message with a raw Ed25519 private key, returning base64.

    Used only by the deterministic provider sandbox, which needs to produce
    signatures the Cobalt adapter will accept.
    """
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

    key = Ed25519PrivateKey.from_private_bytes(private_key_bytes)
    return base64.b64encode(key.sign(message)).decode("ascii")


def verify_ed25519(*, public_key_b64: str, message: bytes, signature_b64: str) -> bool:
    """Verify a base64 Ed25519 signature against a raw base64 public key.

    Asymmetric verification means the orchestrator holds only a public key. Even
    a complete compromise of this service does not let an attacker forge webhooks
    that other consumers of the same provider would accept — which is not true of
    a shared HMAC secret.
    """
    try:
        public_key = Ed25519PublicKey.from_public_bytes(base64.b64decode(public_key_b64))
        public_key.verify(base64.b64decode(signature_b64), message)
    except (InvalidSignature, ValueError, binascii.Error, TypeError):
        return False
    return True


def parse_timestamp(value: str | None) -> datetime | None:
    """Parse a webhook timestamp header expressed as Unix seconds or ISO-8601."""
    if not value:
        return None
    stripped = value.strip()
    try:
        return datetime.fromtimestamp(float(stripped), tz=UTC)
    except (ValueError, OverflowError, OSError):
        pass
    try:
        parsed = datetime.fromisoformat(stripped.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def within_replay_window(
    timestamp: datetime | None,
    *,
    now: datetime,
    window_seconds: int = MAX_TIMESTAMP_SKEW_SECONDS,
) -> bool:
    """Report whether a signed timestamp is recent enough to accept.

    The window is checked in both directions. A timestamp far in the future is as
    suspicious as one far in the past, and accepting it would let an attacker
    mint a webhook that stays valid indefinitely.
    """
    if timestamp is None:
        return False
    delta = abs((now - timestamp).total_seconds())
    return delta <= window_seconds


def signature_digest(signature: str) -> str:
    """Return a short digest of a signature, safe to use as a replay cache key.

    The signature itself is never stored: a stored signature is a reusable
    credential for any system trusting the same secret. A hash is enough to
    recognise a repeat.
    """
    return hashlib.sha256(signature.encode("utf-8")).hexdigest()[:32]
