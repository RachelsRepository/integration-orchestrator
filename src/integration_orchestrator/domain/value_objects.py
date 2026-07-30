"""Immutable domain value objects.

Value objects carry their own validation so that an invalid value cannot exist
anywhere in the system. Once a :class:`ProviderSlug` has been constructed, no
downstream code needs to re-check its shape.
"""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from dataclasses import dataclass
from typing import Any, Self

from integration_orchestrator.domain.errors import ValidationError

_PROVIDER_SLUG_PATTERN = re.compile(r"^[a-z][a-z0-9_-]{1,39}$")
_IDEMPOTENCY_KEY_PATTERN = re.compile(r"^[A-Za-z0-9._:@/-]{8,255}$")
_MAX_EXTERNAL_REFERENCE_LENGTH = 255


@dataclass(frozen=True, slots=True)
class ProviderSlug:
    """Stable identifier for a provider.

    Providers are values rather than an enum on purpose. The domain does not
    know that "northstar" exists; it only knows that a request targets *some*
    provider. Registering a new adapter is therefore a configuration and
    infrastructure change, never a domain change.
    """

    value: str

    def __post_init__(self) -> None:
        if not _PROVIDER_SLUG_PATTERN.match(self.value):
            raise ValidationError(
                "a provider slug must be lowercase alphanumeric with dashes or "
                "underscores, 2 to 40 characters",
                metadata={"provider": self.value},
            )

    @classmethod
    def parse(cls, value: str) -> Self:
        """Normalize and validate caller-supplied provider identity."""
        if not isinstance(value, str) or not value.strip():
            raise ValidationError("a provider must be supplied")
        return cls(value.strip().lower())

    def __str__(self) -> str:
        return self.value


@dataclass(frozen=True, slots=True)
class CorrelationId:
    """Identifier threaded through every log line, span, event and audit row.

    One correlation id spans the whole causal chain: the inbound API call, the
    provider dispatch, the webhook that completes it, and every event published
    along the way.
    """

    value: str

    def __post_init__(self) -> None:
        if not self.value or len(self.value) > 128:
            raise ValidationError("a correlation id must be 1 to 128 characters")

    @classmethod
    def generate(cls) -> Self:
        return cls(str(uuid.uuid4()))

    @classmethod
    def parse(cls, value: str | None) -> Self:
        """Accept an inbound correlation id, or mint one when absent."""
        if value is None or not value.strip():
            return cls.generate()
        return cls(value.strip()[:128])

    def __str__(self) -> str:
        return self.value


@dataclass(frozen=True, slots=True)
class ExternalReference:
    """The caller's own identifier for whatever this request is about.

    Opaque to the platform: it is stored, indexed and echoed back, never parsed.
    """

    value: str

    def __post_init__(self) -> None:
        if not self.value or not self.value.strip():
            raise ValidationError("an external reference must not be empty")
        if len(self.value) > _MAX_EXTERNAL_REFERENCE_LENGTH:
            raise ValidationError(
                f"an external reference must be at most {_MAX_EXTERNAL_REFERENCE_LENGTH} characters"
            )

    def __str__(self) -> str:
        return self.value


@dataclass(frozen=True, slots=True)
class IdempotencyKey:
    """A client-supplied key that makes request creation safely repeatable."""

    value: str

    def __post_init__(self) -> None:
        if not _IDEMPOTENCY_KEY_PATTERN.match(self.value):
            raise ValidationError(
                "an idempotency key must be 8 to 255 characters using letters, "
                "digits, or the characters . _ : @ / -",
            )

    @classmethod
    def parse(cls, value: str | None) -> Self | None:
        if value is None or not value.strip():
            return None
        return cls(value.strip())

    def __str__(self) -> str:
        return self.value


@dataclass(frozen=True, slots=True)
class RequestFingerprint:
    """A canonical hash of a creation request.

    Two requests that differ only in key ordering or float formatting must
    produce the same fingerprint, otherwise a client that serialises its JSON
    slightly differently on retry would be told its idempotency key conflicts.
    Canonicalisation therefore sorts keys, removes insignificant whitespace, and
    normalises numeric types before hashing.
    """

    value: str

    def __post_init__(self) -> None:
        if len(self.value) != 64:
            raise ValidationError("a request fingerprint must be a 64 character SHA-256 digest")

    @classmethod
    def of(
        cls,
        *,
        provider: ProviderSlug,
        operation_type: str,
        external_reference: ExternalReference,
        payload: dict[str, Any],
    ) -> Self:
        canonical = canonical_json(
            {
                "provider": provider.value,
                "operation_type": operation_type,
                "external_reference": external_reference.value,
                "payload": payload,
            }
        )
        return cls(hashlib.sha256(canonical.encode("utf-8")).hexdigest())

    def __str__(self) -> str:
        return self.value


def canonical_json(value: Any) -> str:
    """Render a value as canonical JSON.

    Keys are sorted, separators are tight, non-ASCII is escaped, and integral
    floats collapse to integers so that ``1.0`` and ``1`` hash identically.
    """
    return json.dumps(
        _normalize(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )


def _normalize(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _normalize(item) for key, item in sorted(value.items(), key=_by_key)}
    if isinstance(value, (list, tuple)):
        return [_normalize(item) for item in value]
    if isinstance(value, bool):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return value


def _by_key(item: tuple[Any, Any]) -> str:
    return str(item[0])


@dataclass(frozen=True, slots=True)
class SignatureMetadata:
    """Non-secret facts about how an inbound webhook was signed.

    Signature values themselves are never persisted: storing them would create a
    durable copy of material that only ever needed to be verified once, and would
    let anyone with database read access replay a request against a system that
    trusts the same secret.
    """

    scheme: str
    key_id: str | None = None
    timestamp: str | None = None
    algorithm: str | None = None
    verified: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "scheme": self.scheme,
            "key_id": self.key_id,
            "timestamp": self.timestamp,
            "algorithm": self.algorithm,
            "verified": self.verified,
        }
