"""Redaction of sensitive values.

Two rules govern what leaves the process:

*Deny by key name.* Anything whose key looks like a credential, a token, a
signature, or personal contact information is replaced. Matching is on a
normalised key so ``client_secret``, ``clientSecret`` and ``CLIENT-SECRET`` are
all caught.

*Never trust that a payload is safe.* Provider payloads are arbitrary and change
without notice, so redaction runs over the whole structure rather than over a
list of fields someone remembered to enumerate.

Redaction is applied before anything is written to a log, an audit row, an event
payload, or a trace attribute. It is intentionally lossy: a redacted value cannot
be recovered from the output, which is the point.
"""

from __future__ import annotations

import re
from typing import Any

REDACTED = "[redacted]"

# Substrings that mark a key as sensitive.
_SENSITIVE_KEY_FRAGMENTS: frozenset[str] = frozenset(
    {
        "password",
        "passwd",
        "secret",
        "token",
        "apikey",
        "api_key",
        "authorization",
        "auth",
        "credential",
        "signature",
        "privatekey",
        "private_key",
        "sessionid",
        "cookie",
        "ssn",
        "taxid",
        "cardnumber",
        "cvv",
        "iban",
        "accountnumber",
        "email",
        "phone",
        "address",
        "dateofbirth",
        "date_of_birth",
    }
)

# Keys that contain one of the fragments above but are not actually sensitive.
_ALLOWED_KEYS: frozenset[str] = frozenset(
    {
        "authtype",
        "auth_type",
        "authenticationtype",
        "authentication_type",
        "tokentype",
        "token_type",
        "hasapikey",
        "signaturescheme",
        "signature_scheme",
        "emaildomain",
    }
)

_NON_ALNUM = re.compile(r"[^a-z0-9]")

# Beyond this depth, structures are summarised rather than walked. Provider
# payloads are occasionally deeply nested, and an unbounded walk on a hostile or
# accidental cycle is a denial-of-service vector in the logging path.
MAX_DEPTH = 6
MAX_SEQUENCE_ITEMS = 50
MAX_STRING_LENGTH = 512


def is_sensitive_key(key: str) -> bool:
    """Report whether a key name should have its value redacted."""
    normalised = _NON_ALNUM.sub("", key.lower())
    if normalised in _ALLOWED_KEYS:
        return False
    if any(allowed == normalised for allowed in _ALLOWED_KEYS):
        return False
    return any(fragment.replace("_", "") in normalised for fragment in _SENSITIVE_KEY_FRAGMENTS)


def redact(value: Any, *, depth: int = 0) -> Any:
    """Return a copy of ``value`` with sensitive content replaced."""
    if depth >= MAX_DEPTH:
        return _summarise(value)

    if isinstance(value, dict):
        redacted: dict[str, Any] = {}
        for key, item in value.items():
            key_text = str(key)
            if is_sensitive_key(key_text):
                redacted[key_text] = REDACTED
            else:
                redacted[key_text] = redact(item, depth=depth + 1)
        return redacted

    if isinstance(value, (list, tuple)):
        items = list(value)[:MAX_SEQUENCE_ITEMS]
        rendered = [redact(item, depth=depth + 1) for item in items]
        if len(value) > MAX_SEQUENCE_ITEMS:
            rendered.append(f"[{len(value) - MAX_SEQUENCE_ITEMS} more items omitted]")
        return rendered

    if isinstance(value, str) and len(value) > MAX_STRING_LENGTH:
        return value[:MAX_STRING_LENGTH] + "...[truncated]"

    return value


def redact_headers(headers: dict[str, str]) -> dict[str, str]:
    """Redact HTTP headers, which are flat but frequently carry credentials."""
    return {key: (REDACTED if is_sensitive_key(key) else value) for key, value in headers.items()}


def mask_secret(value: str | None, *, visible: int = 4) -> str:
    """Render a secret as a short, non-reversible hint.

    Used where an operator genuinely needs to tell two credentials apart, for
    example confirming which API key a misconfigured environment is using.
    """
    if not value:
        return REDACTED
    if len(value) <= visible:
        return REDACTED
    return f"{'*' * 8}{value[-visible:]}"


def _summarise(value: Any) -> str:
    if isinstance(value, dict):
        return f"[dict with {len(value)} keys]"
    if isinstance(value, (list, tuple)):
        return f"[sequence with {len(value)} items]"
    return "[truncated]"
