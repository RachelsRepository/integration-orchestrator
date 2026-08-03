"""At-rest protection for cached provider access tokens.

Redis is a soft dependency for availability, but tokens stored there are still
credentials. Fernet encrypts the token value before it is written so a Redis
dump or a compromised cache key does not expose raw bearer tokens.
"""

from __future__ import annotations

import base64
import hashlib

from cryptography.fernet import Fernet, InvalidToken
from pydantic import SecretStr


def derive_fernet(secret: SecretStr) -> Fernet:
    """Derive a Fernet key from an application secret.

    Fernet requires a url-safe base64-encoded 32-byte key. Hashing the configured
    secret produces a stable key without forcing operators to generate Fernet
    material by hand for local development.
    """
    digest = hashlib.sha256(secret.get_secret_value().encode("utf-8")).digest()
    return Fernet(base64.urlsafe_b64encode(digest))


def encrypt_token_value(fernet: Fernet, value: str) -> str:
    return fernet.encrypt(value.encode("utf-8")).decode("ascii")


def decrypt_token_value(fernet: Fernet, ciphertext: str) -> str | None:
    try:
        return fernet.decrypt(ciphertext.encode("ascii")).decode("utf-8")
    except (InvalidToken, ValueError):
        return None
