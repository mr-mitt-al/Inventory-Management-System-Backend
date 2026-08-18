"""Password hashing and refresh-token hashing.

Uses ``bcrypt`` directly rather than passlib: passlib 1.7.4 breaks against
bcrypt >= 4.1 (it reads the removed ``bcrypt.__about__``), and the indirection
buys nothing here.
"""

from __future__ import annotations

import hashlib
import hmac
import secrets

import bcrypt

BCRYPT_ROUNDS = 12

# bcrypt silently truncates input beyond 72 bytes. Truncation is worse than
# rejection: two different long passwords could then be interchangeable.
BCRYPT_MAX_BYTES = 72

# Pre-computed hash of a value nobody can supply. Verified against when an email
# does not exist, so a login attempt costs the same either way and response
# timing does not reveal which emails are registered.
_DUMMY_HASH = bcrypt.hashpw(secrets.token_bytes(32), bcrypt.gensalt(rounds=BCRYPT_ROUNDS))


class PasswordTooLongError(ValueError):
    pass


def hash_password(password: str) -> str:
    raw = password.encode("utf-8")
    if len(raw) > BCRYPT_MAX_BYTES:
        raise PasswordTooLongError(
            f"password must be at most {BCRYPT_MAX_BYTES} bytes when utf-8 encoded"
        )
    return bcrypt.hashpw(raw, bcrypt.gensalt(rounds=BCRYPT_ROUNDS)).decode("utf-8")


def verify_password(password: str, password_hash: str) -> bool:
    raw = password.encode("utf-8")
    if len(raw) > BCRYPT_MAX_BYTES:
        return False
    try:
        return bcrypt.checkpw(raw, password_hash.encode("utf-8"))
    except ValueError:
        # Malformed stored hash. Treat as a failed login, not a 500.
        return False


def waste_a_hash_cycle() -> None:
    """Burn the same bcrypt time as a real verification.

    Called when the email is unknown, so an attacker cannot enumerate accounts
    by timing the response.
    """
    bcrypt.checkpw(b"timing-equalizer", _DUMMY_HASH)


def hash_refresh_token(token: str) -> str:
    """SHA-256 for refresh tokens.

    Deliberately not bcrypt: refresh tokens are already 256+ bits of entropy, so
    there is nothing to brute-force, and lookup must be an indexed equality
    query rather than a scan-and-compare over every stored row.
    """
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def refresh_token_matches(token: str, token_hash: str) -> bool:
    return hmac.compare_digest(hash_refresh_token(token), token_hash)
