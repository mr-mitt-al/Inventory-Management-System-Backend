"""Token creation and verification.

The role travels INSIDE the token. Every service verifies the signature locally
with the shared secret and reads the role from the claims; no service ever calls
the auth service to authorize a request. If it did, auth would become a
synchronous dependency of all five other services and a single point of failure
for the entire system.

The cost of that choice, stated plainly: a role change only takes effect when
the current access token expires (15 minutes). That is the standard trade-off of
stateless auth. If instant revocation is ever needed, the fix is a Redis
denylist of revoked ``jti`` values checked per request - which trades away some
of the statelessness.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from enum import StrEnum
from uuid import UUID, uuid4

import jwt
from pydantic import BaseModel

from common.errors import UnauthorizedError


class Role(StrEnum):
    CUSTOMER = "customer"
    ADMIN = "admin"


class TokenType(StrEnum):
    ACCESS = "access"
    REFRESH = "refresh"


class TokenUser(BaseModel):
    """Authenticated caller, reconstructed from token claims alone."""

    user_id: UUID
    email: str
    role: Role
    token_type: TokenType
    jti: UUID

    @property
    def is_admin(self) -> bool:
        return self.role is Role.ADMIN


class TokenPair(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int  # seconds until the access token expires


def _encode(
    *,
    user_id: UUID,
    email: str,
    role: Role,
    token_type: TokenType,
    expires_delta: timedelta,
    secret: str,
    algorithm: str,
    jti: UUID | None = None,
) -> tuple[str, UUID, datetime]:
    now = datetime.now(UTC)
    expires_at = now + expires_delta
    token_id = jti or uuid4()
    claims = {
        "sub": str(user_id),
        "email": email,
        "role": role.value,
        "type": token_type.value,
        "iat": int(now.timestamp()),
        "exp": int(expires_at.timestamp()),
        "jti": str(token_id),
    }
    return jwt.encode(claims, secret, algorithm=algorithm), token_id, expires_at


def create_access_token(
    *,
    user_id: UUID,
    email: str,
    role: Role,
    secret: str,
    algorithm: str = "HS256",
    expire_minutes: int = 15,
) -> tuple[str, UUID, datetime]:
    return _encode(
        user_id=user_id,
        email=email,
        role=role,
        token_type=TokenType.ACCESS,
        expires_delta=timedelta(minutes=expire_minutes),
        secret=secret,
        algorithm=algorithm,
    )


def create_refresh_token(
    *,
    user_id: UUID,
    email: str,
    role: Role,
    secret: str,
    algorithm: str = "HS256",
    expire_days: int = 7,
) -> tuple[str, UUID, datetime]:
    return _encode(
        user_id=user_id,
        email=email,
        role=role,
        token_type=TokenType.REFRESH,
        expires_delta=timedelta(days=expire_days),
        secret=secret,
        algorithm=algorithm,
    )


def decode_token(
    token: str,
    *,
    secret: str,
    algorithm: str = "HS256",
    expected_type: TokenType | None = None,
) -> TokenUser:
    """Verify signature and expiry, then return the caller.

    ``expected_type`` guards against a refresh token being used as a bearer
    token on a normal endpoint - a long-lived token where a short-lived one was
    intended.
    """
    try:
        claims = jwt.decode(token, secret, algorithms=[algorithm])
    except jwt.ExpiredSignatureError as exc:
        raise UnauthorizedError("token has expired") from exc
    except jwt.InvalidTokenError as exc:
        raise UnauthorizedError("invalid token") from exc

    try:
        user = TokenUser(
            user_id=UUID(claims["sub"]),
            email=claims["email"],
            role=Role(claims["role"]),
            token_type=TokenType(claims["type"]),
            jti=UUID(claims["jti"]),
        )
    except (KeyError, ValueError) as exc:
        raise UnauthorizedError("malformed token claims") from exc

    if expected_type is not None and user.token_type is not expected_type:
        raise UnauthorizedError(f"expected a {expected_type.value} token")

    return user
