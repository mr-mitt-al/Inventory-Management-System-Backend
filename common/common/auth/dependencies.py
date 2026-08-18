"""FastAPI dependencies for authentication and role checks.

Imported identically by every service, so authorization logic exists once
instead of being copy-pasted six times and drifting.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import Depends
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from common.auth.jwt import Role, TokenType, TokenUser, decode_token
from common.errors import ForbiddenError, UnauthorizedError
from common.settings import auth_settings

bearer_scheme = HTTPBearer(auto_error=False, description="Bearer access token")
optional_bearer = HTTPBearer(auto_error=False)


async def get_current_user(
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer_scheme)],
) -> TokenUser:
    if credentials is None:
        raise UnauthorizedError("missing bearer token")

    settings = auth_settings()
    return decode_token(
        credentials.credentials,
        secret=settings.jwt_secret,
        algorithm=settings.jwt_algorithm,
        expected_type=TokenType.ACCESS,
    )


async def get_optional_user(
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(optional_bearer)],
) -> TokenUser | None:
    """For endpoints that are public but behave differently when signed in."""
    if credentials is None:
        return None
    try:
        settings = auth_settings()
        return decode_token(
            credentials.credentials,
            secret=settings.jwt_secret,
            algorithm=settings.jwt_algorithm,
            expected_type=TokenType.ACCESS,
        )
    except UnauthorizedError:
        return None


def require_role(*allowed: Role):
    """Dependency factory restricting an endpoint to the given roles.

        @router.post("/products", dependencies=[Depends(require_role(Role.ADMIN))])

    Role alone is never sufficient for user-owned resources - an endpoint like
    ``GET /orders/{id}`` must ALSO check ``resource.user_id == caller.user_id``
    unless the caller is an admin. See ``assert_can_access``.
    """

    async def _check(user: Annotated[TokenUser, Depends(get_current_user)]) -> TokenUser:
        if user.role not in allowed:
            raise ForbiddenError(
                "insufficient permissions",
                details={"required": [r.value for r in allowed], "actual": user.role.value},
            )
        return user

    return _check


require_admin = require_role(Role.ADMIN)


def assert_can_access(caller: TokenUser, owner_id, *, resource: str = "resource") -> None:
    """Ownership check. Admins pass; everyone else must own the resource.

    Without this a valid token for user A can read user B's orders - the most
    common authorization hole in projects that stop at role checks.
    """
    if caller.is_admin:
        return
    if str(caller.user_id) != str(owner_id):
        raise ForbiddenError(f"you do not have access to this {resource}")
