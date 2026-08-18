from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, status

from common.auth.dependencies import get_current_user
from common.auth.jwt import TokenUser
from common.errors import NotFoundError
from app.dependencies import AuthServiceDep, CorrelationIdDep, SessionDep
from app.repositories import UserRepository
from app.schemas import (
    LoginRequest,
    RefreshRequest,
    RegisterRequest,
    RegisterResponse,
    TokenResponse,
    UserResponse,
)

router = APIRouter(prefix="/auth", tags=["auth"])


@router.post(
    "/register",
    response_model=RegisterResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a customer account",
)
async def register(
    body: RegisterRequest,
    service: AuthServiceDep,
    correlation_id: CorrelationIdDep,
) -> RegisterResponse:
    """Always creates a `customer`.

    `RegisterRequest` has no `role` field, so there is no request that can
    produce an admin. See `app/bootstrap.py` for how the first admin exists.
    """
    user = await service.register(
        email=body.email,
        password=body.password,
        full_name=body.full_name,
        correlation_id=correlation_id,
    )
    return RegisterResponse(user=UserResponse.model_validate(user))


@router.post("/login", response_model=TokenResponse, summary="Exchange credentials for tokens")
async def login(body: LoginRequest, service: AuthServiceDep) -> TokenResponse:
    _, tokens = await service.login(email=body.email, password=body.password)
    return tokens


@router.post("/refresh", response_model=TokenResponse, summary="Rotate the refresh token")
async def refresh(body: RefreshRequest, service: AuthServiceDep) -> TokenResponse:
    return await service.refresh(refresh_token=body.refresh_token)


@router.post(
    "/logout",
    status_code=status.HTTP_204_NO_CONTENT,
    # Explicit None: FastAPI otherwise infers response_model from the `-> None`
    # return annotation and rejects it, since 204 cannot carry a body.
    response_model=None,
    summary="Revoke a refresh token",
)
async def logout(body: RefreshRequest, service: AuthServiceDep) -> None:
    await service.logout(refresh_token=body.refresh_token)


@router.get("/me", response_model=UserResponse, summary="Current user profile")
async def me(
    session: SessionDep,
    caller: Annotated[TokenUser, Depends(get_current_user)],
) -> UserResponse:
    """Reads the database rather than echoing token claims.

    The token could be up to 15 minutes stale - if an admin changed this user's
    role or deactivated them, /me should reflect that immediately even though
    authorization elsewhere still trusts the claim until expiry.
    """
    user = await UserRepository(session).get_by_id(caller.user_id)
    if user is None:
        raise NotFoundError("user not found")
    return UserResponse.model_validate(user)
