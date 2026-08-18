from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Query

from common.auth.dependencies import require_admin
from common.auth.jwt import Role, TokenUser
from common.errors import NotFoundError
from common.pagination import Page, PageParams, page_params
from app.dependencies import AuthServiceDep, SessionDep
from app.repositories import UserRepository
from app.schemas import RoleUpdateRequest, StatusUpdateRequest, UserResponse

# Guarding the whole router beats decorating each route: a new endpoint added
# here is admin-only by default rather than public by accident.
router = APIRouter(
    prefix="/auth/users",
    tags=["auth-admin"],
    dependencies=[Depends(require_admin)],
)


@router.get("", response_model=Page[UserResponse], summary="List users")
async def list_users(
    session: SessionDep,
    params: Annotated[PageParams, Depends(page_params)],
    role: Annotated[Role | None, Query()] = None,
    is_active: Annotated[bool | None, Query()] = None,
    q: Annotated[str | None, Query(description="Match email or name")] = None,
) -> Page[UserResponse]:
    users, total = await UserRepository(session).list_users(
        offset=params.offset,
        limit=params.limit,
        role=role,
        is_active=is_active,
        search=q,
    )
    return Page.build(
        [UserResponse.model_validate(u) for u in users], total=total, params=params
    )


@router.get("/{user_id}", response_model=UserResponse, summary="Get one user")
async def get_user(user_id: UUID, session: SessionDep) -> UserResponse:
    user = await UserRepository(session).get_by_id(user_id)
    if user is None:
        raise NotFoundError("user not found")
    return UserResponse.model_validate(user)


@router.patch("/{user_id}/role", response_model=UserResponse, summary="Promote or demote")
async def change_role(
    user_id: UUID,
    body: RoleUpdateRequest,
    service: AuthServiceDep,
    caller: Annotated[TokenUser, Depends(require_admin)],
) -> UserResponse:
    """Promotion takes effect when the target's current access token expires
    (up to `ACCESS_TOKEN_EXPIRE_MINUTES`), because the role is a token claim.
    """
    user = await service.change_role(
        actor_id=caller.user_id, target_id=user_id, role=body.role
    )
    return UserResponse.model_validate(user)


@router.patch("/{user_id}/status", response_model=UserResponse, summary="Activate or deactivate")
async def change_status(
    user_id: UUID,
    body: StatusUpdateRequest,
    service: AuthServiceDep,
    caller: Annotated[TokenUser, Depends(require_admin)],
) -> UserResponse:
    user = await service.set_active(
        actor_id=caller.user_id, target_id=user_id, is_active=body.is_active
    )
    return UserResponse.model_validate(user)
