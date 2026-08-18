"""Auth business logic.

Raises domain errors, returns models, knows nothing about FastAPI.
"""

from __future__ import annotations

import logging
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from common.auth.jwt import (
    Role,
    TokenType,
    create_access_token,
    create_refresh_token,
    decode_token,
)
from common.db.base import utcnow
from common.db.outbox import enqueue_event
from common.errors import ConflictError, ForbiddenError, NotFoundError, UnauthorizedError
from common.events.envelope import make_envelope
from common.events.schemas import UserRegistered
from common.events.topics import Topics
from app.config import settings
from app.models import Outbox, User
from app.rate_limit import LoginRateLimiter
from app.repositories import RefreshTokenRepository, UserRepository
from app.schemas import TokenResponse
from app.security import (
    hash_password,
    hash_refresh_token,
    verify_password,
    waste_a_hash_cycle,
)

logger = logging.getLogger(__name__)


class AuthService:
    def __init__(
        self,
        session: AsyncSession,
        *,
        rate_limiter: LoginRateLimiter | None = None,
    ) -> None:
        self._session = session
        self._users = UserRepository(session)
        self._tokens = RefreshTokenRepository(session)
        self._rate_limiter = rate_limiter

    # ------------------------------------------------------------------ register
    async def register(
        self,
        *,
        email: str,
        password: str,
        full_name: str,
        correlation_id: UUID | None = None,
    ) -> User:
        if await self._users.email_exists(email):
            raise ConflictError("an account with this email already exists")

        user = await self._users.create(
            email=email,
            password_hash=hash_password(password),
            full_name=full_name,
            role=Role.CUSTOMER,  # hardcoded. never from the request.
        )

        # Announce the fact; do not call the notification service. Auth has no
        # idea notifications exist, and adding a welcome email required zero
        # changes here.
        envelope = make_envelope(
            event_type=Topics.USER_REGISTERED,
            payload=UserRegistered(
                user_id=user.id,
                email=user.email,
                full_name=user.full_name,
                registered_at=user.created_at,
            ),
            producer=settings.service_name,
            correlation_id=correlation_id,
        )
        await enqueue_event(
            self._session,
            Outbox,
            topic=Topics.USER_REGISTERED,
            key=str(user.id),
            envelope=envelope,
            aggregate_id=user.id,
        )

        logger.info("user registered", extra={"user_id": str(user.id)})
        return user

    # --------------------------------------------------------------------- login
    async def login(self, *, email: str, password: str) -> tuple[User, TokenResponse]:
        if self._rate_limiter:
            await self._rate_limiter.check(email)

        user = await self._users.get_by_email(email)

        if user is None:
            # Spend the same bcrypt time as a real verification so response
            # timing cannot be used to enumerate registered emails.
            waste_a_hash_cycle()
            if self._rate_limiter:
                await self._rate_limiter.record_failure(email)
            raise UnauthorizedError("invalid email or password")

        if not verify_password(password, user.password_hash):
            if self._rate_limiter:
                await self._rate_limiter.record_failure(email)
            raise UnauthorizedError("invalid email or password")

        # Checked after the password, so a deactivated account cannot be
        # distinguished from a wrong password without valid credentials.
        if not user.is_active:
            raise ForbiddenError("this account has been deactivated")

        if self._rate_limiter:
            await self._rate_limiter.reset(email)

        await self._users.touch_last_login(user)
        tokens = await self._issue_tokens(user)
        logger.info("login succeeded", extra={"user_id": str(user.id)})
        return user, tokens

    # ------------------------------------------------------------------- refresh
    async def refresh(self, *, refresh_token: str) -> TokenResponse:
        """Rotating refresh: the presented token is revoked and a new pair
        issued, so a stolen token is single-use."""
        claims = decode_token(
            refresh_token,
            secret=settings.jwt_secret,
            algorithm=settings.jwt_algorithm,
            expected_type=TokenType.REFRESH,
        )

        stored = await self._tokens.get_by_hash(hash_refresh_token(refresh_token))
        if stored is None:
            raise UnauthorizedError("refresh token not recognized")

        if stored.revoked_at is not None:
            # A revoked token being presented again means either a replay or a
            # stolen token. Kill every session for the user rather than guess.
            revoked = await self._tokens.revoke_all_for_user(stored.user_id)
            logger.warning(
                "revoked refresh token reused, all sessions terminated",
                extra={"user_id": str(stored.user_id), "sessions_revoked": revoked},
            )
            raise UnauthorizedError("refresh token has been revoked")

        if stored.expires_at <= utcnow():
            raise UnauthorizedError("refresh token has expired")

        user = await self._users.get_by_id(claims.user_id)
        if user is None:
            raise UnauthorizedError("user no longer exists")
        if not user.is_active:
            raise ForbiddenError("this account has been deactivated")

        await self._tokens.revoke(stored)
        return await self._issue_tokens(user)

    # -------------------------------------------------------------------- logout
    async def logout(self, *, refresh_token: str) -> None:
        stored = await self._tokens.get_by_hash(hash_refresh_token(refresh_token))
        if stored is not None and stored.revoked_at is None:
            await self._tokens.revoke(stored)
        # Unknown or already-revoked token still returns success: logout is
        # idempotent, and reporting "not found" would leak token validity.

    # --------------------------------------------------------------------- admin
    async def change_role(self, *, actor_id: UUID, target_id: UUID, role: Role) -> User:
        if actor_id == target_id:
            # An admin demoting themselves can lock every admin out of the
            # system, with no in-app way back.
            raise ForbiddenError("you cannot change your own role")

        user = await self._users.get_by_id(target_id)
        if user is None:
            raise NotFoundError("user not found")

        previous = user.role
        await self._users.set_role(user, role)
        logger.warning(
            "role changed",
            extra={
                "actor_id": str(actor_id),
                "target_id": str(target_id),
                "from": previous,
                "to": role.value,
            },
        )
        return user

    async def set_active(self, *, actor_id: UUID, target_id: UUID, is_active: bool) -> User:
        if actor_id == target_id and not is_active:
            raise ForbiddenError("you cannot deactivate your own account")

        user = await self._users.get_by_id(target_id)
        if user is None:
            raise NotFoundError("user not found")

        await self._users.set_active(user, is_active)
        if not is_active:
            # Access tokens stay valid until they expire (up to 15 min); killing
            # the refresh tokens stops new ones being minted.
            await self._tokens.revoke_all_for_user(target_id)

        logger.warning(
            "account status changed",
            extra={
                "actor_id": str(actor_id),
                "target_id": str(target_id),
                "is_active": is_active,
            },
        )
        return user

    # ------------------------------------------------------------------ internal
    async def _issue_tokens(self, user: User) -> TokenResponse:
        access_token, _, access_expires = create_access_token(
            user_id=user.id,
            email=user.email,
            role=user.role_enum,
            secret=settings.jwt_secret,
            algorithm=settings.jwt_algorithm,
            expire_minutes=settings.access_token_expire_minutes,
        )
        refresh_token, refresh_jti, refresh_expires = create_refresh_token(
            user_id=user.id,
            email=user.email,
            role=user.role_enum,
            secret=settings.jwt_secret,
            algorithm=settings.jwt_algorithm,
            expire_days=settings.refresh_token_expire_days,
        )

        await self._tokens.store(
            token_id=refresh_jti,
            user_id=user.id,
            token_hash=hash_refresh_token(refresh_token),
            expires_at=refresh_expires,
        )

        return TokenResponse(
            access_token=access_token,
            refresh_token=refresh_token,
            expires_in=int((access_expires - utcnow()).total_seconds()),
        )
