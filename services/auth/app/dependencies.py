"""Wiring. Holds the process-wide singletons created during startup."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Annotated
from uuid import UUID, uuid4

from fastapi import Depends, Request
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from common.db.session import Database
from app.config import settings
from app.rate_limit import LoginRateLimiter
from app.services import AuthService


class AppState:
    """Set once in the lifespan; read by dependencies."""

    db: Database | None = None
    redis: Redis | None = None


state = AppState()


def get_db() -> Database:
    if state.db is None:
        raise RuntimeError("database not initialized")
    return state.db


async def get_session() -> AsyncIterator[AsyncSession]:
    async with get_db().session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


def get_rate_limiter() -> LoginRateLimiter | None:
    if state.redis is None:
        return None
    return LoginRateLimiter(
        state.redis,
        max_attempts=settings.login_max_attempts,
        window_seconds=settings.login_attempt_window_seconds,
    )


async def get_auth_service(
    session: Annotated[AsyncSession, Depends(get_session)],
    rate_limiter: Annotated[LoginRateLimiter | None, Depends(get_rate_limiter)],
) -> AuthService:
    return AuthService(session, rate_limiter=rate_limiter)


def get_correlation_id(request: Request) -> UUID:
    """The id minted (or accepted) by CorrelationIdMiddleware, so events
    published by this request inherit it."""
    raw = getattr(request.state, "correlation_id", None)
    try:
        return UUID(str(raw))
    except (TypeError, ValueError):
        return uuid4()


SessionDep = Annotated[AsyncSession, Depends(get_session)]
AuthServiceDep = Annotated[AuthService, Depends(get_auth_service)]
CorrelationIdDep = Annotated[UUID, Depends(get_correlation_id)]
