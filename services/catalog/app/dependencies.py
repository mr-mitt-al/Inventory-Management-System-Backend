from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Annotated
from uuid import UUID, uuid4

from fastapi import Depends, Request
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from common.db.session import Database
from app.cache import CatalogCache
from app.config import settings
from app.services import CatalogService


class AppState:
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


def get_cache() -> CatalogCache:
    return CatalogCache(
        state.redis,
        product_ttl=settings.product_cache_ttl_seconds,
        listing_ttl=settings.listing_cache_ttl_seconds,
        enabled=settings.cache_enabled,
    )


def get_correlation_id(request: Request) -> UUID:
    """The id minted (or accepted) by CorrelationIdMiddleware, so events this
    request publishes inherit it and remain traceable."""
    raw = getattr(request.state, "correlation_id", None)
    try:
        return UUID(str(raw))
    except (TypeError, ValueError):
        return uuid4()


async def get_catalog_service(
    session: Annotated[AsyncSession, Depends(get_session)],
    cache: Annotated[CatalogCache, Depends(get_cache)],
    correlation_id: Annotated[UUID, Depends(get_correlation_id)],
) -> CatalogService:
    return CatalogService(session, cache, correlation_id=correlation_id)


SessionDep = Annotated[AsyncSession, Depends(get_session)]
CatalogServiceDep = Annotated[CatalogService, Depends(get_catalog_service)]
