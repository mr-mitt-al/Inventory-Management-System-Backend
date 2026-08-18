from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from redis.asyncio import Redis

from common.api import create_app
from common.db.session import Database
from common.observability.logging import configure_logging
from app.config import settings
from app.dependencies import state
from app.routers import admin_router, public_router

configure_logging(service_name=settings.service_name, level=settings.log_level)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    state.db = Database(
        settings.database_url,
        pool_size=settings.db_pool_size,
        max_overflow=settings.db_max_overflow,
        echo=settings.db_echo,
    )
    state.redis = Redis.from_url(settings.redis_url, decode_responses=True)

    logger.info("catalog service ready", extra={"env": settings.env})
    try:
        yield
    finally:
        if state.redis is not None:
            await state.redis.aclose()
        if state.db is not None:
            await state.db.dispose()
        logger.info("catalog service stopped")


async def _database_ready() -> bool:
    return await state.db.ping() if state.db is not None else False


async def _redis_ready() -> bool:
    """Redis is a cache, so its absence is not a readiness failure - the service
    degrades to slower reads. Reported for visibility only."""
    if state.redis is None:
        return True
    try:
        await state.redis.ping()
    except Exception:
        logger.warning("redis unreachable, serving uncached")
    return True


app = create_app(
    service_name=settings.service_name,
    title="Catalog Service",
    description=(
        "Products and categories. Read-heavy and cache-fronted.\n\n"
        "`cached_stock` on a product is a DENORMALIZED copy owned by the "
        "inventory service, kept fresh by consuming `inventory.stock.changed`. "
        "It exists so a product page never makes a synchronous call to another "
        "service. Checkout re-validates against inventory, so a stale value here "
        "cannot cause an oversell."
    ),
    lifespan=lifespan,
    readiness_checks={"database": _database_ready, "redis": _redis_ready},
)

app.include_router(public_router)
app.include_router(admin_router)


@app.get("/", include_in_schema=False)
async def root() -> dict[str, str]:
    return {"service": settings.service_name, "docs": "/docs"}
