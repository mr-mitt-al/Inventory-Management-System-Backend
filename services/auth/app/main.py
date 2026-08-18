from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from redis.asyncio import Redis

from common.api import create_app
from common.db.session import Database
from common.observability.logging import configure_logging
from app.bootstrap import seed_admin
from app.config import settings
from app.dependencies import state
from app.routers import admin_router, auth_router

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

    # Migrations already ran in entrypoint.sh, so the schema exists by now.
    await seed_admin(state.db)

    logger.info("auth service ready", extra={"env": settings.env})
    try:
        yield
    finally:
        if state.redis is not None:
            await state.redis.aclose()
        if state.db is not None:
            await state.db.dispose()
        logger.info("auth service stopped")


# Readiness checks resolve `state` at request time, not at import time - the
# Database and Redis clients only exist once the lifespan has run.
async def _database_ready() -> bool:
    return await state.db.ping() if state.db is not None else False


async def _redis_ready() -> bool:
    if state.redis is None:
        return False
    try:
        return bool(await state.redis.ping())
    except Exception:
        return False


app = create_app(
    service_name=settings.service_name,
    title="Auth Service",
    description=(
        "Identity, credentials and roles. The only service that owns user data.\n\n"
        "Other services never call this one to authorize a request - they verify "
        "the JWT signature locally, so auth is not a runtime dependency of the "
        "rest of the system."
    ),
    lifespan=lifespan,
    readiness_checks={"database": _database_ready, "redis": _redis_ready},
)

app.include_router(auth_router)
app.include_router(admin_router)


@app.get("/", include_in_schema=False)
async def root() -> dict[str, str]:
    return {"service": settings.service_name, "docs": "/docs"}
