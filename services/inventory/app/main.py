from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

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
    logger.info(
        "inventory service ready",
        extra={"env": settings.env, "reservation_ttl_minutes": settings.reservation_ttl_minutes},
    )
    try:
        yield
    finally:
        if state.db is not None:
            await state.db.dispose()
        logger.info("inventory service stopped")


async def _database_ready() -> bool:
    return await state.db.ping() if state.db is not None else False


app = create_app(
    service_name=settings.service_name,
    title="Inventory Service",
    description=(
        "The source of truth for stock.\n\n"
        "Stock is RESERVED, never optimistically decremented. A reservation moves "
        "units from `available_qty` to `reserved_qty` under a row lock, so a "
        "hundred simultaneous orders for the last item produce exactly one "
        "winner. Reservations carry a TTL and are swept back if a saga dies "
        "mid-flight.\n\n"
        "The compensating transaction lives here: on `payment.failed` this service "
        "releases its own reservation without being told to."
    ),
    lifespan=lifespan,
    readiness_checks={"database": _database_ready},
)

app.include_router(public_router)
app.include_router(admin_router)


@app.get("/", include_in_schema=False)
async def root() -> dict[str, str]:
    return {"service": settings.service_name, "docs": "/docs"}
