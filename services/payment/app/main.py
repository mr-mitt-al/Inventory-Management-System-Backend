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
from app.routers import admin_router, payments_router

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
        "payment service ready",
        extra={
            "env": settings.env,
            "mock_failure_rate": settings.mock_failure_rate,
            "mock_latency_ms": settings.mock_latency_ms,
        },
    )
    try:
        yield
    finally:
        if state.db is not None:
            await state.db.dispose()
        logger.info("payment service stopped")


async def _database_ready() -> bool:
    return await state.db.ping() if state.db is not None else False


app = create_app(
    service_name=settings.service_name,
    title="Payment Service",
    description=(
        "Mock payment provider with production-grade semantics.\n\n"
        "`payments.order_id` is UNIQUE - the most important constraint in the "
        "system. Kafka delivers at-least-once, so `inventory.reserved` will "
        "sometimes arrive twice; the constraint means the second delivery cannot "
        "charge the customer again. Consumer-side dedup on `event_id` is the first "
        "line of defence and this is the second.\n\n"
        "Outcomes are deterministic per token, and the optional random-failure knob "
        "hashes `order_id` rather than using an RNG - so a retried charge always "
        "reaches the same verdict as the first attempt. "
        "See `GET /payments/test-tokens`."
    ),
    lifespan=lifespan,
    readiness_checks={"database": _database_ready},
)

app.include_router(payments_router)
app.include_router(admin_router)


@app.get("/", include_in_schema=False)
async def root() -> dict[str, str]:
    return {"service": settings.service_name, "docs": "/docs"}
