from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from common.api import create_app
from common.db.session import Database
from common.kafka.producer import EventProducer
from common.observability.logging import configure_logging
from app.config import settings
from app.dependencies import state
from app.routers import admin_router, orders_router

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

    # Used only by DLQ replay, which republishes an existing envelope. Everything
    # the API itself produces goes through the outbox.
    state.producer = EventProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        producer_name=f"{settings.service_name}-api",
    )
    await state.producer.start()

    logger.info("order service ready", extra={"env": settings.env})
    try:
        yield
    finally:
        if state.producer is not None:
            await state.producer.stop()
        if state.db is not None:
            await state.db.dispose()
        logger.info("order service stopped")


async def _database_ready() -> bool:
    return await state.db.ping() if state.db is not None else False


async def _read_model_ready() -> bool:
    """Checks that the product read-model has been populated.

    Reported rather than enforced: an empty read-model means checkout will reject
    every item, which is worth surfacing on /health/ready during setup - but it is
    a data problem, not a reason to pull the service out of rotation.
    """
    if state.db is None:
        return False
    try:
        from app.repositories import ProductSnapshotRepository

        async with state.db.session_factory() as session:
            count = await ProductSnapshotRepository(session).count()
        if count == 0:
            logger.warning(
                "product read-model is empty - run the catalog seed so "
                "catalog.product.upserted events populate product_snapshots"
            )
        return True
    except Exception:
        return False


app = create_app(
    service_name=settings.service_name,
    title="Order Service",
    description=(
        "The saga orchestrator. Owns the order state machine.\n\n"
        "`POST /orders` returns **202 Accepted** - the order exists but is not "
        "confirmed. Stock reservation and payment happen asynchronously and either "
        "can fail. Watch progress on `GET /orders/{id}/stream`.\n\n"
        "Prices are read from this service's own `product_snapshots` table, kept "
        "current by consuming `catalog.product.upserted`. Checkout therefore never "
        "calls the Catalog service and never trusts a price from the client.\n\n"
        "Compensation is not orchestrated from here: when payment fails, this "
        "service records FAILED while Inventory independently releases its own "
        "reservation in response to the same event."
    ),
    lifespan=lifespan,
    readiness_checks={"database": _database_ready, "read_model": _read_model_ready},
)

app.include_router(orders_router)
app.include_router(admin_router)


@app.get("/", include_in_schema=False)
async def root() -> dict[str, str]:
    return {"service": settings.service_name, "docs": "/docs"}
