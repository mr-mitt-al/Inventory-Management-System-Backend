"""Seed real stock records to match the catalog seed.

    docker compose exec inventory-api python -m app.seed

Reads the catalog's product list from `catalog_db` directly. That is a deliberate
exception to the no-cross-database rule and it is why this is a SEED SCRIPT rather
than service code: an operator task run once, not a request path. Nothing in the
running system reads another service's database.
"""

from __future__ import annotations

import asyncio
import logging
from uuid import UUID

from sqlalchemy import text

from common.db.session import Database
from common.observability.logging import configure_logging
from app.config import settings
from app.repositories import StockRepository
from app.services import InventoryService

configure_logging(service_name="inventory-seed", level=settings.log_level, json_output=False)
logger = logging.getLogger(__name__)


def _catalog_url() -> str:
    """Swap the database name in our own URL - same host, same credentials."""
    url = settings.database_url
    base, _, _ = url.rpartition("/")
    return f"{base}/catalog_db"


async def seed() -> None:
    catalog_db = Database(_catalog_url(), pool_size=2, max_overflow=0)
    inventory_db = Database(settings.database_url, pool_size=5, max_overflow=0)

    async with catalog_db.engine.connect() as conn:
        rows = (
            await conn.execute(
                text("SELECT id, sku, cached_stock FROM products WHERE is_active = true")
            )
        ).all()

    if not rows:
        logger.warning("no products found in catalog_db - run the catalog seed first")
        await catalog_db.dispose()
        await inventory_db.dispose()
        return

    created = skipped = 0
    async with inventory_db.transaction() as session:
        service = InventoryService(session)
        repo = StockRepository(session)

        for product_id, sku, cached_stock in rows:
            pid = product_id if isinstance(product_id, UUID) else UUID(str(product_id))
            if await repo.get(pid) is not None:
                # Idempotent: never top up an existing record, or re-running the
                # seed would silently inflate real stock.
                skipped += 1
                continue

            await service.upsert_stock_item(
                product_id=pid,
                sku=sku,
                quantity=int(cached_stock or 0),
                low_stock_threshold=settings.default_low_stock_threshold,
            )
            created += 1

    logger.info(
        "inventory seed complete: %d stock records created, %d already existed", created, skipped
    )
    await catalog_db.dispose()
    await inventory_db.dispose()


if __name__ == "__main__":
    asyncio.run(seed())
