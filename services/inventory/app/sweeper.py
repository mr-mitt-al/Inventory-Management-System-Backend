"""Reservation expiry sweeper. Runs as its own process.

The safety net under the entire reservation model. Every reservation carries a
TTL; if a service dies mid-saga - Order crashes before consuming
`inventory.reserved`, Payment hangs, the broker partitions - the held stock would
otherwise be invisible to every other customer forever, with nothing anywhere
raising an error.

This loop returns it. `FOR UPDATE SKIP LOCKED` means multiple sweeper replicas
never fight over the same reservation.
"""

from __future__ import annotations

import asyncio
import logging

from common.db.session import Database
from common.observability.logging import configure_logging
from common.worker import run_worker
from app.config import settings
from app.services import InventoryService

configure_logging(service_name=settings.service_name, level=settings.log_level)
logger = logging.getLogger(__name__)


async def sweep_once(db: Database) -> int:
    # The stock_changed events this generates are enqueued in the outbox and
    # relayed by the inventory-outbox process, exactly like every other publish.
    async with db.transaction() as session:
        service = InventoryService(session)
        return await service.expire_stale_reservations(limit=settings.sweeper_batch_size)


async def main(stop: asyncio.Event) -> None:
    db = Database(settings.database_url, pool_size=3, max_overflow=1)
    interval = settings.sweeper_interval_seconds

    logger.info(
        "reservation sweeper started",
        extra={"interval_s": interval, "ttl_minutes": settings.reservation_ttl_minutes},
    )

    while not stop.is_set():
        try:
            expired = await sweep_once(db)
            if expired:
                logger.warning("reservations expired and stock reclaimed", extra={"count": expired})
                # A full batch means there is probably a backlog; go again
                # immediately rather than waiting out the interval.
                if expired >= settings.sweeper_batch_size:
                    continue
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("sweep cycle failed")

        try:
            await asyncio.wait_for(stop.wait(), timeout=interval)
        except TimeoutError:
            pass

    await db.dispose()


if __name__ == "__main__":
    run_worker(main, name="inventory-sweeper")
