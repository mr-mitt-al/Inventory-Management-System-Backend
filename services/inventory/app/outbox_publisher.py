"""Outbox publisher for the inventory service.

Publishes inventory.reserved, inventory.reservation_failed,
inventory.stock.changed and inventory.low_stock. These commit in the same
transaction as the stock movement that caused them, so a reservation and its
event can never disagree.
"""

from __future__ import annotations

import asyncio
import logging

from common.db.outbox import OutboxPublisher
from common.db.session import Database
from common.kafka.producer import EventProducer
from common.observability.logging import configure_logging
from common.worker import run_worker
from app.config import settings
from app.models import Outbox

configure_logging(service_name=settings.service_name, level=settings.log_level)
logger = logging.getLogger(__name__)


async def main(stop: asyncio.Event) -> None:
    db = Database(settings.database_url, pool_size=5, max_overflow=2)
    producer = EventProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        producer_name=f"{settings.service_name}-outbox",
    )
    await producer.start()

    publisher = OutboxPublisher(
        db=db,
        producer=producer,
        outbox_model=Outbox,
        poll_interval_ms=settings.outbox_poll_interval_ms,
        batch_size=settings.outbox_batch_size,
        service_name=settings.service_name,
    )

    task = asyncio.create_task(publisher.run())
    try:
        await stop.wait()
    finally:
        publisher.stop()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await producer.stop()
        await db.dispose()


if __name__ == "__main__":
    run_worker(main, name="inventory-outbox-publisher")
