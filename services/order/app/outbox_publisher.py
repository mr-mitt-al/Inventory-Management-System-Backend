"""Outbox publisher for the order service.

Publishes order.created, order.confirmed and order.cancelled. These rows were
committed in the same transaction as the order change that caused them, so an
order and its event can never disagree - and a crash here delays publication
rather than losing it.
"""

from __future__ import annotations

import asyncio
import logging

from common.db.outbox import OutboxPublisher
from common.db.session import Database
from common.kafka.producer import EventProducer
from common.observability.logging import configure_logging
from common.observability.metrics import outbox_pending
from common.worker import run_worker
from app.config import settings
from app.models import Outbox

configure_logging(service_name=settings.service_name, level=settings.log_level)
logger = logging.getLogger(__name__)


async def _report_pending(publisher: OutboxPublisher, stop: asyncio.Event) -> None:
    """Keep the `outbox_pending` gauge current.

    If this climbs, the publisher is stuck and orders are silently stalling in
    PENDING with no error raised anywhere - the single most valuable alert in the
    system.
    """
    while not stop.is_set():
        try:
            outbox_pending.labels(service=settings.service_name).set(
                await publisher.pending_count()
            )
        except Exception:
            logger.debug("failed to sample outbox depth", exc_info=True)
        try:
            await asyncio.wait_for(stop.wait(), timeout=15)
        except TimeoutError:
            pass


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

    tasks = [
        asyncio.create_task(publisher.run()),
        asyncio.create_task(_report_pending(publisher, stop)),
    ]
    try:
        await stop.wait()
    finally:
        publisher.stop()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await producer.stop()
        await db.dispose()


if __name__ == "__main__":
    run_worker(main, name="order-outbox-publisher")
