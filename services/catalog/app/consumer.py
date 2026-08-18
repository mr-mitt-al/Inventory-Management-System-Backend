"""Catalog's kafka consumer. Runs as its own process.

Subscribes to `inventory.stock.changed` and updates the denormalized
`cached_stock` column, then busts the Redis entry for that product.

This is the smallest possible demonstration of the pattern the saga services
follow in phases 3 and 4: the inventory service does not know Catalog exists, and
Catalog never calls Inventory. One publishes a fact, the other reacts to it.
"""

from __future__ import annotations

import asyncio
import logging

from sqlalchemy.ext.asyncio import AsyncSession

from common.db.session import Database
from common.events.envelope import EventEnvelope
from common.events.schemas import StockChanged
from common.events.topics import Topics, consumer_group
from common.kafka.consumer import BaseConsumer
from common.kafka.producer import EventProducer
from common.observability.logging import configure_logging
from common.observability.metrics import events_consumed
from common.worker import run_worker
from app.cache import CatalogCache
from app.config import settings
from app.models import ProcessedEvent
from app.services import CatalogService

configure_logging(service_name=settings.service_name, level=settings.log_level)
logger = logging.getLogger(__name__)


class CatalogConsumer(BaseConsumer):
    def __init__(self, *, db: Database, producer: EventProducer, cache: CatalogCache) -> None:
        self._cache = cache
        super().__init__(
            name="catalog-consumer",
            topics=[Topics.INVENTORY_STOCK_CHANGED],
            group_id=consumer_group("catalog", Topics.INVENTORY_STOCK_CHANGED),
            bootstrap_servers=settings.kafka_bootstrap_servers,
            db=db,
            producer=producer,
            processed_event_model=ProcessedEvent,
            handlers={Topics.INVENTORY_STOCK_CHANGED: self.on_stock_changed},
            max_retries=settings.consumer_max_retries,
            retry_backoff_ms=settings.consumer_retry_backoff_ms,
        )

    async def on_stock_changed(self, session: AsyncSession, envelope: EventEnvelope) -> None:
        """Apply a stock movement to the display copy.

        Runs inside the transaction BaseConsumer opened, together with the
        `processed_events` insert - so a duplicate delivery cannot apply the
        change twice.
        """
        event = envelope.parse(StockChanged)
        service = CatalogService(session, self._cache)

        product = await service.apply_stock_change(
            product_id=event.product_id,
            available_qty=event.available_qty,
            reserved_qty=event.reserved_qty,
        )

        events_consumed.labels(
            consumer=self.name,
            topic=Topics.INVENTORY_STOCK_CHANGED,
            event_type=envelope.event_type,
            status="ok" if product else "ignored",
        ).inc()

        if product is None:
            return

        # Cache invalidation happens AFTER the database write and is not part of
        # the transaction. If it fails, the TTL corrects the entry within minutes
        # - whereas failing the handler here would retry a write that already
        # succeeded.
        await service.invalidate_product_cache(event.product_id)

        logger.info(
            "cached stock updated",
            extra={
                "product_id": str(event.product_id),
                "sku": event.sku,
                "available_qty": event.available_qty,
                "reason": event.reason,
            },
        )


async def main(stop: asyncio.Event) -> None:
    from redis.asyncio import Redis

    db = Database(settings.database_url, pool_size=5, max_overflow=2)
    redis = Redis.from_url(settings.redis_url, decode_responses=True)

    # The consumer needs a producer only so it can dead-letter messages it
    # cannot handle.
    producer = EventProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        producer_name=f"{settings.service_name}-dlq",
    )
    await producer.start()

    cache = CatalogCache(
        redis,
        product_ttl=settings.product_cache_ttl_seconds,
        listing_ttl=settings.listing_cache_ttl_seconds,
        enabled=settings.cache_enabled,
    )

    consumer = CatalogConsumer(db=db, producer=producer, cache=cache)
    await consumer.start()

    task = asyncio.create_task(consumer.run())
    try:
        await stop.wait()
    finally:
        # Stop consuming first so the in-flight handler can commit its offset,
        # then tear down its dependencies. Reversing this order would kill the
        # database mid-handler and force a redelivery on restart.
        await consumer.stop()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await producer.stop()
        await redis.aclose()
        await db.dispose()


if __name__ == "__main__":
    run_worker(main, name="catalog-consumer")
