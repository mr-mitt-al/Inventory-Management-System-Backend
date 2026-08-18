"""Order's kafka consumer - the saga's driver. Runs as its own process.

    inventory.reserved            -> INVENTORY_RESERVED
    inventory.reservation_failed  -> FAILED (nothing to compensate)
    payment.succeeded             -> PAID -> CONFIRMED, publish order.confirmed
    payment.failed                -> FAILED, publish order.cancelled
    catalog.product.upserted      -> refresh the local price read-model
"""

from __future__ import annotations

import asyncio
import logging

from sqlalchemy.ext.asyncio import AsyncSession

from common.db.session import Database
from common.events.envelope import EventEnvelope
from common.events.schemas import (
    InventoryReservationFailed,
    InventoryReserved,
    PaymentFailed,
    PaymentSucceeded,
    ProductUpserted,
)
from common.events.topics import Topics
from common.kafka.consumer import BaseConsumer
from common.kafka.producer import EventProducer
from common.observability.logging import configure_logging
from common.observability.metrics import events_consumed
from common.worker import run_worker
from app.config import settings
from app.models import ProcessedEvent
from app.repositories import OrderRepository, ProductSnapshotRepository
from app.saga import OrderSaga

configure_logging(service_name=settings.service_name, level=settings.log_level)
logger = logging.getLogger(__name__)

TOPICS = [
    Topics.INVENTORY_RESERVED,
    Topics.INVENTORY_RESERVATION_FAILED,
    Topics.PAYMENT_SUCCEEDED,
    Topics.PAYMENT_FAILED,
    Topics.CATALOG_PRODUCT_UPSERTED,
]


class OrderConsumer(BaseConsumer):
    def __init__(self, *, db: Database, producer: EventProducer) -> None:
        super().__init__(
            name="order-consumer",
            topics=TOPICS,
            group_id="order-saga",
            bootstrap_servers=settings.kafka_bootstrap_servers,
            db=db,
            producer=producer,
            processed_event_model=ProcessedEvent,
            handlers={
                Topics.INVENTORY_RESERVED: self.on_inventory_reserved,
                Topics.INVENTORY_RESERVATION_FAILED: self.on_reservation_failed,
                Topics.PAYMENT_SUCCEEDED: self.on_payment_succeeded,
                Topics.PAYMENT_FAILED: self.on_payment_failed,
                Topics.CATALOG_PRODUCT_UPSERTED: self.on_product_upserted,
            },
            max_retries=settings.consumer_max_retries,
            retry_backoff_ms=settings.consumer_retry_backoff_ms,
        )

    # ------------------------------------------------------------- forward steps
    async def on_inventory_reserved(self, session: AsyncSession, envelope: EventEnvelope) -> None:
        event = envelope.parse(InventoryReserved)
        order = await self._locked_order(session, event.order_id, envelope)
        if order is None:
            return

        saga = OrderSaga(session, correlation_id=envelope.correlation_id)
        await saga.on_inventory_reserved(order)
        self._count(envelope, "ok")

    async def on_payment_succeeded(self, session: AsyncSession, envelope: EventEnvelope) -> None:
        event = envelope.parse(PaymentSucceeded)
        order = await self._locked_order(session, event.order_id, envelope)
        if order is None:
            return

        saga = OrderSaga(session, correlation_id=envelope.correlation_id)
        await saga.on_payment_succeeded(order)
        self._count(envelope, "ok")

    # -------------------------------------------------------------- compensation
    async def on_payment_failed(self, session: AsyncSession, envelope: EventEnvelope) -> None:
        """Record the failure and stop the saga.

        Order does not instruct Inventory to release stock. Inventory consumes the
        same `payment.failed` event and releases its own reservation - that is
        what makes this a saga rather than a coordinator.
        """
        event = envelope.parse(PaymentFailed)
        order = await self._locked_order(session, event.order_id, envelope)
        if order is None:
            return

        saga = OrderSaga(session, correlation_id=envelope.correlation_id)
        await saga.on_payment_failed(
            order, failure_code=event.failure_code, message=event.failure_message
        )
        self._count(envelope, "ok")

    async def on_reservation_failed(self, session: AsyncSession, envelope: EventEnvelope) -> None:
        """Out of stock - the cheapest failure. Nothing was done, so nothing needs
        undoing, and Payment is never invoked."""
        event = envelope.parse(InventoryReservationFailed)
        order = await self._locked_order(session, event.order_id, envelope)
        if order is None:
            return

        saga = OrderSaga(session, correlation_id=envelope.correlation_id)
        await saga.on_reservation_failed(order, reason=event.reason)
        self._count(envelope, "ok")

    # ------------------------------------------------------------- read-model
    async def on_product_upserted(self, session: AsyncSession, envelope: EventEnvelope) -> None:
        """Keep the local price read-model current.

        This is what lets checkout price an order without calling Catalog and
        without trusting a price from the browser.
        """
        event = envelope.parse(ProductUpserted)
        await ProductSnapshotRepository(session).upsert(
            product_id=event.product_id,
            sku=event.sku,
            name=event.name,
            price=event.price,
            currency=event.currency,
            is_active=event.is_active,
        )
        logger.info(
            "product snapshot updated",
            extra={
                "product_id": str(event.product_id),
                "sku": event.sku,
                "price": str(event.price),
                "is_active": event.is_active,
            },
        )
        self._count(envelope, "ok")

    # ----------------------------------------------------------------- internals
    async def _locked_order(self, session: AsyncSession, order_id, envelope: EventEnvelope):
        """Fetch the order under a row lock, or log and skip.

        The lock matters because two consumer replicas can process two events for
        the same order concurrently; without it both read the same status and both
        transition, duplicating history rows and published events.

        A missing order is not dead-letter material: it means the event belongs to
        an order this database never had (a replay of very old events, or a wiped
        volume). Retrying will never make it appear.
        """
        order = await OrderRepository(session).get_for_update(order_id)
        if order is None:
            logger.warning(
                "event for unknown order, skipping",
                extra={"order_id": str(order_id), "event_type": envelope.event_type},
            )
            self._count(envelope, "ignored")
        return order

    def _count(self, envelope: EventEnvelope, status: str) -> None:
        events_consumed.labels(
            consumer=self.name,
            topic=envelope.event_type,
            event_type=envelope.event_type,
            status=status,
        ).inc()


async def main(stop: asyncio.Event) -> None:
    db = Database(settings.database_url, pool_size=8, max_overflow=4)
    producer = EventProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        producer_name=f"{settings.service_name}-dlq",
    )
    await producer.start()

    consumer = OrderConsumer(db=db, producer=producer)
    await consumer.start()

    task = asyncio.create_task(consumer.run())
    try:
        await stop.wait()
    finally:
        await consumer.stop()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await producer.stop()
        await db.dispose()


if __name__ == "__main__":
    run_worker(main, name="order-consumer")
