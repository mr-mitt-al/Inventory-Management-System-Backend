"""Inventory's kafka consumer. Runs as its own process.

Four handlers, and between them they are the reason this system is a saga:

    order.created      -> reserve stock          (forward step)
    payment.succeeded  -> commit the reservation (forward step)
    payment.failed     -> RELEASE the reservation (COMPENSATION)
    order.cancelled    -> release or restock      (COMPENSATION)

Note what is absent: any knowledge of the Payment service, or the Order service,
or what they do. Inventory reacts to facts and undoes its own work. That is the
entire trick - there is no distributed transaction and no coordinator telling
anyone to roll back.
"""

from __future__ import annotations

import asyncio
import logging

from sqlalchemy.ext.asyncio import AsyncSession

from common.db.session import Database
from common.events.envelope import EventEnvelope
from common.events.schemas import (
    OrderCancelled,
    OrderCreated,
    PaymentFailed,
    PaymentSucceeded,
)
from common.events.topics import Topics
from common.kafka.consumer import BaseConsumer
from common.kafka.producer import EventProducer
from common.observability.logging import configure_logging
from common.observability.metrics import events_consumed
from common.worker import run_worker
from app.config import settings
from app.models import ProcessedEvent
from app.services import InventoryService

configure_logging(service_name=settings.service_name, level=settings.log_level)
logger = logging.getLogger(__name__)

TOPICS = [
    Topics.ORDER_CREATED,
    Topics.PAYMENT_SUCCEEDED,
    Topics.PAYMENT_FAILED,
    Topics.ORDER_CANCELLED,
]


class InventoryConsumer(BaseConsumer):
    def __init__(self, *, db: Database, producer: EventProducer) -> None:
        super().__init__(
            name="inventory-consumer",
            topics=TOPICS,
            # ONE group for all four topics. A single consumer group means the
            # handlers share offsets and scale together, and - because every
            # order-scoped topic is keyed by order_id - one order's events are
            # processed in order even across topics on the same partition index.
            group_id="inventory-saga",
            bootstrap_servers=settings.kafka_bootstrap_servers,
            db=db,
            producer=producer,
            processed_event_model=ProcessedEvent,
            handlers={
                Topics.ORDER_CREATED: self.on_order_created,
                Topics.PAYMENT_SUCCEEDED: self.on_payment_succeeded,
                Topics.PAYMENT_FAILED: self.on_payment_failed,
                Topics.ORDER_CANCELLED: self.on_order_cancelled,
            },
            max_retries=settings.consumer_max_retries,
            retry_backoff_ms=settings.consumer_retry_backoff_ms,
        )

    # --------------------------------------------------------------- forward path
    async def on_order_created(self, session: AsyncSession, envelope: EventEnvelope) -> None:
        event = envelope.parse(OrderCreated)
        service = InventoryService(session, correlation_id=envelope.correlation_id)

        await service.reserve(
            order_id=event.order_id,
            user_id=event.user_id,
            items=[(item.product_id, item.quantity) for item in event.items],
            total_amount=event.total_amount,
            currency=event.currency,
            payment_method=event.payment_method,
        )
        self._count(envelope, "ok")

    async def on_payment_succeeded(self, session: AsyncSession, envelope: EventEnvelope) -> None:
        event = envelope.parse(PaymentSucceeded)
        service = InventoryService(session, correlation_id=envelope.correlation_id)
        await service.commit_reservation(order_id=event.order_id)
        self._count(envelope, "ok")

    # ---------------------------------------------------------------- compensation
    async def on_payment_failed(self, session: AsyncSession, envelope: EventEnvelope) -> None:
        """Payment declined after stock was already set aside.

        Nobody instructed this. Payment published a fact; Inventory decided on its
        own that the fact means its reservation is void. The units go back on the
        shelf and become available to other customers immediately.
        """
        event = envelope.parse(PaymentFailed)
        service = InventoryService(session, correlation_id=envelope.correlation_id)
        await service.release_reservation(
            order_id=event.order_id,
            reason=f"payment failed: {event.failure_code}",
        )
        self._count(envelope, "ok")

    async def on_order_cancelled(self, session: AsyncSession, envelope: EventEnvelope) -> None:
        """User or admin cancelled.

        `release_reservation` handles both shapes: units still HELD go back to
        available, units already COMMITTED are restocked.
        """
        event = envelope.parse(OrderCancelled)
        service = InventoryService(session, correlation_id=envelope.correlation_id)
        await service.release_reservation(
            order_id=event.order_id, reason=f"order cancelled: {event.reason}"
        )
        self._count(envelope, "ok")

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

    consumer = InventoryConsumer(db=db, producer=producer)
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
    run_worker(main, name="inventory-consumer")
