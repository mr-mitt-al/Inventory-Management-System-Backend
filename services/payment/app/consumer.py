"""Payment's kafka consumer. Runs as its own process.

    inventory.reserved  -> charge the card   (forward step)
    order.cancelled     -> refund if paid    (COMPENSATION)

Payment is triggered by stock being reserved, not by an HTTP request. That
ordering is deliberate: failing on stock costs nothing, whereas charging first and
then discovering the item is unavailable requires a refund.
"""

from __future__ import annotations

import asyncio
import logging

from sqlalchemy.ext.asyncio import AsyncSession

from common.db.session import Database
from common.errors import ConflictError
from common.events.envelope import EventEnvelope
from common.events.schemas import InventoryReserved, OrderCancelled
from common.events.topics import Topics
from common.kafka.consumer import BaseConsumer
from common.kafka.producer import EventProducer
from common.observability.logging import configure_logging
from common.observability.metrics import events_consumed
from common.worker import run_worker
from app.config import settings
from app.models import ProcessedEvent
from app.services import PaymentService

configure_logging(service_name=settings.service_name, level=settings.log_level)
logger = logging.getLogger(__name__)

TOPICS = [Topics.INVENTORY_RESERVED, Topics.ORDER_CANCELLED]


class PaymentConsumer(BaseConsumer):
    def __init__(self, *, db: Database, producer: EventProducer) -> None:
        super().__init__(
            name="payment-consumer",
            topics=TOPICS,
            group_id="payment-saga",
            bootstrap_servers=settings.kafka_bootstrap_servers,
            db=db,
            producer=producer,
            processed_event_model=ProcessedEvent,
            handlers={
                Topics.INVENTORY_RESERVED: self.on_inventory_reserved,
                Topics.ORDER_CANCELLED: self.on_order_cancelled,
            },
            max_retries=settings.consumer_max_retries,
            retry_backoff_ms=settings.consumer_retry_backoff_ms,
        )

    async def on_inventory_reserved(self, session: AsyncSession, envelope: EventEnvelope) -> None:
        """Stock is held, so it is safe to take the money.

        `charge_for_order` returns None for an order that already has a payment -
        which is the duplicate-delivery case, and exactly what must NOT result in a
        second charge.
        """
        event = envelope.parse(InventoryReserved)

        service = PaymentService(session, correlation_id=envelope.correlation_id)
        try:
            payment = await service.charge_for_order(
                order_id=event.order_id,
                user_id=event.user_id,
                amount=event.total_amount,
                currency=event.currency,
                method=event.payment_method.type,
                token=event.payment_method.token,
                card_last4=event.payment_method.last4,
            )
        except ConflictError:
            # Concurrent duplicate, stopped by the UNIQUE constraint on order_id.
            # Not a failure: the correct outcome already exists.
            self._count(envelope, "duplicate")
            return

        self._count(envelope, "ok" if payment is not None else "duplicate")

    async def on_order_cancelled(self, session: AsyncSession, envelope: EventEnvelope) -> None:
        """Refund if money was actually taken.

        The `was_paid` flag lets this service decide for itself rather than being
        told what to do. Order publishes what happened; Payment works out what that
        means for it.
        """
        event = envelope.parse(OrderCancelled)

        if not event.was_paid:
            logger.info(
                "cancelled order was never paid, nothing to refund",
                extra={"order_id": str(event.order_id)},
            )
            self._count(envelope, "ok")
            return

        service = PaymentService(session, correlation_id=envelope.correlation_id)
        await service.refund_for_order(
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

    consumer = PaymentConsumer(db=db, producer=producer)
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
    run_worker(main, name="payment-consumer")
