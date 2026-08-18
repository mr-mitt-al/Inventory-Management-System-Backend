"""Notification service. Consumer-only - no HTTP API, no ingress.

Subscribes to seven topics and sends a message for each. The point worth noticing:

    **No other service knows this one exists.**

Auth does not call it. Order does not call it. Adding it required zero changes to
any other service - it simply started consuming events that were already being
published. Deleting it would break nothing.

That is what event-driven decoupling actually buys you, and it is the easiest part
of this architecture to demonstrate in an interview.
"""

from __future__ import annotations

import asyncio
import logging
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from common.db.base import utcnow
from common.db.session import Database
from common.events.envelope import EventEnvelope
from common.events.schemas import (
    InventoryReservationFailed,
    LowStock,
    OrderCancelled,
    OrderConfirmed,
    PaymentFailed,
    PaymentRefunded,
    UserRegistered,
)
from common.events.topics import Topics
from common.kafka.consumer import BaseConsumer
from common.kafka.producer import EventProducer
from common.observability.logging import configure_logging
from common.observability.metrics import events_consumed
from common.worker import run_worker
from app import templates
from app.config import settings
from app.models import Channel, DeliveryStatus, Notification, ProcessedEvent, UserContact
from app.sender import DeliveryError, deliver

configure_logging(service_name=settings.service_name, level=settings.log_level)
logger = logging.getLogger(__name__)

TOPICS = [
    Topics.USER_REGISTERED,
    Topics.ORDER_CONFIRMED,
    Topics.ORDER_CANCELLED,
    Topics.PAYMENT_FAILED,
    Topics.PAYMENT_REFUNDED,
    Topics.INVENTORY_RESERVATION_FAILED,
    Topics.INVENTORY_LOW_STOCK,
]


class NotificationConsumer(BaseConsumer):
    def __init__(self, *, db: Database, producer: EventProducer) -> None:
        super().__init__(
            name="notification-consumer",
            topics=TOPICS,
            group_id="notification-all",
            bootstrap_servers=settings.kafka_bootstrap_servers,
            db=db,
            producer=producer,
            processed_event_model=ProcessedEvent,
            handlers={
                Topics.USER_REGISTERED: self.on_user_registered,
                Topics.ORDER_CONFIRMED: self.on_order_confirmed,
                Topics.ORDER_CANCELLED: self.on_order_cancelled,
                Topics.PAYMENT_FAILED: self.on_payment_failed,
                Topics.PAYMENT_REFUNDED: self.on_payment_refunded,
                Topics.INVENTORY_RESERVATION_FAILED: self.on_reservation_failed,
                Topics.INVENTORY_LOW_STOCK: self.on_low_stock,
            },
            max_retries=settings.consumer_max_retries,
            retry_backoff_ms=settings.consumer_retry_backoff_ms,
        )

    # ------------------------------------------------------------------ handlers
    async def on_user_registered(self, session: AsyncSession, envelope: EventEnvelope) -> None:
        """Store the contact AND send the welcome message.

        The contact row must be written first, because every later notification for
        this user depends on it.
        """
        event = envelope.parse(UserRegistered)

        contact = await session.get(UserContact, event.user_id)
        if contact is None:
            session.add(
                UserContact(
                    user_id=event.user_id, email=event.email, full_name=event.full_name
                )
            )
        else:
            contact.email = event.email
            contact.full_name = event.full_name
            contact.updated_at = utcnow()
        await session.flush()

        subject, body = templates.welcome(full_name=event.full_name)
        await self._send(
            session,
            envelope=envelope,
            user_id=event.user_id,
            recipient=event.email,
            template="welcome",
            subject=subject,
            body=body,
            ref_id=event.user_id,
        )

    async def on_order_confirmed(self, session: AsyncSession, envelope: EventEnvelope) -> None:
        event = envelope.parse(OrderConfirmed)
        subject, body = templates.order_confirmed(
            order_id=event.order_id,
            total_amount=event.total_amount,
            currency=event.currency,
            items=[
                {
                    "name": item.name,
                    "quantity": item.quantity,
                    "unit_price": item.unit_price,
                }
                for item in event.items
            ],
        )
        await self._send_to_user(
            session,
            envelope=envelope,
            user_id=event.user_id,
            template="order_confirmed",
            subject=subject,
            body=body,
            ref_id=event.order_id,
        )

    async def on_payment_failed(self, session: AsyncSession, envelope: EventEnvelope) -> None:
        """Tells the customer two things: no money was taken, and their reserved
        items went back to stock. The second half is the compensation being made
        visible - without it they would not know whether the items are still held
        for them."""
        event = envelope.parse(PaymentFailed)
        subject, body = templates.payment_failed(
            order_id=event.order_id,
            amount=event.amount,
            currency=event.currency,
            failure_code=event.failure_code,
            failure_message=event.failure_message,
            retryable=event.retryable,
        )
        await self._send_to_user(
            session,
            envelope=envelope,
            user_id=event.user_id,
            template="payment_failed",
            subject=subject,
            body=body,
            ref_id=event.order_id,
        )

    async def on_reservation_failed(self, session: AsyncSession, envelope: EventEnvelope) -> None:
        event = envelope.parse(InventoryReservationFailed)
        subject, body = templates.order_out_of_stock(
            order_id=event.order_id, reason=event.reason
        )
        await self._send_to_user(
            session,
            envelope=envelope,
            user_id=event.user_id,
            template="order_out_of_stock",
            subject=subject,
            body=body,
            ref_id=event.order_id,
        )

    async def on_order_cancelled(self, session: AsyncSession, envelope: EventEnvelope) -> None:
        event = envelope.parse(OrderCancelled)
        subject, body = templates.order_cancelled(
            order_id=event.order_id, reason=event.reason, was_paid=event.was_paid
        )
        await self._send_to_user(
            session,
            envelope=envelope,
            user_id=event.user_id,
            template="order_cancelled",
            subject=subject,
            body=body,
            ref_id=event.order_id,
        )

    async def on_payment_refunded(self, session: AsyncSession, envelope: EventEnvelope) -> None:
        event = envelope.parse(PaymentRefunded)
        subject, body = templates.payment_refunded(
            order_id=event.order_id,
            amount=event.amount,
            currency=event.currency,
            reason=event.reason,
        )
        await self._send_to_user(
            session,
            envelope=envelope,
            user_id=event.user_id,
            template="payment_refunded",
            subject=subject,
            body=body,
            ref_id=event.order_id,
        )

    async def on_low_stock(self, session: AsyncSession, envelope: EventEnvelope) -> None:
        """Goes to the admin address, not a customer - this event has no user."""
        event = envelope.parse(LowStock)
        subject, body = templates.low_stock_alert(
            sku=event.sku, available_qty=event.available_qty, threshold=event.threshold
        )
        await self._send(
            session,
            envelope=envelope,
            user_id=None,
            recipient=settings.admin_alert_email,
            template="low_stock_alert",
            subject=subject,
            body=body,
            ref_id=event.product_id,
        )

    # ----------------------------------------------------------------- internals
    async def _send_to_user(
        self,
        session: AsyncSession,
        *,
        envelope: EventEnvelope,
        user_id: UUID,
        template: str,
        subject: str,
        body: str,
        ref_id: UUID | None,
    ) -> None:
        contact = await session.get(UserContact, user_id)
        if contact is None:
            # No contact row: this user registered before the service first ran, or
            # user.registered has not been consumed yet. Record the intent so it is
            # visible, but do NOT raise - failing here would dead-letter a
            # perfectly valid order event over a missing email address.
            logger.warning(
                "no contact details for user, notification not delivered",
                extra={"user_id": str(user_id), "template": template},
            )
            session.add(
                Notification(
                    user_id=user_id,
                    recipient="unknown",
                    channel=Channel.EMAIL.value,
                    template=template,
                    subject=subject,
                    body=body,
                    trigger_event_type=envelope.event_type,
                    trigger_ref_id=ref_id,
                    status=DeliveryStatus.FAILED.value,
                    error="no contact details on file",
                )
            )
            await session.flush()
            self._count(envelope, "ignored")
            return

        await self._send(
            session,
            envelope=envelope,
            user_id=user_id,
            recipient=contact.email,
            template=template,
            subject=subject,
            body=body,
            ref_id=ref_id,
        )

    async def _send(
        self,
        session: AsyncSession,
        *,
        envelope: EventEnvelope,
        user_id: UUID | None,
        recipient: str,
        template: str,
        subject: str,
        body: str,
        ref_id: UUID | None,
    ) -> None:
        notification = Notification(
            user_id=user_id,
            recipient=recipient,
            channel=Channel.EMAIL.value,
            template=template,
            subject=subject,
            body=body,
            trigger_event_type=envelope.event_type,
            trigger_ref_id=ref_id,
            context={"correlation_id": str(envelope.correlation_id)},
            status=DeliveryStatus.PENDING.value,
        )
        session.add(notification)
        await session.flush()

        try:
            await deliver(
                channel=Channel.EMAIL, recipient=recipient, subject=subject, body=body
            )
        except DeliveryError as exc:
            # Recorded as FAILED rather than re-raised. Re-raising would retry and
            # then dead-letter the ORIGINAL business event - so a bad email address
            # would look like a broken order pipeline.
            notification.status = DeliveryStatus.FAILED.value
            notification.error = str(exc)[:500]
            await session.flush()
            logger.error(
                "notification delivery failed",
                extra={"template": template, "recipient": recipient},
            )
            self._count(envelope, "failed")
            return

        notification.status = DeliveryStatus.SENT.value
        notification.sent_at = utcnow()
        await session.flush()
        self._count(envelope, "ok")

    def _count(self, envelope: EventEnvelope, status: str) -> None:
        events_consumed.labels(
            consumer=self.name,
            topic=envelope.event_type,
            event_type=envelope.event_type,
            status=status,
        ).inc()


async def main(stop: asyncio.Event) -> None:
    db = Database(settings.database_url, pool_size=6, max_overflow=3)
    producer = EventProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        producer_name=f"{settings.service_name}-dlq",
    )
    await producer.start()

    consumer = NotificationConsumer(db=db, producer=producer)
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
    run_worker(main, name="notification-consumer")
