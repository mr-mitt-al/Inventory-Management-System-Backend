"""The order saga.

This service is the ORCHESTRATOR. It owns the order's state machine and decides
what the next step is, rather than each service chaining blindly to the next
(choreography). The trade-off, stated plainly: Order knows the shape of the
workflow, which is less decoupled - but there is one place to read, one place to
debug, and one place where an illegal transition is caught.

There is no distributed transaction and no coordinator issuing rollbacks. Each
service undoes its own work in response to a fact:

    order.created                 -> Inventory reserves
    inventory.reserved            -> Order marks INVENTORY_RESERVED
                                     Payment charges
    payment.succeeded             -> Order marks PAID then CONFIRMED
                                     Inventory commits the reservation
    payment.failed                -> Order marks FAILED
                                     Inventory RELEASES the reservation
    inventory.reservation_failed  -> Order marks FAILED (nothing to undo)
"""

from __future__ import annotations

import logging
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from common.db.base import utcnow
from common.db.outbox import enqueue_event
from common.errors import InvalidStateTransition
from common.events.envelope import make_envelope
from common.events.schemas import LineItem, OrderCancelled, OrderConfirmed
from common.events.topics import Topics
from common.observability.metrics import saga_duration_seconds
from common.order_status import (
    PAID_STATUSES,
    OrderStatus,
    can_transition,
    is_terminal,
)
from app.config import settings
from app.models import Order, OrderStatusHistory, Outbox

logger = logging.getLogger(__name__)


class OrderSaga:
    """State transitions for one order. Always used inside a transaction."""

    def __init__(self, session: AsyncSession, *, correlation_id: UUID | None = None) -> None:
        self._session = session
        self._correlation_id = correlation_id

    # ---------------------------------------------------------------- transitions
    async def transition(
        self,
        order: Order,
        target: OrderStatus,
        *,
        reason: str | None = None,
        strict: bool = True,
    ) -> bool:
        """Move an order to a new status, recording history.

        Returns False if the move was a no-op (already there) or refused.

        `strict=False` is for event-driven callers: an event arriving for an order
        that has already moved on is not an error worth dead-lettering, it is a
        duplicate or a race. It gets logged and dropped.
        """
        current = order.status_enum

        if current is target:
            logger.info(
                "order already in target status, ignoring",
                extra={"order_id": str(order.id), "status": target.value},
            )
            return False

        if not can_transition(current, target):
            message = (
                f"cannot move order {order.id} from {current.value} to {target.value}"
            )
            if strict:
                # Raised rather than ignored on purpose: in an event-driven system
                # this usually means events arrived out of order, which is worth
                # seeing rather than silently absorbing.
                raise InvalidStateTransition(message)
            logger.warning(
                "illegal transition ignored",
                extra={
                    "order_id": str(order.id),
                    "from": current.value,
                    "to": target.value,
                    "reason": reason,
                },
            )
            return False

        order.status = target.value
        if reason:
            order.failure_reason = reason if target in {OrderStatus.FAILED} else order.failure_reason

        self._session.add(
            OrderStatusHistory(
                order_id=order.id,
                from_status=current.value,
                to_status=target.value,
                reason=reason,
            )
        )

        if is_terminal(target) or target is OrderStatus.CONFIRMED:
            if order.completed_at is None and is_terminal(target):
                order.completed_at = utcnow()
            self._observe_duration(order, target)

        await self._session.flush()
        logger.info(
            "order transitioned",
            extra={
                "order_id": str(order.id),
                "from": current.value,
                "to": target.value,
                "reason": reason,
            },
        )
        return True

    def _observe_duration(self, order: Order, target: OrderStatus) -> None:
        try:
            elapsed = (utcnow() - order.created_at).total_seconds()
            saga_duration_seconds.labels(terminal_status=target.value).observe(elapsed)
        except Exception:  # metrics must never break the saga
            logger.debug("failed to record saga duration", exc_info=True)

    # ------------------------------------------------------------- forward steps
    async def on_inventory_reserved(self, order: Order) -> bool:
        return await self.transition(
            order, OrderStatus.INVENTORY_RESERVED, reason="stock reserved", strict=False
        )

    async def on_payment_succeeded(self, order: Order) -> bool:
        """PAID then immediately CONFIRMED.

        Two transitions rather than one because they mean different things - money
        taken, and order accepted - and the history table should show both. In a
        larger system fraud checks or address validation would sit between them.
        """
        if not await self.transition(
            order, OrderStatus.PAID, reason="payment captured", strict=False
        ):
            return False

        await self.transition(order, OrderStatus.CONFIRMED, reason="order confirmed")
        await self._publish_confirmed(order)
        return True

    # --------------------------------------------------------------- compensation
    async def on_payment_failed(self, order: Order, *, failure_code: str, message: str) -> bool:
        """Payment declined after stock was already reserved.

        Order does NOT tell Inventory to release anything. Payment published
        `payment.failed`; Inventory consumes the same event and releases its own
        reservation. Order's job is only to record the outcome and stop the saga.
        """
        moved = await self.transition(
            order,
            OrderStatus.FAILED,
            reason=f"payment failed ({failure_code}): {message}"[:500],
            strict=False,
        )
        if moved:
            # Announce cancellation so Notification can inform the customer.
            # Inventory ignores this one - it already acted on payment.failed -
            # and its release is idempotent anyway.
            await self._publish_cancelled(
                order, reason=f"payment failed: {failure_code}", was_paid=False
            )
        return moved

    async def on_reservation_failed(self, order: Order, *, reason: str) -> bool:
        """Out of stock. The cheapest possible failure: nothing has been done yet,
        so there is nothing to compensate. Payment is never invoked."""
        return await self.transition(
            order, OrderStatus.FAILED, reason=f"out of stock: {reason}"[:500], strict=False
        )

    async def cancel(self, order: Order, *, reason: str, actor: str = "customer") -> bool:
        """Cancel an order, fanning out whatever compensation its state requires.

        A PAID order needs a refund from Payment and a restock from Inventory; a
        PENDING one needs neither. The `was_paid` flag in the event is what lets
        each service decide for itself, without Order instructing them.
        """
        was_paid = order.status_enum in PAID_STATUSES

        moved = await self.transition(
            order, OrderStatus.CANCELLED, reason=f"cancelled by {actor}: {reason}"[:500]
        )
        if moved:
            await self._publish_cancelled(order, reason=reason, was_paid=was_paid)
        return moved

    # -------------------------------------------------------------------- publish
    async def _publish_confirmed(self, order: Order) -> None:
        envelope = make_envelope(
            event_type=Topics.ORDER_CONFIRMED,
            payload=OrderConfirmed(
                order_id=order.id,
                user_id=order.user_id,
                total_amount=order.total_amount,
                currency=order.currency,
                items=[_to_line_item(item) for item in order.items],
                confirmed_at=utcnow(),
            ),
            producer=settings.service_name,
            correlation_id=self._correlation_id or order.correlation_id,
        )
        await enqueue_event(
            self._session,
            Outbox,
            topic=Topics.ORDER_CONFIRMED,
            key=str(order.id),
            envelope=envelope,
            aggregate_id=order.id,
        )

    async def _publish_cancelled(self, order: Order, *, reason: str, was_paid: bool) -> None:
        envelope = make_envelope(
            event_type=Topics.ORDER_CANCELLED,
            payload=OrderCancelled(
                order_id=order.id,
                user_id=order.user_id,
                reason=reason,
                was_paid=was_paid,
                items=[_to_line_item(item) for item in order.items],
            ),
            producer=settings.service_name,
            correlation_id=self._correlation_id or order.correlation_id,
        )
        await enqueue_event(
            self._session,
            Outbox,
            topic=Topics.ORDER_CANCELLED,
            key=str(order.id),
            envelope=envelope,
            aggregate_id=order.id,
        )


def _to_line_item(item) -> LineItem:
    return LineItem(
        product_id=item.product_id,
        sku=item.sku,
        name=item.name,
        unit_price=item.unit_price,
        quantity=item.quantity,
    )
