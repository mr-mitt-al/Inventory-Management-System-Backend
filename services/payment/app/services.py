"""Payment domain logic.

The rule this module exists to uphold: **a customer is charged at most once per
order, no matter how many times Kafka delivers the same event.**
"""

from __future__ import annotations

import logging
from decimal import Decimal
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from common.db.base import utcnow
from common.db.outbox import enqueue_event
from common.errors import ConflictError, NotFoundError, ValidationError
from common.events.envelope import make_envelope
from common.events.schemas import PaymentFailed, PaymentRefunded, PaymentSucceeded
from common.events.topics import Topics
from common.observability.metrics import payment_failures
from app.config import settings
from app.mock_gateway import PaymentError, gateway
from app.models import Outbox, Payment, PaymentStatus, Refund, RefundStatus

logger = logging.getLogger(__name__)


class PaymentService:
    def __init__(self, session: AsyncSession, *, correlation_id: UUID | None = None) -> None:
        self._session = session
        self._correlation_id = correlation_id

    # -------------------------------------------------------------------- charge
    async def charge_for_order(
        self,
        *,
        order_id: UUID,
        user_id: UUID,
        amount: Decimal,
        currency: str,
        method: str,
        token: str,
        card_last4: str | None,
    ) -> Payment | None:
        """Charge once for an order.

        Returns None when this order has already been charged - a duplicate event,
        which is normal Kafka behaviour rather than an error.

        The insert happens BEFORE the gateway call, so the UNIQUE constraint on
        order_id is what serialises concurrent duplicates. Calling the gateway
        first and inserting afterwards would mean two concurrent deliveries both
        charge the card and only then discover the conflict - money already gone.
        """
        existing = await self._get_by_order(order_id)
        if existing is not None:
            logger.info(
                "payment already exists for order, not charging again",
                extra={"order_id": str(order_id), "status": existing.status},
            )
            return None

        payment = Payment(
            order_id=order_id,
            user_id=user_id,
            amount=amount,
            currency=currency,
            status=PaymentStatus.PENDING.value,
            method=method,
            card_last4=card_last4,
            attempts=1,
        )
        self._session.add(payment)

        try:
            await self._session.flush()
        except IntegrityError:
            # A concurrent delivery won the race. The constraint did its job: the
            # card has not been touched by this attempt.
            logger.info(
                "concurrent duplicate payment rejected by unique constraint",
                extra={"order_id": str(order_id)},
            )
            raise ConflictError("a payment already exists for this order") from None

        await self._attempt_charge(payment, token=token)
        return payment

    async def retry_payment(self, *, order_id: UUID, token: str, caller_id: UUID) -> Payment:
        """Customer-initiated retry of a FAILED payment.

        Only a failed payment can be retried, and only by its owner. A SUCCEEDED
        payment is never re-charged - that is the whole point.
        """
        payment = await self._get_by_order(order_id, for_update=True)
        if payment is None:
            raise NotFoundError("no payment found for this order")
        if payment.user_id != caller_id:
            from common.errors import ForbiddenError

            raise ForbiddenError("you do not have access to this payment")

        if payment.status_enum is PaymentStatus.SUCCEEDED:
            raise ConflictError("this order has already been paid")
        if payment.status_enum is PaymentStatus.REFUNDED:
            raise ConflictError("this payment has been refunded")

        payment.attempts += 1
        payment.status = PaymentStatus.PENDING.value
        payment.failure_code = None
        payment.failure_message = None
        await self._session.flush()

        await self._attempt_charge(payment, token=token)
        return payment

    async def _attempt_charge(self, payment: Payment, *, token: str) -> None:
        """Call the gateway and record the outcome as an event either way.

        Both branches publish. `payment.succeeded` drives the saga forward;
        `payment.failed` is what Inventory reacts to by releasing its reservation -
        without anyone instructing it to.
        """
        try:
            result = await gateway.charge(
                order_id=payment.order_id,
                amount=payment.amount,
                currency=payment.currency,
                token=token,
            )
        except PaymentError as exc:
            payment.status = PaymentStatus.FAILED.value
            payment.failure_code = exc.failure_code
            payment.failure_message = exc.message[:500]
            await self._session.flush()

            payment_failures.labels(code=exc.failure_code).inc()

            await self._publish(
                topic=Topics.PAYMENT_FAILED,
                order_id=payment.order_id,
                payload=PaymentFailed(
                    order_id=payment.order_id,
                    user_id=payment.user_id,
                    payment_id=payment.id,
                    amount=payment.amount,
                    currency=payment.currency,
                    failure_code=exc.failure_code,
                    failure_message=exc.message[:500],
                    retryable=exc.retryable,
                ),
            )
            logger.warning(
                "payment failed",
                extra={
                    "order_id": str(payment.order_id),
                    "failure_code": exc.failure_code,
                    "retryable": exc.retryable,
                },
            )
            return

        payment.status = PaymentStatus.SUCCEEDED.value
        payment.provider_ref = result.provider_ref
        payment.paid_at = utcnow()
        await self._session.flush()

        await self._publish(
            topic=Topics.PAYMENT_SUCCEEDED,
            order_id=payment.order_id,
            payload=PaymentSucceeded(
                order_id=payment.order_id,
                user_id=payment.user_id,
                payment_id=payment.id,
                amount=payment.amount,
                currency=payment.currency,
                method=payment.method,
                provider_ref=result.provider_ref,
                paid_at=payment.paid_at,
            ),
        )
        logger.info(
            "payment succeeded",
            extra={
                "order_id": str(payment.order_id),
                "payment_id": str(payment.id),
                "provider_ref": result.provider_ref,
            },
        )

    # -------------------------------------------------------------------- refund
    async def refund_for_order(
        self, *, order_id: UUID, reason: str, amount: Decimal | None = None
    ) -> Refund | None:
        """Compensation for a cancelled order that was already paid.

        Returns None when there is nothing to refund - no payment, or it never
        succeeded. That is the common case for a cancelled PENDING order and is
        not an error.
        """
        payment = await self._get_by_order(order_id, for_update=True)
        if payment is None:
            logger.info("no payment to refund", extra={"order_id": str(order_id)})
            return None

        if payment.status_enum is PaymentStatus.REFUNDED:
            logger.info("payment already refunded", extra={"order_id": str(order_id)})
            return None

        if payment.status_enum is not PaymentStatus.SUCCEEDED:
            logger.info(
                "payment was never captured, nothing to refund",
                extra={"order_id": str(order_id), "status": payment.status},
            )
            return None

        refund_amount = amount if amount is not None else payment.amount
        if refund_amount <= 0:
            raise ValidationError("refund amount must be positive")
        if refund_amount > payment.amount - payment.refunded_amount:
            raise ValidationError("refund would exceed the captured amount")

        refund = Refund(
            payment_id=payment.id,
            amount=refund_amount,
            reason=reason[:300],
            status=RefundStatus.PENDING.value,
        )
        self._session.add(refund)
        await self._session.flush()

        try:
            result = await gateway.refund(
                provider_ref=payment.provider_ref or "", amount=refund_amount
            )
        except PaymentError as exc:
            refund.status = RefundStatus.FAILED.value
            await self._session.flush()
            # A failed refund must be visible and fixed by a human - silently
            # swallowing it means the customer never gets their money back.
            logger.error(
                "refund failed at the provider - manual intervention needed",
                extra={"order_id": str(order_id), "error": exc.message},
            )
            return refund

        refund.status = RefundStatus.COMPLETED.value
        refund.provider_ref = result.provider_ref
        payment.status = PaymentStatus.REFUNDED.value
        await self._session.flush()

        await self._publish(
            topic=Topics.PAYMENT_REFUNDED,
            order_id=order_id,
            payload=PaymentRefunded(
                order_id=order_id,
                user_id=payment.user_id,
                payment_id=payment.id,
                refund_id=refund.id,
                amount=refund_amount,
                currency=payment.currency,
                reason=reason[:300],
            ),
        )
        logger.info(
            "refund completed",
            extra={
                "order_id": str(order_id),
                "refund_id": str(refund.id),
                "amount": str(refund_amount),
            },
        )
        return refund

    # ----------------------------------------------------------------- internals
    async def _get_by_order(self, order_id: UUID, *, for_update: bool = False) -> Payment | None:
        stmt = select(Payment).where(Payment.order_id == order_id)
        if for_update:
            stmt = stmt.with_for_update(of=Payment)
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none()

    async def _publish(self, *, topic: str, order_id: UUID, payload) -> None:
        envelope = make_envelope(
            event_type=topic,
            payload=payload,
            producer=settings.service_name,
            correlation_id=self._correlation_id,
        )
        await enqueue_event(
            self._session,
            Outbox,
            topic=topic,
            key=str(order_id),
            envelope=envelope,
            aggregate_id=order_id,
        )
