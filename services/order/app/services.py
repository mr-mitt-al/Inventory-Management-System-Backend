"""Order creation and queries.

Saga transitions live in `saga.py`; this module handles the request-driven side.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from uuid import UUID

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from common.db.outbox import enqueue_event
from common.errors import ConflictError, ForbiddenError, NotFoundError, ValidationError
from common.events.envelope import make_envelope
from common.events.schemas import Address, LineItem, OrderCreated, PaymentMethod
from common.events.topics import Topics
from common.order_status import CUSTOMER_CANCELLABLE, OrderStatus
from app.config import settings
from app.models import Order, OrderItem, OrderStatusHistory, Outbox
from app.repositories import OrderRepository, ProductSnapshotRepository
from app.saga import OrderSaga
from app.schemas import CreateOrderRequest

logger = logging.getLogger(__name__)


class OrderService:
    def __init__(self, session: AsyncSession, *, correlation_id: UUID) -> None:
        self._session = session
        self._orders = OrderRepository(session)
        self._products = ProductSnapshotRepository(session)
        self._correlation_id = correlation_id

    # -------------------------------------------------------------------- create
    async def create_order(
        self,
        *,
        user_id: UUID,
        body: CreateOrderRequest,
        idempotency_key: str,
    ) -> tuple[Order, bool]:
        """Create an order. Returns (order, was_created).

        THE CRITICAL PROPERTY: the order row, its items, its first history entry
        and the `order.created` outbox row all commit in ONE transaction. Either
        the whole order exists and its event is guaranteed to be published, or
        nothing happened.

        Publishing to Kafka directly here instead would mean one of two bugs:
        publish-then-crash leaves a phantom order downstream, and commit-then-
        crash leaves an order stuck in PENDING with no event to drive it.
        """
        existing = await self._orders.get_by_idempotency_key(idempotency_key)
        if existing is not None:
            # A retry - double click, flaky network, browser retry. Return the
            # original rather than charging the customer twice.
            if existing.user_id != user_id:
                # Same key from a different user: reject rather than leak.
                raise ConflictError("idempotency key already used")
            logger.info(
                "idempotent replay of order creation",
                extra={"order_id": str(existing.id), "idempotency_key": idempotency_key},
            )
            return existing, False

        # Price from OUR OWN read-model, never from the request body.
        product_ids = [item.product_id for item in body.items]
        snapshots = await self._products.get_many(product_ids)

        missing = [pid for pid in product_ids if pid not in snapshots]
        if missing:
            raise ValidationError(
                "some products are not available",
                details={"unknown_products": [str(p) for p in missing]},
            )

        inactive = [pid for pid in product_ids if not snapshots[pid].is_active]
        if inactive:
            raise ValidationError(
                "some products are no longer for sale",
                details={"inactive_products": [str(p) for p in inactive]},
            )

        currencies = {snapshots[pid].currency for pid in product_ids}
        if len(currencies) > 1:
            # Summing mixed currencies would produce a meaningless total.
            raise ValidationError(
                "all items in an order must share one currency",
                details={"currencies": sorted(currencies)},
            )
        currency = currencies.pop()

        total = Decimal("0.00")
        order = Order(
            user_id=user_id,
            status=OrderStatus.PENDING.value,
            total_amount=total,
            currency=currency,
            shipping_address=body.shipping_address.model_dump(mode="json"),
            payment_method=body.payment_method.model_dump(mode="json"),
            idempotency_key=idempotency_key,
            correlation_id=self._correlation_id,
        )
        self._session.add(order)

        try:
            await self._session.flush()
        except IntegrityError:
            # Lost a race against a concurrent request carrying the same
            # idempotency key. The UNIQUE constraint is the real guarantee; the
            # lookup above is only the fast path.
            #
            # The rollback is mandatory, not stylistic: after an IntegrityError
            # postgres has aborted the transaction and refuses any further
            # statement until it is unwound.
            await self._session.rollback()
            duplicate = await self._orders.get_by_idempotency_key(idempotency_key)
            if duplicate is not None:
                return duplicate, False
            # The other request holds the key but has not committed yet. Ask the
            # client to retry rather than risk creating a second order.
            logger.warning(
                "idempotency key contended but no committed order found",
                extra={"idempotency_key": idempotency_key},
            )
            raise ConflictError(
                "a request with this idempotency key is already in flight, retry shortly"
            ) from None

        line_items: list[LineItem] = []
        for item in body.items:
            snapshot = snapshots[item.product_id]
            # SNAPSHOT the price and name. A later admin edit must not rewrite
            # what this customer agreed to pay.
            order_item = OrderItem(
                order_id=order.id,
                product_id=snapshot.product_id,
                sku=snapshot.sku,
                name=snapshot.name,
                unit_price=snapshot.price,
                quantity=item.quantity,
            )
            self._session.add(order_item)
            total += snapshot.price * item.quantity
            line_items.append(
                LineItem(
                    product_id=snapshot.product_id,
                    sku=snapshot.sku,
                    name=snapshot.name,
                    unit_price=snapshot.price,
                    quantity=item.quantity,
                )
            )

        order.total_amount = total

        self._session.add(
            OrderStatusHistory(
                order_id=order.id,
                from_status=None,
                to_status=OrderStatus.PENDING.value,
                reason="order placed",
            )
        )

        envelope = make_envelope(
            event_type=Topics.ORDER_CREATED,
            payload=OrderCreated(
                order_id=order.id,
                user_id=user_id,
                total_amount=total,
                currency=currency,
                items=line_items,
                shipping_address=Address.model_validate(order.shipping_address),
                payment_method=PaymentMethod.model_validate(order.payment_method),
            ),
            producer=settings.service_name,
            correlation_id=self._correlation_id,
        )
        await enqueue_event(
            self._session,
            Outbox,
            topic=Topics.ORDER_CREATED,
            # Keyed by order_id: every event for one order lands on the same
            # partition, so the saga's steps stay in order while different orders
            # process in parallel.
            key=str(order.id),
            envelope=envelope,
            aggregate_id=order.id,
        )

        await self._session.flush()
        logger.info(
            "order created",
            extra={
                "order_id": str(order.id),
                "user_id": str(user_id),
                "total": str(total),
                "items": len(line_items),
            },
        )
        return order, True

    # --------------------------------------------------------------------- reads
    async def get_for_user(self, order_id: UUID, *, caller_id: UUID, is_admin: bool) -> Order:
        order = await self._orders.get(order_id)
        if order is None:
            raise NotFoundError("order not found")

        # Ownership check. A role check alone would let any signed-in customer
        # read every other customer's orders by guessing ids.
        if not is_admin and order.user_id != caller_id:
            raise ForbiddenError("you do not have access to this order")
        return order

    # -------------------------------------------------------------------- cancel
    async def cancel_order(
        self, order_id: UUID, *, caller_id: UUID, is_admin: bool, reason: str
    ) -> Order:
        order = await self._orders.get_for_update(order_id)
        if order is None:
            raise NotFoundError("order not found")
        if not is_admin and order.user_id != caller_id:
            raise ForbiddenError("you do not have access to this order")

        if order.status_enum not in CUSTOMER_CANCELLABLE:
            raise ConflictError(
                f"an order in status {order.status} cannot be cancelled",
                details={"status": order.status},
            )

        saga = OrderSaga(self._session, correlation_id=self._correlation_id)
        await saga.cancel(order, reason=reason, actor="admin" if is_admin else "customer")
        return order

    # --------------------------------------------------------------- admin moves
    async def ship_order(self, order_id: UUID) -> Order:
        order = await self._orders.get_for_update(order_id)
        if order is None:
            raise NotFoundError("order not found")
        saga = OrderSaga(self._session, correlation_id=self._correlation_id)
        await saga.transition(order, OrderStatus.SHIPPED, reason="dispatched")
        return order

    async def deliver_order(self, order_id: UUID) -> Order:
        order = await self._orders.get_for_update(order_id)
        if order is None:
            raise NotFoundError("order not found")
        saga = OrderSaga(self._session, correlation_id=self._correlation_id)
        await saga.transition(order, OrderStatus.DELIVERED, reason="delivered")
        return order
