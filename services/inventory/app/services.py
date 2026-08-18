"""Inventory domain logic.

This is where overselling is prevented, and it is the one place in the system
where the correctness argument is genuinely subtle. Read `reserve` carefully.
"""

from __future__ import annotations

import logging
from datetime import timedelta
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from common.db.base import utcnow
from common.db.outbox import enqueue_event
from common.errors import ConflictError, NotFoundError, ValidationError
from common.events.envelope import make_envelope
from common.events.schemas import (
    InsufficientItem,
    InventoryReservationFailed,
    InventoryReserved,
    LowStock,
    PaymentMethod,
    ReservedItem,
    StockChanged,
)
from common.events.topics import Topics
from app.config import settings
from app.models import (
    LedgerReason,
    Outbox,
    Reservation,
    ReservationItem,
    ReservationStatus,
    StockItem,
    StockLedger,
)

logger = logging.getLogger(__name__)


class InsufficientStock(ConflictError):
    code = "insufficient_stock"


class InventoryService:
    def __init__(self, session: AsyncSession, *, correlation_id: UUID | None = None) -> None:
        self._session = session
        self._correlation_id = correlation_id

    # ------------------------------------------------------------------- reserve
    async def reserve(
        self,
        *,
        order_id: UUID,
        user_id: UUID,
        items: list[tuple[UUID, int]],
        total_amount,
        currency: str,
        payment_method: PaymentMethod,
    ) -> Reservation | None:
        """Set stock aside for an order.

        Returns the reservation, or None if this order was already reserved
        (duplicate event).

        Three things make this correct under concurrency:

        1. ``with_for_update()`` takes a row lock on each stock row. Without it,
           two orders for the last unit both read available_qty=1, both decide
           "yes", and both succeed. The lock forces them to take turns, so the
           second one sees available_qty=0 and fails.

        2. ``order_by(product_id)`` locks rows in a deterministic order. Two
           multi-item orders that overlap - A wants {X,Y}, B wants {Y,X} - would
           otherwise each hold one lock and wait for the other forever. Postgres
           detects the deadlock and kills one transaction, but a consistent lock
           order means it never happens.

        3. All-or-nothing. If ANY item is short, nothing is reserved. A partially
           reserved order would need partial compensation, which is a category of
           bug not worth inventing.
        """
        if not items:
            raise ValidationError("cannot reserve an empty order")

        existing = await self._get_reservation_by_order(order_id)
        if existing is not None:
            logger.info(
                "reservation already exists for order, skipping",
                extra={"order_id": str(order_id), "status": existing.status},
            )
            return None

        wanted: dict[UUID, int] = {}
        for product_id, quantity in items:
            if quantity <= 0:
                raise ValidationError("quantity must be positive")
            # Aggregate duplicate lines for the same product: two lines of 1 must
            # reserve 2, and must be checked against stock as 2.
            wanted[product_id] = wanted.get(product_id, 0) + quantity

        rows = (
            (
                await self._session.execute(
                    select(StockItem)
                    .where(StockItem.product_id.in_(list(wanted)))
                    .order_by(StockItem.product_id)   # see (2)
                    .with_for_update()                # see (1)
                )
            )
            .scalars()
            .all()
        )
        stock_by_id = {row.product_id: row for row in rows}

        insufficient: list[InsufficientItem] = []
        for product_id, quantity in wanted.items():
            row = stock_by_id.get(product_id)
            if row is None:
                insufficient.append(
                    InsufficientItem(product_id=product_id, requested=quantity, available=0)
                )
            elif row.available_qty < quantity:
                insufficient.append(
                    InsufficientItem(
                        product_id=product_id,
                        requested=quantity,
                        available=row.available_qty,
                    )
                )

        if insufficient:
            await self._publish_reservation_failed(
                order_id=order_id,
                user_id=user_id,
                reason="insufficient stock",
                insufficient=insufficient,
            )
            logger.warning(
                "reservation failed, insufficient stock",
                extra={"order_id": str(order_id), "items": len(insufficient)},
            )
            return None

        reservation = Reservation(
            order_id=order_id,
            user_id=user_id,
            status=ReservationStatus.HELD.value,
            expires_at=utcnow() + timedelta(minutes=settings.reservation_ttl_minutes),
        )
        self._session.add(reservation)

        try:
            await self._session.flush()
        except IntegrityError:
            # Lost the race against a concurrent delivery of the same event. The
            # UNIQUE on order_id did its job; treat it as the duplicate it is.
            logger.info(
                "concurrent duplicate reservation rejected by unique constraint",
                extra={"order_id": str(order_id)},
            )
            raise ConflictError("reservation already exists for this order") from None

        for product_id, quantity in sorted(wanted.items(), key=lambda kv: str(kv[0])):
            row = stock_by_id[product_id]
            row.available_qty -= quantity
            row.reserved_qty += quantity

            self._session.add(
                ReservationItem(
                    reservation_id=reservation.id, product_id=product_id, quantity=quantity
                )
            )
            self._record_ledger(
                product_id=product_id,
                delta=-quantity,
                reason=LedgerReason.RESERVE,
                order_id=order_id,
                balance_after=row.available_qty,
            )
            await self._publish_stock_changed(row, LedgerReason.RESERVE, order_id)
            await self._maybe_publish_low_stock(row)

        await self._session.flush()

        await self._publish_reserved(
            reservation=reservation,
            wanted=wanted,
            total_amount=total_amount,
            currency=currency,
            payment_method=payment_method,
        )

        logger.info(
            "stock reserved",
            extra={
                "order_id": str(order_id),
                "reservation_id": str(reservation.id),
                "items": len(wanted),
                "expires_at": reservation.expires_at.isoformat(),
            },
        )
        return reservation

    # -------------------------------------------------------------------- commit
    async def commit_reservation(self, *, order_id: UUID) -> Reservation | None:
        """Payment succeeded: the held units leave the building permanently.

        `reserved_qty` drops; `available_qty` is untouched, because those units
        were already moved out of available at reservation time.
        """
        reservation = await self._get_reservation_by_order(order_id, for_update=True)
        if reservation is None:
            # Payment succeeded for an order with no reservation. Possible if the
            # reservation expired first. Log loudly - it means stock was oversold
            # relative to what the customer paid for, and a human must intervene.
            logger.error(
                "payment succeeded but no reservation exists - manual review needed",
                extra={"order_id": str(order_id)},
            )
            return None

        if reservation.status_enum is ReservationStatus.COMMITTED:
            logger.info("reservation already committed", extra={"order_id": str(order_id)})
            return reservation

        if reservation.status_enum is not ReservationStatus.HELD:
            logger.error(
                "cannot commit a reservation that is not held - stock may have been released",
                extra={"order_id": str(order_id), "status": reservation.status},
            )
            return reservation

        for item in reservation.items:
            row = await self._lock_stock_item(item.product_id)
            if row is None:
                continue
            row.reserved_qty -= item.quantity
            self._record_ledger(
                product_id=item.product_id,
                delta=0,  # available unchanged; units move out of reserved
                reason=LedgerReason.COMMIT,
                order_id=order_id,
                balance_after=row.available_qty,
            )
            await self._publish_stock_changed(row, LedgerReason.COMMIT, order_id)

        reservation.status = ReservationStatus.COMMITTED.value
        await self._session.flush()
        logger.info("reservation committed", extra={"order_id": str(order_id)})
        return reservation

    # ------------------------------------------------------------------- release
    async def release_reservation(
        self, *, order_id: UUID, reason: str = "payment failed"
    ) -> Reservation | None:
        """THE COMPENSATING TRANSACTION.

        Inventory succeeded, Payment failed. There is no shared transaction to
        roll back - Inventory's change was committed and its database is separate.
        So Inventory undoes its own work: the held units go back on the shelf and
        become sellable again.
        """
        reservation = await self._get_reservation_by_order(order_id, for_update=True)
        if reservation is None:
            logger.info(
                "nothing to release for order", extra={"order_id": str(order_id)}
            )
            return None

        if reservation.status_enum in {ReservationStatus.RELEASED, ReservationStatus.EXPIRED}:
            logger.info("reservation already released", extra={"order_id": str(order_id)})
            return reservation

        if reservation.status_enum is ReservationStatus.COMMITTED:
            # Cancelling a paid order: units were already deducted, so this is a
            # restock rather than a release.
            return await self._restock_committed(reservation, reason)

        for item in reservation.items:
            row = await self._lock_stock_item(item.product_id)
            if row is None:
                continue
            row.reserved_qty -= item.quantity
            row.available_qty += item.quantity  # back on the shelf
            self._record_ledger(
                product_id=item.product_id,
                delta=item.quantity,
                reason=LedgerReason.RELEASE,
                order_id=order_id,
                balance_after=row.available_qty,
            )
            await self._publish_stock_changed(row, LedgerReason.RELEASE, order_id)

        reservation.status = ReservationStatus.RELEASED.value
        await self._session.flush()
        logger.info(
            "reservation released, stock returned to available",
            extra={"order_id": str(order_id), "reason": reason},
        )
        return reservation

    async def _restock_committed(self, reservation: Reservation, reason: str) -> Reservation:
        """A paid order was cancelled: put committed units back into available."""
        for item in reservation.items:
            row = await self._lock_stock_item(item.product_id)
            if row is None:
                continue
            row.available_qty += item.quantity
            self._record_ledger(
                product_id=item.product_id,
                delta=item.quantity,
                reason=LedgerReason.RESTOCK,
                order_id=reservation.order_id,
                balance_after=row.available_qty,
            )
            await self._publish_stock_changed(row, LedgerReason.RESTOCK, reservation.order_id)

        reservation.status = ReservationStatus.RELEASED.value
        await self._session.flush()
        logger.info(
            "committed units restocked after cancellation",
            extra={"order_id": str(reservation.order_id), "reason": reason},
        )
        return reservation

    # ------------------------------------------------------------------- sweeper
    async def expire_stale_reservations(self, *, limit: int = 100) -> int:
        """Return stock from reservations whose TTL has passed.

        The safety net that makes the whole reservation model trustworthy: if any
        service dies mid-saga, stock comes back on its own instead of being
        stranded until someone notices.
        """
        now = utcnow()
        stale = (
            (
                await self._session.execute(
                    select(Reservation)
                    .where(
                        Reservation.status == ReservationStatus.HELD.value,
                        Reservation.expires_at < now,
                    )
                    .order_by(Reservation.expires_at)
                    .limit(limit)
                    .with_for_update(skip_locked=True)  # several sweepers can run
                )
            )
            .scalars()
            .all()
        )

        for reservation in stale:
            for item in reservation.items:
                row = await self._lock_stock_item(item.product_id)
                if row is None:
                    continue
                row.reserved_qty -= item.quantity
                row.available_qty += item.quantity
                self._record_ledger(
                    product_id=item.product_id,
                    delta=item.quantity,
                    reason=LedgerReason.EXPIRE,
                    order_id=reservation.order_id,
                    balance_after=row.available_qty,
                )
                await self._publish_stock_changed(row, LedgerReason.EXPIRE, reservation.order_id)

            reservation.status = ReservationStatus.EXPIRED.value
            logger.warning(
                "reservation expired, stock reclaimed",
                extra={
                    "order_id": str(reservation.order_id),
                    "reservation_id": str(reservation.id),
                },
            )

        if stale:
            await self._session.flush()
        return len(stale)

    # --------------------------------------------------------------------- admin
    async def upsert_stock_item(
        self, *, product_id: UUID, sku: str, quantity: int, low_stock_threshold: int | None = None
    ) -> StockItem:
        """Create or restock a stock row. Used by admin restock and seeding."""
        row = await self._lock_stock_item(product_id)
        if row is None:
            row = StockItem(
                product_id=product_id,
                sku=sku.upper(),
                available_qty=quantity,
                reserved_qty=0,
                low_stock_threshold=(
                    low_stock_threshold
                    if low_stock_threshold is not None
                    else settings.default_low_stock_threshold
                ),
            )
            self._session.add(row)
            await self._session.flush()
            balance_reason = LedgerReason.RESTOCK
        else:
            row.available_qty += quantity
            if low_stock_threshold is not None:
                row.low_stock_threshold = low_stock_threshold
            balance_reason = LedgerReason.RESTOCK

        self._record_ledger(
            product_id=product_id,
            delta=quantity,
            reason=balance_reason,
            order_id=None,
            balance_after=row.available_qty,
        )
        await self._publish_stock_changed(row, balance_reason, None)
        await self._session.flush()
        logger.info(
            "stock restocked",
            extra={"product_id": str(product_id), "delta": quantity, "now": row.available_qty},
        )
        return row

    async def adjust_stock(
        self, *, product_id: UUID, available_qty: int, low_stock_threshold: int | None
    ) -> StockItem:
        """Set an absolute available quantity - a stock-take correction.

        Deliberately separate from restock: "we counted and there are 40" is a
        different operation from "40 more arrived", and conflating them makes the
        ledger meaningless.
        """
        row = await self._lock_stock_item(product_id)
        if row is None:
            raise NotFoundError("no stock record for this product")

        delta = available_qty - row.available_qty
        row.available_qty = available_qty
        if low_stock_threshold is not None:
            row.low_stock_threshold = low_stock_threshold

        self._record_ledger(
            product_id=product_id,
            delta=delta,
            reason=LedgerReason.ADJUST,
            order_id=None,
            balance_after=row.available_qty,
        )
        await self._publish_stock_changed(row, LedgerReason.ADJUST, None)
        await self._maybe_publish_low_stock(row)
        await self._session.flush()
        return row

    # ----------------------------------------------------------------- internals
    async def _lock_stock_item(self, product_id: UUID) -> StockItem | None:
        result = await self._session.execute(
            select(StockItem).where(StockItem.product_id == product_id).with_for_update()
        )
        return result.scalar_one_or_none()

    async def _get_reservation_by_order(
        self, order_id: UUID, *, for_update: bool = False
    ) -> Reservation | None:
        stmt = select(Reservation).where(Reservation.order_id == order_id)
        if for_update:
            # of=Reservation: lock the reservation row only. Without it, the
            # selectin-loaded items would also be locked and the lock set would
            # differ between callers.
            stmt = stmt.with_for_update(of=Reservation)
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none()

    def _record_ledger(
        self,
        *,
        product_id: UUID,
        delta: int,
        reason: LedgerReason,
        order_id: UUID | None,
        balance_after: int,
    ) -> None:
        self._session.add(
            StockLedger(
                product_id=product_id,
                delta=delta,
                reason=reason.value,
                ref_order_id=order_id,
                balance_after=balance_after,
            )
        )

    async def _publish_reserved(
        self,
        *,
        reservation: Reservation,
        wanted: dict[UUID, int],
        total_amount,
        currency: str,
        payment_method: PaymentMethod,
    ) -> None:
        envelope = make_envelope(
            event_type=Topics.INVENTORY_RESERVED,
            payload=InventoryReserved(
                order_id=reservation.order_id,
                user_id=reservation.user_id,
                reservation_id=reservation.id,
                expires_at=reservation.expires_at,
                items=[
                    ReservedItem(product_id=pid, quantity=qty) for pid, qty in wanted.items()
                ],
                total_amount=total_amount,
                currency=currency,
                payment_method=payment_method,
            ),
            producer=settings.service_name,
            correlation_id=self._correlation_id,
        )
        await enqueue_event(
            self._session,
            Outbox,
            topic=Topics.INVENTORY_RESERVED,
            key=str(reservation.order_id),
            envelope=envelope,
            aggregate_id=reservation.order_id,
        )

    async def _publish_reservation_failed(
        self,
        *,
        order_id: UUID,
        user_id: UUID,
        reason: str,
        insufficient: list[InsufficientItem],
    ) -> None:
        envelope = make_envelope(
            event_type=Topics.INVENTORY_RESERVATION_FAILED,
            payload=InventoryReservationFailed(
                order_id=order_id,
                user_id=user_id,
                reason=reason,
                insufficient_items=insufficient,
            ),
            producer=settings.service_name,
            correlation_id=self._correlation_id,
        )
        await enqueue_event(
            self._session,
            Outbox,
            topic=Topics.INVENTORY_RESERVATION_FAILED,
            key=str(order_id),
            envelope=envelope,
            aggregate_id=order_id,
        )

    async def _publish_stock_changed(
        self, row: StockItem, reason: LedgerReason, order_id: UUID | None
    ) -> None:
        envelope = make_envelope(
            event_type=Topics.INVENTORY_STOCK_CHANGED,
            payload=StockChanged(
                product_id=row.product_id,
                sku=row.sku,
                available_qty=row.available_qty,
                reserved_qty=row.reserved_qty,
                reason=reason.value,
                ref_order_id=order_id,
            ),
            producer=settings.service_name,
            correlation_id=self._correlation_id,
        )
        await enqueue_event(
            self._session,
            Outbox,
            topic=Topics.INVENTORY_STOCK_CHANGED,
            # Keyed by product, not order: Catalog cares about per-product
            # ordering of stock updates, and two orders touching one product must
            # land on the same partition to stay in sequence.
            key=str(row.product_id),
            envelope=envelope,
            aggregate_id=row.product_id,
        )

    async def _maybe_publish_low_stock(self, row: StockItem) -> None:
        if not row.is_low:
            return
        envelope = make_envelope(
            event_type=Topics.INVENTORY_LOW_STOCK,
            payload=LowStock(
                product_id=row.product_id,
                sku=row.sku,
                available_qty=row.available_qty,
                threshold=row.low_stock_threshold,
            ),
            producer=settings.service_name,
            correlation_id=self._correlation_id,
        )
        await enqueue_event(
            self._session,
            Outbox,
            topic=Topics.INVENTORY_LOW_STOCK,
            key=str(row.product_id),
            envelope=envelope,
            aggregate_id=row.product_id,
        )
