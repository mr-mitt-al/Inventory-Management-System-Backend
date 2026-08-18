from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from common.order_status import OrderStatus
from app.models import DeadLetter, Order, ProductSnapshot


class OrderRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get(self, order_id: UUID) -> Order | None:
        return await self._session.get(Order, order_id)

    async def get_for_update(self, order_id: UUID) -> Order | None:
        """Lock the order row before a saga transition.

        Two events for the same order can be handled concurrently by two consumer
        replicas. Without the lock, both read status=INVENTORY_RESERVED and both
        try to transition, producing duplicate history rows and duplicate
        published events.
        """
        result = await self._session.execute(
            select(Order).where(Order.id == order_id).with_for_update(of=Order)
        )
        return result.scalar_one_or_none()

    async def get_by_idempotency_key(self, key: str) -> Order | None:
        result = await self._session.execute(
            select(Order).where(Order.idempotency_key == key)
        )
        return result.scalar_one_or_none()

    async def list_for_user(
        self,
        user_id: UUID,
        *,
        offset: int,
        limit: int,
        status: OrderStatus | None = None,
    ) -> tuple[list[Order], int]:
        conditions = [Order.user_id == user_id]
        if status is not None:
            conditions.append(Order.status == status.value)
        return await self._paginate(conditions, offset=offset, limit=limit)

    async def list_all(
        self,
        *,
        offset: int,
        limit: int,
        status: OrderStatus | None = None,
        user_id: UUID | None = None,
        created_after: datetime | None = None,
        created_before: datetime | None = None,
    ) -> tuple[list[Order], int]:
        conditions = []
        if status is not None:
            conditions.append(Order.status == status.value)
        if user_id is not None:
            conditions.append(Order.user_id == user_id)
        if created_after is not None:
            conditions.append(Order.created_at >= created_after)
        if created_before is not None:
            conditions.append(Order.created_at <= created_before)
        return await self._paginate(conditions, offset=offset, limit=limit)

    async def _paginate(
        self, conditions: list, *, offset: int, limit: int
    ) -> tuple[list[Order], int]:
        stmt = select(Order)
        count_stmt = select(func.count()).select_from(Order)
        for condition in conditions:
            stmt = stmt.where(condition)
            count_stmt = count_stmt.where(condition)

        total = int((await self._session.execute(count_stmt)).scalar_one())
        rows = (
            (
                await self._session.execute(
                    # id as tiebreaker: without it, orders created in the same
                    # instant can shuffle between pages.
                    stmt.order_by(Order.created_at.desc(), Order.id).offset(offset).limit(limit)
                )
            )
            .scalars()
            .all()
        )
        return list(rows), total

    async def count_by_status(self) -> dict[str, int]:
        rows = await self._session.execute(
            select(Order.status, func.count()).group_by(Order.status)
        )
        return {status: int(count) for status, count in rows.all()}

    async def revenue_since(self, since: datetime) -> float:
        """Confirmed-and-beyond revenue. Excludes FAILED and CANCELLED."""
        counted = [
            OrderStatus.PAID.value,
            OrderStatus.CONFIRMED.value,
            OrderStatus.SHIPPED.value,
            OrderStatus.DELIVERED.value,
        ]
        result = await self._session.execute(
            select(func.coalesce(func.sum(Order.total_amount), 0)).where(
                Order.created_at >= since, Order.status.in_(counted)
            )
        )
        return float(result.scalar_one())


class ProductSnapshotRepository:
    """Order's local view of the catalog, fed by `catalog.product.upserted`."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_many(self, product_ids: list[UUID]) -> dict[UUID, ProductSnapshot]:
        rows = (
            (
                await self._session.execute(
                    select(ProductSnapshot).where(ProductSnapshot.product_id.in_(product_ids))
                )
            )
            .scalars()
            .all()
        )
        return {row.product_id: row for row in rows}

    async def upsert(
        self,
        *,
        product_id: UUID,
        sku: str,
        name: str,
        price,
        currency: str,
        is_active: bool,
    ) -> ProductSnapshot:
        from common.db.base import utcnow

        existing = await self._session.get(ProductSnapshot, product_id)
        if existing is None:
            existing = ProductSnapshot(
                product_id=product_id,
                sku=sku,
                name=name,
                price=price,
                currency=currency,
                is_active=is_active,
            )
            self._session.add(existing)
        else:
            existing.sku = sku
            existing.name = name
            existing.price = price
            existing.currency = currency
            existing.is_active = is_active
            existing.updated_at = utcnow()

        await self._session.flush()
        return existing

    async def count(self) -> int:
        result = await self._session.execute(select(func.count()).select_from(ProductSnapshot))
        return int(result.scalar_one())


class DeadLetterRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get(self, dead_letter_id: UUID) -> DeadLetter | None:
        return await self._session.get(DeadLetter, dead_letter_id)

    async def exists(self, dlq_event_id: UUID) -> bool:
        result = await self._session.execute(
            select(DeadLetter.id).where(DeadLetter.dlq_event_id == dlq_event_id)
        )
        return result.scalar_one_or_none() is not None

    async def list(
        self, *, offset: int, limit: int, status: str | None = None, topic: str | None = None
    ) -> tuple[list[DeadLetter], int]:
        conditions = []
        if status is not None:
            conditions.append(DeadLetter.status == status)
        if topic is not None:
            conditions.append(DeadLetter.original_topic == topic)

        stmt = select(DeadLetter)
        count_stmt = select(func.count()).select_from(DeadLetter)
        for condition in conditions:
            stmt = stmt.where(condition)
            count_stmt = count_stmt.where(condition)

        total = int((await self._session.execute(count_stmt)).scalar_one())
        rows = (
            (
                await self._session.execute(
                    stmt.order_by(DeadLetter.created_at.desc()).offset(offset).limit(limit)
                )
            )
            .scalars()
            .all()
        )
        return list(rows), total

    async def depth(self) -> int:
        """Parked count - the number worth alerting on."""
        result = await self._session.execute(
            select(func.count()).select_from(DeadLetter).where(DeadLetter.status == "PARKED")
        )
        return int(result.scalar_one())
