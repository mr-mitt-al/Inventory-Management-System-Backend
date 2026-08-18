from __future__ import annotations

from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Reservation, ReservationStatus, StockItem, StockLedger


class StockRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get(self, product_id: UUID) -> StockItem | None:
        return await self._session.get(StockItem, product_id)

    async def get_many(self, product_ids: list[UUID]) -> list[StockItem]:
        result = await self._session.execute(
            select(StockItem).where(StockItem.product_id.in_(product_ids))
        )
        return list(result.scalars().all())

    async def list_low_stock(self, *, offset: int, limit: int) -> tuple[list[StockItem], int]:
        condition = StockItem.available_qty <= StockItem.low_stock_threshold
        total = int(
            (
                await self._session.execute(
                    select(func.count()).select_from(StockItem).where(condition)
                )
            ).scalar_one()
        )
        rows = (
            (
                await self._session.execute(
                    select(StockItem)
                    .where(condition)
                    .order_by(StockItem.available_qty, StockItem.product_id)
                    .offset(offset)
                    .limit(limit)
                )
            )
            .scalars()
            .all()
        )
        return list(rows), total

    async def list_all(self, *, offset: int, limit: int) -> tuple[list[StockItem], int]:
        total = int(
            (await self._session.execute(select(func.count()).select_from(StockItem))).scalar_one()
        )
        rows = (
            (
                await self._session.execute(
                    select(StockItem).order_by(StockItem.sku).offset(offset).limit(limit)
                )
            )
            .scalars()
            .all()
        )
        return list(rows), total

    async def ledger_for_product(
        self, product_id: UUID, *, offset: int, limit: int
    ) -> tuple[list[StockLedger], int]:
        condition = StockLedger.product_id == product_id
        total = int(
            (
                await self._session.execute(
                    select(func.count()).select_from(StockLedger).where(condition)
                )
            ).scalar_one()
        )
        rows = (
            (
                await self._session.execute(
                    select(StockLedger)
                    .where(condition)
                    .order_by(StockLedger.created_at.desc())
                    .offset(offset)
                    .limit(limit)
                )
            )
            .scalars()
            .all()
        )
        return list(rows), total


class ReservationRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_by_order(self, order_id: UUID) -> Reservation | None:
        result = await self._session.execute(
            select(Reservation).where(Reservation.order_id == order_id)
        )
        return result.scalar_one_or_none()

    async def list_active(self, *, offset: int, limit: int) -> tuple[list[Reservation], int]:
        condition = Reservation.status == ReservationStatus.HELD.value
        total = int(
            (
                await self._session.execute(
                    select(func.count()).select_from(Reservation).where(condition)
                )
            ).scalar_one()
        )
        rows = (
            (
                await self._session.execute(
                    select(Reservation)
                    .where(condition)
                    .order_by(Reservation.expires_at)
                    .offset(offset)
                    .limit(limit)
                )
            )
            .scalars()
            .all()
        )
        return list(rows), total

    async def count_by_status(self) -> dict[str, int]:
        rows = await self._session.execute(
            select(Reservation.status, func.count()).group_by(Reservation.status)
        )
        return {status: int(count) for status, count in rows.all()}
