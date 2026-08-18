from __future__ import annotations

from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Payment, PaymentStatus


class PaymentRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_by_order(self, order_id: UUID) -> Payment | None:
        result = await self._session.execute(
            select(Payment).where(Payment.order_id == order_id)
        )
        return result.scalar_one_or_none()

    async def get(self, payment_id: UUID) -> Payment | None:
        return await self._session.get(Payment, payment_id)

    async def list_for_user(
        self, user_id: UUID, *, offset: int, limit: int
    ) -> tuple[list[Payment], int]:
        return await self._paginate([Payment.user_id == user_id], offset=offset, limit=limit)

    async def list_all(
        self,
        *,
        offset: int,
        limit: int,
        status: PaymentStatus | None = None,
        user_id: UUID | None = None,
    ) -> tuple[list[Payment], int]:
        conditions = []
        if status is not None:
            conditions.append(Payment.status == status.value)
        if user_id is not None:
            conditions.append(Payment.user_id == user_id)
        return await self._paginate(conditions, offset=offset, limit=limit)

    async def _paginate(
        self, conditions: list, *, offset: int, limit: int
    ) -> tuple[list[Payment], int]:
        stmt = select(Payment)
        count_stmt = select(func.count()).select_from(Payment)
        for condition in conditions:
            stmt = stmt.where(condition)
            count_stmt = count_stmt.where(condition)

        total = int((await self._session.execute(count_stmt)).scalar_one())
        rows = (
            (
                await self._session.execute(
                    stmt.order_by(Payment.created_at.desc(), Payment.id)
                    .offset(offset)
                    .limit(limit)
                )
            )
            .scalars()
            .all()
        )
        return list(rows), total

    async def stats(self) -> dict:
        by_status = {
            status: int(count)
            for status, count in (
                await self._session.execute(
                    select(Payment.status, func.count()).group_by(Payment.status)
                )
            ).all()
        }
        captured = float(
            (
                await self._session.execute(
                    select(func.coalesce(func.sum(Payment.amount), 0)).where(
                        Payment.status == PaymentStatus.SUCCEEDED.value
                    )
                )
            ).scalar_one()
        )
        failures = {
            code: int(count)
            for code, count in (
                await self._session.execute(
                    select(Payment.failure_code, func.count())
                    .where(Payment.failure_code.is_not(None))
                    .group_by(Payment.failure_code)
                )
            ).all()
        }
        return {"by_status": by_status, "captured_total": captured, "failures_by_code": failures}
