"""Consumer-side deduplication.

Kafka delivers at-least-once. Duplicate delivery is normal operation, not a
fault: a rebalance, a slow commit, or a retried producer send all cause it.
Without protection a duplicate ``inventory.reserved`` charges the customer
twice.

Each service owns a ``processed_events`` table. The critical detail is that the
row is inserted in the SAME transaction as the handler's business writes - if
the bookkeeping commits separately, a crash in between either double-processes
or silently skips.
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy import DateTime, String, func, select
from sqlalchemy.dialects.postgresql import UUID as PgUUID
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column


class ProcessedEventMixin:
    """Mix into a service's own ``Base`` to get its ``processed_events`` table.

    A mixin rather than a shared model because each service has its own
    metadata and migration history.
    """

    __tablename__ = "processed_events"

    event_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True)
    event_type: Mapped[str] = mapped_column(String(100), nullable=False)
    consumer: Mapped[str] = mapped_column(String(100), nullable=False)
    consumed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


async def already_processed(
    session: AsyncSession,
    model: type[ProcessedEventMixin],
    event_id: UUID,
) -> bool:
    result = await session.execute(
        select(model.event_id).where(model.event_id == event_id)  # type: ignore[attr-defined]
    )
    return result.scalar_one_or_none() is not None


async def mark_processed(
    session: AsyncSession,
    model: type[ProcessedEventMixin],
    *,
    event_id: UUID,
    event_type: str,
    consumer: str,
) -> None:
    """Record the event as handled. Caller must NOT commit separately."""
    session.add(model(event_id=event_id, event_type=event_type, consumer=consumer))  # type: ignore[call-arg]
    await session.flush()
