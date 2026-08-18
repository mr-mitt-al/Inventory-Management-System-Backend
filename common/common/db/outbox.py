"""Transactional outbox - solves the dual-write problem.

You cannot atomically commit to Postgres and publish to Kafka. Two ways to get
it wrong:

  publish first, then commit  -> transaction rolls back, but downstream services
                                 already reserved stock for an order that does
                                 not exist.
  commit first, then publish  -> process dies in between, order sits in PENDING
                                 forever with no event to drive the saga.

So the event is written to an ``outbox`` table inside the same transaction as
the business change. The database becomes the single commit point. A separate
publisher process relays unpublished rows to Kafka, retrying until they land.

This gives at-least-once publishing, which is exactly why consumers must be
idempotent (see ``common.db.idempotency``).
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import DateTime, Integer, String, func, select, update
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PgUUID
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column

from common.db.session import Database
from common.events.envelope import EventEnvelope
from common.kafka.producer import EventProducer

logger = logging.getLogger(__name__)


class OutboxMixin:
    """Mix into a service's own ``Base`` to get its ``outbox`` table."""

    __tablename__ = "outbox"

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    aggregate_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    topic: Mapped[str] = mapped_column(String(100), nullable=False)
    partition_key: Mapped[str] = mapped_column(String(100), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_error: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    published_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )


async def enqueue_event(
    session: AsyncSession,
    model: type[OutboxMixin],
    *,
    topic: str,
    key: str,
    envelope: EventEnvelope,
    aggregate_id: UUID,
) -> None:
    """Queue an event for publication.

    Must be called inside the same transaction as the business write. Does not
    commit - that is the caller's job, and the whole point.
    """
    session.add(
        model(  # type: ignore[call-arg]
            aggregate_id=aggregate_id,
            topic=topic,
            partition_key=key,
            payload=envelope.model_dump(mode="json"),
        )
    )
    await session.flush()


class OutboxPublisher:
    """Relays unpublished outbox rows to Kafka. Runs as its own process.

    ``FOR UPDATE SKIP LOCKED`` means several publisher replicas can run
    concurrently without publishing the same row twice - each grabs a disjoint
    batch instead of blocking on the other's locks.
    """

    def __init__(
        self,
        *,
        db: Database,
        producer: EventProducer,
        outbox_model: type[OutboxMixin],
        poll_interval_ms: int = 500,
        batch_size: int = 100,
        service_name: str = "outbox-publisher",
    ) -> None:
        self._db = db
        self._producer = producer
        self._model = outbox_model
        self._interval = poll_interval_ms / 1000
        self._batch_size = batch_size
        self._service = service_name
        self._stopping = asyncio.Event()

    async def run(self) -> None:
        logger.info(
            "outbox publisher started",
            extra={"service": self._service, "interval_s": self._interval},
        )
        while not self._stopping.is_set():
            try:
                published = await self._publish_batch()
                # Drain a backlog at full speed; idle politely when caught up.
                if published == 0:
                    await self._sleep(self._interval)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("outbox publish cycle failed")
                await self._sleep(self._interval)

    def stop(self) -> None:
        self._stopping.set()

    async def _sleep(self, seconds: float) -> None:
        try:
            await asyncio.wait_for(self._stopping.wait(), timeout=seconds)
        except TimeoutError:
            pass

    async def _publish_batch(self) -> int:
        model = self._model
        async with self._db.transaction() as session:
            rows = (
                (
                    await session.execute(
                        select(model)
                        .where(model.published_at.is_(None))  # type: ignore[attr-defined]
                        .order_by(model.created_at)  # type: ignore[attr-defined]
                        .limit(self._batch_size)
                        .with_for_update(skip_locked=True)
                    )
                )
                .scalars()
                .all()
            )

            if not rows:
                return 0

            published_ids: list[UUID] = []
            for row in rows:
                try:
                    envelope = EventEnvelope.model_validate(row.payload)
                    await self._producer.send(
                        topic=row.topic,
                        key=row.partition_key,
                        envelope=envelope,
                    )
                    published_ids.append(row.id)
                except Exception as exc:  # keep the row, retry next cycle
                    row.attempts += 1
                    row.last_error = str(exc)[:1000]
                    logger.warning(
                        "outbox row publish failed",
                        extra={
                            "outbox_id": str(row.id),
                            "topic": row.topic,
                            "attempts": row.attempts,
                        },
                        exc_info=True,
                    )

            if published_ids:
                await session.execute(
                    update(model)
                    .where(model.id.in_(published_ids))  # type: ignore[attr-defined]
                    .values(published_at=func.now())
                )
                logger.info("outbox published", extra={"count": len(published_ids)})

            return len(published_ids)

    async def pending_count(self) -> int:
        """Backing value for the ``outbox_pending`` gauge.

        If this climbs, the publisher is stuck and orders are silently stalling
        in PENDING - the single most valuable alert in the system.
        """
        model = self._model
        async with self._db.session_factory() as session:
            result = await session.execute(
                select(func.count())
                .select_from(model)
                .where(model.published_at.is_(None))  # type: ignore[attr-defined]
            )
            return int(result.scalar_one())
