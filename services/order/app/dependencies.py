from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Annotated
from uuid import UUID, uuid4

from fastapi import Depends, Header, Request
from sqlalchemy.ext.asyncio import AsyncSession

from common.db.session import Database
from common.errors import ValidationError
from common.kafka.producer import EventProducer
from app.services import OrderService


class AppState:
    db: Database | None = None
    # The API needs a producer only for DLQ replay, which republishes an event
    # directly rather than through the outbox (the original event already exists;
    # there is no new business state to commit alongside it).
    producer: EventProducer | None = None


state = AppState()


def get_db() -> Database:
    if state.db is None:
        raise RuntimeError("database not initialized")
    return state.db


def get_producer() -> EventProducer:
    if state.producer is None:
        raise RuntimeError("producer not initialized")
    return state.producer


async def get_session() -> AsyncIterator[AsyncSession]:
    async with get_db().session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


def get_correlation_id(request: Request) -> UUID:
    raw = getattr(request.state, "correlation_id", None)
    try:
        return UUID(str(raw))
    except (TypeError, ValueError):
        return uuid4()


async def get_idempotency_key(
    idempotency_key: Annotated[
        str | None,
        Header(
            alias="Idempotency-Key",
            description="Required. A UUID generated once per checkout attempt.",
        ),
    ] = None,
) -> str:
    """Required, not optional.

    Without it, a double-clicked Place Order button creates two orders, reserves
    stock twice and charges the customer twice. Making the client generate it once
    per checkout - not once per click - is what makes the retry safe.
    """
    if not idempotency_key or not idempotency_key.strip():
        raise ValidationError(
            "the Idempotency-Key header is required on order creation",
            details={"hint": "generate one uuid per checkout attempt and reuse it on retries"},
        )
    key = idempotency_key.strip()
    if len(key) > 200:
        raise ValidationError("Idempotency-Key must be at most 200 characters")
    return key


async def get_order_service(
    session: Annotated[AsyncSession, Depends(get_session)],
    correlation_id: Annotated[UUID, Depends(get_correlation_id)],
) -> OrderService:
    return OrderService(session, correlation_id=correlation_id)


SessionDep = Annotated[AsyncSession, Depends(get_session)]
OrderServiceDep = Annotated[OrderService, Depends(get_order_service)]
CorrelationIdDep = Annotated[UUID, Depends(get_correlation_id)]
IdempotencyKeyDep = Annotated[str, Depends(get_idempotency_key)]
