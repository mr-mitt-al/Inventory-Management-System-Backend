from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Query

from common.auth.dependencies import require_admin
from common.auth.jwt import TokenUser
from common.db.base import utcnow
from common.errors import ConflictError, NotFoundError
from common.events.envelope import EventEnvelope
from common.order_status import OrderStatus
from common.pagination import Page, PageParams, page_params
from app.dependencies import OrderServiceDep, SessionDep, get_producer
from app.repositories import DeadLetterRepository, OrderRepository
from app.schemas import (
    CancelOrderRequest,
    DeadLetterActionRequest,
    DeadLetterDetailResponse,
    DeadLetterResponse,
    OrderDetailResponse,
    OrderResponse,
    OrderSummaryResponse,
)

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/admin",
    tags=["order-admin"],
    dependencies=[Depends(require_admin)],
)


# ------------------------------------------------------------------------ orders
@router.get("/orders", response_model=Page[OrderSummaryResponse], summary="All orders")
async def list_all_orders(
    session: SessionDep,
    params: Annotated[PageParams, Depends(page_params)],
    order_status: Annotated[OrderStatus | None, Query(alias="status")] = None,
    user_id: Annotated[UUID | None, Query()] = None,
    created_after: Annotated[datetime | None, Query()] = None,
    created_before: Annotated[datetime | None, Query()] = None,
) -> Page[OrderSummaryResponse]:
    orders, total = await OrderRepository(session).list_all(
        offset=params.offset,
        limit=params.limit,
        status=order_status,
        user_id=user_id,
        created_after=created_after,
        created_before=created_before,
    )
    return Page.build(
        [OrderSummaryResponse.model_validate(o) for o in orders], total=total, params=params
    )


@router.get("/orders/stats", summary="Dashboard counters")
async def order_stats(session: SessionDep) -> dict:
    """Powers the admin dashboard: counts by status, today's revenue, and DLQ
    depth - the last being the number actually worth alerting on."""
    orders = OrderRepository(session)
    since = datetime.now(UTC) - timedelta(days=1)
    return {
        "by_status": await orders.count_by_status(),
        "revenue_24h": await orders.revenue_since(since),
        "dlq_depth": await DeadLetterRepository(session).depth(),
    }


@router.get(
    "/orders/{order_id}", response_model=OrderDetailResponse, summary="Any order's detail"
)
async def admin_get_order(order_id: UUID, session: SessionDep) -> OrderDetailResponse:
    order = await OrderRepository(session).get(order_id)
    if order is None:
        raise NotFoundError("order not found")
    return OrderDetailResponse.model_validate(order)


@router.post("/orders/{order_id}/ship", response_model=OrderResponse, summary="CONFIRMED -> SHIPPED")
async def ship_order(order_id: UUID, service: OrderServiceDep) -> OrderResponse:
    order = await service.ship_order(order_id)
    return OrderResponse.model_validate(order)


@router.post(
    "/orders/{order_id}/deliver", response_model=OrderResponse, summary="SHIPPED -> DELIVERED"
)
async def deliver_order(order_id: UUID, service: OrderServiceDep) -> OrderResponse:
    order = await service.deliver_order(order_id)
    return OrderResponse.model_validate(order)


@router.post(
    "/orders/{order_id}/cancel", response_model=OrderResponse, summary="Force-cancel an order"
)
async def admin_cancel_order(
    order_id: UUID,
    body: CancelOrderRequest,
    service: OrderServiceDep,
    caller: Annotated[TokenUser, Depends(require_admin)],
) -> OrderResponse:
    order = await service.cancel_order(
        order_id, caller_id=caller.user_id, is_admin=True, reason=body.reason
    )
    return OrderResponse.model_validate(order)


# --------------------------------------------------------------------------- DLQ
@router.get("/dlq", response_model=Page[DeadLetterResponse], summary="Parked messages")
async def list_dead_letters(
    session: SessionDep,
    params: Annotated[PageParams, Depends(page_params)],
    dlq_status: Annotated[str | None, Query(alias="status", pattern="^(PARKED|REPLAYED|DISCARDED)$")] = None,
    topic: Annotated[str | None, Query()] = None,
) -> Page[DeadLetterResponse]:
    """Messages that failed handling everywhere, after retries were exhausted.

    A non-zero PARKED count means a consumer is failing permanently on something
    and orders may be stuck. This is the screen that turns "the system is broken
    somewhere" into a specific message with a stack trace.
    """
    rows, total = await DeadLetterRepository(session).list(
        offset=params.offset, limit=params.limit, status=dlq_status, topic=topic
    )
    return Page.build(
        [DeadLetterResponse.model_validate(r) for r in rows], total=total, params=params
    )


@router.get("/dlq/{dead_letter_id}", response_model=DeadLetterDetailResponse, summary="Full payload")
async def get_dead_letter(dead_letter_id: UUID, session: SessionDep) -> DeadLetterDetailResponse:
    row = await DeadLetterRepository(session).get(dead_letter_id)
    if row is None:
        raise NotFoundError("dead letter not found")
    return DeadLetterDetailResponse.model_validate(row)


@router.post(
    "/dlq/{dead_letter_id}/replay",
    response_model=DeadLetterResponse,
    summary="Re-publish to the original topic",
)
async def replay_dead_letter(
    dead_letter_id: UUID,
    body: DeadLetterActionRequest,
    session: SessionDep,
    caller: Annotated[TokenUser, Depends(require_admin)],
) -> DeadLetterResponse:
    """Republish the original event so its consumer sees it again.

    Manual by design. A message that failed deterministically will fail again, and
    automatic replay of a poison message is a self-inflicted denial of service. A
    human should fix the cause, then press this.

    Safe to press even if the consumer already partially handled it: consumers
    dedupe on `event_id`, and the replay reuses the original envelope rather than
    minting a new one.
    """
    repo = DeadLetterRepository(session)
    row = await repo.get(dead_letter_id)
    if row is None:
        raise NotFoundError("dead letter not found")

    if row.status != "PARKED":
        raise ConflictError(
            f"this message is already {row.status.lower()}", details={"status": row.status}
        )
    if row.original_event is None:
        raise ConflictError(
            "this message could not be deserialized, so it cannot be replayed - "
            "inspect raw_message and fix the producer"
        )

    envelope = EventEnvelope.model_validate(row.original_event)

    # Published directly, not through the outbox: the event already exists and
    # there is no new business state to commit atomically alongside it.
    await get_producer().send(
        topic=row.original_topic, key=row.original_key, envelope=envelope
    )

    row.status = "REPLAYED"
    row.replayed_at = utcnow()
    row.replayed_by = caller.user_id
    row.note = body.note
    await session.flush()

    logger.warning(
        "dead letter replayed",
        extra={
            "dead_letter_id": str(dead_letter_id),
            "topic": row.original_topic,
            "event_id": str(envelope.event_id),
            "replayed_by": str(caller.user_id),
        },
    )
    return DeadLetterResponse.model_validate(row)


@router.post(
    "/dlq/{dead_letter_id}/discard",
    response_model=DeadLetterResponse,
    summary="Mark as reviewed and not replayable",
)
async def discard_dead_letter(
    dead_letter_id: UUID,
    body: DeadLetterActionRequest,
    session: SessionDep,
    caller: Annotated[TokenUser, Depends(require_admin)],
) -> DeadLetterResponse:
    """Not a delete - the row stays for the audit trail, marked DISCARDED with a
    note explaining why it was not replayed."""
    repo = DeadLetterRepository(session)
    row = await repo.get(dead_letter_id)
    if row is None:
        raise NotFoundError("dead letter not found")
    if row.status != "PARKED":
        raise ConflictError(f"this message is already {row.status.lower()}")

    row.status = "DISCARDED"
    row.note = body.note
    row.replayed_by = caller.user_id
    await session.flush()

    logger.warning(
        "dead letter discarded",
        extra={"dead_letter_id": str(dead_letter_id), "note": body.note},
    )
    return DeadLetterResponse.model_validate(row)
