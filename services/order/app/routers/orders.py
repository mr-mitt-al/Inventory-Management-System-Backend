"""Customer-facing order endpoints."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Query, Response, status
from fastapi.responses import StreamingResponse

from common.auth.dependencies import get_current_user
from common.auth.jwt import TokenUser
from common.order_status import OrderStatus, is_terminal
from common.pagination import Page, PageParams, page_params
from app.config import settings
from app.dependencies import (
    IdempotencyKeyDep,
    OrderServiceDep,
    SessionDep,
    get_db,
)
from app.repositories import OrderRepository
from app.schemas import (
    CancelOrderRequest,
    CreateOrderRequest,
    CreateOrderResponse,
    OrderDetailResponse,
    OrderResponse,
    OrderSummaryResponse,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/orders", tags=["orders"])


@router.post(
    "",
    response_model=CreateOrderResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Place an order",
)
async def create_order(
    body: CreateOrderRequest,
    service: OrderServiceDep,
    idempotency_key: IdempotencyKeyDep,
    caller: Annotated[TokenUser, Depends(get_current_user)],
    response: Response,
) -> CreateOrderResponse:
    """Returns **202 Accepted**, not 201 Created.

    The order exists. It is NOT confirmed - stock has not been reserved and the
    card has not been charged. Both happen asynchronously over the next second or
    two, and either can fail.

    Telling the client "order placed successfully" here would be a lie that the
    compensation path immediately exposes. Poll `GET /orders/{id}` or subscribe to
    `GET /orders/{id}/stream` to watch the saga progress.

    Prices come from the Order service's own product read-model, never from the
    request body - `OrderItemRequest` cannot carry a price.
    """
    order, created = await service.create_order(
        user_id=caller.user_id, body=body, idempotency_key=idempotency_key
    )

    if not created:
        # Idempotent replay. 200 rather than 202 tells the client this is the
        # original order, not a newly accepted one.
        response.status_code = status.HTTP_200_OK

    return CreateOrderResponse(
        order_id=order.id,
        status=order.status_enum,
        total_amount=order.total_amount,
        currency=order.currency,
        message=(
            "order accepted and is being processed"
            if created
            else "this order was already placed"
        ),
        track_url=f"/orders/{order.id}",
    )


@router.get("", response_model=Page[OrderSummaryResponse], summary="Your order history")
async def list_my_orders(
    session: SessionDep,
    caller: Annotated[TokenUser, Depends(get_current_user)],
    params: Annotated[PageParams, Depends(page_params)],
    order_status: Annotated[OrderStatus | None, Query(alias="status")] = None,
) -> Page[OrderSummaryResponse]:
    orders, total = await OrderRepository(session).list_for_user(
        caller.user_id, offset=params.offset, limit=params.limit, status=order_status
    )
    return Page.build(
        [OrderSummaryResponse.model_validate(o) for o in orders], total=total, params=params
    )


@router.get("/{order_id}", response_model=OrderDetailResponse, summary="Order detail + timeline")
async def get_order(
    order_id: UUID,
    service: OrderServiceDep,
    caller: Annotated[TokenUser, Depends(get_current_user)],
) -> OrderDetailResponse:
    """Includes the full status history, which is what the frontend renders as the
    saga stepper."""
    order = await service.get_for_user(
        order_id, caller_id=caller.user_id, is_admin=caller.is_admin
    )
    return OrderDetailResponse.model_validate(order)


@router.get(
    "/{order_id}/stream",
    summary="Live order status (Server-Sent Events)",
    response_class=StreamingResponse,
)
async def stream_order(
    order_id: UUID,
    caller: Annotated[TokenUser, Depends(get_current_user)],
) -> StreamingResponse:
    """Pushes an event whenever the order's status changes.

    Implemented by re-reading the order on an interval rather than subscribing to
    Redis. One moving part instead of two, and the frontend polls as a fallback
    anyway - so a dropped stream degrades latency, never correctness.

    The connection closes as soon as the order reaches a terminal state, so a
    client watching a finished order does not hold a socket open forever.

    Honest limitation: one database query per connected client per interval. Fine
    at this scale; a Redis pub/sub fan-out is the upgrade if it ever isn't.
    """
    db = get_db()

    async def event_stream():
        started = time.monotonic()
        last_status: str | None = None
        last_heartbeat = started

        # Authorize once, before opening the stream - not inside the loop.
        async with db.session_factory() as session:
            order = await OrderRepository(session).get(order_id)
            if order is None:
                yield _sse({"error": "order not found"}, event="error")
                return
            if not caller.is_admin and order.user_id != caller.user_id:
                yield _sse({"error": "forbidden"}, event="error")
                return

        try:
            while time.monotonic() - started < settings.sse_max_duration_seconds:
                async with db.session_factory() as session:
                    order = await OrderRepository(session).get(order_id)
                    if order is None:
                        yield _sse({"error": "order disappeared"}, event="error")
                        return

                    current = order.status
                    if current != last_status:
                        last_status = current
                        last_heartbeat = time.monotonic()
                        yield _sse(
                            {
                                "order_id": str(order.id),
                                "status": current,
                                "failure_reason": order.failure_reason,
                                "total_amount": str(order.total_amount),
                                "updated_at": order.updated_at.isoformat(),
                            },
                            event="status",
                        )

                    if is_terminal(order.status_enum):
                        yield _sse({"order_id": str(order.id), "status": current}, event="done")
                        return

                # Comment-only heartbeat keeps proxies from closing an idle
                # connection while an order sits in one state.
                if time.monotonic() - last_heartbeat >= settings.sse_heartbeat_seconds:
                    last_heartbeat = time.monotonic()
                    yield ": keepalive\n\n"

                await asyncio.sleep(settings.sse_poll_interval_seconds)

            yield _sse({"reason": "stream timeout, reconnect"}, event="timeout")
        except asyncio.CancelledError:
            # Client disconnected. Normal, not an error.
            logger.debug("sse client disconnected", extra={"order_id": str(order_id)})
            raise

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",  # stops nginx buffering the stream
        },
    )


@router.post("/{order_id}/cancel", response_model=OrderResponse, summary="Cancel an order")
async def cancel_order(
    order_id: UUID,
    body: CancelOrderRequest,
    service: OrderServiceDep,
    caller: Annotated[TokenUser, Depends(get_current_user)],
) -> OrderResponse:
    """Publishes `order.cancelled` with a `was_paid` flag.

    Each service then decides its own compensation: Payment refunds if money was
    taken, Inventory releases a held reservation or restocks committed units.
    Order does not instruct either of them.
    """
    order = await service.cancel_order(
        order_id,
        caller_id=caller.user_id,
        is_admin=caller.is_admin,
        reason=body.reason,
    )
    return OrderResponse.model_validate(order)


def _sse(data: dict, *, event: str) -> str:
    """Format one Server-Sent Event frame. The blank line terminates it."""
    return f"event: {event}\ndata: {json.dumps(data)}\n\n"
