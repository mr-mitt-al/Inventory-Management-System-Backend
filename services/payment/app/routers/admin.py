from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Query

from common.auth.dependencies import require_admin
from common.errors import ConflictError, NotFoundError
from common.pagination import Page, PageParams, page_params
from app.dependencies import PaymentServiceDep, SessionDep
from app.models import PaymentStatus
from app.repositories import PaymentRepository
from app.schemas import AdminRefundRequest, PaymentResponse, RefundResponse

router = APIRouter(
    prefix="/admin/payments",
    tags=["payment-admin"],
    dependencies=[Depends(require_admin)],
)


@router.get("", response_model=Page[PaymentResponse], summary="All transactions")
async def list_payments(
    session: SessionDep,
    params: Annotated[PageParams, Depends(page_params)],
    payment_status: Annotated[PaymentStatus | None, Query(alias="status")] = None,
    user_id: Annotated[UUID | None, Query()] = None,
) -> Page[PaymentResponse]:
    payments, total = await PaymentRepository(session).list_all(
        offset=params.offset, limit=params.limit, status=payment_status, user_id=user_id
    )
    return Page.build(
        [PaymentResponse.model_validate(p) for p in payments], total=total, params=params
    )


@router.get("/stats", summary="Counts, captured total, failures by code")
async def payment_stats(session: SessionDep) -> dict:
    return await PaymentRepository(session).stats()


@router.get("/{payment_id}", response_model=PaymentResponse, summary="One payment")
async def get_payment(payment_id: UUID, session: SessionDep) -> PaymentResponse:
    payment = await PaymentRepository(session).get(payment_id)
    if payment is None:
        raise NotFoundError("payment not found")
    return PaymentResponse.model_validate(payment)


@router.post(
    "/{payment_id}/refund", response_model=RefundResponse, summary="Manual refund (full or partial)"
)
async def refund_payment(
    payment_id: UUID,
    body: AdminRefundRequest,
    session: SessionDep,
    service: PaymentServiceDep,
) -> RefundResponse:
    """Refund outside the cancellation flow - a goodwill gesture or a dispute.

    Cancelling an order refunds automatically via `order.cancelled`; this is for
    the cases that are not a cancellation.
    """
    payment = await PaymentRepository(session).get(payment_id)
    if payment is None:
        raise NotFoundError("payment not found")

    refund = await service.refund_for_order(
        order_id=payment.order_id, reason=body.reason, amount=body.amount
    )
    if refund is None:
        raise ConflictError(
            "this payment cannot be refunded",
            details={"status": payment.status},
        )
    return RefundResponse.model_validate(refund)
