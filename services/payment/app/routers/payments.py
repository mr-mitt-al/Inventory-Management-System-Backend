from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends

from common.auth.dependencies import assert_can_access, get_current_user
from common.auth.jwt import TokenUser
from common.errors import NotFoundError
from common.pagination import Page, PageParams, page_params
from app.dependencies import PaymentServiceDep, SessionDep
from app.mock_gateway import TEST_TOKENS
from app.repositories import PaymentRepository
from app.schemas import PaymentResponse, RetryPaymentRequest

router = APIRouter(prefix="/payments", tags=["payments"])


@router.get("/test-tokens", summary="Test tokens the mock gateway understands")
async def test_tokens() -> dict[str, str]:
    """Exposed so the frontend checkout can offer a deliberate failure.

    Being able to trigger the compensation path on demand is what makes the saga
    demonstrable instead of theoretical.
    """
    return TEST_TOKENS


@router.get("", response_model=Page[PaymentResponse], summary="Your payments")
async def list_my_payments(
    session: SessionDep,
    caller: Annotated[TokenUser, Depends(get_current_user)],
    params: Annotated[PageParams, Depends(page_params)],
) -> Page[PaymentResponse]:
    payments, total = await PaymentRepository(session).list_for_user(
        caller.user_id, offset=params.offset, limit=params.limit
    )
    return Page.build(
        [PaymentResponse.model_validate(p) for p in payments], total=total, params=params
    )


@router.get(
    "/order/{order_id}", response_model=PaymentResponse, summary="Payment status for an order"
)
async def get_payment_for_order(
    order_id: UUID,
    session: SessionDep,
    caller: Annotated[TokenUser, Depends(get_current_user)],
) -> PaymentResponse:
    payment = await PaymentRepository(session).get_by_order(order_id)
    if payment is None:
        raise NotFoundError("no payment found for this order")
    # Role alone is not enough: without this, any signed-in user could read any
    # other user's payment by guessing an order id.
    assert_can_access(caller, payment.user_id, resource="payment")
    return PaymentResponse.model_validate(payment)


@router.post(
    "/order/{order_id}/retry",
    response_model=PaymentResponse,
    summary="Retry a failed payment with a new token",
)
async def retry_payment(
    order_id: UUID,
    body: RetryPaymentRequest,
    service: PaymentServiceDep,
    caller: Annotated[TokenUser, Depends(get_current_user)],
) -> PaymentResponse:
    """Only a FAILED payment can be retried.

    A SUCCEEDED payment is never re-charged, and the endpoint refuses rather than
    silently doing nothing - a silent success would let a broken frontend think it
    had taken a second payment.
    """
    payment = await service.retry_payment(
        order_id=order_id, token=body.token, caller_id=caller.user_id
    )
    return PaymentResponse.model_validate(payment)
