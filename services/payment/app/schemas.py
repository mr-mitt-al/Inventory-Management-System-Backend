from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class RefundResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    amount: Decimal
    reason: str | None
    status: str
    provider_ref: str | None
    created_at: datetime


class PaymentResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    order_id: UUID
    user_id: UUID
    amount: Decimal
    currency: str
    status: str
    method: str
    card_last4: str | None
    provider_ref: str | None
    failure_code: str | None
    failure_message: str | None
    attempts: int
    paid_at: datetime | None
    created_at: datetime
    refunds: list[RefundResponse] = Field(default_factory=list)


class RetryPaymentRequest(BaseModel):
    """A new token - typically a different card after a decline.

    The customer must supply payment details again rather than the backend
    re-using the old token: retrying the same declined card is pointless, and
    storing a reusable token to replay later is a liability.
    """

    token: str = Field(min_length=1, max_length=100)
    last4: str | None = Field(default=None, pattern=r"^\d{4}$")


class AdminRefundRequest(BaseModel):
    reason: str = Field(min_length=1, max_length=300)
    # Omit for a full refund. Present for a partial one.
    amount: Decimal | None = Field(default=None, gt=0)
