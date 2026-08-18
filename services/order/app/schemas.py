from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, computed_field, field_validator

from common.events.schemas import Address
from common.order_status import OrderStatus


class PaymentMethodRequest(BaseModel):
    """A TOKEN, never a card number.

    The frontend maps its test cards to these tokens:

        4242 4242 4242 4242 -> tok_test_success
        4000 0000 0000 0002 -> tok_test_declined
        4000 0000 0000 9995 -> tok_test_insufficient
        4000 0000 0000 0127 -> tok_test_timeout

    A real integration would swap this for a PSP token from Stripe/Razorpay
    Elements, so a card number never reaches this backend at all.
    """

    type: str = Field(default="CARD", pattern="^(CARD|UPI|COD)$")
    token: str = Field(min_length=1, max_length=100)
    last4: str | None = Field(default=None, pattern=r"^\d{4}$")
    label: str | None = Field(default=None, max_length=100)


class OrderItemRequest(BaseModel):
    """Product and quantity ONLY.

    Note what the client cannot send: a price. Order looks the price up in its own
    `product_snapshots` read-model, so a manipulated request body cannot buy a
    laptop for one rupee.
    """

    product_id: UUID
    quantity: int = Field(gt=0, le=100)


class CreateOrderRequest(BaseModel):
    items: list[OrderItemRequest] = Field(min_length=1, max_length=50)
    shipping_address: Address
    payment_method: PaymentMethodRequest

    @field_validator("items")
    @classmethod
    def no_duplicate_products(cls, v: list[OrderItemRequest]) -> list[OrderItemRequest]:
        seen = {item.product_id for item in v}
        if len(seen) != len(v):
            raise ValueError("send one line per product; combine duplicate products")
        return v


class OrderItemResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    product_id: UUID
    sku: str
    name: str
    unit_price: Decimal
    quantity: int

    @computed_field  # type: ignore[prop-decorator]
    @property
    def line_total(self) -> Decimal:
        return self.unit_price * self.quantity


class StatusHistoryResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    from_status: str | None
    to_status: str
    reason: str | None
    created_at: datetime


class OrderResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    user_id: UUID
    status: OrderStatus
    total_amount: Decimal
    currency: str
    shipping_address: dict
    payment_method: dict
    failure_reason: str | None
    correlation_id: UUID
    created_at: datetime
    updated_at: datetime
    completed_at: datetime | None
    items: list[OrderItemResponse]

    @computed_field  # type: ignore[prop-decorator]
    @property
    def is_terminal(self) -> bool:
        """Tells the frontend when to stop polling."""
        from common.order_status import is_terminal

        return is_terminal(self.status)


class OrderDetailResponse(OrderResponse):
    """Adds the saga timeline, which drives the tracking page's stepper."""

    history: list[StatusHistoryResponse]


class OrderSummaryResponse(BaseModel):
    """Lighter shape for list views - no address, no payment metadata."""

    model_config = ConfigDict(from_attributes=True)

    id: UUID
    status: OrderStatus
    total_amount: Decimal
    currency: str
    created_at: datetime
    items: list[OrderItemResponse]


class CreateOrderResponse(BaseModel):
    """Returned with 202 Accepted, not 201 Created.

    The order exists; it is NOT confirmed. Stock has not been reserved and the
    card has not been charged. Saying "order placed successfully" here would be a
    lie that the compensation path then exposes.
    """

    order_id: UUID
    status: OrderStatus
    total_amount: Decimal
    currency: str
    message: str = "order accepted and is being processed"
    track_url: str


class CancelOrderRequest(BaseModel):
    reason: str = Field(default="cancelled by customer", max_length=300)


class DeadLetterResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    original_topic: str
    original_key: str | None
    failed_by: str
    error_type: str
    error_message: str
    attempts: int
    status: str
    note: str | None
    created_at: datetime
    replayed_at: datetime | None


class DeadLetterDetailResponse(DeadLetterResponse):
    original_event: dict | None
    raw_message: str | None
    stack_trace: str | None


class DeadLetterActionRequest(BaseModel):
    note: str | None = Field(default=None, max_length=500)
