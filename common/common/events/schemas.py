"""Every event payload in the system, in one place.

Defined up front - including topics whose producing service is not built yet -
so that when Inventory starts publishing ``inventory.reserved`` the Order
service is already validating against the identical model. One definition, both
ends of the wire.

Events carry FACTS, not commands: ``payment.failed``, never ``release_stock``.
The producer does not know or care who reacts, which is what lets you add a
consumer without touching a producer.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class _Payload(BaseModel):
    model_config = ConfigDict(extra="ignore")


# --------------------------------------------------------------------- shared
class Address(_Payload):
    line1: str
    line2: str | None = None
    city: str
    state: str
    postal_code: str
    country: str = "IN"
    phone: str | None = None


class LineItem(_Payload):
    product_id: UUID
    sku: str
    name: str
    unit_price: Decimal
    quantity: int = Field(gt=0)


class PaymentMethod(_Payload):
    """How to charge for an order.

    A TOKEN, never a card number. Raw PANs must not travel through Kafka - the
    topic is retained for seven days, replayed into the DLQ on failure, and
    visible in Kafka UI. The mock gateway maps well-known test tokens to
    outcomes (see payment service ``mock_gateway.py``).
    """

    type: str = "CARD"  # CARD | UPI | COD
    token: str
    last4: str | None = None
    label: str | None = None


# ----------------------------------------------------------------------- auth
class UserRegistered(_Payload):
    user_id: UUID
    email: str
    full_name: str
    registered_at: datetime


# -------------------------------------------------------------------- catalog
class ProductUpserted(_Payload):
    """A product was created or changed.

    Consumed by the Order service into a local ``product_snapshots`` table, which
    is where checkout reads prices from. This is what lets Order snapshot a
    trustworthy price without a synchronous call to Catalog, and without trusting
    a price sent by the client.
    """

    product_id: UUID
    sku: str
    name: str
    price: Decimal
    currency: str = "INR"
    is_active: bool = True


# ---------------------------------------------------------------------- order
class OrderCreated(_Payload):
    order_id: UUID
    user_id: UUID
    total_amount: Decimal
    currency: str = "INR"
    items: list[LineItem]
    shipping_address: Address
    payment_method: PaymentMethod


class OrderConfirmed(_Payload):
    order_id: UUID
    user_id: UUID
    total_amount: Decimal
    currency: str = "INR"
    items: list[LineItem]
    confirmed_at: datetime


class OrderCancelled(_Payload):
    order_id: UUID
    user_id: UUID
    reason: str
    # Tells Payment whether there is anything to refund, and Inventory whether
    # to release a held reservation or restock committed units.
    was_paid: bool = False
    items: list[LineItem] = Field(default_factory=list)


# ------------------------------------------------------------------ inventory
class ReservedItem(_Payload):
    product_id: UUID
    quantity: int = Field(gt=0)


class InventoryReserved(_Payload):
    order_id: UUID
    user_id: UUID
    reservation_id: UUID
    expires_at: datetime
    items: list[ReservedItem]
    total_amount: Decimal
    currency: str = "INR"

    # Carried through from order.created so Payment - which is triggered by this
    # event - knows what to charge without calling anyone.
    #
    # Trade-off, stated plainly: Inventory relays payment data it has no interest
    # in. The alternative is Order emitting a `payment.requested` command after
    # it sees this event, which decouples Inventory but adds a topic and a hop to
    # every order. Documented in DESIGN.md; the payload is a token, not a card.
    payment_method: PaymentMethod


class InsufficientItem(_Payload):
    product_id: UUID
    requested: int
    available: int


class InventoryReservationFailed(_Payload):
    order_id: UUID
    user_id: UUID
    reason: str
    insufficient_items: list[InsufficientItem] = Field(default_factory=list)


class StockChanged(_Payload):
    product_id: UUID
    sku: str
    available_qty: int
    reserved_qty: int
    reason: str  # RESERVE | RELEASE | COMMIT | RESTOCK | ADJUST | EXPIRE
    ref_order_id: UUID | None = None


class LowStock(_Payload):
    product_id: UUID
    sku: str
    available_qty: int
    threshold: int


# -------------------------------------------------------------------- payment
class PaymentSucceeded(_Payload):
    order_id: UUID
    user_id: UUID
    payment_id: UUID
    amount: Decimal
    currency: str = "INR"
    method: str
    provider_ref: str
    paid_at: datetime


class PaymentFailed(_Payload):
    order_id: UUID
    user_id: UUID
    payment_id: UUID | None = None
    amount: Decimal
    currency: str = "INR"
    failure_code: str  # card_declined | insufficient_funds | timeout | ...
    failure_message: str
    retryable: bool = True


class PaymentRefunded(_Payload):
    order_id: UUID
    user_id: UUID
    payment_id: UUID
    refund_id: UUID
    amount: Decimal
    currency: str = "INR"
    reason: str


# Topic -> payload model. Handy for generic tooling such as the DLQ inspector.
PAYLOAD_BY_EVENT_TYPE: dict[str, type[_Payload]] = {
    "user.registered": UserRegistered,
    "catalog.product.upserted": ProductUpserted,
    "order.created": OrderCreated,
    "order.confirmed": OrderConfirmed,
    "order.cancelled": OrderCancelled,
    "inventory.reserved": InventoryReserved,
    "inventory.reservation_failed": InventoryReservationFailed,
    "inventory.stock.changed": StockChanged,
    "inventory.low_stock": LowStock,
    "payment.succeeded": PaymentSucceeded,
    "payment.failed": PaymentFailed,
    "payment.refunded": PaymentRefunded,
}
