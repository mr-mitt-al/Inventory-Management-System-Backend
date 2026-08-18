"""Order status and the legal transitions between them.

Lives in ``common`` because Order owns the machine but Notification, the gateway
and the frontend all reason about the same values. One definition beats four
copies that drift.

    PENDING -> INVENTORY_RESERVED -> PAID -> CONFIRMED -> SHIPPED -> DELIVERED
       |               |                       |
       +-> FAILED      +-> FAILED              +-> CANCELLED
"""

from __future__ import annotations

from enum import StrEnum


class OrderStatus(StrEnum):
    PENDING = "PENDING"
    INVENTORY_RESERVED = "INVENTORY_RESERVED"
    PAID = "PAID"
    CONFIRMED = "CONFIRMED"
    SHIPPED = "SHIPPED"
    DELIVERED = "DELIVERED"
    CANCELLED = "CANCELLED"
    FAILED = "FAILED"


TERMINAL_STATUSES: frozenset[OrderStatus] = frozenset(
    {OrderStatus.DELIVERED, OrderStatus.CANCELLED, OrderStatus.FAILED}
)

# Statuses where stock is held but not yet permanently deducted.
RESERVED_STATUSES: frozenset[OrderStatus] = frozenset({OrderStatus.INVENTORY_RESERVED})

# Statuses where money has actually been taken, so cancelling requires a refund.
PAID_STATUSES: frozenset[OrderStatus] = frozenset(
    {OrderStatus.PAID, OrderStatus.CONFIRMED, OrderStatus.SHIPPED, OrderStatus.DELIVERED}
)

ALLOWED_TRANSITIONS: dict[OrderStatus, frozenset[OrderStatus]] = {
    OrderStatus.PENDING: frozenset(
        {OrderStatus.INVENTORY_RESERVED, OrderStatus.FAILED, OrderStatus.CANCELLED}
    ),
    OrderStatus.INVENTORY_RESERVED: frozenset(
        {OrderStatus.PAID, OrderStatus.FAILED, OrderStatus.CANCELLED}
    ),
    OrderStatus.PAID: frozenset({OrderStatus.CONFIRMED, OrderStatus.CANCELLED}),
    OrderStatus.CONFIRMED: frozenset({OrderStatus.SHIPPED, OrderStatus.CANCELLED}),
    OrderStatus.SHIPPED: frozenset({OrderStatus.DELIVERED}),
    # Terminal: nothing leaves.
    OrderStatus.DELIVERED: frozenset(),
    OrderStatus.CANCELLED: frozenset(),
    OrderStatus.FAILED: frozenset(),
}

# Which statuses a customer may cancel from. SHIPPED is excluded - once it is
# with the courier, cancellation is a returns problem, not an order problem.
CUSTOMER_CANCELLABLE: frozenset[OrderStatus] = frozenset(
    {OrderStatus.PENDING, OrderStatus.INVENTORY_RESERVED, OrderStatus.PAID, OrderStatus.CONFIRMED}
)


def can_transition(current: OrderStatus, target: OrderStatus) -> bool:
    return target in ALLOWED_TRANSITIONS.get(current, frozenset())


def is_terminal(status: OrderStatus) -> bool:
    return status in TERMINAL_STATUSES
