"""Topic names and consumer group names as constants.

String literals for topics scattered across services are how you end up with a
producer writing to ``order.created`` and a consumer subscribed to
``orders.created``, silently receiving nothing.
"""

from __future__ import annotations

DLQ_SUFFIX = ".DLQ"


class Topics:
    # ---- auth ---------------------------------------------------------------
    USER_REGISTERED = "user.registered"

    # ---- catalog ------------------------------------------------------------
    # Feeds the Order service's local product read-model. Order must snapshot
    # the price at order time, and it cannot call Catalog to ask - that would
    # make Catalog a synchronous dependency of checkout. So Catalog announces
    # product changes and Order keeps its own copy.
    CATALOG_PRODUCT_UPSERTED = "catalog.product.upserted"

    # ---- order --------------------------------------------------------------
    ORDER_CREATED = "order.created"
    ORDER_CONFIRMED = "order.confirmed"
    ORDER_CANCELLED = "order.cancelled"

    # ---- inventory ----------------------------------------------------------
    INVENTORY_RESERVED = "inventory.reserved"
    INVENTORY_RESERVATION_FAILED = "inventory.reservation_failed"
    INVENTORY_STOCK_CHANGED = "inventory.stock.changed"
    INVENTORY_LOW_STOCK = "inventory.low_stock"

    # ---- payment ------------------------------------------------------------
    PAYMENT_SUCCEEDED = "payment.succeeded"
    PAYMENT_FAILED = "payment.failed"
    PAYMENT_REFUNDED = "payment.refunded"

    @classmethod
    def all(cls) -> list[str]:
        return [
            v
            for k, v in vars(cls).items()
            if not k.startswith("_") and isinstance(v, str)
        ]


def dlq_topic(topic: str) -> str:
    """DLQ name for a topic. Idempotent - a DLQ name maps to itself."""
    return topic if topic.endswith(DLQ_SUFFIX) else f"{topic}{DLQ_SUFFIX}"


def consumer_group(service: str, topic: str) -> str:
    """Group id convention: ``<service>-<topic-with-dashes>``.

    Each service is its own consumer group, so every interested service
    receives every event. Replicas WITHIN a group share the partitions.
    """
    return f"{service}-{topic.replace('.', '-')}"
