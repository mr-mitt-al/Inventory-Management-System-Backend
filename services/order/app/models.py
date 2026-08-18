from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PgUUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from common.db.base import Base, TimestampMixin
from common.db.idempotency import ProcessedEventMixin
from common.db.outbox import OutboxMixin
from common.order_status import OrderStatus


class Order(Base, TimestampMixin):
    __tablename__ = "orders"

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)

    # No FK - users live in auth_db. A uuid, validated by the JWT that carried it.
    user_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False, index=True)

    status: Mapped[str] = mapped_column(
        String(30), nullable=False, server_default=text(f"'{OrderStatus.PENDING.value}'")
    )
    total_amount: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False, server_default=text("'INR'"))

    shipping_address: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)

    # Payment TOKEN and display metadata only. Never a card number - this row is
    # copied into a Kafka event that is retained for days.
    payment_method: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)

    # UNIQUE. A retried POST /orders - double click, flaky network, browser
    # retry - returns the original order instead of creating a second one.
    idempotency_key: Mapped[str | None] = mapped_column(
        String(200), nullable=True, unique=True
    )

    correlation_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    failure_reason: Mapped[str | None] = mapped_column(String(500), nullable=True)

    # Set when the saga reaches a terminal state, so saga duration is measurable
    # without joining the history table.
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    items: Mapped[list[OrderItem]] = relationship(
        back_populates="order", cascade="all, delete-orphan", lazy="selectin"
    )
    history: Mapped[list[OrderStatusHistory]] = relationship(
        back_populates="order",
        cascade="all, delete-orphan",
        lazy="selectin",
        order_by="OrderStatusHistory.created_at",
    )

    __table_args__ = (
        CheckConstraint("total_amount >= 0", name="ck_orders_total_non_negative"),
        Index("ix_orders_user_created", "user_id", "created_at"),
        Index("ix_orders_status", "status"),
    )

    @property
    def status_enum(self) -> OrderStatus:
        return OrderStatus(self.status)


class OrderItem(Base):
    __tablename__ = "order_items"

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    order_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("orders.id", ondelete="CASCADE"), nullable=False
    )
    product_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)

    # SNAPSHOTS, not references. If an admin renames a product or changes its
    # price tomorrow, this order must still show what the customer actually
    # bought and paid. This is a correctness requirement, not denormalization for
    # speed - and there is no FK to protect it, because products live in
    # catalog_db.
    sku: Mapped[str] = mapped_column(String(64), nullable=False)
    name: Mapped[str] = mapped_column(String(250), nullable=False)
    unit_price: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)

    quantity: Mapped[int] = mapped_column(Integer, nullable=False)

    order: Mapped[Order] = relationship(back_populates="items", lazy="noload")

    __table_args__ = (
        CheckConstraint("quantity > 0", name="ck_order_items_quantity_positive"),
        CheckConstraint("unit_price >= 0", name="ck_order_items_price_non_negative"),
    )

    @property
    def line_total(self) -> Decimal:
        return self.unit_price * self.quantity


class OrderStatusHistory(Base):
    """Every transition, with its cause. Powers the frontend saga timeline and
    makes "why did this order fail" answerable after the fact."""

    __tablename__ = "order_status_history"

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    order_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("orders.id", ondelete="CASCADE"), nullable=False
    )
    from_status: Mapped[str | None] = mapped_column(String(30), nullable=True)
    to_status: Mapped[str] = mapped_column(String(30), nullable=False)
    reason: Mapped[str | None] = mapped_column(String(500), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )

    order: Mapped[Order] = relationship(back_populates="history", lazy="noload")

    __table_args__ = (Index("ix_order_status_history_order", "order_id", "created_at"),)


class ProductSnapshot(Base):
    """Local read-model of the catalog, maintained by consuming
    `catalog.product.upserted`.

    Checkout prices from this table. Two things it avoids:

      - calling the Catalog service during checkout, which would make Catalog a
        synchronous dependency of order creation and a single point of failure
      - trusting a price sent by the browser, which is obviously not acceptable

    Eventually consistent by design. A price change lands here within one event,
    and because the order snapshots the price into `order_items` at creation
    time, a later change cannot rewrite history.
    """

    __tablename__ = "product_snapshots"

    product_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True)
    sku: Mapped[str] = mapped_column(String(64), nullable=False)
    name: Mapped[str] = mapped_column(String(250), nullable=False)
    price: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False, server_default=text("'INR'"))
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("true"))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )


class DeadLetter(Base):
    """Persisted copy of a message that failed handling everywhere in the system.

    The `dlq_consumer` process subscribes to every `*.DLQ` topic and writes rows
    here, which is what makes the admin DLQ screen and replay possible. Reading
    them straight off Kafka would work but gives no way to mark one replayed or
    discarded.
    """

    __tablename__ = "dead_letters"

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)

    # Dedup key: the same DLQ message must not create two rows if the dlq
    # consumer is redelivered.
    dlq_event_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False, unique=True)

    original_topic: Mapped[str] = mapped_column(String(100), nullable=False)
    original_key: Mapped[str | None] = mapped_column(String(200), nullable=True)
    original_event: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    raw_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    failed_by: Mapped[str] = mapped_column(String(100), nullable=False)  # which consumer
    error_type: Mapped[str] = mapped_column(String(200), nullable=False)
    error_message: Mapped[str] = mapped_column(Text, nullable=False)
    stack_trace: Mapped[str | None] = mapped_column(Text, nullable=True)
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))

    status: Mapped[str] = mapped_column(
        String(20), nullable=False, server_default=text("'PARKED'")
    )  # PARKED | REPLAYED | DISCARDED
    replayed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    replayed_by: Mapped[UUID | None] = mapped_column(PgUUID(as_uuid=True), nullable=True)
    note: Mapped[str | None] = mapped_column(String(500), nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )

    __table_args__ = (
        Index("ix_dead_letters_status_created", "status", "created_at"),
        Index("ix_dead_letters_topic", "original_topic"),
    )


class ProcessedEvent(Base, ProcessedEventMixin):
    pass


class Outbox(Base, OutboxMixin):
    pass
