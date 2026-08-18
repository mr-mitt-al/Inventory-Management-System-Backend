from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from uuid import UUID, uuid4

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    text,
)
from sqlalchemy.dialects.postgresql import UUID as PgUUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from common.db.base import Base, TimestampMixin, utcnow
from common.db.idempotency import ProcessedEventMixin
from common.db.outbox import OutboxMixin


class ReservationStatus(StrEnum):
    HELD = "HELD"            # stock set aside, order still in flight
    COMMITTED = "COMMITTED"  # payment succeeded, stock permanently deducted
    RELEASED = "RELEASED"    # order failed or cancelled, stock returned
    EXPIRED = "EXPIRED"      # TTL elapsed, swept back to available


class LedgerReason(StrEnum):
    RESERVE = "RESERVE"
    RELEASE = "RELEASE"
    COMMIT = "COMMIT"
    RESTOCK = "RESTOCK"
    ADJUST = "ADJUST"
    EXPIRE = "EXPIRE"


class StockItem(Base):
    """The source of truth for stock. Catalog's `cached_stock` is a copy of this.

    Two counters rather than one:

      available_qty  can still be sold
      reserved_qty   held for orders that have not paid yet

    A reservation moves units from available to reserved; committing removes them
    from reserved; releasing moves them back. Total physical stock is the sum, so
    the invariant `available >= 0 AND reserved >= 0` catches any arithmetic slip
    at the database level rather than after a customer complains.
    """

    __tablename__ = "stock_items"

    # PK is the catalog's product_id - no surrogate key, because there is exactly
    # one stock row per product and no other service needs a reference to it.
    product_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True)
    sku: Mapped[str] = mapped_column(String(64), nullable=False, unique=True, index=True)

    available_qty: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    reserved_qty: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    low_stock_threshold: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default=text("10")
    )

    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()"), onupdate=utcnow
    )

    __table_args__ = (
        CheckConstraint("available_qty >= 0", name="ck_stock_items_available_non_negative"),
        CheckConstraint("reserved_qty >= 0", name="ck_stock_items_reserved_non_negative"),
    )

    @property
    def total_qty(self) -> int:
        return self.available_qty + self.reserved_qty

    @property
    def is_low(self) -> bool:
        return self.available_qty <= self.low_stock_threshold


class Reservation(Base, TimestampMixin):
    __tablename__ = "reservations"

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)

    # UNIQUE, and load-bearing. A redelivered `order.created` hits this
    # constraint instead of reserving the same stock a second time. This is the
    # second line of defence behind processed_events - if dedup bookkeeping is
    # ever wrong, the database still refuses to double-reserve.
    order_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False, unique=True)

    user_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, server_default=text(f"'{ReservationStatus.HELD.value}'")
    )

    # Every reservation expires. Without this, a service that dies mid-saga holds
    # stock hostage forever with nothing to notice.
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    items: Mapped[list[ReservationItem]] = relationship(
        back_populates="reservation", cascade="all, delete-orphan", lazy="selectin"
    )

    __table_args__ = (
        Index("ix_reservations_status_expires", "status", "expires_at"),
    )

    @property
    def status_enum(self) -> ReservationStatus:
        return ReservationStatus(self.status)


class ReservationItem(Base):
    __tablename__ = "reservation_items"

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    reservation_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("reservations.id", ondelete="CASCADE"),
        nullable=False,
    )
    product_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)

    reservation: Mapped[Reservation] = relationship(back_populates="items", lazy="noload")

    __table_args__ = (
        CheckConstraint("quantity > 0", name="ck_reservation_items_quantity_positive"),
    )


class StockLedger(Base):
    """Append-only record of every stock movement.

    Not strictly required for the system to work, which is exactly why it is
    worth having: when someone asks "why does this product show 3 units when we
    received 50", the answer is a query rather than a guess.
    """

    __tablename__ = "stock_ledger"

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    product_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    delta: Mapped[int] = mapped_column(Integer, nullable=False)  # signed
    reason: Mapped[str] = mapped_column(String(20), nullable=False)
    ref_order_id: Mapped[UUID | None] = mapped_column(PgUUID(as_uuid=True), nullable=True)
    balance_after: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )

    __table_args__ = (
        Index("ix_stock_ledger_product_created", "product_id", "created_at"),
        Index("ix_stock_ledger_order", "ref_order_id"),
    )


class ProcessedEvent(Base, ProcessedEventMixin):
    pass


class Outbox(Base, OutboxMixin):
    pass
