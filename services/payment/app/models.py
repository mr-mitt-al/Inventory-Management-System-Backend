from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from uuid import UUID, uuid4

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    text,
)
from sqlalchemy.dialects.postgresql import UUID as PgUUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from common.db.base import Base, TimestampMixin
from common.db.idempotency import ProcessedEventMixin
from common.db.outbox import OutboxMixin


class PaymentStatus(StrEnum):
    PENDING = "PENDING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    REFUNDED = "REFUNDED"


class RefundStatus(StrEnum):
    PENDING = "PENDING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class Payment(Base, TimestampMixin):
    """One payment per order. Enforced by the database.

    `order_id UNIQUE` is the single most important constraint in the entire
    system. Kafka delivers at-least-once, so `inventory.reserved` WILL sometimes
    arrive twice - a rebalance, a slow offset commit, an outbox republish after a
    crash. Without this constraint, the second delivery charges the customer a
    second time.

    Two independent layers protect against it:

      1. `processed_events` - the consumer skips an event_id it has already seen
      2. this constraint - even if (1) is somehow wrong, the insert fails

    Belt and braces, deliberately. Money is the one place where a single
    safeguard is not enough.
    """

    __tablename__ = "payments"

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    order_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False, unique=True)
    user_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False, index=True)

    amount: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False, server_default=text("'INR'"))

    status: Mapped[str] = mapped_column(
        String(20), nullable=False, server_default=text(f"'{PaymentStatus.PENDING.value}'")
    )
    method: Mapped[str] = mapped_column(String(20), nullable=False, server_default=text("'CARD'"))

    # Display metadata only. The token itself is not stored after the charge -
    # there is no reason to keep it and every reason not to.
    card_last4: Mapped[str | None] = mapped_column(String(4), nullable=True)

    provider_ref: Mapped[str | None] = mapped_column(String(100), nullable=True)
    failure_code: Mapped[str | None] = mapped_column(String(50), nullable=True)
    failure_message: Mapped[str | None] = mapped_column(String(500), nullable=True)
    # Counts customer-initiated retries of a failed payment, not consumer retries.
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    paid_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    refunds: Mapped[list[Refund]] = relationship(
        back_populates="payment", cascade="all, delete-orphan", lazy="selectin"
    )

    __table_args__ = (
        CheckConstraint("amount >= 0", name="ck_payments_amount_non_negative"),
        Index("ix_payments_status_created", "status", "created_at"),
    )

    @property
    def status_enum(self) -> PaymentStatus:
        return PaymentStatus(self.status)

    @property
    def refunded_amount(self) -> Decimal:
        return sum(
            (r.amount for r in self.refunds if r.status == RefundStatus.COMPLETED.value),
            Decimal("0.00"),
        )


class Refund(Base):
    __tablename__ = "refunds"

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    payment_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("payments.id", ondelete="CASCADE"), nullable=False
    )
    amount: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    reason: Mapped[str | None] = mapped_column(String(300), nullable=True)
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, server_default=text(f"'{RefundStatus.PENDING.value}'")
    )
    provider_ref: Mapped[str | None] = mapped_column(String(100), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )

    payment: Mapped[Payment] = relationship(back_populates="refunds", lazy="noload")

    __table_args__ = (
        CheckConstraint("amount > 0", name="ck_refunds_amount_positive"),
        Index("ix_refunds_payment", "payment_id"),
    )


class ProcessedEvent(Base, ProcessedEventMixin):
    pass


class Outbox(Base, OutboxMixin):
    pass
