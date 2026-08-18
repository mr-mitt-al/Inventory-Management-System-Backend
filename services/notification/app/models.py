from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import DateTime, Index, String, Text, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PgUUID
from sqlalchemy.orm import Mapped, mapped_column

from common.db.base import Base
from common.db.idempotency import ProcessedEventMixin


class Channel(StrEnum):
    EMAIL = "EMAIL"
    SMS = "SMS"
    IN_APP = "IN_APP"


class DeliveryStatus(StrEnum):
    PENDING = "PENDING"
    SENT = "SENT"
    FAILED = "FAILED"


class Notification(Base):
    """A record of every message the system decided to send.

    Persisted rather than fire-and-forget so "did the customer get told their
    payment failed?" is answerable.
    """

    __tablename__ = "notifications"

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)

    # Nullable: admin alerts such as low stock belong to no particular user.
    user_id: Mapped[UUID | None] = mapped_column(PgUUID(as_uuid=True), nullable=True, index=True)
    recipient: Mapped[str] = mapped_column(String(320), nullable=False)

    channel: Mapped[str] = mapped_column(String(10), nullable=False)
    template: Mapped[str] = mapped_column(String(60), nullable=False)
    subject: Mapped[str | None] = mapped_column(String(250), nullable=True)
    body: Mapped[str] = mapped_column(Text, nullable=False)

    # What triggered it. Makes a notification traceable back to its cause.
    trigger_event_type: Mapped[str] = mapped_column(String(100), nullable=False)
    trigger_ref_id: Mapped[UUID | None] = mapped_column(PgUUID(as_uuid=True), nullable=True)
    context: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)

    status: Mapped[str] = mapped_column(
        String(10), nullable=False, server_default=text(f"'{DeliveryStatus.PENDING.value}'")
    )
    error: Mapped[str | None] = mapped_column(String(500), nullable=True)
    sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )

    __table_args__ = (
        Index("ix_notifications_template_created", "template", "created_at"),
        Index("ix_notifications_ref", "trigger_ref_id"),
    )


class UserContact(Base):
    """Local read-model of who to contact, built from `user.registered`.

    Notification needs an email address, but users live in `auth_db` and calling
    the Auth service per notification would make Auth a runtime dependency of
    every event this service handles - the exact coupling the architecture avoids
    everywhere else.

    So Auth announces registrations and this service keeps its own copy. Same
    pattern as Order's `product_snapshots`.

    Consequence worth knowing: a user who registered before this service first ran
    has no row here. Their notifications are logged with the reason rather than
    silently dropped, and replaying `user.registered` from Kafka (7-day retention)
    backfills them.
    """

    __tablename__ = "user_contacts"

    user_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True)
    email: Mapped[str] = mapped_column(String(320), nullable=False)
    full_name: Mapped[str] = mapped_column(String(200), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )


class ProcessedEvent(Base, ProcessedEventMixin):
    pass
