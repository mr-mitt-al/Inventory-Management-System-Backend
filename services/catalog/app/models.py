from __future__ import annotations

from decimal import Decimal
from uuid import UUID, uuid4

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    text,
)
from sqlalchemy.dialects.postgresql import UUID as PgUUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from common.db.base import Base, TimestampMixin
from common.db.idempotency import ProcessedEventMixin
from common.db.outbox import OutboxMixin


class Category(Base, TimestampMixin):
    __tablename__ = "categories"

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    name: Mapped[str] = mapped_column(String(120), nullable=False)
    slug: Mapped[str] = mapped_column(String(140), nullable=False, unique=True, index=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)

    products: Mapped[list[Product]] = relationship(back_populates="category", lazy="noload")


class Product(Base, TimestampMixin):
    __tablename__ = "products"

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    sku: Mapped[str] = mapped_column(String(64), nullable=False, unique=True, index=True)
    name: Mapped[str] = mapped_column(String(250), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Numeric, never float. Binary floating point cannot represent 0.1, and
    # money that does not add up is not a rounding curiosity, it is a bug.
    price: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False, server_default=text("'INR'"))

    category_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("categories.id", ondelete="SET NULL"), nullable=True
    )
    image_url: Mapped[str | None] = mapped_column(String(1000), nullable=True)

    # DENORMALIZED COPY - display only, NOT the source of truth.
    #
    # Inventory owns stock. This column exists so a product page does not make a
    # synchronous call to the inventory service on every request; it is updated
    # by consuming `inventory.stock.changed`. Checkout re-validates against
    # Inventory, so a stale value here can never cause an oversell - at worst a
    # customer sees "in stock" and gets "just sold out" at checkout, which is
    # what real stores do too.
    cached_stock: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    cached_reserved: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))

    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("true"))

    category: Mapped[Category | None] = relationship(back_populates="products", lazy="joined")

    __table_args__ = (
        CheckConstraint("price >= 0", name="ck_products_price_non_negative"),
        Index("ix_products_active_category", "is_active", "category_id"),
        Index("ix_products_price", "price"),
    )


class ProcessedEvent(Base, ProcessedEventMixin):
    """Consumer-side dedup table. Kafka delivers at-least-once, so a duplicate
    `inventory.stock.changed` must not be applied twice."""


class Outbox(Base, OutboxMixin):
    """Publishes `catalog.product.upserted` so the Order service can keep a local
    price read-model instead of calling Catalog during checkout."""
