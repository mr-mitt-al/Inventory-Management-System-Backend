"""initial inventory schema

Revision ID: 0001_inventory_initial
Revises:
Create Date: 2026-08-18
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001_inventory_initial"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "stock_items",
        # PK is the catalog's product_id. No FK - products live in another
        # database, which is the point of database-per-service.
        sa.Column("product_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("sku", sa.String(length=64), nullable=False),
        sa.Column("available_qty", sa.Integer(), server_default="0", nullable=False),
        sa.Column("reserved_qty", sa.Integer(), server_default="0", nullable=False),
        sa.Column("low_stock_threshold", sa.Integer(), server_default="10", nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True),
                  server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("product_id", name="pk_stock_items"),
        sa.UniqueConstraint("sku", name="uq_stock_items_sku"),
        # Last line of defence against arithmetic errors: the database refuses to
        # store negative stock, so a bug surfaces as a failed transaction rather
        # than as an oversold product.
        sa.CheckConstraint("available_qty >= 0", name="ck_stock_items_available_non_negative"),
        sa.CheckConstraint("reserved_qty >= 0", name="ck_stock_items_reserved_non_negative"),
    )
    op.create_index("ix_stock_items_sku", "stock_items", ["sku"])
    op.create_index(
        "ix_stock_items_low",
        "stock_items",
        ["available_qty"],
    )

    op.create_table(
        "reservations",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("order_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("user_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("status", sa.String(length=20), server_default="HELD", nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True),
                  server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True),
                  server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_reservations"),
        # LOAD-BEARING. A redelivered order.created hits this instead of
        # reserving the same stock twice. Second line of defence behind
        # processed_events.
        sa.UniqueConstraint("order_id", name="uq_reservations_order_id"),
        sa.CheckConstraint(
            "status IN ('HELD', 'COMMITTED', 'RELEASED', 'EXPIRED')",
            name="ck_reservations_status_valid",
        ),
    )
    # Drives the sweeper query: WHERE status='HELD' AND expires_at < now()
    op.create_index("ix_reservations_status_expires", "reservations", ["status", "expires_at"])

    op.create_table(
        "reservation_items",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("reservation_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("product_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("quantity", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_reservation_items"),
        sa.ForeignKeyConstraint(
            ["reservation_id"], ["reservations.id"],
            name="fk_reservation_items_reservation_id_reservations",
            ondelete="CASCADE",
        ),
        sa.CheckConstraint("quantity > 0", name="ck_reservation_items_quantity_positive"),
    )
    op.create_index("ix_reservation_items_reservation", "reservation_items", ["reservation_id"])

    op.create_table(
        "stock_ledger",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("product_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("delta", sa.Integer(), nullable=False),
        sa.Column("reason", sa.String(length=20), nullable=False),
        sa.Column("ref_order_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("balance_after", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True),
                  server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_stock_ledger"),
    )
    op.create_index("ix_stock_ledger_product_created", "stock_ledger", ["product_id", "created_at"])
    op.create_index("ix_stock_ledger_order", "stock_ledger", ["ref_order_id"])

    op.create_table(
        "processed_events",
        sa.Column("event_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("event_type", sa.String(length=100), nullable=False),
        sa.Column("consumer", sa.String(length=100), nullable=False),
        sa.Column("consumed_at", sa.DateTime(timezone=True),
                  server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("event_id", name="pk_processed_events"),
    )

    op.create_table(
        "outbox",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("aggregate_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("topic", sa.String(length=100), nullable=False),
        sa.Column("partition_key", sa.String(length=100), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("attempts", sa.Integer(), server_default="0", nullable=False),
        sa.Column("last_error", sa.String(length=1000), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True),
                  server_default=sa.text("now()"), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id", name="pk_outbox"),
    )
    op.create_index(
        "ix_outbox_unpublished", "outbox", ["created_at"],
        postgresql_where=sa.text("published_at IS NULL"),
    )


def downgrade() -> None:
    op.drop_index("ix_outbox_unpublished", table_name="outbox")
    op.drop_table("outbox")
    op.drop_table("processed_events")
    op.drop_index("ix_stock_ledger_order", table_name="stock_ledger")
    op.drop_index("ix_stock_ledger_product_created", table_name="stock_ledger")
    op.drop_table("stock_ledger")
    op.drop_index("ix_reservation_items_reservation", table_name="reservation_items")
    op.drop_table("reservation_items")
    op.drop_index("ix_reservations_status_expires", table_name="reservations")
    op.drop_table("reservations")
    op.drop_index("ix_stock_items_low", table_name="stock_items")
    op.drop_index("ix_stock_items_sku", table_name="stock_items")
    op.drop_table("stock_items")
