"""initial order schema

Revision ID: 0001_order_initial
Revises:
Create Date: 2026-08-18
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001_order_initial"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

ORDER_STATUSES = (
    "'PENDING', 'INVENTORY_RESERVED', 'PAID', 'CONFIRMED', "
    "'SHIPPED', 'DELIVERED', 'CANCELLED', 'FAILED'"
)


def upgrade() -> None:
    op.create_table(
        "orders",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        # No FK: users live in auth_db.
        sa.Column("user_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("status", sa.String(length=30), server_default="PENDING", nullable=False),
        sa.Column("total_amount", sa.Numeric(precision=12, scale=2), nullable=False),
        sa.Column("currency", sa.String(length=3), server_default="INR", nullable=False),
        sa.Column("shipping_address", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        # Payment TOKEN and display metadata only - never a card number.
        sa.Column("payment_method", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        # UNIQUE: a retried POST returns the original order instead of creating a
        # second one and charging the customer twice.
        sa.Column("idempotency_key", sa.String(length=200), nullable=True),
        sa.Column("correlation_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("failure_reason", sa.String(length=500), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True),
                  server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True),
                  server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_orders"),
        sa.UniqueConstraint("idempotency_key", name="uq_orders_idempotency_key"),
        sa.CheckConstraint("total_amount >= 0", name="ck_orders_total_non_negative"),
        sa.CheckConstraint(f"status IN ({ORDER_STATUSES})", name="ck_orders_status_valid"),
    )
    op.create_index("ix_orders_user_id", "orders", ["user_id"])
    op.create_index("ix_orders_user_created", "orders", ["user_id", "created_at"])
    op.create_index("ix_orders_status", "orders", ["status"])

    op.create_table(
        "order_items",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("order_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("product_id", postgresql.UUID(as_uuid=True), nullable=False),
        # SNAPSHOTS. A later price or name change must not rewrite what the
        # customer agreed to buy, and there is no FK to catalog_db to enforce it.
        sa.Column("sku", sa.String(length=64), nullable=False),
        sa.Column("name", sa.String(length=250), nullable=False),
        sa.Column("unit_price", sa.Numeric(precision=12, scale=2), nullable=False),
        sa.Column("quantity", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_order_items"),
        sa.ForeignKeyConstraint(
            ["order_id"], ["orders.id"],
            name="fk_order_items_order_id_orders", ondelete="CASCADE",
        ),
        sa.CheckConstraint("quantity > 0", name="ck_order_items_quantity_positive"),
        sa.CheckConstraint("unit_price >= 0", name="ck_order_items_price_non_negative"),
    )
    op.create_index("ix_order_items_order", "order_items", ["order_id"])

    op.create_table(
        "order_status_history",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("order_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("from_status", sa.String(length=30), nullable=True),
        sa.Column("to_status", sa.String(length=30), nullable=False),
        sa.Column("reason", sa.String(length=500), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True),
                  server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_order_status_history"),
        sa.ForeignKeyConstraint(
            ["order_id"], ["orders.id"],
            name="fk_order_status_history_order_id_orders", ondelete="CASCADE",
        ),
    )
    op.create_index(
        "ix_order_status_history_order", "order_status_history", ["order_id", "created_at"]
    )

    # Local read-model of the catalog. Checkout prices from here, so it never
    # calls Catalog and never trusts a client-supplied price.
    op.create_table(
        "product_snapshots",
        sa.Column("product_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("sku", sa.String(length=64), nullable=False),
        sa.Column("name", sa.String(length=250), nullable=False),
        sa.Column("price", sa.Numeric(precision=12, scale=2), nullable=False),
        sa.Column("currency", sa.String(length=3), server_default="INR", nullable=False),
        sa.Column("is_active", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True),
                  server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("product_id", name="pk_product_snapshots"),
    )

    op.create_table(
        "dead_letters",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("dlq_event_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("original_topic", sa.String(length=100), nullable=False),
        sa.Column("original_key", sa.String(length=200), nullable=True),
        sa.Column("original_event", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("raw_message", sa.Text(), nullable=True),
        sa.Column("failed_by", sa.String(length=100), nullable=False),
        sa.Column("error_type", sa.String(length=200), nullable=False),
        sa.Column("error_message", sa.Text(), nullable=False),
        sa.Column("stack_trace", sa.Text(), nullable=True),
        sa.Column("attempts", sa.Integer(), server_default="0", nullable=False),
        sa.Column("status", sa.String(length=20), server_default="PARKED", nullable=False),
        sa.Column("replayed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("replayed_by", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("note", sa.String(length=500), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True),
                  server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_dead_letters"),
        # Stops the dlq collector recording the same parked message twice.
        sa.UniqueConstraint("dlq_event_id", name="uq_dead_letters_dlq_event_id"),
        sa.CheckConstraint(
            "status IN ('PARKED', 'REPLAYED', 'DISCARDED')",
            name="ck_dead_letters_status_valid",
        ),
    )
    op.create_index("ix_dead_letters_status_created", "dead_letters", ["status", "created_at"])
    op.create_index("ix_dead_letters_topic", "dead_letters", ["original_topic"])

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
    op.drop_index("ix_dead_letters_topic", table_name="dead_letters")
    op.drop_index("ix_dead_letters_status_created", table_name="dead_letters")
    op.drop_table("dead_letters")
    op.drop_table("product_snapshots")
    op.drop_index("ix_order_status_history_order", table_name="order_status_history")
    op.drop_table("order_status_history")
    op.drop_index("ix_order_items_order", table_name="order_items")
    op.drop_table("order_items")
    op.drop_index("ix_orders_status", table_name="orders")
    op.drop_index("ix_orders_user_created", table_name="orders")
    op.drop_index("ix_orders_user_id", table_name="orders")
    op.drop_table("orders")
