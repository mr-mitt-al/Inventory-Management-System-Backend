from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, computed_field


class StockResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    product_id: UUID
    sku: str
    available_qty: int
    reserved_qty: int
    low_stock_threshold: int
    updated_at: datetime

    @computed_field  # type: ignore[prop-decorator]
    @property
    def total_qty(self) -> int:
        return self.available_qty + self.reserved_qty

    @computed_field  # type: ignore[prop-decorator]
    @property
    def is_low(self) -> bool:
        return self.available_qty <= self.low_stock_threshold


class RestockRequest(BaseModel):
    """Add newly received units. Relative, not absolute."""

    quantity: int = Field(gt=0, description="Units to ADD to available stock")
    low_stock_threshold: int | None = Field(default=None, ge=0)


class AdjustStockRequest(BaseModel):
    """Set an absolute available quantity - a stock-take correction.

    Separate from restock on purpose: "we counted 40" and "40 more arrived" are
    different facts, and merging them makes the ledger unreadable.
    """

    available_qty: int = Field(ge=0)
    low_stock_threshold: int | None = Field(default=None, ge=0)


class CreateStockItemRequest(BaseModel):
    product_id: UUID
    sku: str = Field(min_length=1, max_length=64)
    quantity: int = Field(ge=0)
    low_stock_threshold: int | None = Field(default=None, ge=0)


class ReservationItemResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    product_id: UUID
    quantity: int


class ReservationResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    order_id: UUID
    user_id: UUID
    status: str
    expires_at: datetime
    created_at: datetime
    items: list[ReservationItemResponse]


class LedgerEntryResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    product_id: UUID
    delta: int
    reason: str
    ref_order_id: UUID | None
    balance_after: int
    created_at: datetime
