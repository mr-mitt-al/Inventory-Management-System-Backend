from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, computed_field, field_validator


class SortOption(StrEnum):
    NEWEST = "newest"
    PRICE_ASC = "price_asc"
    PRICE_DESC = "price_desc"
    NAME_ASC = "name_asc"


class CategoryResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    name: str
    slug: str
    description: str | None = None


class CategoryCreateRequest(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    slug: str = Field(min_length=1, max_length=140, pattern=r"^[a-z0-9]+(?:-[a-z0-9]+)*$")
    description: str | None = Field(default=None, max_length=2000)


class ProductResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    sku: str
    name: str
    description: str | None
    price: Decimal
    currency: str
    image_url: str | None
    is_active: bool
    category: CategoryResponse | None = None
    created_at: datetime
    updated_at: datetime

    # Named to make the staleness explicit at the API boundary. A field called
    # `stock` would read as authoritative; this one does not.
    cached_stock: int = Field(
        description="Display stock, eventually consistent. Inventory is the source of truth."
    )

    @computed_field  # type: ignore[prop-decorator]
    @property
    def in_stock(self) -> bool:
        return self.cached_stock > 0


class ProductCreateRequest(BaseModel):
    sku: str = Field(min_length=1, max_length=64)
    name: str = Field(min_length=1, max_length=250)
    description: str | None = Field(default=None, max_length=5000)
    price: Decimal = Field(ge=0, decimal_places=2)
    currency: str = Field(default="INR", min_length=3, max_length=3)
    category_id: UUID | None = None
    image_url: str | None = Field(default=None, max_length=1000)
    # Opening stock is forwarded to the inventory service, which owns it. It is
    # not written into `cached_stock` directly.
    initial_stock: int = Field(default=0, ge=0)

    @field_validator("sku")
    @classmethod
    def normalize_sku(cls, v: str) -> str:
        return v.strip().upper()

    @field_validator("currency")
    @classmethod
    def normalize_currency(cls, v: str) -> str:
        return v.strip().upper()


class ProductUpdateRequest(BaseModel):
    """All fields optional - PATCH semantics.

    Note there is no `cached_stock`: stock is changed through the inventory
    service, never by editing a product. Allowing it here would create a second
    source of truth that silently disagrees with the first.
    """

    name: str | None = Field(default=None, min_length=1, max_length=250)
    description: str | None = Field(default=None, max_length=5000)
    price: Decimal | None = Field(default=None, ge=0, decimal_places=2)
    category_id: UUID | None = None
    image_url: str | None = Field(default=None, max_length=1000)
    is_active: bool | None = None
