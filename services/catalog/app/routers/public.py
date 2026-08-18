"""Public storefront endpoints. No authentication - browsing is anonymous."""

from __future__ import annotations

from decimal import Decimal
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Query

from common.errors import ValidationError
from common.pagination import Page, PageParams, page_params
from app.dependencies import CatalogServiceDep
from app.schemas import CategoryResponse, ProductResponse, SortOption

router = APIRouter(tags=["catalog"])


@router.get("/products", response_model=Page[ProductResponse], summary="Browse products")
async def list_products(
    service: CatalogServiceDep,
    params: Annotated[PageParams, Depends(page_params)],
    category: Annotated[str | None, Query(description="Category slug")] = None,
    q: Annotated[str | None, Query(min_length=1, max_length=100)] = None,
    min_price: Annotated[Decimal | None, Query(ge=0)] = None,
    max_price: Annotated[Decimal | None, Query(ge=0)] = None,
    in_stock: Annotated[bool, Query(description="Only items with display stock > 0")] = False,
    sort: Annotated[SortOption, Query()] = SortOption.NEWEST,
) -> Page[ProductResponse]:
    """Cached for `LISTING_CACHE_TTL_SECONDS`, keyed on the full query."""
    if min_price is not None and max_price is not None and min_price > max_price:
        raise ValidationError("min_price cannot exceed max_price")

    return await service.list_products(
        params=params,
        category=category,
        query=q,
        min_price=min_price,
        max_price=max_price,
        in_stock_only=in_stock,
        sort=sort,
    )


@router.get("/products/{product_id}", response_model=ProductResponse, summary="Product detail")
async def get_product(product_id: UUID, service: CatalogServiceDep) -> ProductResponse:
    """`cached_stock` here is a denormalized copy owned by the inventory service
    and is eventually consistent. Checkout re-validates, so a stale value cannot
    cause an oversell."""
    return await service.get_product(product_id)


@router.get("/categories", response_model=list[CategoryResponse], summary="All categories")
async def list_categories(service: CatalogServiceDep) -> list[CategoryResponse]:
    return await service.list_categories()
