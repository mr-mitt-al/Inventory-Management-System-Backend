"""Admin product and category management.

Authorized purely from the JWT `role` claim, verified locally with the shared
secret. This service never calls the auth service.
"""

from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Query, status

from common.auth.dependencies import require_admin
from common.pagination import Page, PageParams, page_params
from app.dependencies import CatalogServiceDep, SessionDep
from app.repositories import ProductRepository
from app.schemas import (
    CategoryCreateRequest,
    CategoryResponse,
    ProductCreateRequest,
    ProductResponse,
    ProductUpdateRequest,
    SortOption,
)

# Router-level guard: a new endpoint added here is admin-only by default rather
# than public because someone forgot a decorator.
router = APIRouter(
    prefix="/admin",
    tags=["catalog-admin"],
    dependencies=[Depends(require_admin)],
)


@router.get(
    "/products",
    response_model=Page[ProductResponse],
    summary="List products including deactivated ones",
)
async def admin_list_products(
    session: SessionDep,
    params: Annotated[PageParams, Depends(page_params)],
    q: Annotated[str | None, Query(min_length=1, max_length=100)] = None,
) -> Page[ProductResponse]:
    products, total = await ProductRepository(session).search(
        offset=params.offset,
        limit=params.limit,
        query=q,
        sort=SortOption.NEWEST,
        include_inactive=True,  # admins need to see what they soft-deleted
    )
    return Page.build(
        [ProductResponse.model_validate(p) for p in products], total=total, params=params
    )


@router.post(
    "/products",
    response_model=ProductResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a product",
)
async def create_product(
    body: ProductCreateRequest, service: CatalogServiceDep
) -> ProductResponse:
    return await service.create_product(body)


@router.patch("/products/{product_id}", response_model=ProductResponse, summary="Update a product")
async def update_product(
    product_id: UUID, body: ProductUpdateRequest, service: CatalogServiceDep
) -> ProductResponse:
    """Cannot change stock - that belongs to the inventory service. Accepting it
    here would create a second source of truth for the same number."""
    return await service.update_product(product_id, body)


@router.delete(
    "/products/{product_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    # Explicit None: FastAPI otherwise infers response_model from the `-> None`
    # return annotation and rejects it, since 204 cannot carry a body.
    response_model=None,
    summary="Deactivate a product (soft delete)",
)
async def deactivate_product(product_id: UUID, service: CatalogServiceDep) -> None:
    """Soft delete: historical orders reference this product from another
    database, where no foreign key can protect them from a hard delete."""
    await service.deactivate_product(product_id)


@router.post(
    "/categories",
    response_model=CategoryResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a category",
)
async def create_category(
    body: CategoryCreateRequest, service: CatalogServiceDep
) -> CategoryResponse:
    return await service.create_category(
        name=body.name, slug=body.slug, description=body.description
    )
