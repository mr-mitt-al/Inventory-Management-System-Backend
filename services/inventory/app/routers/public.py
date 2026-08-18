"""Public stock lookup.

Read-only and authoritative - unlike Catalog's `cached_stock`, this is the source
of truth. The frontend uses it on the cart page to re-validate availability
before checkout.
"""

from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Query

from common.errors import NotFoundError
from app.dependencies import SessionDep
from app.repositories import StockRepository
from app.schemas import StockResponse

router = APIRouter(prefix="/stock", tags=["inventory"])


@router.get("/{product_id}", response_model=StockResponse, summary="Authoritative stock level")
async def get_stock(product_id: UUID, session: SessionDep) -> StockResponse:
    row = await StockRepository(session).get(product_id)
    if row is None:
        raise NotFoundError("no stock record for this product")
    return StockResponse.model_validate(row)


@router.get("", response_model=list[StockResponse], summary="Stock for several products")
async def get_stock_batch(
    session: SessionDep,
    product_ids: list[UUID] = Query(alias="product_id", description="Repeat per product"),
) -> list[StockResponse]:
    """Batch lookup so the cart page makes one request instead of N.

    Products with no stock record are simply absent from the response rather than
    erroring - the caller asked about several things and one being unknown should
    not fail the rest.
    """
    if len(product_ids) > 100:
        from common.errors import ValidationError

        raise ValidationError("at most 100 product ids per request")

    rows = await StockRepository(session).get_many(product_ids)
    return [StockResponse.model_validate(row) for row in rows]
