from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, status

from common.auth.dependencies import require_admin
from common.errors import NotFoundError
from common.pagination import Page, PageParams, page_params
from app.dependencies import InventoryServiceDep, SessionDep
from app.repositories import ReservationRepository, StockRepository
from app.schemas import (
    AdjustStockRequest,
    CreateStockItemRequest,
    LedgerEntryResponse,
    ReservationResponse,
    RestockRequest,
    StockResponse,
)

router = APIRouter(
    prefix="/admin",
    tags=["inventory-admin"],
    dependencies=[Depends(require_admin)],
)


@router.get("/stock", response_model=Page[StockResponse], summary="All stock records")
async def list_stock(
    session: SessionDep, params: Annotated[PageParams, Depends(page_params)]
) -> Page[StockResponse]:
    rows, total = await StockRepository(session).list_all(
        offset=params.offset, limit=params.limit
    )
    return Page.build([StockResponse.model_validate(r) for r in rows], total=total, params=params)


@router.get("/stock/low", response_model=Page[StockResponse], summary="Items at or below threshold")
async def list_low_stock(
    session: SessionDep, params: Annotated[PageParams, Depends(page_params)]
) -> Page[StockResponse]:
    rows, total = await StockRepository(session).list_low_stock(
        offset=params.offset, limit=params.limit
    )
    return Page.build([StockResponse.model_validate(r) for r in rows], total=total, params=params)


@router.post(
    "/stock",
    response_model=StockResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a stock record for a product",
)
async def create_stock_item(
    body: CreateStockItemRequest, service: InventoryServiceDep
) -> StockResponse:
    row = await service.upsert_stock_item(
        product_id=body.product_id,
        sku=body.sku,
        quantity=body.quantity,
        low_stock_threshold=body.low_stock_threshold,
    )
    return StockResponse.model_validate(row)


@router.post("/stock/{product_id}/restock", response_model=StockResponse, summary="Add units")
async def restock(
    product_id: UUID, body: RestockRequest, service: InventoryServiceDep, session: SessionDep
) -> StockResponse:
    """Relative: ADDS to available stock. For a stock-take correction use PATCH."""
    existing = await StockRepository(session).get(product_id)
    if existing is None:
        raise NotFoundError("no stock record for this product - create one first")

    row = await service.upsert_stock_item(
        product_id=product_id,
        sku=existing.sku,
        quantity=body.quantity,
        low_stock_threshold=body.low_stock_threshold,
    )
    return StockResponse.model_validate(row)


@router.patch("/stock/{product_id}", response_model=StockResponse, summary="Stock-take correction")
async def adjust_stock(
    product_id: UUID, body: AdjustStockRequest, service: InventoryServiceDep
) -> StockResponse:
    """Absolute: SETS available stock to the given number, and records the signed
    delta in the ledger so the correction is auditable."""
    row = await service.adjust_stock(
        product_id=product_id,
        available_qty=body.available_qty,
        low_stock_threshold=body.low_stock_threshold,
    )
    return StockResponse.model_validate(row)


@router.get(
    "/stock/{product_id}/ledger",
    response_model=Page[LedgerEntryResponse],
    summary="Movement history for one product",
)
async def stock_ledger(
    product_id: UUID, session: SessionDep, params: Annotated[PageParams, Depends(page_params)]
) -> Page[LedgerEntryResponse]:
    """Answers "why does this show 3 units when we received 50" with a query
    rather than a guess."""
    rows, total = await StockRepository(session).ledger_for_product(
        product_id, offset=params.offset, limit=params.limit
    )
    return Page.build(
        [LedgerEntryResponse.model_validate(r) for r in rows], total=total, params=params
    )


@router.get(
    "/reservations",
    response_model=Page[ReservationResponse],
    summary="Reservations currently holding stock",
)
async def list_reservations(
    session: SessionDep, params: Annotated[PageParams, Depends(page_params)]
) -> Page[ReservationResponse]:
    rows, total = await ReservationRepository(session).list_active(
        offset=params.offset, limit=params.limit
    )
    return Page.build(
        [ReservationResponse.model_validate(r) for r in rows], total=total, params=params
    )


@router.get("/reservations/stats", summary="Reservation counts by status")
async def reservation_stats(session: SessionDep) -> dict[str, int]:
    return await ReservationRepository(session).count_by_status()


@router.get(
    "/reservations/order/{order_id}",
    response_model=ReservationResponse,
    summary="Reservation for one order",
)
async def reservation_for_order(order_id: UUID, session: SessionDep) -> ReservationResponse:
    row = await ReservationRepository(session).get_by_order(order_id)
    if row is None:
        raise NotFoundError("no reservation for this order")
    return ReservationResponse.model_validate(row)
