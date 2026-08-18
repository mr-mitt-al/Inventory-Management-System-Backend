from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Annotated
from uuid import UUID, uuid4

from fastapi import Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from common.db.session import Database
from app.services import InventoryService


class AppState:
    db: Database | None = None


state = AppState()


def get_db() -> Database:
    if state.db is None:
        raise RuntimeError("database not initialized")
    return state.db


async def get_session() -> AsyncIterator[AsyncSession]:
    async with get_db().session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


def get_correlation_id(request: Request) -> UUID:
    raw = getattr(request.state, "correlation_id", None)
    try:
        return UUID(str(raw))
    except (TypeError, ValueError):
        return uuid4()


async def get_inventory_service(
    session: Annotated[AsyncSession, Depends(get_session)],
    correlation_id: Annotated[UUID, Depends(get_correlation_id)],
) -> InventoryService:
    return InventoryService(session, correlation_id=correlation_id)


SessionDep = Annotated[AsyncSession, Depends(get_session)]
InventoryServiceDep = Annotated[InventoryService, Depends(get_inventory_service)]
