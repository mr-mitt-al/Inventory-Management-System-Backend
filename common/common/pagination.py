"""Shared pagination contract, so every list endpoint looks the same."""

from __future__ import annotations

from typing import Annotated, Generic, TypeVar

from fastapi import Query
from pydantic import BaseModel, Field

T = TypeVar("T")


class PageParams(BaseModel):
    page: int = Field(default=1, ge=1)
    size: int = Field(default=20, ge=1, le=100)

    @property
    def offset(self) -> int:
        return (self.page - 1) * self.size

    @property
    def limit(self) -> int:
        return self.size


async def page_params(
    page: Annotated[int, Query(ge=1, description="1-indexed page number")] = 1,
    size: Annotated[int, Query(ge=1, le=100, description="Items per page")] = 20,
) -> PageParams:
    return PageParams(page=page, size=size)


class Page(BaseModel, Generic[T]):
    items: list[T]
    page: int
    size: int
    total: int
    pages: int

    @classmethod
    def build(cls, items: list[T], *, total: int, params: PageParams) -> Page[T]:
        pages = (total + params.size - 1) // params.size if total else 0
        return cls(items=items, page=params.page, size=params.size, total=total, pages=pages)
