"""Prefix -> upstream routing table.

Longest prefix wins, which is what makes the `/admin/*` split work: those paths
live in four different services, so `/admin/orders` must resolve to Order while
`/admin/stock` resolves to Inventory. Matching shortest-first would send every
admin call to whichever service happened to be listed earliest.
"""

from __future__ import annotations

from dataclasses import dataclass

from app.config import settings


@dataclass(frozen=True)
class Route:
    prefix: str
    upstream: str
    name: str
    # SSE responses must be streamed through rather than buffered, or the client
    # receives nothing until the order reaches a terminal state.
    stream: bool = False


def build_routes() -> list[Route]:
    routes = [
        # ---- auth ----------------------------------------------------------
        Route("/auth", settings.auth_url, "auth"),
        # ---- catalog (public storefront) -----------------------------------
        Route("/products", settings.catalog_url, "catalog"),
        Route("/categories", settings.catalog_url, "catalog"),
        # ---- inventory -----------------------------------------------------
        Route("/stock", settings.inventory_url, "inventory"),
        # ---- orders --------------------------------------------------------
        Route("/orders", settings.order_url, "order", stream=True),
        # ---- payments ------------------------------------------------------
        Route("/payments", settings.payment_url, "payment"),
        # ---- admin: same prefix, four different owners ---------------------
        Route("/admin/products", settings.catalog_url, "catalog"),
        Route("/admin/categories", settings.catalog_url, "catalog"),
        Route("/admin/stock", settings.inventory_url, "inventory"),
        Route("/admin/reservations", settings.inventory_url, "inventory"),
        Route("/admin/orders", settings.order_url, "order"),
        Route("/admin/dlq", settings.order_url, "order"),
        Route("/admin/payments", settings.payment_url, "payment"),
    ]
    # Longest first, so /admin/orders beats /orders and /admin/stock beats /stock.
    return sorted(routes, key=lambda r: len(r.prefix), reverse=True)


ROUTES = build_routes()

UPSTREAMS: dict[str, str] = {
    "auth": settings.auth_url,
    "catalog": settings.catalog_url,
    "inventory": settings.inventory_url,
    "order": settings.order_url,
    "payment": settings.payment_url,
}


def resolve(path: str) -> Route | None:
    """Find the upstream for a request path.

    Matches on a segment boundary, so `/ordersomething` does not resolve to the
    `/orders` route.
    """
    for route in ROUTES:
        if path == route.prefix or path.startswith(route.prefix + "/"):
            return route
    return None
