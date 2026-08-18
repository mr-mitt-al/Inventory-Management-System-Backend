"""Gateway routing.

The `/admin/*` cases are the ones worth testing: those paths live in four
different services, so shortest-prefix matching would send every admin request to
whichever route happened to be listed first.
"""

from __future__ import annotations

import pytest

from app.config import settings
from app.routing import ROUTES, resolve


class TestPublicRoutes:
    @pytest.mark.parametrize(
        ("path", "expected"),
        [
            ("/auth/login", "auth"),
            ("/auth/register", "auth"),
            ("/auth/users", "auth"),
            ("/products", "catalog"),
            ("/products/abc-123", "catalog"),
            ("/categories", "catalog"),
            ("/stock/abc-123", "inventory"),
            ("/orders", "order"),
            ("/orders/abc-123", "order"),
            ("/orders/abc-123/cancel", "order"),
            ("/payments/order/abc", "payment"),
            ("/payments/test-tokens", "payment"),
        ],
    )
    def test_routes_to_the_owning_service(self, path: str, expected: str) -> None:
        route = resolve(path)
        assert route is not None, f"{path} did not resolve"
        assert route.name == expected


class TestAdminRoutesSplitAcrossServices:
    @pytest.mark.parametrize(
        ("path", "expected"),
        [
            ("/admin/products", "catalog"),
            ("/admin/categories", "catalog"),
            ("/admin/stock", "inventory"),
            ("/admin/stock/abc/ledger", "inventory"),
            ("/admin/reservations", "inventory"),
            ("/admin/orders", "order"),
            ("/admin/orders/abc/ship", "order"),
            ("/admin/dlq", "order"),
            ("/admin/dlq/abc/replay", "order"),
            ("/admin/payments", "payment"),
            ("/admin/payments/abc/refund", "payment"),
        ],
    )
    def test_admin_paths_reach_the_right_owner(self, path: str, expected: str) -> None:
        route = resolve(path)
        assert route is not None, f"{path} did not resolve"
        assert route.name == expected


class TestLongestPrefixWins:
    def test_admin_orders_beats_orders(self) -> None:
        # With shortest-first matching, /admin/orders would resolve via /orders and
        # every admin order call would hit the wrong path on the right service.
        assert resolve("/admin/orders").name == "order"
        assert resolve("/admin/stock").name == "inventory"

    def test_routes_are_sorted_longest_first(self) -> None:
        lengths = [len(r.prefix) for r in ROUTES]
        assert lengths == sorted(lengths, reverse=True)


class TestSegmentBoundaries:
    def test_partial_segment_does_not_match(self) -> None:
        # /ordersomething must not resolve to the /orders route.
        assert resolve("/ordersomething") is None
        assert resolve("/productsfoo") is None

    def test_exact_prefix_matches(self) -> None:
        assert resolve("/orders") is not None

    def test_unknown_path_returns_none(self) -> None:
        assert resolve("/nonsense") is None
        assert resolve("/") is None


class TestStreamingFlag:
    def test_order_route_is_marked_streamable(self) -> None:
        # SSE must be streamed through, not buffered - buffering would hold the
        # whole response until the order reached a terminal state.
        assert resolve("/orders/abc/stream").stream is True

    def test_non_streaming_routes_are_not_marked(self) -> None:
        assert resolve("/products").stream is False
        assert resolve("/auth/login").stream is False


class TestUpstreamConfiguration:
    def test_every_route_has_a_non_empty_upstream(self) -> None:
        for route in ROUTES:
            assert route.upstream.startswith("http"), route.prefix

    def test_upstreams_are_distinct_per_service(self) -> None:
        # A copy-paste error pointing two services at one URL would be invisible
        # at runtime until requests started 404ing.
        urls = {
            settings.auth_url,
            settings.catalog_url,
            settings.order_url,
            settings.inventory_url,
            settings.payment_url,
        }
        assert len(urls) == 5
