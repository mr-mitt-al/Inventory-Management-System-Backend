"""API Gateway - single entry point for the frontend.

What it does:
  - routes by path prefix to the owning service (longest prefix wins)
  - originates the correlation id so one trace covers a whole checkout
  - rate limits per user (or per IP when anonymous)
  - streams SSE responses through instead of buffering them

What it deliberately does NOT do: validate tokens. Each service verifies the JWT
signature itself with the shared secret. Verifying here as well would mean either
duplicating the logic or - worse - services trusting a header the gateway set,
which makes every service reachable-and-authenticated to anything that can talk to
it directly.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse, StreamingResponse
from redis.asyncio import Redis

from common.api import create_app
from common.observability.logging import configure_logging
from app.config import settings
from app.rate_limit import RateLimiter
from app.routing import ROUTES, UPSTREAMS, resolve

configure_logging(service_name=settings.service_name, level=settings.log_level)
logger = logging.getLogger(__name__)

# Hop-by-hop headers must not be forwarded - they describe THIS connection, not the
# proxied one. Passing Content-Length through after httpx has decoded the body is
# a classic source of truncated responses.
HOP_BY_HOP = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
    "content-length",
    "content-encoding",
    "host",
}


class GatewayState:
    client: httpx.AsyncClient | None = None
    redis: Redis | None = None
    limiter: RateLimiter | None = None


state = GatewayState()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    # One pooled client for the process. Creating a client per request would open a
    # new TCP connection every time and exhaust ephemeral ports under load.
    state.client = httpx.AsyncClient(
        timeout=httpx.Timeout(settings.upstream_timeout_seconds),
        limits=httpx.Limits(
            max_connections=settings.max_connections,
            max_keepalive_connections=settings.max_keepalive_connections,
        ),
        follow_redirects=False,
    )
    state.redis = Redis.from_url(settings.redis_url, decode_responses=True)
    state.limiter = RateLimiter(state.redis)

    logger.info(
        "gateway ready",
        extra={"routes": len(ROUTES), "upstreams": sorted(UPSTREAMS)},
    )
    try:
        yield
    finally:
        if state.client is not None:
            await state.client.aclose()
        if state.redis is not None:
            await state.redis.aclose()
        logger.info("gateway stopped")


async def _redis_ready() -> bool:
    if state.redis is None:
        return True
    try:
        await state.redis.ping()
    except Exception:
        logger.warning("redis unreachable, rate limiting disabled")
    return True  # rate limiting is best-effort, not a readiness gate


app = create_app(
    service_name=settings.service_name,
    title="API Gateway",
    description=(
        "Single entry point for the frontend. Routes by path prefix, originates "
        "correlation ids, and rate limits.\n\n"
        "Does NOT validate tokens - every service verifies the JWT locally with the "
        "shared secret, so a service is never dependent on the gateway having done "
        "it, and never trusts a header the gateway set."
    ),
    lifespan=lifespan,
    readiness_checks={"redis": _redis_ready},
)


@app.get("/", include_in_schema=False)
async def root() -> dict:
    return {
        "service": settings.service_name,
        "routes": {r.prefix: r.name for r in ROUTES},
        "docs": {name: f"{url}/docs" for name, url in UPSTREAMS.items()},
    }


def _client_identity(request: Request) -> str:
    """Rate-limit key: the user when signed in, the client IP otherwise.

    The token is read WITHOUT verification - a forged one only buys the attacker
    their own rate-limit bucket, and real authorization happens at the service.
    """
    auth = request.headers.get("authorization", "")
    if auth.lower().startswith("bearer "):
        token = auth[7:]
        try:
            import jwt

            claims = jwt.decode(token, options={"verify_signature": False})
            sub = claims.get("sub")
            if sub:
                return f"user:{sub}"
        except Exception:
            pass

    # X-Forwarded-For's first entry is the original client when a real proxy sits
    # in front. Trusted here only because this is a development gateway.
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        return f"ip:{forwarded.split(',')[0].strip()}"
    return f"ip:{request.client.host if request.client else 'unknown'}"


@app.api_route(
    "/{full_path:path}",
    methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"],
    include_in_schema=False,
)
async def proxy(full_path: str, request: Request) -> Response:
    path = "/" + full_path.lstrip("/")

    route = resolve(path)
    if route is None:
        return JSONResponse(
            status_code=404,
            content={
                "error": {"code": "no_route", "message": f"no upstream serves {path}"},
                "available": sorted({r.prefix for r in ROUTES}),
            },
        )

    if state.client is None or state.limiter is None:
        return JSONResponse(
            status_code=503, content={"error": {"code": "not_ready", "message": "starting up"}}
        )

    # Rate limit before proxying, so a throttled request costs an upstream nothing.
    try:
        limit, remaining = await state.limiter.check(
            identity=_client_identity(request), method=request.method
        )
    except Exception as exc:
        from common.errors import RateLimitedError

        if isinstance(exc, RateLimitedError):
            return JSONResponse(
                status_code=429,
                content={
                    "error": {
                        "code": exc.code,
                        "message": exc.message,
                        "details": exc.details,
                    }
                },
                headers={"Retry-After": str(settings.rate_limit_window_seconds)},
            )
        raise

    correlation_id = request.headers.get("X-Correlation-Id") or str(
        getattr(request.state, "correlation_id", "")
    )

    headers = {k: v for k, v in request.headers.items() if k.lower() not in HOP_BY_HOP}
    headers["X-Correlation-Id"] = correlation_id
    headers["X-Forwarded-Host"] = request.headers.get("host", "")
    headers["X-Forwarded-Prefix"] = ""

    url = f"{route.upstream}{path}"
    body = await request.body()

    # SSE must be streamed. Buffering it would hold the whole response until the
    # order reached a terminal state, defeating the point of the endpoint.
    if route.stream and path.endswith("/stream"):
        return await _proxy_stream(request, url, headers, route)

    try:
        upstream = await state.client.request(
            request.method,
            url,
            headers=headers,
            content=body or None,
            params=request.query_params,
        )
    except httpx.TimeoutException:
        logger.error("upstream timeout", extra={"upstream": route.name, "path": path})
        return JSONResponse(
            status_code=504,
            content={
                "error": {
                    "code": "upstream_timeout",
                    "message": f"{route.name} did not respond in time",
                }
            },
        )
    except httpx.RequestError as exc:
        logger.error(
            "upstream unreachable",
            extra={"upstream": route.name, "path": path, "error": str(exc)},
        )
        return JSONResponse(
            status_code=502,
            content={
                "error": {
                    "code": "upstream_unavailable",
                    "message": f"{route.name} is unavailable",
                }
            },
        )

    response_headers = {
        k: v for k, v in upstream.headers.items() if k.lower() not in HOP_BY_HOP
    }
    response_headers["X-Correlation-Id"] = correlation_id
    if limit:
        response_headers["X-RateLimit-Limit"] = str(limit)
        response_headers["X-RateLimit-Remaining"] = str(remaining)
    response_headers["X-Upstream-Service"] = route.name

    return Response(
        content=upstream.content,
        status_code=upstream.status_code,
        headers=response_headers,
        media_type=upstream.headers.get("content-type"),
    )


async def _proxy_stream(request: Request, url: str, headers: dict, route) -> Response:
    """Pass an SSE response through chunk by chunk.

    The upstream connection is opened inside the generator so its lifetime matches
    the response body's - opening it outside and returning would close it before
    the client had read anything.
    """
    client = state.client
    assert client is not None

    async def stream() -> AsyncIterator[bytes]:
        try:
            async with client.stream(
                request.method,
                url,
                headers=headers,
                params=request.query_params,
                timeout=httpx.Timeout(settings.stream_timeout_seconds),
            ) as upstream:
                async for chunk in upstream.aiter_raw():
                    yield chunk
        except httpx.RequestError as exc:
            logger.warning(
                "sse upstream dropped", extra={"upstream": route.name, "error": str(exc)}
            )
            yield b'event: error\ndata: {"error":"upstream unavailable"}\n\n'

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
            "X-Upstream-Service": route.name,
        },
    )
