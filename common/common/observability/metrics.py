"""Prometheus metrics.

The one worth alerting on is ``outbox_pending``: if it climbs, the publisher is
stuck and orders are silently stalling in PENDING with no error anywhere.
``dlq_messages_total`` is the second - any increase means a handler is failing
permanently.
"""

from __future__ import annotations

from fastapi import APIRouter, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest

events_consumed = Counter(
    "events_consumed_total",
    "Events consumed, by topic and outcome",
    ["consumer", "topic", "event_type", "status"],  # status: ok | duplicate | retried | dlq
)

event_processing_seconds = Histogram(
    "event_processing_seconds",
    "Handler duration per event type",
    ["consumer", "event_type"],
    buckets=(0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)

dlq_messages = Counter(
    "dlq_messages_total",
    "Messages dead-lettered, by original topic",
    ["consumer", "topic", "error_type"],
)

outbox_pending = Gauge(
    "outbox_pending",
    "Unpublished rows in the outbox table",
    ["service"],
)

events_published = Counter(
    "events_published_total",
    "Events published to kafka",
    ["producer", "topic"],
)

payment_failures = Counter(
    "payment_failures_total",
    "Failed charge attempts, by failure code",
    ["code"],
)

saga_duration_seconds = Histogram(
    "saga_duration_seconds",
    "Order placed -> terminal state",
    ["terminal_status"],
    buckets=(0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 300.0),
)

metrics_router = APIRouter(tags=["observability"])


@metrics_router.get("/metrics", include_in_schema=False)
async def metrics() -> Response:
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
