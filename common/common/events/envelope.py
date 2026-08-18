"""The envelope every event on every topic is wrapped in.

Consumers can read metadata - dedup key, correlation id, version - without
knowing anything about the payload type. ``payload`` stays a plain dict on the
wire and is validated into a concrete model by the handler that cares
(``envelope.parse(OrderCreated)``), which keeps deserialization independent of
which payload models a given service happens to import.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any, TypeVar
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field

TPayload = TypeVar("TPayload", bound=BaseModel)


def _utcnow() -> datetime:
    return datetime.now(UTC)


class EventEnvelope(BaseModel):
    model_config = ConfigDict(extra="allow")  # tolerate fields added by newer producers

    event_id: UUID = Field(default_factory=uuid4)
    """Unique per event. THE deduplication key for idempotent consumers."""

    event_type: str
    """Matches the topic name, e.g. ``order.created``."""

    event_version: int = 1
    """Bumped on breaking payload changes so consumers can branch."""

    occurred_at: datetime = Field(default_factory=_utcnow)
    """When the fact happened - not when it was published. The outbox means
    those can differ by seconds."""

    correlation_id: UUID
    """Originates at the API edge and threads through every event and log line
    caused by one user request. Traces a checkout across all six services."""

    causation_id: UUID | None = None
    """event_id of the event that caused this one. Lets you reconstruct the
    causal chain of a saga."""

    producer: str
    """Service that emitted it, e.g. ``order-service``."""

    payload: dict[str, Any]

    def parse(self, model: type[TPayload]) -> TPayload:
        """Validate the payload into a concrete model."""
        return model.model_validate(self.payload)

    def to_bytes(self) -> bytes:
        return self.model_dump_json().encode("utf-8")

    @classmethod
    def from_bytes(cls, raw: bytes) -> EventEnvelope:
        return cls.model_validate(json.loads(raw.decode("utf-8")))


def make_envelope(
    *,
    event_type: str,
    payload: BaseModel,
    producer: str,
    correlation_id: UUID | None = None,
    causation_id: UUID | None = None,
    event_version: int = 1,
    occurred_at: datetime | None = None,
) -> EventEnvelope:
    """Build an envelope around a typed payload.

    ``correlation_id`` falls back to a fresh uuid, but callers handling an HTTP
    request or a consumed event should always pass the inbound one through -
    that is what makes tracing work.
    """
    return EventEnvelope(
        event_type=event_type,
        event_version=event_version,
        occurred_at=occurred_at or _utcnow(),
        correlation_id=correlation_id or uuid4(),
        causation_id=causation_id,
        producer=producer,
        payload=payload.model_dump(mode="json"),
    )
