"""Dead-letter collector. Runs as its own process.

Subscribes to every `*.DLQ` topic across the system and persists each parked
message into `dead_letters`. Reading them straight off Kafka would technically
work, but gives no way to mark one replayed or discarded - and no way to build an
admin screen that is useful rather than merely informative.

Deliberately NOT auto-replaying. A message that failed deterministically will
fail again, and an automatic retry loop on a poison message is a denial of
service you inflicted on yourself. Replay is a human decision, exposed at
POST /admin/dlq/{id}/replay.
"""

from __future__ import annotations

import asyncio
import logging

from sqlalchemy.ext.asyncio import AsyncSession

from common.db.session import Database
from common.events.envelope import EventEnvelope
from common.events.topics import Topics, dlq_topic
from common.kafka.consumer import BaseConsumer, DeadLetterPayload
from common.kafka.producer import EventProducer
from common.observability.logging import configure_logging
from common.observability.metrics import dlq_messages
from common.worker import run_worker
from app.config import settings
from app.models import DeadLetter, ProcessedEvent
from app.repositories import DeadLetterRepository

configure_logging(service_name=settings.service_name, level=settings.log_level)
logger = logging.getLogger(__name__)

# Every DLQ topic in the system, derived from the topic list so a new topic
# cannot be forgotten here.
DLQ_TOPICS = [dlq_topic(topic) for topic in Topics.all()]


class DeadLetterConsumer(BaseConsumer):
    def __init__(self, *, db: Database, producer: EventProducer) -> None:
        # Every DLQ envelope has event_type "<original topic>.dead_letter", so
        # one handler is registered per topic rather than hardcoding a list.
        handlers = {f"{topic}.dead_letter": self.on_dead_letter for topic in Topics.all()}
        super().__init__(
            name="order-dlq-consumer",
            topics=DLQ_TOPICS,
            group_id="order-dlq-collector",
            bootstrap_servers=settings.kafka_bootstrap_servers,
            db=db,
            producer=producer,
            processed_event_model=ProcessedEvent,
            handlers=handlers,
            # No retries: this handler only inserts a row. If that fails, the
            # database is down and retrying in a tight loop will not help.
            max_retries=1,
            retry_backoff_ms=settings.consumer_retry_backoff_ms,
        )

    async def on_dead_letter(self, session: AsyncSession, envelope: EventEnvelope) -> None:
        payload = envelope.parse(DeadLetterPayload)
        repo = DeadLetterRepository(session)

        if await repo.exists(envelope.event_id):
            logger.info(
                "dead letter already recorded", extra={"dlq_event_id": str(envelope.event_id)}
            )
            return

        session.add(
            DeadLetter(
                dlq_event_id=envelope.event_id,
                original_topic=payload.original_topic,
                original_key=payload.original_key,
                original_event=payload.original_event,
                raw_message=payload.raw_message,
                failed_by=envelope.producer,
                error_type=payload.error_type,
                error_message=payload.error_message,
                stack_trace=payload.stack_trace,
                attempts=payload.attempts,
                status="PARKED",
            )
        )
        await session.flush()

        dlq_messages.labels(
            consumer=envelope.producer,
            topic=payload.original_topic,
            error_type=payload.error_type,
        ).inc()

        logger.error(
            "message dead-lettered and parked for review",
            extra={
                "original_topic": payload.original_topic,
                "failed_by": envelope.producer,
                "error_type": payload.error_type,
                "attempts": payload.attempts,
            },
        )


async def main(stop: asyncio.Event) -> None:
    db = Database(settings.database_url, pool_size=4, max_overflow=2)
    producer = EventProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        producer_name=f"{settings.service_name}-dlq-collector",
    )
    await producer.start()

    consumer = DeadLetterConsumer(db=db, producer=producer)
    await consumer.start()

    task = asyncio.create_task(consumer.run())
    try:
        await stop.wait()
    finally:
        await consumer.stop()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await producer.stop()
        await db.dispose()


if __name__ == "__main__":
    run_worker(main, name="order-dlq-consumer")
