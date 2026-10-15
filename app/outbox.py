import asyncio
import logging
from datetime import UTC, datetime

from faststream.rabbit import RabbitBroker
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.broker import EXCHANGE, NEW
from app.models import OutboxEvent

log = logging.getLogger(__name__)


async def relay_once(sessionmaker: async_sessionmaker[AsyncSession], broker: RabbitBroker) -> int:
    """Publish unpublished events and mark them; returns how many were published.

    Rows are locked so several relays can run at once; a crash after publish and before
    commit republishes the event, so consumers must tolerate duplicates.
    """
    async with sessionmaker() as session, session.begin():
        events = await session.scalars(
            select(OutboxEvent)
            .where(OutboxEvent.published_at.is_(None))
            .order_by(OutboxEvent.id)
            .limit(100)
            .with_for_update(skip_locked=True)
        )
        count = 0
        for event in events:
            await broker.publish(event.payload, exchange=EXCHANGE, routing_key=NEW, persist=True)
            event.published_at = datetime.now(UTC)
            count += 1
        return count


async def run_relay(
    sessionmaker: async_sessionmaker[AsyncSession], broker: RabbitBroker, poll_interval: float
) -> None:
    while True:
        try:
            published = await relay_once(sessionmaker, broker)
        except Exception:
            log.exception("outbox relay failed; retrying")
            published = 0
        if published == 0:
            await asyncio.sleep(poll_interval)
