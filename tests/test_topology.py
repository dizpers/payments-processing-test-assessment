"""End to end on a real RabbitMQ: outbox relay, retry queues and the dead letter queue."""

import asyncio
import json
from collections.abc import Iterator

import aio_pika
import pytest
from faststream.rabbit import TestApp
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from testcontainers.community.rabbitmq import RabbitMqContainer

from app.broker import DLQ
from app.config import Settings
from app.consumer import make_app
from app.models import OutboxEvent, PaymentStatus
from tests.test_consumer import Recorder, make_payment, reload


@pytest.fixture(scope="session")
def rabbitmq_url() -> Iterator[str]:
    with RabbitMqContainer("rabbitmq:4-alpine") as rabbitmq:
        params = rabbitmq.get_connection_params()
        yield f"amqp://guest:guest@{params.host}:{params.port}/"


async def test_failed_webhook_is_retried_then_dead_lettered(
    sessionmaker: async_sessionmaker[AsyncSession], rabbitmq_url: str
) -> None:
    settings = Settings(
        database_url="unused",
        rabbitmq_url=rabbitmq_url,
        api_key="unused",
        retry_delays=[0.2, 0.2, 0.2],
        outbox_poll_interval=0.05,
    )
    recorder = Recorder(fail_webhook=True)
    app = make_app(settings, sessionmaker, recorder.gateway, recorder.webhook)
    payment = await make_payment(sessionmaker)

    async with TestApp(app):
        for _ in range(100):
            if len(recorder.webhooks) == 4:
                break
            await asyncio.sleep(0.1)
        dead_letter = await _get_message(rabbitmq_url, DLQ)

    assert len(recorder.webhooks) == 4, "one delivery plus three retries"
    assert json.loads(dead_letter.body) == {"payment_id": str(payment.id)}
    assert dead_letter.headers["x-attempt"] == 3
    assert dead_letter.headers["x-death"][0]["queue"] == "payments.new"

    stored = await reload(sessionmaker, payment)
    assert stored.status == PaymentStatus.SUCCEEDED
    assert stored.webhook_sent_at is None
    async with sessionmaker() as session:
        event = (await session.scalars(select(OutboxEvent))).one()
    assert event.published_at is not None


async def _get_message(url: str, queue_name: str) -> aio_pika.abc.AbstractIncomingMessage:
    connection = await aio_pika.connect(url)
    async with connection:
        channel = await connection.channel()
        queue = await channel.get_queue(queue_name)
        for _ in range(50):
            message = await queue.get(fail=False)
            if message is not None:
                await message.ack()
                return message
            await asyncio.sleep(0.1)
    raise AssertionError(f"no message in {queue_name}")
