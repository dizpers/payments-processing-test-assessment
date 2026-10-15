import asyncio
from decimal import Decimal
from uuid import uuid4

import pytest
from faststream.rabbit import RabbitMessage, TestRabbitBroker
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app import service
from app.broker import EXCHANGE, NEW, retry_queues
from app.consumer import make_app, process_payment
from app.models import Payment, PaymentStatus
from app.schemas import PaymentCreate, PaymentEvent
from tests.conftest import TEST_SETTINGS


async def make_payment(sessionmaker: async_sessionmaker[AsyncSession]) -> Payment:
    async with sessionmaker() as session:
        return await service.create_payment(
            session,
            f"key-{uuid4()}",
            PaymentCreate(
                amount=Decimal("10.50"),
                currency="USD",
                metadata={"order_id": 1},
                webhook_url="http://hooks.example/ok",
            ),
        )


async def reload(sessionmaker: async_sessionmaker[AsyncSession], payment: Payment) -> Payment:
    async with sessionmaker() as session:
        return await session.get_one(Payment, payment.id)


class Recorder:
    def __init__(self, status: PaymentStatus = PaymentStatus.SUCCEEDED, fail_webhook: bool = False):
        self.status = status
        self.fail_webhook = fail_webhook
        self.charges = 0
        self.webhooks: list[Payment] = []

    async def gateway(self) -> PaymentStatus:
        self.charges += 1
        return self.status

    async def webhook(self, payment: Payment) -> None:
        self.webhooks.append(payment)
        if self.fail_webhook:
            raise RuntimeError("webhook endpoint is down")


@pytest.mark.parametrize("status", [PaymentStatus.SUCCEEDED, PaymentStatus.FAILED])
async def test_gateway_result_is_stored_and_sent_to_the_webhook(
    sessionmaker: async_sessionmaker[AsyncSession], status: PaymentStatus
) -> None:
    recorder = Recorder(status)
    app = make_app(TEST_SETTINGS, sessionmaker, recorder.gateway, recorder.webhook)
    payment = await make_payment(sessionmaker)

    async with TestRabbitBroker(app.broker) as broker:
        await broker.publish({"payment_id": str(payment.id)}, exchange=EXCHANGE, routing_key=NEW)

    stored = await reload(sessionmaker, payment)
    assert stored.status == status
    assert stored.processed_at is not None
    assert stored.webhook_sent_at is not None
    assert [p.id for p in recorder.webhooks] == [payment.id]


async def test_redelivery_does_not_charge_or_notify_twice(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    recorder = Recorder()
    app = make_app(TEST_SETTINGS, sessionmaker, recorder.gateway, recorder.webhook)
    payment = await make_payment(sessionmaker)

    async with TestRabbitBroker(app.broker) as broker:
        for _ in range(2):
            await broker.publish(
                {"payment_id": str(payment.id)}, exchange=EXCHANGE, routing_key=NEW
            )

    assert recorder.charges == 1
    assert len(recorder.webhooks) == 1


async def test_failed_webhook_keeps_the_charge_and_schedules_a_retry(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    recorder = Recorder(fail_webhook=True)
    app = make_app(TEST_SETTINGS, sessionmaker, recorder.gateway, recorder.webhook)
    payment = await make_payment(sessionmaker)
    retried: list[dict[str, object]] = []

    @app.broker.subscriber(retry_queues(TEST_SETTINGS.retry_delays)[0], EXCHANGE)
    async def spy(event: PaymentEvent, message: RabbitMessage) -> None:
        retried.append({"payment_id": str(event.payment_id), **message.headers})

    async with TestRabbitBroker(app.broker) as broker:
        await broker.publish({"payment_id": str(payment.id)}, exchange=EXCHANGE, routing_key=NEW)

    stored = await reload(sessionmaker, payment)
    assert stored.status == PaymentStatus.SUCCEEDED
    assert stored.webhook_sent_at is None
    assert retried == [{"payment_id": str(payment.id), "x-attempt": 1}]


async def test_failure_after_the_last_retry_rejects_the_message(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    recorder = Recorder(fail_webhook=True)
    app = make_app(TEST_SETTINGS, sessionmaker, recorder.gateway, recorder.webhook)
    payment = await make_payment(sessionmaker)
    last_attempt = len(TEST_SETTINGS.retry_delays)

    async with TestRabbitBroker(app.broker) as broker:
        with pytest.raises(RuntimeError):
            await broker.publish(
                {"payment_id": str(payment.id)},
                exchange=EXCHANGE,
                routing_key=NEW,
                headers={"x-attempt": last_attempt},
            )


async def test_failed_retry_publish_does_not_reject_the_message(
    sessionmaker: async_sessionmaker[AsyncSession], monkeypatch: pytest.MonkeyPatch
) -> None:
    recorder = Recorder(fail_webhook=True)
    app = make_app(TEST_SETTINGS, sessionmaker, recorder.gateway, recorder.webhook)
    payment = await make_payment(sessionmaker)
    nacked: list[bool] = []

    async def failing_publish(*args: object, **kwargs: object) -> None:
        raise ConnectionError("broker gone")

    async with TestRabbitBroker(app.broker) as broker:
        real_publish = broker.publish
        monkeypatch.setattr(RabbitMessage, "nack", lambda self, **kw: _record(nacked))
        monkeypatch.setattr(app.broker, "publish", failing_publish)
        await real_publish({"payment_id": str(payment.id)}, exchange=EXCHANGE, routing_key=NEW)

    assert nacked == [True], "the message is handed back to the queue, not rejected"


async def _record(into: list[bool]) -> None:
    into.append(True)


async def test_concurrent_deliveries_charge_and_notify_once(
    sessionmaker: async_sessionmaker[AsyncSession],
) -> None:
    recorder = Recorder()
    charges_started = 0

    async def slow_gateway() -> PaymentStatus:
        nonlocal charges_started
        charges_started += 1
        await asyncio.sleep(0.2)
        return await recorder.gateway()

    payment = await make_payment(sessionmaker)

    await asyncio.gather(
        *(
            process_payment(sessionmaker, payment.id, slow_gateway, recorder.webhook)
            for _ in range(3)
        )
    )

    assert charges_started == 1
    assert len(recorder.webhooks) == 1
