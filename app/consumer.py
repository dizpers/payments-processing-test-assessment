import asyncio
import logging
import random
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from uuid import UUID

import httpx
from faststream import FastStream
from faststream.rabbit import RabbitMessage
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.broker import EXCHANGE, declare_topology, make_broker, new_queue, retry_queue_name
from app.config import Settings, get_settings
from app.db import make_sessionmaker
from app.models import Payment, PaymentStatus
from app.outbox import run_relay
from app.schemas import PaymentEvent, WebhookPayload

log = logging.getLogger(__name__)

Gateway = Callable[[], Awaitable[PaymentStatus]]
Webhook = Callable[[Payment], Awaitable[None]]


def emulated_gateway(settings: Settings, rng: random.Random) -> Gateway:
    async def charge() -> PaymentStatus:
        await asyncio.sleep(rng.uniform(settings.gateway_delay_min, settings.gateway_delay_max))
        if rng.random() < settings.gateway_success_rate:
            return PaymentStatus.SUCCEEDED
        return PaymentStatus.FAILED

    return charge


def http_webhook(client: httpx.AsyncClient, timeout: float) -> Webhook:
    async def send(payment: Payment) -> None:
        response = await client.post(
            payment.webhook_url,
            content=WebhookPayload.model_validate(payment).model_dump_json(),
            headers={"content-type": "application/json"},
            timeout=timeout,
        )
        response.raise_for_status()

    return send


async def process_payment(
    sessionmaker: async_sessionmaker[AsyncSession],
    payment_id: UUID,
    gateway: Gateway,
    webhook: Webhook,
) -> None:
    async with sessionmaker() as session:
        async with session.begin():
            # The row stays locked while the gateway runs, so a duplicate delivery waits
            # and then sees the final status instead of charging twice.
            payment = await session.get(Payment, payment_id, with_for_update=True)
            if payment is None:
                raise LookupError(f"payment {payment_id} not found")
            if payment.status == PaymentStatus.PENDING:
                payment.status = await gateway()
                payment.processed_at = datetime.now(UTC)
        # The gateway result is committed before the webhook: a failed webhook must not
        # roll back the charge. The webhook runs under a fresh row lock so concurrent
        # deliveries cannot both send it.
        async with session.begin():
            payment = await session.get_one(
                Payment, payment_id, with_for_update=True, populate_existing=True
            )
            if payment.webhook_sent_at is None:
                await webhook(payment)
                payment.webhook_sent_at = datetime.now(UTC)


def make_app(
    settings: Settings,
    sessionmaker: async_sessionmaker[AsyncSession],
    gateway: Gateway,
    webhook: Webhook,
) -> FastStream:
    broker = make_broker(settings.rabbitmq_url)
    app = FastStream(broker)
    max_attempts = len(settings.retry_delays)

    @broker.subscriber(new_queue, EXCHANGE)
    async def handle_payment(event: PaymentEvent, message: RabbitMessage) -> None:
        payment_id = event.payment_id
        attempt = int(message.headers.get("x-attempt", 0))
        try:
            await process_payment(sessionmaker, payment_id, gateway, webhook)
        except Exception:
            if attempt >= max_attempts:
                # Raising rejects the message; the queue's DLX moves it to payments.dlq.
                log.exception("payment %s failed after %s retries", payment_id, attempt)
                raise
            log.warning("payment %s attempt %s failed, scheduling retry", payment_id, attempt)
            try:
                await broker.publish(
                    {"payment_id": str(payment_id)},
                    exchange=EXCHANGE,
                    routing_key=retry_queue_name(attempt + 1),
                    headers={"x-attempt": attempt + 1},
                    persist=True,
                )
            except Exception:
                # Could not park the message: give it back to the queue instead of
                # letting the reject-on-error policy dead-letter it early.
                log.exception("payment %s: retry publish failed, requeueing", payment_id)
                await message.nack()

    relay: asyncio.Task[None] | None = None

    @app.after_startup
    async def start_relay() -> None:
        nonlocal relay
        await declare_topology(broker, settings.retry_delays)
        relay = asyncio.create_task(run_relay(sessionmaker, broker, settings.outbox_poll_interval))

    @app.on_shutdown
    async def stop_relay() -> None:
        if relay is not None:
            relay.cancel()

    return app


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    settings = get_settings()
    app = make_app(
        settings,
        make_sessionmaker(settings.database_url),
        emulated_gateway(settings, random.Random()),
        http_webhook(httpx.AsyncClient(), settings.webhook_timeout),
    )
    asyncio.run(app.run())


if __name__ == "__main__":
    main()
