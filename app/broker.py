from faststream.rabbit import Channel, RabbitBroker, RabbitExchange, RabbitQueue

EXCHANGE = RabbitExchange("payments")
NEW = "payments.new"
DLQ = "payments.dlq"


def retry_queue_name(attempt: int) -> str:
    return f"payments.retry.{attempt}"


# Rejected messages leave payments.new through the exchange with the DLQ routing key.
new_queue = RabbitQueue(
    NEW,
    routing_key=NEW,
    arguments={"x-dead-letter-exchange": EXCHANGE.name, "x-dead-letter-routing-key": DLQ},
)
dead_letter_queue = RabbitQueue(DLQ, routing_key=DLQ)


def retry_queues(delays: list[float]) -> list[RabbitQueue]:
    """One parking queue per attempt; after its TTL a message is dead-lettered back to NEW."""
    return [
        RabbitQueue(
            retry_queue_name(attempt),
            routing_key=retry_queue_name(attempt),
            arguments={
                "x-message-ttl": int(delay * 1000),
                "x-dead-letter-exchange": EXCHANGE.name,
                "x-dead-letter-routing-key": NEW,
            },
        )
        for attempt, delay in enumerate(delays, start=1)
    ]


def make_broker(url: str) -> RabbitBroker:
    return RabbitBroker(url, default_channel=Channel(prefetch_count=10))


async def declare_topology(broker: RabbitBroker, delays: list[float]) -> None:
    exchange = await broker.declare_exchange(EXCHANGE)
    for spec in (new_queue, dead_letter_queue, *retry_queues(delays)):
        queue = await broker.declare_queue(spec)
        await queue.bind(exchange, routing_key=spec.routing_key)
