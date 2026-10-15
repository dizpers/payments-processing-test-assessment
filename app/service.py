import uuid

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import OutboxEvent, Payment
from app.schemas import PaymentCreate


class IdempotencyConflict(Exception):
    """The Idempotency-Key was already used with a different request body."""


async def create_payment(
    session: AsyncSession, idempotency_key: str, data: PaymentCreate
) -> Payment:
    payment = Payment(
        id=uuid.uuid4(),
        amount=data.amount,
        currency=data.currency,
        description=data.description,
        metadata_=data.metadata,
        webhook_url=str(data.webhook_url),
        idempotency_key=idempotency_key,
    )
    session.add(payment)
    session.add(OutboxEvent(event_type="payment.created", payload={"payment_id": str(payment.id)}))
    try:
        await session.commit()
    except IntegrityError:
        # The key is taken: either a retry of the same request or a conflicting one.
        await session.rollback()
        existing = await session.scalar(
            select(Payment).where(Payment.idempotency_key == idempotency_key)
        )
        if existing is None:
            raise
        if not _same_request(existing, data):
            raise IdempotencyConflict from None
        return existing
    return payment


def _same_request(payment: Payment, data: PaymentCreate) -> bool:
    return (
        payment.amount == data.amount
        and payment.currency == data.currency
        and payment.description == data.description
        and payment.metadata_ == data.metadata
        and payment.webhook_url == str(data.webhook_url)
    )
