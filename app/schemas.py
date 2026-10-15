from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, HttpUrl

from app.models import Currency, PaymentStatus


class PaymentCreate(BaseModel):
    amount: Decimal = Field(gt=0, max_digits=18, decimal_places=2)
    currency: Currency
    description: str = ""
    metadata: dict[str, Any] = {}
    webhook_url: HttpUrl


class PaymentAccepted(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    payment_id: UUID = Field(validation_alias="id")
    status: PaymentStatus
    created_at: datetime


class PaymentEvent(BaseModel):
    """Body of every message on the payments exchange."""

    payment_id: UUID


class WebhookPayload(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    payment_id: UUID = Field(validation_alias="id")
    status: PaymentStatus
    amount: Decimal
    currency: Currency
    processed_at: datetime | None
    metadata: dict[str, Any] = Field(validation_alias="metadata_")


class PaymentOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any] = Field(validation_alias="metadata_")
    status: PaymentStatus
    idempotency_key: str
    webhook_url: str
    created_at: datetime
    processed_at: datetime | None
    webhook_sent_at: datetime | None
