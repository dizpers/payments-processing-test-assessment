import hmac
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated
from uuid import UUID

from fastapi import Depends, FastAPI, Header, HTTPException, Request
from sqlalchemy.ext.asyncio import AsyncSession

from app import service
from app.config import Settings, get_settings
from app.db import make_sessionmaker
from app.models import Payment
from app.schemas import PaymentAccepted, PaymentCreate, PaymentOut


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    app.state.sessionmaker = make_sessionmaker(get_settings().database_url)
    yield
    await app.state.sessionmaker.kw["bind"].dispose()


def require_api_key(
    settings: Annotated[Settings, Depends(get_settings)],
    x_api_key: Annotated[str | None, Header()] = None,
) -> None:
    if not hmac.compare_digest((x_api_key or "").encode(), settings.api_key.encode()):
        raise HTTPException(status_code=401, detail="Invalid API key")


async def get_session(request: Request) -> AsyncIterator[AsyncSession]:
    async with request.app.state.sessionmaker() as session:
        yield session


SessionDep = Annotated[AsyncSession, Depends(get_session)]

# No OpenAPI routes: the spec puts every endpoint behind the API key, and Swagger UI cannot
# fetch the schema with a custom header.
app = FastAPI(
    title="Payments",
    lifespan=lifespan,
    dependencies=[Depends(require_api_key)],
    openapi_url=None,
    docs_url=None,
    redoc_url=None,
)


@app.post("/api/v1/payments", status_code=202, response_model=PaymentAccepted)
async def create_payment(
    data: PaymentCreate,
    idempotency_key: Annotated[str, Header(min_length=1, max_length=255)],
    session: SessionDep,
) -> Payment:
    try:
        return await service.create_payment(session, idempotency_key, data)
    except service.IdempotencyConflict:
        raise HTTPException(
            status_code=409, detail="Idempotency-Key was already used with a different request"
        ) from None


@app.get("/api/v1/payments/{payment_id}", response_model=PaymentOut)
async def get_payment(payment_id: UUID, session: SessionDep) -> Payment:
    payment = await session.get(Payment, payment_id)
    if payment is None:
        raise HTTPException(status_code=404, detail="Payment not found")
    return payment
