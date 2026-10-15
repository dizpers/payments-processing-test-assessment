from uuid import UUID, uuid4

from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models import OutboxEvent, Payment
from tests.conftest import API_KEY

HEADERS = {"X-API-Key": API_KEY, "Idempotency-Key": "key-1"}
BODY = {
    "amount": "10.50",
    "currency": "USD",
    "description": "Order 1",
    "metadata": {"order_id": 1},
    "webhook_url": "http://hooks.example/ok",
}


async def test_create_returns_202_with_pending_payment(client: AsyncClient) -> None:
    response = await client.post("/api/v1/payments", json=BODY, headers=HEADERS)

    assert response.status_code == 202
    body = response.json()
    assert set(body) == {"payment_id", "status", "created_at"}
    UUID(body["payment_id"])
    assert body["status"] == "pending"


async def test_get_returns_payment_details(client: AsyncClient) -> None:
    created = (await client.post("/api/v1/payments", json=BODY, headers=HEADERS)).json()

    response = await client.get(f"/api/v1/payments/{created['payment_id']}", headers=HEADERS)

    assert response.status_code == 200
    assert response.json() == {
        "id": created["payment_id"],
        "amount": "10.50",
        "currency": "USD",
        "description": "Order 1",
        "metadata": {"order_id": 1},
        "status": "pending",
        "idempotency_key": "key-1",
        "webhook_url": "http://hooks.example/ok",
        "created_at": created["created_at"],
        "processed_at": None,
        "webhook_sent_at": None,
    }


async def test_unknown_payment_is_404(client: AsyncClient) -> None:
    response = await client.get(f"/api/v1/payments/{uuid4()}", headers=HEADERS)
    assert response.status_code == 404


async def test_missing_or_wrong_api_key_is_401(client: AsyncClient) -> None:
    assert (await client.get(f"/api/v1/payments/{uuid4()}")).status_code == 401
    wrong = {**HEADERS, "X-API-Key": "nope"}
    assert (await client.post("/api/v1/payments", json=BODY, headers=wrong)).status_code == 401


async def test_missing_idempotency_key_is_422(client: AsyncClient) -> None:
    response = await client.post("/api/v1/payments", json=BODY, headers={"X-API-Key": API_KEY})
    assert response.status_code == 422


async def test_invalid_body_is_422(client: AsyncClient) -> None:
    for bad in (
        {"amount": "0"},
        {"amount": "1.234"},
        {"currency": "GBP"},
        {"webhook_url": "ftp://hooks.example/ok"},
    ):
        response = await client.post("/api/v1/payments", json={**BODY, **bad}, headers=HEADERS)
        assert response.status_code == 422, bad


async def test_same_key_and_body_returns_the_same_payment(
    client: AsyncClient, sessionmaker: async_sessionmaker[AsyncSession]
) -> None:
    first = await client.post("/api/v1/payments", json=BODY, headers=HEADERS)
    second = await client.post("/api/v1/payments", json=BODY, headers=HEADERS)

    assert second.status_code == 202
    assert second.json() == first.json()
    async with sessionmaker() as session:
        assert len((await session.scalars(select(Payment))).all()) == 1
        assert len((await session.scalars(select(OutboxEvent))).all()) == 1


async def test_same_key_with_equivalent_amount_notation_is_a_replay(client: AsyncClient) -> None:
    first = await client.post("/api/v1/payments", json=BODY, headers=HEADERS)
    second = await client.post("/api/v1/payments", json={**BODY, "amount": "10.5"}, headers=HEADERS)

    assert second.status_code == 202
    assert second.json()["payment_id"] == first.json()["payment_id"]


async def test_same_key_with_different_body_is_409(client: AsyncClient) -> None:
    await client.post("/api/v1/payments", json=BODY, headers=HEADERS)

    response = await client.post(
        "/api/v1/payments", json={**BODY, "amount": "99.00"}, headers=HEADERS
    )

    assert response.status_code == 409


async def test_create_writes_an_outbox_event_for_the_payment(
    client: AsyncClient, sessionmaker: async_sessionmaker[AsyncSession]
) -> None:
    created = (await client.post("/api/v1/payments", json=BODY, headers=HEADERS)).json()

    async with sessionmaker() as session:
        event = (await session.scalars(select(OutboxEvent))).one()
    assert event.event_type == "payment.created"
    assert event.payload == {"payment_id": created["payment_id"]}
    assert event.published_at is None


async def test_openapi_routes_are_disabled(client: AsyncClient) -> None:
    for path in ("/docs", "/redoc", "/openapi.json"):
        assert (await client.get(path, headers=HEADERS)).status_code == 404, path
