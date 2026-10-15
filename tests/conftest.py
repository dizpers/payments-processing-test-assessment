import os
from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from httpx import ASGITransport, AsyncClient
from sqlalchemy import Connection
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from testcontainers.community.postgres import PostgresContainer

from app.api import app
from app.config import Settings, get_settings
from app.db import make_sessionmaker
from app.models import Base

# Tests only ever talk to the throwaway containers started below.
os.environ.pop("DATABASE_URL", None)
os.environ.pop("RABBITMQ_URL", None)
# The reaper sidecar cannot mount the Docker Desktop socket on macOS; context managers stop
# the containers anyway.
os.environ.setdefault("TESTCONTAINERS_RYUK_DISABLED", "true")

API_KEY = "test-key"
TEST_SETTINGS = Settings(database_url="unused", rabbitmq_url="unused", api_key=API_KEY)


@pytest.fixture(scope="session")
def database_url() -> Iterator[str]:
    with PostgresContainer("postgres:17-alpine", driver="asyncpg") as postgres:
        yield postgres.get_connection_url()


def _upgrade_to_head(connection: Connection) -> None:
    config = Config(Path(__file__).parent.parent / "alembic.ini")
    config.attributes["connection"] = connection
    command.upgrade(config, "head")


@pytest.fixture(scope="session")
async def sessionmaker(database_url: str) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    engine = create_async_engine(database_url)
    async with engine.begin() as connection:
        await connection.run_sync(_upgrade_to_head)
    await engine.dispose()
    sessionmaker = make_sessionmaker(database_url)
    yield sessionmaker
    await sessionmaker.kw["bind"].dispose()


@pytest.fixture(autouse=True)
async def _clean_tables(sessionmaker: async_sessionmaker[AsyncSession]) -> AsyncIterator[None]:
    yield
    async with sessionmaker() as session:
        for table in reversed(Base.metadata.sorted_tables):
            await session.execute(table.delete())
        await session.commit()


@pytest.fixture
async def client(sessionmaker: async_sessionmaker[AsyncSession]) -> AsyncIterator[AsyncClient]:
    app.state.sessionmaker = sessionmaker
    app.dependency_overrides[get_settings] = lambda: TEST_SETTINGS
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        yield client
    app.dependency_overrides.clear()
