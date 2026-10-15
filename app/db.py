from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine


def make_sessionmaker(database_url: str) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(create_async_engine(database_url), expire_on_commit=False)
