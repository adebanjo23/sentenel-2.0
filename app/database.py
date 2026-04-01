"""SQLite-only async database setup."""

import logging
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import StaticPool

from app.models import Base

logger = logging.getLogger(__name__)

engine: AsyncEngine | None = None
AsyncSessionLocal: async_sessionmaker[AsyncSession] | None = None


def _create_engine(database_url: str) -> AsyncEngine:
    return create_async_engine(
        database_url,
        echo=False,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )


async def init_db(database_url: str = "sqlite+aiosqlite:///./data/sentinel.db"):
    """Create engine, session factory, and all tables."""
    global engine, AsyncSessionLocal

    import os
    os.makedirs("data", exist_ok=True)

    engine = _create_engine(database_url)
    AsyncSessionLocal = async_sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autoflush=False,
    )

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    logger.info(f"Database initialized: {database_url}")


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency — yields a database session."""
    if AsyncSessionLocal is None:
        raise RuntimeError("Database not initialized. Call init_db() first.")
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()


async def get_session() -> AsyncSession:
    """Get a standalone session (for background tasks that outlive the request)."""
    if AsyncSessionLocal is None:
        raise RuntimeError("Database not initialized. Call init_db() first.")
    return AsyncSessionLocal()
