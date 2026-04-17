"""
isolane/api/db/connection.py

Async PostgreSQL connection pool using asyncpg.


"""

import os
import asyncpg
from typing import Optional

# ── Singleton pool — initialised once at app startup ──────────────
_pool: Optional[asyncpg.Pool] = None


async def create_pool() -> asyncpg.Pool:
    """
    Create the global connection pool.
    Called once in api/main.py on startup.
    """
    global _pool
    _pool = await asyncpg.create_pool(
        dsn=_build_dsn(),
        min_size=2,
        max_size=10,
        command_timeout=30,
        server_settings={
            # Ensure UTC everywhere
            "timezone": "UTC",
            # Statement timeout: 10s, prevents runaway queries
            "statement_timeout": "10000",
        },
    )
    return _pool


async def close_pool() -> None:
    """
    Gracefully close the pool.
    """
    global _pool
    if _pool:
        await _pool.close()
        _pool = None


def get_pool() -> asyncpg.Pool:
    """
    Return the active pool.
    """
    if _pool is None:
        raise RuntimeError(
            "Database pool is not initialised. "
            "Ensure create_pool() is called at application startup."
        )
    return _pool


async def set_tenant_context(conn: asyncpg.Connection, namespace: str) -> None:
    """
    Set the tenant namespace on the current connection transaction.
    """
    await conn.execute(
        "SELECT set_tenant_context($1)",
        namespace,
    )


# ── FastAPI dependencies ──────────────────────────────────────────

async def get_conn():
    pool = get_pool()
    async with pool.acquire() as conn:
        yield conn


async def get_tenant_conn(namespace: str):
    """
    Yields a connection with tenant context already set.
    """
    pool = get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await set_tenant_context(conn, namespace)
            yield conn


# ── Internal helpers ──────────────────────────────────────────────

def _build_dsn() -> str:
    """
    Build the PostgreSQL DSN from environment variables.
    """
    host     = os.environ.get("POSTGRES_HOST", "localhost")
    port     = os.environ.get("POSTGRES_PORT", "5432")
    user     = os.environ.get("POSTGRES_USER", "mtpif")
    password = os.environ.get("POSTGRES_PASSWORD", "mtpif_dev")
    dbname   = os.environ.get("POSTGRES_DB", "mtpif")
    return f"postgresql://{user}:{password}@{host}:{port}/{dbname}"