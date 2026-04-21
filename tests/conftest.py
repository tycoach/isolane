"""
isolane/tests/conftest.py

Shared pytest fixtures for all test suites.
"""

import asyncio
import os
import pytest
import asyncpg
import redis


# ── Database fixtures ─────────────────────────────────────────────

@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


def _env(key: str, fallback: str = None) -> str:
    """Read env var. If no fallback and missing, skip the test."""
    val = os.environ.get(key, fallback)
    if val is None:
        pytest.skip(f"Required env var {key} not set")
    return val


@pytest.fixture(scope="session")
async def db_pool():
    pool = await asyncpg.create_pool(
        host     = os.environ.get("POSTGRES_HOST", "postgres"),
        port     = int(os.environ.get("POSTGRES_PORT", 5432)),
        user     = _env("POSTGRES_USER"),
        password = _env("POSTGRES_PASSWORD"),
        database = _env("POSTGRES_DB"),
        min_size = 1,
        max_size = 5,
    )
    yield pool
    await pool.close()


@pytest.fixture
async def db_conn(db_pool):
    async with db_pool.acquire() as conn:
        yield conn


# ── Redis fixture ─────────────────────────────────────────────────

@pytest.fixture(scope="session")
def redis_client():
    r = redis.Redis(
        host             = os.environ.get("REDIS_HOST", "redis-master"),
        port             = int(os.environ.get("REDIS_PORT", 6379)),
        decode_responses = True,
    )
    yield r
    r.close()


# ── Tenant namespaces for isolation tests ─────────────────────────

@pytest.fixture(scope="session")
def tenant_a():
    return "analytics"


@pytest.fixture(scope="session")
def tenant_b():
    return "finance"


# ── Sample records ────────────────────────────────────────────────

@pytest.fixture
def customer_records():
    return [
        {"customer_id": f"CUST-{i:04d}", "email": f"user{i}@test.com",
         "city": "Lagos", "created_at": "2026-01-01T00:00:00+00:00"}
        for i in range(1, 6)
    ]


@pytest.fixture
def transaction_records():
    return [
        {"transaction_id": f"TXN-{i:04d}", "amount": float(i * 10),
         "status": "settled", "created_at": "2026-01-01T00:00:00+00:00"}
        for i in range(1, 6)
    ]