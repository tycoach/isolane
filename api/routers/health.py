"""
isolane/api/routers/health.py

Health and readiness endpoints.

"""

import os
import time
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel
from typing import Optional

from api.db.connection import get_pool

router = APIRouter(prefix="/health", tags=["health"])

# Record startup time for uptime calculation
_started_at = time.monotonic()
_started_dt = datetime.now(timezone.utc)


# ── Response models ───────────────────────────────────────────────

class LivenessResponse(BaseModel):
    status:    str       = "ok"
    timestamp: datetime


class DependencyStatus(BaseModel):
    name:    str
    healthy: bool
    detail:  Optional[str] = None


class ReadinessResponse(BaseModel):
    status:       str   # ok | degraded | unavailable
    dependencies: list[DependencyStatus]


class InfoResponse(BaseModel):
    version:     str
    environment: str
    uptime_s:    float
    started_at:  datetime


# ── Routes ────────────────────────────────────────────────────────

@router.get("/live", response_model=LivenessResponse)
async def liveness():
    """
    Liveness probe.
    """
    return LivenessResponse(timestamp=datetime.now(timezone.utc))


@router.get("/ready", response_model=ReadinessResponse)
async def readiness():
    """
    Readiness probe.
    """
    deps: list[DependencyStatus] = []

    # ── PostgreSQL ─────────────────────────────────────────────
    try:
        pool = get_pool()
        async with pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        deps.append(DependencyStatus(name="postgres", healthy=True))
    except Exception as e:
        deps.append(DependencyStatus(
            name="postgres",
            healthy=False,
            detail=str(e),
        ))

    # ── Redis ──────────────────────────────────────────────────
    try:
        import redis.asyncio as aioredis
        redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379")
        r = aioredis.from_url(redis_url, socket_connect_timeout=2)
        await r.ping()
        await r.aclose()
        deps.append(DependencyStatus(name="redis", healthy=True))
    except Exception as e:
        deps.append(DependencyStatus(
            name="redis",
            healthy=False,
            detail=str(e),
        ))

    all_healthy = all(d.healthy for d in deps)
    overall     = "ok" if all_healthy else "unavailable"

    if not all_healthy:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "status":       overall,
                "dependencies": [d.model_dump() for d in deps],
            },
        )

    return ReadinessResponse(status=overall, dependencies=deps)


@router.get("/info", response_model=InfoResponse)
async def info():
    """
    Build and runtime info.

    """
    return InfoResponse(
        version     = os.environ.get("APP_VERSION", "dev"),
        environment = os.environ.get("ENV", "development"),
        uptime_s    = round(time.monotonic() - _started_at, 2),
        started_at  = _started_dt,
    )