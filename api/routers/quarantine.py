"""
isolane/api/routers/quarantine.py

Quarantine review endpoints. Namespace-scoped.

Quarantine records live in the per-tenant PostgreSQL schema
quarantine_{ns}, not in the shared metadata store.
The API queries them directly using the scoped DB connection.

GET    /{namespace}/quarantine                         — list quarantined records
GET    /{namespace}/quarantine/{pipeline_id}           — by pipeline
GET    /{namespace}/quarantine/{pipeline_id}/{record_id} — single record
POST   /{namespace}/quarantine/{pipeline_id}/{record_id}/reprocess — retry
DELETE /{namespace}/quarantine/{pipeline_id}/{record_id} — discard
"""

import json
from typing import Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel
from datetime import datetime

from api.middleware.namespace_guard import (
    require_namespace_match,
    require_namespace_engineer,
)
from api.auth.jwt import TokenPayload
from api.db.connection import get_pool, set_tenant_context

router = APIRouter(tags=["quarantine"])


# ── Response models ───────────────────────────────────────────────

class QuarantineRecord(BaseModel):
    id:            UUID
    pipeline_id:   str
    batch_offset:  str
    reason:        str
    raw_record:    dict
    quarantined_at: datetime


class QuarantineListResponse(BaseModel):
    records: list[QuarantineRecord]
    total:   int


class ReprocessResponse(BaseModel):
    record_id:   UUID
    pipeline_id: str
    status:      str = "queued"
    message:     str = "Record queued for reprocessing via Redis Streams"


# ── Routes ────────────────────────────────────────────────────────

@router.get("/{namespace}/quarantine", response_model=QuarantineListResponse)
async def list_quarantine(
    namespace: str,
    pipeline_id: Optional[str] = Query(
        default=None,
        description="Filter by pipeline_id",
    ),
    reason: Optional[str] = Query(
        default=None,
        description="Filter by quarantine reason",
    ),
    limit:  int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    payload: TokenPayload = Depends(require_namespace_match),
):
    """
    List quarantined records for this namespace.
    """
    pool = get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await set_tenant_context(conn, namespace)

            # Quarantine lives in its own schema per tenant
            schema = f"quarantine_{namespace}"

            conditions = []
            params     = []
            idx        = 1

            if pipeline_id:
                conditions.append(f"pipeline_id = ${idx}")
                params.append(pipeline_id)
                idx += 1

            if reason:
                conditions.append(f"reason = ${idx}")
                params.append(reason)
                idx += 1

            where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

            try:
                rows = await conn.fetch(
                    f"""
                    SELECT id, pipeline_id, batch_offset, reason,
                           raw_record, quarantined_at
                    FROM {schema}.records
                    {where}
                    ORDER BY quarantined_at DESC
                    LIMIT ${idx} OFFSET ${idx + 1}
                    """,
                    *params, limit, offset,
                )
                total = await conn.fetchval(
                    f"SELECT COUNT(*) FROM {schema}.records {where}",
                    *params,
                )
            except Exception:
                # Schema may not exist if no records have been quarantined yet
                return QuarantineListResponse(records=[], total=0)

    records = [
        QuarantineRecord(
            id=r["id"],
            pipeline_id=r["pipeline_id"],
            batch_offset=r["batch_offset"],
            reason=r["reason"],
            raw_record=json.loads(r["raw_record"]),
            quarantined_at=r["quarantined_at"],
        )
        for r in rows
    ]
    return QuarantineListResponse(records=records, total=total)


@router.get(
    "/{namespace}/quarantine/{pipeline_id}/{record_id}",
    response_model=QuarantineRecord,
)
async def get_quarantine_record(
    namespace:   str,
    pipeline_id: str,
    record_id:   UUID,
    payload:     TokenPayload = Depends(require_namespace_match),
):
    """Get a single quarantine record. All roles."""
    pool   = get_pool()
    schema = f"quarantine_{namespace}"

    async with pool.acquire() as conn:
        async with conn.transaction():
            await set_tenant_context(conn, namespace)
            try:
                row = await conn.fetchrow(
                    f"""
                    SELECT id, pipeline_id, batch_offset, reason,
                           raw_record, quarantined_at
                    FROM {schema}.records
                    WHERE pipeline_id = $1 AND id = $2
                    """,
                    pipeline_id, record_id,
                )
            except Exception:
                row = None

    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Quarantine record '{record_id}' not found",
        )
    return QuarantineRecord(
        id=row["id"],
        pipeline_id=row["pipeline_id"],
        batch_offset=row["batch_offset"],
        reason=row["reason"],
        raw_record=json.loads(row["raw_record"]),
        quarantined_at=row["quarantined_at"],
    )


@router.post(
    "/{namespace}/quarantine/{pipeline_id}/{record_id}/reprocess",
    response_model=ReprocessResponse,
)
async def reprocess_quarantine_record(
    namespace:   str,
    pipeline_id: str,
    record_id:   UUID,
    payload:     TokenPayload = Depends(require_namespace_engineer),
):
    """
    Queue a quarantined record for reprocessing.
    Engineer role or above required.

    Pushes the raw record back onto the Redis Streams work queue
    as a new work item. The original quarantine record is left
    intact until the reprocessing succeeds — at which point the
    engine deletes it from the quarantine schema.
    """
    import redis.asyncio as aioredis
    import os
    # import json as _json
    # import uuid as _uuid

    pool   = get_pool()
    schema = f"quarantine_{namespace}"

    # Fetch the record
    async with pool.acquire() as conn:
        async with conn.transaction():
            await set_tenant_context(conn, namespace)
            try:
                row = await conn.fetchrow(
                    f"""
                    SELECT id, pipeline_id, batch_offset, raw_record
                    FROM {schema}.records
                    WHERE pipeline_id = $1 AND id = $2
                    """,
                    pipeline_id, record_id,
                )
            except Exception:
                row = None

    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Quarantine record '{record_id}' not found",
        )

    # Push back onto Redis Streams work queue
    redis_url   = os.environ.get("REDIS_URL", "redis://localhost:6379")
    stream_key  = f"{namespace}.work-queue"
    group       = f"{namespace}-consumer-group"

    r = aioredis.from_url(redis_url)
    await r.xadd(
        stream_key,
        {
            "pipeline_id":      pipeline_id,
            "batch_offset":     row["batch_offset"],
            "raw_record":       row["raw_record"],
            "reprocess_id":     str(record_id),
            "is_reprocess":     "true",
        },
    )
    await r.aclose()

    return ReprocessResponse(pipeline_id=pipeline_id, record_id=record_id)


@router.delete(
    "/{namespace}/quarantine/{pipeline_id}/{record_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def discard_quarantine_record(
    namespace:   str,
    pipeline_id: str,
    record_id:   UUID,
    payload:     TokenPayload = Depends(require_namespace_engineer),
):
    """
    Permanently discard a quarantined record. Engineer role or above.
    Use when the record is known bad and reprocessing is not appropriate.
    This action is irreversible.
    """
    pool   = get_pool()
    schema = f"quarantine_{namespace}"

    async with pool.acquire() as conn:
        async with conn.transaction():
            await set_tenant_context(conn, namespace)
            try:
                result = await conn.execute(
                    f"""
                    DELETE FROM {schema}.records
                    WHERE pipeline_id = $1 AND id = $2
                    """,
                    pipeline_id, record_id,
                )
            except Exception:
                result = "DELETE 0"

    if result == "DELETE 0":
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Quarantine record '{record_id}' not found",
        )