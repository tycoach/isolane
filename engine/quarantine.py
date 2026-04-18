"""
isolane/engine/quarantine.py

Write dirty records to the per-tenant quarantine schema.
"""

import json
import uuid
from datetime import datetime, timezone
from typing import Any

import asyncpg


async def write_quarantine_records(
    conn:         asyncpg.Connection,
    namespace:    str,
    pipeline_id:  str,
    batch_offset: str,
    dirty:        list[tuple[dict[str, Any], str]],
) -> int:
    """
    Write a list of dirty records to quarantine_{ns}.records.
    """
    if not dirty:
        return 0

    schema = f"quarantine_{namespace}"
    now    = datetime.now(timezone.utc)

    rows = [
        (
            uuid.uuid4(),
            pipeline_id,
            batch_offset,
            reason,
            json.dumps(record, default=str),  # JSONB — serialise non-JSON types
            now,
        )
        for record, reason in dirty
    ]

    await conn.executemany(
        f"""
        INSERT INTO {schema}.records
            (id, pipeline_id, batch_offset, reason, raw_record, quarantined_at)
        VALUES ($1, $2, $3, $4, $5::jsonb, $6)
        """,
        rows,
    )

    return len(rows)


async def write_single_quarantine_record(
    conn:         asyncpg.Connection,
    namespace:    str,
    pipeline_id:  str,
    batch_offset: str,
    record:       dict[str, Any],
    reason:       str,
) -> None:
    """
    Write a single dirty record to quarantine..
    """
    schema = f"quarantine_{namespace}"

    await conn.execute(
        f"""
        INSERT INTO {schema}.records
            (id, pipeline_id, batch_offset, reason, raw_record, quarantined_at)
        VALUES ($1, $2, $3, $4, $5::jsonb, $6)
        """,
        uuid.uuid4(),
        pipeline_id,
        batch_offset,
        reason,
        json.dumps(record, default=str),
        datetime.now(timezone.utc),
    )


async def get_quarantine_count(
    conn:        asyncpg.Connection,
    namespace:   str,
    pipeline_id: str,
) -> int:
    """
    Return the total number of quarantined records for a pipeline.
    """
    schema = f"quarantine_{namespace}"
    try:
        count = await conn.fetchval(
            f"SELECT COUNT(*) FROM {schema}.records WHERE pipeline_id = $1",
            pipeline_id,
        )
        return count or 0
    except Exception:
        return 0


async def delete_quarantine_record(
    conn:        asyncpg.Connection,
    namespace:   str,
    pipeline_id: str,
    record_id:   uuid.UUID,
) -> bool:
    """
    Delete a single quarantine record by ID.
    """
    schema = f"quarantine_{namespace}"
    result = await conn.execute(
        f"DELETE FROM {schema}.records WHERE pipeline_id=$1 AND id=$2",
        pipeline_id,
        record_id,
    )
    return result == "DELETE 1"