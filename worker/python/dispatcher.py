"""
isolane/worker/python/dispatcher.py

Routes work items from Redis Streams to the engine.

The dispatcher is the bridge between the Redis consumer and the
batch processing engine. It:
  1. Deserialises the work item payload
  2. Fetches a DB connection scoped to the tenant namespace
  3. Calls engine/batch.py process_batch()
  4. ACKs the message on success
  5. Does NOT ACK on failure — XAUTOCLAIM will retry

Work item fields (set by the producer / seed script):
  pipeline_id       — which pipeline to run
  batch_offset      — Kafka offset or batch reference
  records_json      — JSON-encoded list of record dicts
  _claim_count      — incremented on each XAUTOCLAIM re-delivery
"""

import asyncio
import json
import os
from typing import Any, Optional

import asyncpg

from engine.batch import process_batch, BatchResult
from engine.rocksdb_client import get_client
from worker.python.metrics import (
    batch_processed_total,
    quarantine_total,
    batch_duration_seconds,
    touch_namespace,
)


async def _get_db_conn(namespace: str) -> asyncpg.Connection:
    """
    Get a DB connection with tenant context set.
    """
    conn = await asyncpg.connect(
        host     = os.environ.get("POSTGRES_HOST", "postgres"),
        port     = int(os.environ.get("POSTGRES_PORT", 5432)),
        user     = os.environ.get("POSTGRES_USER", "mtpif"),
        password = os.environ.get("POSTGRES_PASSWORD", "mtpif_dev"),
        database = os.environ.get("POSTGRES_DB", "mtpif"),
    )
    await conn.execute("SELECT set_tenant_context($1)", namespace)
    return conn


async def dispatch_async(
    namespace:  str,
    stream_id:  str,
    fields:     dict[str, str],
) -> Optional[BatchResult]:
    """
    Async dispatch — called by the consumer for each work item.
    Returns BatchResult on success, None on failure.
    """
    touch_namespace(namespace)

    pipeline_id  = fields.get("pipeline_id", "unknown")
    batch_offset = fields.get("batch_offset", stream_id)

    # Deserialise records from JSON
    records_raw = fields.get("records_json", "[]")
    try:
        records: list[dict[str, Any]] = json.loads(records_raw)
    except json.JSONDecodeError as e:
        print(
            f"[dispatcher] {namespace}.{pipeline_id} | "
            f"invalid records_json: {e}"
        )
        return None

    if not records:
        print(
            f"[dispatcher] {namespace}.{pipeline_id} | "
            f"empty batch — skipping"
        )
        return None

    rdb  = get_client()
    conn = await _get_db_conn(namespace)

    try:
        result = await process_batch(
            conn              = conn,
            rdb               = rdb,
            namespace         = namespace,
            pipeline_id       = pipeline_id,
            records           = records,
            batch_offset      = batch_offset,
            stream_message_id = stream_id,
        )

        # Update Prometheus metrics
        batch_processed_total.labels(pipeline=pipeline_id).inc(
            result.records_out
        )
        if result.records_quarantined > 0:
            quarantine_total.labels(pipeline=pipeline_id).inc(
                result.records_quarantined
            )
        batch_duration_seconds.labels(pipeline=pipeline_id).observe(
            result.duration_ms / 1000
        )

        return result

    except Exception as e:
        print(
            f"[dispatcher] {namespace}.{pipeline_id} | "
            f"unhandled error: {e}"
        )
        return None
    finally:
        await conn.close()


def dispatch(
    namespace: str,
    stream_id: str,
    fields:    dict[str, str],
) -> Optional[BatchResult]:
    """
    Sync wrapper around dispatch_async.
    """
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # Already in an async context — create a task
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as pool:
                future = pool.submit(
                    asyncio.run,
                    dispatch_async(namespace, stream_id, fields)
                )
                return future.result(timeout=300)
        else:
            return loop.run_until_complete(
                dispatch_async(namespace, stream_id, fields)
            )
    except Exception as e:
        print(f"[dispatcher] {namespace} | dispatch error: {e}")
        return None