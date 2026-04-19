"""
isolane/engine/batch.py

Main batch processing entry point.

This is what the worker calls for each work item dequeued from
Redis Streams.
"""

import os
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

import asyncpg
import polars as pl

from engine.schema import validate_batch, SchemaDriftError, SchemaValidationError
from engine.edge_cases import (
    EdgeCaseConfig,
    EdgeCaseResult,
    FailFastError,
    run_edge_case_checks,
)
from engine.quarantine import write_quarantine_records
from engine.rocksdb_client import RocksDBClient


# ── Batch result ──────────────────────────────────────────────────

@dataclass
class BatchResult:
    """Summary of a completed batch processing run."""
    run_id:              uuid.UUID
    namespace:           str
    pipeline_id:         str
    batch_offset:        str
    stream_message_id:   Optional[str]
    status:              str       # done | failed | quarantined
    records_in:          int
    records_out:         int
    records_quarantined: int
    duration_ms:         float
    error_message:       Optional[str] = None


# ── Pipeline config loader ────────────────────────────────────────

def load_pipeline_config(namespace: str, pipeline_id: str) -> dict:
    """
    Load pipeline YAML config from config/{ns}/pipelines/{pipeline_id}.yml.
    Falls back to DB config if file not found.
    """
    import yaml
    config_path = os.path.join(
        "config", namespace, "pipelines", f"{pipeline_id}.yml"
    )
    if os.path.exists(config_path):
        with open(config_path) as f:
            return yaml.safe_load(f)

    # Minimal fallback config
    return {
        "pipeline_id":    pipeline_id,
        "natural_key":    "id",
        "scd_type":       1,
        "edge_case_mode": "quarantine",
        "null_threshold": 0.05,
        "fields":         {},
    }


# ── dbt subprocess ────────────────────────────────────────────────

def run_dbt(namespace: str, pipeline_id: str, config: dict) -> bool:
    """
    Trigger dbt run for this pipeline as a subprocess.
    Connects as the scoped warehouse role for this namespace.

    Returns True on success, False on dbt failure.
    """
    dbt_project_dir = os.environ.get("DBT_PROJECT_DIR", "./dbt")
    target_schema   = f"{namespace}_active"

    dbt_model = (
        config.get("dbt", {}).get("model", pipeline_id)
        if config.get("dbt") else pipeline_id
    )
    cmd = [
        "dbt", "run",
        "--project-dir", dbt_project_dir,
        "--profiles-dir", dbt_project_dir,
        "--vars", f'{{"target_schema": "{target_schema}"}}',
        "--target", namespace,
        "--select", dbt_model,
    ]

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=300,  # 5-minute timeout
    )

    if result.returncode != 0:
        print(
            f"[batch] dbt run failed for {namespace}.{pipeline_id}: "
            f"{result.stderr[-500:]}"
        )
        return False

    return True


# ── Staging table write ───────────────────────────────────────────

async def write_to_staging(
    conn:        asyncpg.Connection,
    namespace:   str,
    pipeline_id: str,
    df:          pl.DataFrame,
) -> None:
    """
    Write clean records to the staging schema for dbt to consume.
    Creates the staging table if it doesn't exist.
    Uses TRUNCATE + INSERT to ensure idempotency on re-runs.
    """
    if len(df) == 0:
        return

    staging_schema = f"{namespace}_staging"
    table_name     = f"{staging_schema}.{pipeline_id}_staged"

    # Create staging table dynamically from DataFrame schema
    col_defs = []
    for col_name, dtype in zip(df.columns, df.dtypes):
        pg_type = _polars_to_pg_type(dtype)
        col_defs.append(f'"{col_name}" {pg_type}')

    await conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {', '.join(col_defs)}
        )
    """)

    # Truncate and reload — idempotent
    await conn.execute(f"TRUNCATE TABLE {table_name}")

    # Insert rows
    rows = df.to_dicts()
    if rows:
        cols       = ', '.join(f'"{c}"' for c in df.columns)
        placeholders = ', '.join(
            f'${i+1}' for i in range(len(df.columns))
        )
        await conn.executemany(
            f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})",
            [tuple(row[c] for c in df.columns) for row in rows],
        )


def _polars_to_pg_type(dtype: pl.DataType) -> str:
    """Map Polars dtype to PostgreSQL type string."""
    mapping = {
        pl.Utf8:      "TEXT",
        pl.Int64:     "BIGINT",
        pl.Int32:     "INTEGER",
        pl.Float64:   "DOUBLE PRECISION",
        pl.Float32:   "REAL",
        pl.Boolean:   "BOOLEAN",
        pl.Datetime:  "TIMESTAMPTZ",
        pl.Date:      "DATE",
    }
    return mapping.get(type(dtype), "TEXT")


# ── Run recorder ──────────────────────────────────────────────────

async def record_run(
    conn:   asyncpg.Connection,
    result: BatchResult,
) -> None:
    """Write the batch run result to pipeline_runs."""
    try:
        await conn.execute(
            """
            INSERT INTO pipeline_runs (
                id, tenant_id, pipeline_id, batch_offset,
                stream_message_id, status,
                records_in, records_out, records_quarantined,
                started_at, completed_at, error_message
            )
            SELECT $1,
                   t.id,
                   $3, $4, $5, $6, $7, $8, $9,
                   NOW() - ($10 || ' milliseconds')::INTERVAL,
                   NOW(),
                   $11
            FROM tenants t
            WHERE t.namespace = $2
            """,
            result.run_id,
            result.namespace,
            result.pipeline_id,
            result.batch_offset,
            result.stream_message_id,
            result.status,
            result.records_in,
            result.records_out,
            result.records_quarantined,
            str(result.duration_ms),
            result.error_message,
        )
    except Exception as e:
        # Never let run recording failure block the pipeline
        print(f"[batch] Warning: failed to record run: {e}")


# ── Main entry point ──────────────────────────────────────────────

async def process_batch(
    conn:              asyncpg.Connection,
    rdb:               RocksDBClient,
    namespace:         str,
    pipeline_id:       str,
    records:           list[dict[str, Any]],
    batch_offset:      str,
    stream_message_id: Optional[str] = None,
) -> BatchResult:
    """
    Process one batch of records end to end.

    Called by the worker dispatcher for each work item.
    Returns a BatchResult summarising what happened.
    """
    run_id   = uuid.uuid4()
    start_ms = time.monotonic() * 1000

    config      = load_pipeline_config(namespace, pipeline_id)
    edge_config = EdgeCaseConfig.from_pipeline_config(config)
    records_in  = len(records)

    print(
        f"[batch] {namespace}.{pipeline_id} | "
        f"{records_in} records | offset {batch_offset}"
    )

    # ── 1. Schema validation ───────────────────────────────────────
    try:
        df = validate_batch(
            records,
            config.get("fields", {}),
            namespace,
            pipeline_id,
            rdb,
        )
    except SchemaDriftError as e:
        # Route entire batch to quarantine on schema drift
        await write_quarantine_records(
            conn, namespace, pipeline_id, batch_offset,
            [(r, str(e)) for r in records]
        )
        duration = time.monotonic() * 1000 - start_ms
        result   = BatchResult(
            run_id=run_id, namespace=namespace, pipeline_id=pipeline_id,
            batch_offset=batch_offset, stream_message_id=stream_message_id,
            status="quarantined", records_in=records_in,
            records_out=0, records_quarantined=records_in,
            duration_ms=duration,
            error_message=f"Schema drift: {e}",
        )
        await record_run(conn, result)
        return result

    except SchemaValidationError as e:
        duration = time.monotonic() * 1000 - start_ms
        result   = BatchResult(
            run_id=run_id, namespace=namespace, pipeline_id=pipeline_id,
            batch_offset=batch_offset, stream_message_id=stream_message_id,
            status="failed", records_in=records_in,
            records_out=0, records_quarantined=0,
            duration_ms=duration,
            error_message=str(e),
        )
        await record_run(conn, result)
        return result

    # ── 2. Edge case checks ────────────────────────────────────────
    try:
        ec_result: EdgeCaseResult = run_edge_case_checks(df, edge_config)
    except FailFastError as e:
        # fail_fast mode — hard stop, nothing written
        duration = time.monotonic() * 1000 - start_ms
        result   = BatchResult(
            run_id=run_id, namespace=namespace, pipeline_id=pipeline_id,
            batch_offset=batch_offset, stream_message_id=stream_message_id,
            status="failed", records_in=records_in,
            records_out=0, records_quarantined=0,
            duration_ms=duration,
            error_message=f"fail_fast: {e.reason}",
        )
        await record_run(conn, result)
        return result

    # ── 3. Write dirty records to quarantine ───────────────────────
    quarantine_count = 0
    if ec_result.dirty:
        quarantine_count = await write_quarantine_records(
            conn, namespace, pipeline_id, batch_offset, ec_result.dirty
        )

    # ── 4. Stage clean records ─────────────────────────────────────
    if len(ec_result.clean) > 0:
        await write_to_staging(conn, namespace, pipeline_id, ec_result.clean)

        # ── 5. Trigger dbt ─────────────────────────────────────────
        dbt_ok = run_dbt(namespace, pipeline_id, config)
        if not dbt_ok:
            duration = time.monotonic() * 1000 - start_ms
            result   = BatchResult(
                run_id=run_id, namespace=namespace, pipeline_id=pipeline_id,
                batch_offset=batch_offset, stream_message_id=stream_message_id,
                status="failed", records_in=records_in,
                records_out=0, records_quarantined=quarantine_count,
                duration_ms=duration,
                error_message="dbt run failed",
            )
            await record_run(conn, result)
            return result

    # ── 6. Update offset in RocksDB ───────────────────────────────
    if stream_message_id:
        rdb.set_last_offset(namespace, pipeline_id, stream_message_id)

    # ── 7. Record successful run ───────────────────────────────────
    duration = time.monotonic() * 1000 - start_ms
    status   = "quarantined" if quarantine_count > 0 and len(ec_result.clean) == 0 \
               else "done"

    result = BatchResult(
        run_id=run_id, namespace=namespace, pipeline_id=pipeline_id,
        batch_offset=batch_offset, stream_message_id=stream_message_id,
        status=status,
        records_in=records_in,
        records_out=ec_result.clean_count,
        records_quarantined=quarantine_count,
        duration_ms=duration,
    )
    await record_run(conn, result)

    print(
        f"[batch] {namespace}.{pipeline_id} | "
        f"done in {duration:.0f}ms | "
        f"out={result.records_out} quarantined={quarantine_count}"
    )
    return result