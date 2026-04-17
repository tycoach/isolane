"""
isolane/provisioner/steps/pg_schema.py

Create per-tenant PostgreSQL schemas.
"""

import os
import asyncpg
import asyncio
from typing import Any


def run(ns: str, cfg: dict[str, Any]) -> None:
    asyncio.run(_run_async(ns, cfg))


async def _run_async(ns: str, cfg: dict[str, Any]) -> None:
    conn = await asyncpg.connect(dsn=_dsn())
    try:
        schemas = [
            f"{ns}_active",
            f"{ns}_history",
            f"{ns}_snapshots",
            f"{ns}_staging",
            f"quarantine_{ns}",
        ]
        for schema in schemas:
            await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            print(f"  [pg_schema] Schema {schema} ready")

        # Create the quarantine records table inside the quarantine schema
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS quarantine_{ns}.records (
                id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                pipeline_id   TEXT NOT NULL,
                batch_offset  TEXT NOT NULL,
                reason        TEXT NOT NULL,
                raw_record    JSONB NOT NULL,
                quarantined_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
        await conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_qr_{ns}_pipeline "
            f"ON quarantine_{ns}.records(pipeline_id, quarantined_at DESC)"
        )
        print(f"  [pg_schema] quarantine_{ns}.records table ready")
    finally:
        await conn.close()


def _dsn() -> str:
    return (
        f"postgresql://{os.environ.get('POSTGRES_USER','mtpif')}"
        f":{os.environ.get('POSTGRES_PASSWORD','mtpif_dev')}"
        f"@{os.environ.get('POSTGRES_HOST','localhost')}"
        f":{os.environ.get('POSTGRES_PORT','5432')}"
        f"/{os.environ.get('POSTGRES_DB','mtpif')}"
    )