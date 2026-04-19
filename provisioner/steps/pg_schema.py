"""
isolane/provisioner/steps/pg_schema.py

Create per-tenant PostgreSQL schemas.
Uses the connection passed from the provisioner — no nested asyncio.run().
"""

import os
from typing import Any


def run(ns: str, cfg: dict[str, Any]) -> None:
    """
    Synchronous entry point — creates schemas using psycopg2
    to avoid nested asyncio.run() issues.
    """
    import psycopg2

    conn = psycopg2.connect(
        host     = os.environ.get("POSTGRES_HOST", "postgres"),
        port     = int(os.environ.get("POSTGRES_PORT", 5432)),
        user     = os.environ.get("POSTGRES_USER", "mtpif"),
        password = os.environ.get("POSTGRES_PASSWORD", "mtpif_dev"),
        dbname   = os.environ.get("POSTGRES_DB", "mtpif"),
    )
    conn.autocommit = True

    schemas = [
        f"{ns}_active",
        f"{ns}_history",
        f"{ns}_snapshots",
        f"{ns}_staging",
        f"quarantine_{ns}",
    ]

    try:
        with conn.cursor() as cur:
            for schema in schemas:
                cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
                print(f"  [pg_schema] Schema {schema} ready")

            # Create quarantine records table
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS quarantine_{ns}.records (
                    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    pipeline_id    TEXT NOT NULL,
                    batch_offset   TEXT NOT NULL,
                    reason         TEXT NOT NULL,
                    raw_record     JSONB NOT NULL,
                    quarantined_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute(
                f"CREATE INDEX IF NOT EXISTS idx_qr_{ns}_pipeline "
                f"ON quarantine_{ns}.records(pipeline_id, quarantined_at DESC)"
            )
            print(f"  [pg_schema] quarantine_{ns}.records table ready")
    finally:
        conn.close()