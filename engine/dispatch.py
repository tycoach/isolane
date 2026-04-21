"""
isolane/engine/dispatch.py

Python engine entry point called by the Go worker as a subprocess.
"""

import argparse
import asyncio
import json
import os
import sys

import asyncpg




async def run(
    namespace:    str,
    pipeline_id:  str,
    stream_id:    str,
    batch_offset: str,
    records:      list,
) -> dict:
    from engine.rocksdb_client import get_client
    from engine.batch import process_batch

    conn = await asyncpg.connect(
        host     = os.environ.get("POSTGRES_HOST", "postgres"),
        port     = int(os.environ.get("POSTGRES_PORT", 5432)),
        user     = os.environ.get("POSTGRES_USER", "mtpif"),
        password = os.environ.get("POSTGRES_PASSWORD", "postgres"),
        database = os.environ.get("POSTGRES_DB", "mtpif"),
    )

    await conn.execute("SELECT set_tenant_context($1)", namespace)

    rdb    = get_client()
    result = await process_batch(
        conn              = conn,
        rdb               = rdb,
        namespace         = namespace,
        pipeline_id       = pipeline_id,
        records           = records,
        batch_offset      = batch_offset,
        stream_message_id = stream_id,
    )
    await conn.close()

    return {
        "status":              result.status,
        "records_in":          result.records_in,
        "records_out":         result.records_out,
        "records_quarantined": result.records_quarantined,
        "duration_ms":         result.duration_ms,
        "error_message":       result.error_message or "",
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--namespace",    required=True)
    parser.add_argument("--pipeline",     required=True)
    parser.add_argument("--stream-id",    required=True)
    parser.add_argument("--batch-offset", required=True)
    args = parser.parse_args()

    # Read records from stdin
    try:
        records = json.loads(sys.stdin.read())
    except json.JSONDecodeError as e:
        result = {
            "status": "failed",
            "records_in": 0, "records_out": 0,
            "records_quarantined": 0, "duration_ms": 0,
            "error_message": f"Invalid JSON input: {e}",
        }
        sys.stdout.write(json.dumps(result) + "\n")
        sys.stdout.flush()
        sys.exit(1)

    try:
        result = asyncio.run(run(
            namespace    = args.namespace,
            pipeline_id  = args.pipeline,
            stream_id    = args.stream_id,
            batch_offset = args.batch_offset,
            records      = records,
        ))
        output = json.dumps(result)
        sys.stdout.write(output + "\n")
        sys.stdout.flush()
        sys.exit(0 if result["status"] in ("done", "quarantined") else 1)
    except Exception as e:
        result = {
            "status": "failed",
            "records_in": len(records), "records_out": 0,
            "records_quarantined": 0, "duration_ms": 0,
            "error_message": str(e),
        }
        output = json.dumps(result)
        sys.stdout.write(output + "\n")
        sys.stdout.flush()
        sys.exit(1)


if __name__ == "__main__":
    main()