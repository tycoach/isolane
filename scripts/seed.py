"""
isolane/scripts/seed.py

Push synthetic test data for a namespace into Redis Streams.

Usage:
    python scripts/seed.py --ns analytics
    python scripts/seed.py --ns finance --pipeline transactions --count 50

Each record is pushed as a work item onto {ns}.work-queue
in the format the worker expects:
  pipeline_id   — which pipeline to process
  batch_offset  — synthetic batch reference
  records_json  — JSON list of records
"""

import argparse
import json
import os
import time
import uuid
from datetime import datetime, timezone, timedelta
from typing import Any

import redis


# ── Synthetic data generators ─────────────────────────────────────

def make_customers(count: int) -> list[dict[str, Any]]:
    records = []
    for i in range(count):
        cid = f"CUST-{i+1:04d}"
        records.append({
            "customer_id": cid,
            "email":       f"user{i+1}@example.com",
            "city":        ["Lagos", "Abuja", "London", "New York", "Berlin"][i % 5],
            "created_at":  (
                datetime.now(timezone.utc) - timedelta(days=i)
            ).isoformat(),
        })
    # Add one duplicate to test deduplication
    if count > 1:
        records.append({
            "customer_id": "CUST-0001",   # duplicate
            "email":       "duplicate@example.com",
            "city":        "Lagos",
            "created_at":  datetime.now(timezone.utc).isoformat(),
        })
    return records


def make_transactions(count: int) -> list[dict[str, Any]]:
    records = []
    statuses = ["pending", "settled", "failed"]
    for i in range(count):
        records.append({
            "transaction_id": f"TXN-{uuid.uuid4().hex[:8].upper()}",
            "amount":         round(10.0 + i * 7.5, 2),
            "status":         statuses[i % 3],
            "created_at":     datetime.now(timezone.utc).isoformat(),
        })
    return records


def make_events(count: int) -> list[dict[str, Any]]:
    event_types = ["page_view", "click", "signup", "purchase", "logout"]
    records = []
    for i in range(count):
        records.append({
            "event_id":   f"EVT-{uuid.uuid4().hex[:8].upper()}",
            "event_type": event_types[i % len(event_types)],
            "user_id":    f"USR-{(i % 20) + 1:04d}",
            "occurred_at": datetime.now(timezone.utc).isoformat(),
        })
    return records


GENERATORS = {
    "customers":    make_customers,
    "transactions": make_transactions,
    "events":       make_events,
}

# Default pipeline per namespace
NS_DEFAULT_PIPELINE = {
    "analytics": "customers",
    "finance":   "transactions",
    "growth":    "events",
}


# ── Redis push ────────────────────────────────────────────────────

def push_work_item(
    r:           redis.Redis,
    namespace:   str,
    pipeline_id: str,
    records:     list[dict],
) -> str:
    """Push one work item onto the namespace work queue."""
    stream_key   = f"{namespace}.work-queue"
    batch_offset = f"seed-{int(time.time() * 1000)}"

    fields = {
        "pipeline_id":   pipeline_id,
        "batch_offset":  batch_offset,
        "records_json":  json.dumps(records, default=str),
        "_claim_count":  "0",
    }

    stream_id = r.xadd(stream_key, fields)
    return stream_id


def get_redis() -> redis.Redis:
    sentinel_addrs = os.environ.get("REDIS_SENTINEL_ADDRS", "")
    if sentinel_addrs:
        sentinel = redis.Sentinel(
            [tuple(a.strip().split(":")) for a in sentinel_addrs.split(",")],
            socket_timeout=2,
        )
        return sentinel.master_for(
            os.environ.get("REDIS_MASTER_NAME", "mtpif-master")
        )
    return redis.Redis(
        host             = os.environ.get("REDIS_HOST", "redis-master"),
        port             = int(os.environ.get("REDIS_PORT", 6379)),
        decode_responses = True,
    )


# ── Entry point ───────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Push synthetic test data")
    parser.add_argument("--ns",       required=True, help="Namespace e.g. analytics")
    parser.add_argument("--pipeline", default=None,  help="Pipeline ID (default: ns default)")
    parser.add_argument("--count",    type=int, default=20, help="Number of records")
    args = parser.parse_args()

    pipeline_id = args.pipeline or NS_DEFAULT_PIPELINE.get(args.ns, "customers")
    generator   = GENERATORS.get(pipeline_id)

    if not generator:
        print(f"Error: no generator for pipeline '{pipeline_id}'")
        print(f"Available: {', '.join(GENERATORS.keys())}")
        raise SystemExit(1)

    records = generator(args.count)
    r       = get_redis()

    print(f"\nSeeding {args.ns}.{pipeline_id}")
    print(f"  Records:    {len(records)}")

    stream_id = push_work_item(r, args.ns, pipeline_id, records)

    print(f"  Stream ID:  {stream_id}")
    print(f"  Stream key: {args.ns}.work-queue")
    print(f"\nCheck the work queue:")
    print(f"  make shell-redis")
    print(f"  XLEN {args.ns}.work-queue")
    print(f"\nOr run the worker:")
    print(f"  docker compose up -d engine-worker")


if __name__ == "__main__":
    main()