"""
isolane/worker/python/dlq.py

Dead-letter queue routing for orphaned work items.
"""

import os
import time
from typing import Any

import redis

MAX_CLAIM_ATTEMPTS = int(os.environ.get("MAX_CLAIM_ATTEMPTS", 3))


def get_claim_count(fields: dict[str, str]) -> int:
    """Extract the _claim_count field from a work item."""
    try:
        return int(fields.get("_claim_count", 0))
    except (ValueError, TypeError):
        return 0


def should_route_to_dlq(fields: dict[str, str]) -> bool:
    """Return True if this item has exceeded the max claim attempts."""
    return get_claim_count(fields) >= MAX_CLAIM_ATTEMPTS


def route_to_dlq(
    r:            redis.Redis,
    namespace:    str,
    stream_id:    str,
    fields:       dict[str, str],
    reason:       str = "max_claims_exceeded",
) -> None:
    """
    Write a work item to the namespace DLQ stream.
    """
    dlq_key = f"{namespace}.dlq"

    dlq_fields = {
        "original_stream_id": stream_id,
        "namespace":          namespace,
        "pipeline_id":        fields.get("pipeline_id", "unknown"),
        "reason":             reason,
        "claim_count":        str(get_claim_count(fields)),
        "routed_at":          str(int(time.time() * 1000)),
    }

    r.xadd(dlq_key, dlq_fields, maxlen=10000, approximate=True)

    print(
        f"[dlq] {namespace} | message {stream_id} routed to DLQ "
        f"after {get_claim_count(fields)} claims | reason: {reason}"
    )

    # Increment Prometheus counter
    try:
        from worker.python.metrics import dlq_total
        dlq_total.labels(namespace=namespace).inc()
    except Exception:
        pass


def increment_claim_count(fields: dict[str, str]) -> dict[str, str]:
    """
    Return a copy of fields with _claim_count incremented.
    """
    updated = dict(fields)
    updated["_claim_count"] = str(get_claim_count(fields) + 1)
    return updated