"""
isolane/provisioner/steps/redis_group.py

Create Redis Streams consumer group and DLQ stream for a namespace.
"""

import os
from typing import Any
import redis


def run(ns: str, cfg: dict[str, Any]) -> None:
    r = _client()

    stream_key = f"{ns}.work-queue"
    group      = f"{ns}-consumer-group"
    dlq_key    = f"{ns}.dlq"

    # Create consumer group (MKSTREAM creates the stream if absent)
    try:
        r.xgroup_create(stream_key, group, id="0", mkstream=True)
        print(f"  [redis_group] Consumer group '{group}' created on '{stream_key}'")
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" in str(e):
            print(f"  [redis_group] Consumer group '{group}' already exists — ok")
        else:
            raise

    # Ensure DLQ stream exists with a sentinel entry
    # MAXLEN 1 keeps the stream alive without accumulating data
    existing_dlq = r.xlen(dlq_key)
    if existing_dlq == 0:
        r.xadd(dlq_key, {"init": "provisioner", "ns": ns}, maxlen=10000)
        print(f"  [redis_group] DLQ stream '{dlq_key}' initialised")
    else:
        print(f"  [redis_group] DLQ stream '{dlq_key}' already exists — ok")

    r.close()


def _client() -> redis.Redis:
    sentinel_addrs = os.environ.get("REDIS_SENTINEL_ADDRS", "")
    if sentinel_addrs:
        sentinel = redis.Sentinel(
            [tuple(addr.split(":")) for addr in sentinel_addrs.split(",")],
            socket_timeout=2,
        )
        return sentinel.master_for(
            os.environ.get("REDIS_MASTER_NAME", "mtpif-master"),
        )
    return redis.Redis(
        host=os.environ.get("REDIS_HOST", "localhost"),
        port=int(os.environ.get("REDIS_PORT", 6379)),
        decode_responses=True,
    )