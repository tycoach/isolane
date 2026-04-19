"""
isolane/provisioner/steps/redis_group.py

Create Redis Streams consumer group and DLQ stream for a namespace.
"""

import os
from typing import Any
import redis as redis_lib


def run(ns: str, cfg: dict[str, Any]) -> None:
    r = _client()

    stream_key = f"{ns}.work-queue"
    group      = f"{ns}-consumer-group"
    dlq_key    = f"{ns}.dlq"

    try:
        r.xgroup_create(stream_key, group, id="0", mkstream=True)
        print(f"  [redis_group] Consumer group '{group}' created")
    except redis_lib.exceptions.ResponseError as e:
        if "BUSYGROUP" in str(e):
            print(f"  [redis_group] Consumer group '{group}' already exists — ok")
        else:
            raise

    if r.xlen(dlq_key) == 0:
        r.xadd(dlq_key, {"init": "provisioner", "ns": ns}, maxlen=10000)
        print(f"  [redis_group] DLQ stream '{dlq_key}' initialised")
    else:
        print(f"  [redis_group] DLQ stream '{dlq_key}' already exists — ok")

    r.close()


def _client() -> redis_lib.Redis:
    sentinel_addrs = os.environ.get("REDIS_SENTINEL_ADDRS", "")
    if sentinel_addrs:
        try:
            sentinel = redis_lib.Sentinel(
                [tuple(a.strip().split(":")) for a in sentinel_addrs.split(",")],
                socket_timeout=2,
            )
            return sentinel.master_for(
                os.environ.get("REDIS_MASTER_NAME", "mtpif-master"),
            )
        except Exception:
            pass
    return redis_lib.Redis(
        host             = os.environ.get("REDIS_HOST", "redis-master"),
        port             = int(os.environ.get("REDIS_PORT", 6379)),
        decode_responses = True,
    )