"""
isolane/worker/python/claimer.py

XAUTOCLAIM poller — one thread per active namespace.

When a worker crashes mid-batch, the work item stays in the
Pending Entries List (PEL) with no ACK. XAUTOCLAIM re-delivers
entries that have been idle longer than the claim timeout.

Claim timeout is configured per pipeline (xautoclaim_ms).
Default is p99 batch processing time + 30s.

If an entry has been claimed more than MAX_CLAIM_ATTEMPTS times
it is routed to the DLQ instead of being re-delivered.
"""

import os
import threading
import time
from typing import Callable, Optional

import redis

from worker.python.dlq import (
    should_route_to_dlq,
    route_to_dlq,
    increment_claim_count,
)
from worker.python.metrics import xautoclaim_cycles_total

DEFAULT_CLAIM_TIMEOUT_MS = int(
    os.environ.get("CLAIM_TIMEOUT_MS", 45000)
)
CLAIM_POLL_INTERVAL_S = DEFAULT_CLAIM_TIMEOUT_MS / 2 / 1000


class NamespaceClaimer:
    """
    XAUTOCLAIM poller for a single namespace.
    """

    def __init__(
        self,
        r:          redis.Redis,
        namespace:  str,
        worker_id:  str,
        dispatch_fn: Callable[[str, str, dict], None],
        claim_timeout_ms: int = DEFAULT_CLAIM_TIMEOUT_MS,
    ):
        self.r                = r
        self.namespace        = namespace
        self.worker_id        = worker_id
        self.dispatch_fn      = dispatch_fn
        self.claim_timeout_ms = claim_timeout_ms
        self.stream_key       = f"{namespace}.work-queue"
        self.group            = f"{namespace}-consumer-group"
        self._stop_event      = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._run,
            daemon=True,
            name=f"claimer-{self.namespace}",
        )
        self._thread.start()
        print(f"[claimer] {self.namespace} — started (timeout={self.claim_timeout_ms}ms)")

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)
        print(f"[claimer] {self.namespace} — stopped")

    def _run(self) -> None:
        poll_interval = self.claim_timeout_ms / 2 / 1000  # half the claim window
        while not self._stop_event.is_set():
            try:
                self._run_one_cycle()
                xautoclaim_cycles_total.labels(namespace=self.namespace).inc()
            except Exception as e:
                print(f"[claimer] {self.namespace} — error: {e}")
            self._stop_event.wait(timeout=poll_interval)

    def _run_one_cycle(self) -> None:
        """
        Run one XAUTOCLAIM cycle.
        Claims up to 10 idle entries per cycle.
        """
        try:
            result = self.r.xautoclaim(
                name         = self.stream_key,
                groupname    = self.group,
                consumername = self.worker_id,
                min_idle_time = self.claim_timeout_ms,
                start_id     = "0-0",
                count        = 10,
            )
        except redis.exceptions.ResponseError as e:
            # Stream or group doesn't exist yet — not an error
            if "NOGROUP" in str(e) or "no such key" in str(e).lower():
                return
            raise

        # result is (next_start_id, messages, deleted_ids)
        if not result or len(result) < 2:
            return

        messages = result[1]
        if not messages:
            return

        for stream_id, fields in messages:
            # Decode bytes if needed
            if isinstance(stream_id, bytes):
                stream_id = stream_id.decode()
            fields = {
                (k.decode() if isinstance(k, bytes) else k):
                (v.decode() if isinstance(v, bytes) else v)
                for k, v in fields.items()
            }

            if should_route_to_dlq(fields):
                route_to_dlq(self.r, self.namespace, stream_id, fields)
                # ACK to remove from PEL
                self.r.xack(self.stream_key, self.group, stream_id)
                continue

            # Increment claim count and re-dispatch
            updated_fields = increment_claim_count(fields)
            print(
                f"[claimer] {self.namespace} | reclaiming {stream_id} "
                f"(attempt {updated_fields['_claim_count']})"
            )

            try:
                self.dispatch_fn(
                    self.namespace,
                    stream_id,
                    updated_fields,
                )
            except Exception as e:
                print(
                    f"[claimer] {self.namespace} | dispatch failed "
                    f"for {stream_id}: {e}"
                )


# ── Claimer registry ──────────────────────────────────────────────
# One claimer per namespace, shared across the worker process.

_claimers: dict[str, NamespaceClaimer] = {}
_claimers_lock = threading.Lock()


def ensure_claimer(
    r:           redis.Redis,
    namespace:   str,
    worker_id:   str,
    dispatch_fn: Callable,
    claim_timeout_ms: int = DEFAULT_CLAIM_TIMEOUT_MS,
) -> None:
    """
    Start a claimer for this namespace if one isn't already running.
    """
    with _claimers_lock:
        if namespace not in _claimers:
            claimer = NamespaceClaimer(
                r=r,
                namespace=namespace,
                worker_id=worker_id,
                dispatch_fn=dispatch_fn,
                claim_timeout_ms=claim_timeout_ms,
            )
            claimer.start()
            _claimers[namespace] = claimer


def stop_all_claimers() -> None:
    """Stop all running claimers. Called on worker shutdown."""
    with _claimers_lock:
        for ns, claimer in _claimers.items():
            claimer.stop()
        _claimers.clear()
    print("[claimer] All claimers stopped")