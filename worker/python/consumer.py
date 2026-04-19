"""
isolane/worker/python/consumer.py

Redis Streams XREADGROUP consumer — the Python reference implementation.

This is the reference implementation that the Go worker replaces
in Phase 3. Both implement identical behaviour:
  - XREADGROUP loop across all provisioned namespaces
  - Per-namespace XAUTOCLAIM via claimer.py
  - Dispatch to engine via dispatcher.py
  - ACK on success, no ACK on failure

Namespace discovery:
  On startup, scans Redis for *.work-queue streams and registers
  a claimer for each. New namespaces provisioned after startup
  are detected on the next scan cycle (every 60s).
"""

import os
import signal
import sys
import time
import threading
from typing import Optional

import redis

from worker.python.claimer import ensure_claimer, stop_all_claimers
from worker.python.dispatcher import dispatch
from worker.python.metrics import start_metrics_server, stop_metrics_server


WORKER_ID    = os.environ.get("WORKER_ID", f"worker-{os.getpid()}")
BLOCK_MS     = 2000    # XREADGROUP block timeout — 2 seconds
BATCH_COUNT  = 10      # messages per XREADGROUP call
NS_SCAN_INTERVAL = 60  # seconds between namespace rediscovery


def get_redis_client() -> redis.Redis:
    """Connect to Redis master directly or via Sentinel."""
    sentinel_addrs = os.environ.get("REDIS_SENTINEL_ADDRS", "")
    if sentinel_addrs:
        try:
            sentinel = redis.Sentinel(
                [
                    tuple(addr.strip().split(":"))
                    for addr in sentinel_addrs.split(",")
                    if addr.strip()
                ],
                socket_timeout        = 2,
                socket_connect_timeout = 2,
            )
            client = sentinel.master_for(
                os.environ.get("REDIS_MASTER_NAME", "mtpif-master"),
                socket_timeout         = 10,
                socket_connect_timeout = 5,
                decode_responses       = True,
            )
            # Test the connection
            client.ping()
            print("[worker] Connected via Redis Sentinel")
            return client
        except Exception as e:
            print(f"[worker] Sentinel connection failed ({e}) — falling back to direct")

    # Direct connection fallback
    client = redis.Redis(
        host                   = os.environ.get("REDIS_HOST", "redis-master"),
        port                   = int(os.environ.get("REDIS_PORT", 6379)),
        decode_responses       = True,
        socket_timeout         = 10,
        socket_connect_timeout = 5,
        retry_on_timeout       = True,
    )
    client.ping()
    print("[worker] Connected directly to Redis master")
    return client


def discover_namespaces(r: redis.Redis) -> list[str]:
    """
    Scan Redis for *.work-queue streams.
    Returns the namespace portion of each key found.
    """
    namespaces = []
    cursor     = 0

    while True:
        cursor, keys = r.scan(cursor, match="*.work-queue", count=100)
        for key in keys:
            # Decode bytes if decode_responses=False
            if isinstance(key, bytes):
                key = key.decode()
            ns = key.replace(".work-queue", "")
            if ns not in namespaces:
                namespaces.append(ns)
        if cursor == 0:
            break

    return namespaces


def build_stream_args(namespaces: list[str]) -> list[str]:
    """
    Build the streams argument for XREADGROUP.
    Format: [stream1, stream2, ..., ">", ">", ...]
    ">" means deliver only new, undelivered messages.
    """
    streams = [f"{ns}.work-queue" for ns in namespaces]
    ids     = [">"] * len(namespaces)
    return streams + ids


class Worker:
    """
    Stateless worker — no tenant affinity.
    Reads from all namespace streams in a single XREADGROUP call.
    """

    def __init__(self):
        self.r             = get_redis_client()
        self.namespaces    = []
        self._running      = False
        self._stop_event   = threading.Event()

    def start(self) -> None:
        """Start the worker — blocks until stop() is called."""
        self._running = True
        print(f"[worker] {WORKER_ID} starting")

        # Start metrics server
        start_metrics_server()

        # Initial namespace discovery
        self._refresh_namespaces()

        # Background namespace refresh thread
        refresh_thread = threading.Thread(
            target=self._ns_refresh_loop,
            daemon=True,
            name="ns-refresh",
        )
        refresh_thread.start()

        # Main consumer loop
        try:
            self._consume_loop()
        finally:
            self._shutdown()

    def stop(self) -> None:
        """Signal the worker to stop gracefully."""
        print(f"[worker] {WORKER_ID} stopping...")
        self._stop_event.set()
        self._running = False

    def _consume_loop(self) -> None:
        """
        Main XREADGROUP loop.
        Reads from all namespace streams simultaneously.
        Dispatches each message to the engine.
        ACKs on success, leaves unACK'd on failure for XAUTOCLAIM.
        """
        while not self._stop_event.is_set():
            if not self.namespaces:
                print("[worker] No namespaces found — waiting...")
                self._stop_event.wait(timeout=10)
                continue

            stream_args = build_stream_args(self.namespaces)

            try:
                # XREADGROUP across all namespace streams
                # Block for BLOCK_MS ms — returns None on timeout
                results = self.r.xreadgroup(
                    groupname    = self._group_for_ns(self.namespaces[0]),
                    consumername = WORKER_ID,
                    streams      = dict(zip(
                        [f"{ns}.work-queue" for ns in self.namespaces],
                        [">"] * len(self.namespaces),
                    )),
                    count = BATCH_COUNT,
                    block = BLOCK_MS,
                )
            except redis.exceptions.ResponseError as e:
                if "NOGROUP" in str(e):
                    self._refresh_namespaces()
                    continue
                print(f"[worker] XREADGROUP error: {e}")
                self._stop_event.wait(timeout=1)
                continue
            except (redis.exceptions.ConnectionError,
                    redis.exceptions.TimeoutError) as e:
                print(f"[worker] Redis connection error: {e} — reconnecting")
                self._stop_event.wait(timeout=3)
                try:
                    self.r = get_redis_client()
                except Exception as re:
                    print(f"[worker] Reconnect failed: {re}")
                    self._stop_event.wait(timeout=5)
                continue

            if not results:
                continue  # timeout — no messages

            for stream_key, messages in results:
                ns = stream_key.replace(".work-queue", "")

                # Ensure claimer is running for this namespace
                ensure_claimer(
                    r           = self.r,
                    namespace   = ns,
                    worker_id   = WORKER_ID,
                    dispatch_fn = dispatch,
                )

                for stream_id, fields in messages:
                    self._handle_message(ns, stream_key, stream_id, fields)

    def _handle_message(
        self,
        namespace:  str,
        stream_key: str,
        stream_id:  str,
        fields:     dict,
    ) -> None:
        """Process one message and ACK on success."""
        result = dispatch(namespace, stream_id, fields)

        if result is not None and result.status in ("done", "quarantined"):
            # ACK — remove from PEL
            self.r.xack(stream_key, self._group_for_ns(namespace), stream_id)
        else:
            # Do NOT ACK — XAUTOCLAIM will re-deliver after timeout
            print(
                f"[worker] {namespace} | {stream_id} NOT ACK'd — "
                f"will be reclaimed after timeout"
            )

    def _refresh_namespaces(self) -> None:
        """Scan Redis for active namespace streams."""
        found = discover_namespaces(self.r)
        if found != self.namespaces:
            added   = set(found) - set(self.namespaces)
            removed = set(self.namespaces) - set(found)
            if added:
                print(f"[worker] New namespaces: {sorted(added)}")
            if removed:
                print(f"[worker] Removed namespaces: {sorted(removed)}")
            self.namespaces = found
            print(f"[worker] Active namespaces: {self.namespaces}")

    def _ns_refresh_loop(self) -> None:
        """Background thread that refreshes namespaces periodically."""
        while not self._stop_event.is_set():
            self._stop_event.wait(timeout=NS_SCAN_INTERVAL)
            if not self._stop_event.is_set():
                self._refresh_namespaces()

    def _shutdown(self) -> None:
        """Clean up on shutdown."""
        stop_all_claimers()
        stop_metrics_server()
        print(f"[worker] {WORKER_ID} shutdown complete")

    @staticmethod
    def _group_for_ns(namespace: str) -> str:
        return f"{namespace}-consumer-group"


# ── Entry point ───────────────────────────────────────────────────

def main():
    worker = Worker()

    # Graceful shutdown on SIGTERM / SIGINT
    def handle_signal(signum, frame):
        worker.stop()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    worker.start()


if __name__ == "__main__":
    main()