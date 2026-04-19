"""
isolane/worker/python/metrics.py

Per-namespace Prometheus metrics server.

Workers expose /metrics/{ns} per namespace they currently
hold work for.
"""

import os
import threading
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Optional

from prometheus_client import (
    Counter,
    Histogram,
    CollectorRegistry,
    generate_latest,
    CONTENT_TYPE_LATEST,
)

# ── Global metrics (label injected by scrape config) ─────────────
# Workers emit with pipeline= label only.
# tenant= is injected by Prometheus scrape config.

batch_processed_total = Counter(
    "ude_batch_records_processed_total",
    "Total records processed.",
    ["pipeline"],
)

quarantine_total = Counter(
    "ude_quarantine_total",
    "Total records routed to quarantine.",
    ["pipeline"],
)

batch_duration_seconds = Histogram(
    "ude_batch_duration_seconds",
    "Batch processing duration.",
    ["pipeline"],
    buckets=[0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0],
)

dlq_total = Counter(
    "ude_dlq_total",
    "Total messages routed to DLQ.",
    ["namespace"],
)

xautoclaim_cycles_total = Counter(
    "ude_xautoclaim_cycles_total",
    "Total XAUTOCLAIM cycles run.",
    ["namespace"],
)


# ── Per-namespace endpoint registry ──────────────────────────────

class NSMetricsEndpoint:
    """Tracks last-used time for a namespace metrics endpoint."""
    def __init__(self, namespace: str):
        self.namespace = namespace
        self.last_used = time.monotonic()

    def touch(self):
        self.last_used = time.monotonic()

    def idle_seconds(self) -> float:
        return time.monotonic() - self.last_used


_endpoints: dict[str, NSMetricsEndpoint] = {}
_endpoints_lock = threading.Lock()
METRICS_TTL = int(os.environ.get("METRICS_TTL_SECONDS", 60))


def touch_namespace(namespace: str) -> None:
    """Register or refresh a namespace endpoint."""
    with _endpoints_lock:
        if namespace not in _endpoints:
            _endpoints[namespace] = NSMetricsEndpoint(namespace)
            print(f"[metrics] Endpoint /metrics/{namespace} registered")
        else:
            _endpoints[namespace].touch()


def gc_stale_endpoints() -> None:
    """Remove endpoints idle longer than METRICS_TTL."""
    with _endpoints_lock:
        stale = [
            ns for ns, ep in _endpoints.items()
            if ep.idle_seconds() > METRICS_TTL
        ]
        for ns in stale:
            del _endpoints[ns]
            print(f"[metrics] Endpoint /metrics/{ns} GC'd (idle)")


def is_namespace_active(namespace: str) -> bool:
    with _endpoints_lock:
        return namespace in _endpoints


# ── HTTP handler ──────────────────────────────────────────────────

class MetricsHandler(BaseHTTPRequestHandler):
    """
    Serves /metrics/{ns} for active namespaces.
    Returns 404 for unknown namespaces.
    Returns global /metrics for worker health.
    """

    def do_GET(self):
        path = self.path

        if path == "/metrics" or path == "/metrics/":
            # Global worker metrics
            self._serve_metrics()
            return

        if path.startswith("/metrics/"):
            ns = path[len("/metrics/"):]
            if not ns:
                self._serve_metrics()
                return

            if is_namespace_active(ns):
                touch_namespace(ns)
                self._serve_metrics()
            else:
                self.send_response(404)
                self.end_headers()
                self.wfile.write(
                    f"No active metrics for namespace '{ns}'".encode()
                )
            return

        self.send_response(404)
        self.end_headers()

    def _serve_metrics(self):
        output = generate_latest()
        self.send_response(200)
        self.send_header("Content-Type", CONTENT_TYPE_LATEST)
        self.send_header("Content-Length", str(len(output)))
        self.end_headers()
        self.wfile.write(output)

    def log_message(self, format, *args):
        # Suppress default request logging — too noisy for scrape interval
        pass


# ── Server lifecycle ──────────────────────────────────────────────

_server: Optional[HTTPServer] = None
_server_thread: Optional[threading.Thread] = None
_gc_thread: Optional[threading.Thread] = None


def start_metrics_server(port: Optional[int] = None) -> None:
    """
    Start the metrics HTTP server in a background thread.
    """
    global _server, _server_thread, _gc_thread

    metrics_port = port or int(os.environ.get("WORKER_METRICS_PORT", 9090))

    _server = HTTPServer(("0.0.0.0", metrics_port), MetricsHandler)
    _server_thread = threading.Thread(
        target=_server.serve_forever,
        daemon=True,
        name="metrics-server",
    )
    _server_thread.start()
    print(f"[metrics] Server started on :{metrics_port}")

    # GC thread — sweeps stale endpoints every TTL/2 seconds
    def gc_loop():
        while True:
            time.sleep(METRICS_TTL / 2)
            gc_stale_endpoints()

    _gc_thread = threading.Thread(target=gc_loop, daemon=True, name="metrics-gc")
    _gc_thread.start()


def stop_metrics_server() -> None:
    global _server
    if _server:
        _server.shutdown()
        _server = None
        print("[metrics] Server stopped")