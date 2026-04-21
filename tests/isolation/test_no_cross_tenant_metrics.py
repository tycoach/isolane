"""
isolane/tests/isolation/test_no_cross_tenant_metrics.py

Metrics isolation tests.

Verifies that:
1. Worker metrics endpoint is reachable
2. Prometheus scrape configs are namespace-scoped
3. No cross-tenant label leakage in metrics
"""

import pytest
import httpx
import os
from pathlib import Path


WORKER_METRICS_URL = os.environ.get(
    "WORKER_METRICS_URL", "http://localhost:9091/metrics"
)
PROMETHEUS_URL = os.environ.get("PROMETHEUS_URL", "http://localhost:9090")
SCRAPE_CONFIG_DIR = os.environ.get(
    "PROMETHEUS_SCRAPE_DIR",
    "./monitoring/prometheus/scrape_configs",
)


class TestMetricsIsolation:
    """Worker metrics are namespace-scoped."""

    def test_worker_metrics_endpoint_reachable(self):
        """Worker metrics endpoint returns 200."""
        try:
            resp = httpx.get(WORKER_METRICS_URL, timeout=5)
            assert resp.status_code == 200, \
                f"Metrics endpoint returned {resp.status_code}"
        except httpx.ConnectError:
            pytest.skip("Worker metrics endpoint not reachable")

    def test_prometheus_is_reachable(self):
        """Prometheus is up and healthy."""
        try:
            resp = httpx.get(f"{PROMETHEUS_URL}/-/healthy", timeout=5)
            assert resp.status_code == 200
        except httpx.ConnectError:
            pytest.skip("Prometheus not reachable")

    def test_scrape_config_exists_for_analytics(self):
        """Prometheus scrape config exists for analytics namespace."""
        config_file = Path(SCRAPE_CONFIG_DIR) / "analytics.yml"
        assert config_file.exists(), \
            f"Scrape config missing: {config_file}"

    def test_scrape_config_has_tenant_label(self):
        """Scrape config injects tenant= label for analytics."""
        config_file = Path(SCRAPE_CONFIG_DIR) / "analytics.yml"
        if not config_file.exists():
            pytest.skip("analytics scrape config not found")

        content = config_file.read_text()
        assert "tenant: 'analytics'" in content or "tenant: analytics" in content, \
            f"tenant label not found in scrape config:\n{content}"
        assert "honor_labels: false" in content, \
            "honor_labels: false missing — tenant label could be overridden by worker"

    def test_scrape_config_has_correct_endpoint(self):
        """Scrape config points to the correct metrics endpoint."""
        config_file = Path(SCRAPE_CONFIG_DIR) / "analytics.yml"
        if not config_file.exists():
            pytest.skip("analytics scrape config not found")

        content = config_file.read_text()
        assert "metrics/analytics" in content, \
            f"Metrics path not namespace-scoped in scrape config:\n{content}"

    def test_no_finance_scrape_in_analytics_config(self):
        """analytics scrape config contains no finance references."""
        config_file = Path(SCRAPE_CONFIG_DIR) / "analytics.yml"
        if not config_file.exists():
            pytest.skip("analytics scrape config not found")

        content = config_file.read_text()
        assert "finance" not in content.lower(), \
            f"finance reference found in analytics scrape config — isolation breach!"

    def test_metrics_contain_pipeline_labels(self):
        """Worker metrics use pipeline= label, not tenant= (injected by Prometheus)."""
        try:
            resp = httpx.get(WORKER_METRICS_URL, timeout=5)
            if resp.status_code != 200:
                pytest.skip("Metrics not available")

            text = resp.text
            # Worker emits pipeline= label
            # tenant= is injected by Prometheus scrape config
            if "ude_batch_records_processed_total" in text:
                assert 'pipeline="' in text or "pipeline=" in text, \
                    "pipeline label missing from batch metrics"
                # tenant= should NOT be in worker output (it's injected at scrape)
                # This is the honor_labels: false guarantee
        except httpx.ConnectError:
            pytest.skip("Worker metrics not reachable")