"""
isolane/tests/isolation/test_no_cross_tenant_rocksdb.py

RocksDB column family isolation tests.

Verifies that:
1. Each namespace gets its own column family
2. A handle for ns_analytics cannot address ns_finance
3. Keys written to analytics CF are not visible in finance CF
4. Offset tracking is namespace-scoped
"""

import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../.."))

from engine.rocksdb_client import RocksDBClient


@pytest.fixture
def rdb(tmp_path):
    """Fresh RocksDB client for each test (uses temp directory)."""
    client = RocksDBClient(str(tmp_path / "rocksdb"))
    yield client
    client.close()


class TestRocksDBColumnFamilyIsolation:
    """Column families are isolated by namespace."""

    def test_analytics_write_not_visible_in_finance(self, rdb):
        """Data written to analytics CF is not visible in finance CF."""
        rdb.put_str("analytics", "test_key", "analytics_value")

        # Finance CF should not see this key
        finance_val = rdb.get_str("finance", "test_key")
        assert finance_val is None, \
            f"analytics key visible in finance CF: {finance_val}"

    def test_finance_write_not_visible_in_analytics(self, rdb):
        """Data written to finance CF is not visible in analytics CF."""
        rdb.put_str("finance", "test_key", "finance_value")

        analytics_val = rdb.get_str("analytics", "test_key")
        assert analytics_val is None, \
            f"finance key visible in analytics CF: {analytics_val}"

    def test_same_key_different_values_per_namespace(self, rdb):
        """Same key in different namespaces holds different values."""
        rdb.put_str("analytics", "shared_key", "analytics_data")
        rdb.put_str("finance", "shared_key", "finance_data")

        assert rdb.get_str("analytics", "shared_key") == "analytics_data"
        assert rdb.get_str("finance", "shared_key") == "finance_data"

    def test_delete_in_analytics_does_not_affect_finance(self, rdb):
        """Deleting a key in analytics does not delete it in finance."""
        rdb.put_str("analytics", "key_to_delete", "value")
        rdb.put_str("finance", "key_to_delete", "value")

        rdb.delete("analytics", "key_to_delete")

        assert rdb.get_str("analytics", "key_to_delete") is None
        assert rdb.get_str("finance", "key_to_delete") == "value"

    def test_offset_tracking_is_namespace_scoped(self, rdb):
        """Offsets are tracked separately per namespace."""
        rdb.set_last_offset("analytics", "customers", "1712345678901-0")
        rdb.set_last_offset("finance", "transactions", "1712345678902-0")

        assert rdb.get_last_offset("analytics", "customers") == "1712345678901-0"
        assert rdb.get_last_offset("finance", "transactions") == "1712345678902-0"

        # Analytics offset for transactions should be None
        assert rdb.get_last_offset("analytics", "transactions") is None
        # Finance offset for customers should be None
        assert rdb.get_last_offset("finance", "customers") is None

    def test_schema_cache_is_namespace_scoped(self, rdb):
        """Schema cache is namespace-scoped."""
        import json
        analytics_schema = json.dumps({"customer_id": "string"}).encode()
        finance_schema   = json.dumps({"transaction_id": "string"}).encode()

        rdb.set_schema("analytics", "customers", analytics_schema)
        rdb.set_schema("finance", "transactions", finance_schema)

        assert rdb.get_schema("analytics", "customers") == analytics_schema
        assert rdb.get_schema("finance", "transactions") == finance_schema

        # Cross-namespace access returns None
        assert rdb.get_schema("finance", "customers") is None
        assert rdb.get_schema("analytics", "transactions") is None

    def test_assert_namespace_creates_isolated_cf(self, rdb):
        """assert_namespace creates isolated column families."""
        rdb.assert_namespace("analytics")
        rdb.assert_namespace("finance")
        rdb.assert_namespace("growth")

        # Each namespace should be independently writable
        rdb.put_str("analytics", "k", "analytics")
        rdb.put_str("finance", "k", "finance")
        rdb.put_str("growth", "k", "growth")

        assert rdb.get_str("analytics", "k") == "analytics"
        assert rdb.get_str("finance", "k") == "finance"
        assert rdb.get_str("growth", "k") == "growth"