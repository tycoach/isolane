"""
isolane/tests/unit/test_engine.py

Unit tests for the engine layer.
Tests schema inference, edge cases, and batch logic in isolation.
"""

import pytest
from datetime import datetime, timezone, timedelta

from engine.schema import infer_schema, SchemaSnapshot, coerce_to_dataframe
from engine.edge_cases import (
    EdgeCaseConfig, EdgeCaseResult,
    run_edge_case_checks, FailFastError,
    check_nulls, check_duplicates, check_late_arrivals,
)
from engine.rocksdb_client import RocksDBClient


# ── Schema tests ──────────────────────────────────────────────────

class TestSchemaInference:

    def test_infers_string_type(self):
        records = [{"name": "Alice"}]
        schema = infer_schema(records)
        assert schema.columns["name"] == "string"

    def test_infers_integer_type(self):
        records = [{"age": 30}]
        schema = infer_schema(records)
        assert schema.columns["age"] == "integer"

    def test_infers_float_type(self):
        records = [{"amount": 9.99}]
        schema = infer_schema(records)
        assert schema.columns["amount"] == "float"

    def test_infers_boolean_type(self):
        records = [{"active": True}]
        schema = infer_schema(records)
        assert schema.columns["active"] == "boolean"

    def test_empty_records_returns_empty_schema(self):
        schema = infer_schema([])
        assert schema.columns == {}

    def test_union_of_columns_across_records(self):
        records = [{"a": 1}, {"b": 2}]
        schema = infer_schema(records)
        assert "a" in schema.columns
        assert "b" in schema.columns


class TestSchemaDrift:

    def test_no_drift_returns_empty_diff(self):
        s1 = SchemaSnapshot({"id": "string", "name": "string"})
        s2 = SchemaSnapshot({"id": "string", "name": "string"})
        assert s1.diff(s2) == []

    def test_new_column_detected(self):
        s1 = SchemaSnapshot({"id": "string"})
        s2 = SchemaSnapshot({"id": "string", "email": "string"})
        diffs = s1.diff(s2)
        assert len(diffs) == 1
        assert "email" in diffs[0]

    def test_removed_column_detected(self):
        s1 = SchemaSnapshot({"id": "string", "email": "string"})
        s2 = SchemaSnapshot({"id": "string"})
        diffs = s1.diff(s2)
        assert len(diffs) == 1
        assert "email" in diffs[0]

    def test_type_change_detected(self):
        s1 = SchemaSnapshot({"amount": "string"})
        s2 = SchemaSnapshot({"amount": "float"})
        diffs = s1.diff(s2)
        assert len(diffs) == 1
        assert "amount" in diffs[0]


class TestCoerceToDataframe:

    def test_basic_coercion(self):
        records = [{"id": "1", "amount": "9.99"}]
        fields  = {"amount": {"type": "float"}}
        df = coerce_to_dataframe(records, fields)
        assert len(df) == 1

    def test_empty_records_returns_empty_df(self):
        df = coerce_to_dataframe([], {})
        assert len(df) == 0


# ── Edge case tests ───────────────────────────────────────────────

class TestNullChecks:

    def test_null_in_non_nullable_field_quarantined(self):
        import polars as pl
        df = pl.DataFrame([
            {"customer_id": None, "email": "a@b.com"},
            {"customer_id": "001", "email": "c@d.com"},
        ])
        config = EdgeCaseConfig(
            natural_key    = "customer_id",
            null_threshold = 0.0,
        )
        clean, dirty = check_nulls(df, config)
        assert len(dirty) == 1
        assert "customer_id" in dirty[0][1]

    def test_null_in_nullable_field_passes(self):
        import polars as pl
        df = pl.DataFrame([{"customer_id": "001", "email": None}])
        config = EdgeCaseConfig(
            natural_key    = "customer_id",
            nullable_fields = {"email"},
        )
        clean, dirty = check_nulls(df, config)
        assert len(dirty) == 0

    def test_fail_fast_raises_on_null(self):
        import polars as pl
        df = pl.DataFrame([{"customer_id": None}])
        config = EdgeCaseConfig(
            natural_key    = "customer_id",
            edge_case_mode = "fail_fast",
            null_threshold = 0.0,
        )
        with pytest.raises(FailFastError):
            check_nulls(df, config)


class TestDuplicateChecks:

    def test_duplicate_natural_key_quarantined(self):
        import polars as pl
        df = pl.DataFrame([
            {"customer_id": "001", "email": "a@b.com"},
            {"customer_id": "001", "email": "c@d.com"},  # duplicate
        ])
        config = EdgeCaseConfig(natural_key="customer_id")
        seen   = set()
        clean, dirty = check_duplicates(df, config, seen)
        assert len(dirty) == 1
        assert len(clean) == 1

    def test_unique_records_all_pass(self):
        import polars as pl
        df = pl.DataFrame([
            {"customer_id": "001"},
            {"customer_id": "002"},
            {"customer_id": "003"},
        ])
        config = EdgeCaseConfig(natural_key="customer_id")
        seen   = set()
        clean, dirty = check_duplicates(df, config, seen)
        assert len(dirty) == 0
        assert len(clean) == 3


class TestEdgeCaseIntegration:

    def test_full_pipeline_catches_duplicate(self):
        import polars as pl
        records = [
            {"customer_id": "001", "email": "a@b.com", "created_at": "2026-01-01"},
            {"customer_id": "002", "email": "b@b.com", "created_at": "2026-01-01"},
            {"customer_id": "001", "email": "dup@b.com", "created_at": "2026-01-01"},
        ]
        df     = pl.DataFrame(records)
        config = EdgeCaseConfig(natural_key="customer_id")
        result = run_edge_case_checks(df, config)

        assert result.clean_count == 2
        assert result.quarantine_count == 1

    def test_full_pipeline_all_clean(self):
        import polars as pl
        records = [
            {"customer_id": "001", "email": "a@b.com"},
            {"customer_id": "002", "email": "b@b.com"},
        ]
        df     = pl.DataFrame(records)
        config = EdgeCaseConfig(natural_key="customer_id")
        result = run_edge_case_checks(df, config)

        assert result.clean_count == 2
        assert result.quarantine_count == 0


# ── RocksDB unit tests ────────────────────────────────────────────

class TestRocksDBClient:

    @pytest.fixture
    def rdb(self, tmp_path):
        client = RocksDBClient(str(tmp_path / "rocksdb"))
        yield client
        client.close()

    def test_put_and_get(self, rdb):
        rdb.put_str("analytics", "key1", "value1")
        assert rdb.get_str("analytics", "key1") == "value1"

    def test_get_missing_key_returns_none(self, rdb):
        assert rdb.get_str("analytics", "missing") is None

    def test_delete_removes_key(self, rdb):
        rdb.put_str("analytics", "key", "value")
        rdb.delete("analytics", "key")
        assert rdb.get_str("analytics", "key") is None

    def test_namespace_isolation(self, rdb):
        rdb.put_str("analytics", "k", "a")
        rdb.put_str("finance", "k", "f")
        assert rdb.get_str("analytics", "k") == "a"
        assert rdb.get_str("finance", "k") == "f"

    def test_offset_tracking(self, rdb):
        rdb.set_last_offset("analytics", "customers", "1234-0")
        assert rdb.get_last_offset("analytics", "customers") == "1234-0"
        assert rdb.get_last_offset("analytics", "transactions") is None