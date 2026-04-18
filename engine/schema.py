"""
isolane/engine/schema.py

Schema inference and type coercion using Polars.
"""

import json
from dataclasses import dataclass
from typing import Any

import polars as pl

from engine.rocksdb_client import RocksDBClient


# ── Type mapping ──────────────────────────────────────────────────
# Pipeline config field types → Polars dtypes
TYPE_MAP: dict[str, pl.DataType] = {
    "string":   pl.Utf8,
    "integer":  pl.Int64,
    "float":    pl.Float64,
    "boolean":  pl.Boolean,
    "datetime": pl.Datetime,
}


# ── Exceptions ────────────────────────────────────────────────────

class SchemaDriftError(Exception):
    """
    Raised when the incoming batch schema differs from the cached schema.
    """
    def __init__(self, message: str, diffs: list[str]):
        super().__init__(message)
        self.diffs = diffs


class SchemaValidationError(Exception):
    """
    Raised when a batch cannot be coerced to the declared field types.
    """
    pass


# ── Schema snapshot ───────────────────────────────────────────────

@dataclass
class SchemaSnapshot:
    """Serialisable representation of a pipeline's expected schema."""
    columns: dict[str, str]   # column name → type string

    def to_json(self) -> bytes:
        return json.dumps(self.columns, sort_keys=True).encode()

    @classmethod
    def from_json(cls, data: bytes) -> "SchemaSnapshot":
        return cls(columns=json.loads(data.decode()))

    @classmethod
    def from_field_config(cls, fields: dict[str, dict]) -> "SchemaSnapshot":
        """Build snapshot from pipeline YAML field config."""
        return cls(columns={
            name: cfg.get("type", "string")
            for name, cfg in fields.items()
        })

    def diff(self, other: "SchemaSnapshot") -> list[str]:
        """
        Return a list of human-readable differences between this
        snapshot and another. 
        """
        diffs = []
        all_cols = set(self.columns) | set(other.columns)

        for col in sorted(all_cols):
            if col not in self.columns:
                diffs.append(f"New column: '{col}' ({other.columns[col]})")
            elif col not in other.columns:
                diffs.append(f"Removed column: '{col}'")
            elif self.columns[col] != other.columns[col]:
                diffs.append(
                    f"Type change: '{col}' "
                    f"{self.columns[col]} → {other.columns[col]}"
                )
        return diffs


# ── Core functions ────────────────────────────────────────────────

def infer_schema(records: list[dict[str, Any]]) -> SchemaSnapshot:
    """
    Infer the schema of a batch by inspecting all records.
    Returns the union of all columns seen across all records.
    """
    columns: dict[str, str] = {}

    for record in records:
        for key, value in record.items():
            if key in columns:
                continue
            if isinstance(value, bool):
                columns[key] = "boolean"
            elif isinstance(value, int):
                columns[key] = "integer"
            elif isinstance(value, float):
                columns[key] = "float"
            else:
                columns[key] = "string"

    return SchemaSnapshot(columns=columns)


def coerce_to_dataframe(
    records:     list[dict[str, Any]],
    field_config: dict[str, dict],
) -> pl.DataFrame:
    """
    Convert a list of raw records to a typed Polars DataFrame.
    """
    if not records:
        return pl.DataFrame()

    try:
        df = pl.DataFrame(records, infer_schema_length=len(records))
    except Exception as e:
        raise SchemaValidationError(
            f"Failed to construct DataFrame from batch: {e}"
        )

    # Coerce declared fields to their configured types
    cast_exprs = []
    for col_name, field_def in field_config.items():
        if col_name not in df.columns:
            continue
        type_str = field_def.get("type", "string")
        polars_type = TYPE_MAP.get(type_str, pl.Utf8)

        try:
            cast_exprs.append(
                pl.col(col_name).cast(polars_type, strict=False)
            )
        except Exception:
            pass  # individual cast failures caught per-record in edge_cases.py

    if cast_exprs:
        df = df.with_columns(cast_exprs)

    return df


def check_drift(
    namespace:   str,
    pipeline_id: str,
    incoming:    SchemaSnapshot,
    rdb:         RocksDBClient,
) -> None:
    """
    Compare the incoming schema against the cached schema.
    """
    cached_bytes = rdb.get_schema(namespace, pipeline_id)

    if cached_bytes is None:
        # First batch — cache this schema as the baseline
        rdb.set_schema(namespace, pipeline_id, incoming.to_json())
        return

    cached = SchemaSnapshot.from_json(cached_bytes)
    diffs  = cached.diff(incoming)

    if diffs:
        raise SchemaDriftError(
            f"Schema drift detected for {namespace}.{pipeline_id}: "
            f"{len(diffs)} change(s)",
            diffs=diffs,
        )

    # Schema matches — update cache (handles new nullable columns gracefully)
    rdb.set_schema(namespace, pipeline_id, incoming.to_json())


def validate_batch(
    records:      list[dict[str, Any]],
    field_config: dict[str, dict],
    namespace:    str,
    pipeline_id:  str,
    rdb:          RocksDBClient,
) -> pl.DataFrame:
    """
    Full validation pipeline for an incoming batch
    """
    incoming = infer_schema(records)
    check_drift(namespace, pipeline_id, incoming, rdb)
    return coerce_to_dataframe(records, field_config)