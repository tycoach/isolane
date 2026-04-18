"""
isolane/engine/edge_cases.py

Edge case detection and routing using Polars.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Optional

import polars as pl


# ── Result container ──────────────────────────────────────────────

@dataclass
class EdgeCaseResult:
    """
    Output of the edge case checker.
    """
    clean: pl.DataFrame
    dirty: list[tuple[dict, str]] = field(default_factory=list)

    @property
    def quarantine_count(self) -> int:
        return len(self.dirty)

    @property
    def clean_count(self) -> int:
        return len(self.clean)


# ── Exceptions ────────────────────────────────────────────────────

class FailFastError(Exception):
    """
    Raised in fail_fast mode when any dirty record is detected.
    The entire batch is rejected — nothing is written.
    """
    def __init__(self, reason: str, record: dict):
        super().__init__(reason)
        self.reason = reason
        self.record = record


# ── Pipeline config helper ────────────────────────────────────────

@dataclass
class EdgeCaseConfig:
    natural_key:         str
    edge_case_mode:      str    = "quarantine"   # quarantine | fail_fast
    null_threshold:      float  = 0.05
    late_arrival_window: str    = "24h"
    duplicate_window:    str    = "30m"
    nullable_fields:     set    = field(default_factory=set)
    timestamp_field:     Optional[str] = None

    @classmethod
    def from_pipeline_config(cls, config: dict) -> "EdgeCaseConfig":
        fields_cfg      = config.get("fields", {})
        nullable_fields = {
            name for name, cfg in fields_cfg.items()
            if cfg.get("nullable", True)
        }
        # Detect timestamp field for late arrival checking
        timestamp_field = None
        for name, cfg in fields_cfg.items():
            if cfg.get("type") == "datetime":
                timestamp_field = name
                break

        return cls(
            natural_key         = config.get("natural_key", "id"),
            edge_case_mode      = config.get("edge_case_mode", "quarantine"),
            null_threshold      = float(config.get("null_threshold", 0.05)),
            late_arrival_window = config.get("late_arrival_window", "24h"),
            duplicate_window    = config.get("duplicate_window", "30m"),
            nullable_fields     = nullable_fields,
            timestamp_field     = timestamp_field,
        )

    def parse_window(self, window_str: str) -> timedelta:
        """Parse a window string like '24h', '30m', '7d' to timedelta."""
        unit  = window_str[-1].lower()
        value = int(window_str[:-1])
        if unit == "m":
            return timedelta(minutes=value)
        elif unit == "h":
            return timedelta(hours=value)
        elif unit == "d":
            return timedelta(days=value)
        else:
            raise ValueError(f"Unknown window unit: {unit}. Use m, h, or d.")


# ── Core edge case checks ─────────────────────────────────────────

def check_nulls(
    df:     pl.DataFrame,
    config: EdgeCaseConfig,
) -> tuple[pl.DataFrame, list[tuple[dict, str]]]:
    """
    Check each record for null values in non-nullable fields.
    Records exceeding the null threshold are quarantined.
    """
    non_nullable = [
        col for col in df.columns
        if col not in config.nullable_fields
        and col in df.columns
    ]

    if not non_nullable:
        return df, []

    dirty_records = []
    clean_mask    = pl.Series([True] * len(df))

    for col in non_nullable:
        null_mask = df[col].is_null()
        null_count = null_mask.sum()

        if null_count == 0:
            continue

        null_fraction = null_count / len(df)

        if null_fraction > config.null_threshold:
            # Mark individual null records as dirty
            for i, is_null in enumerate(null_mask.to_list()):
                if is_null:
                    record = df.row(i, named=True)
                    reason = (
                        f"Null value in non-nullable field '{col}' "
                        f"(threshold: {config.null_threshold})"
                    )
                    if config.edge_case_mode == "fail_fast":
                        raise FailFastError(reason, record)
                    dirty_records.append((record, reason))
                    clean_mask[i] = False

    clean_df = df.filter(clean_mask)
    return clean_df, dirty_records


def check_duplicates(
    df:     pl.DataFrame,
    config: EdgeCaseConfig,
    seen_keys: set[str],
) -> tuple[pl.DataFrame, list[tuple[dict, str]]]:
    """
    Remove duplicate records by natural_key.
 
    """
    if config.natural_key not in df.columns:
        return df, []

    dirty_records = []
    clean_rows    = []

    for row in df.iter_rows(named=True):
        key = str(row.get(config.natural_key, ""))
        if key in seen_keys:
            reason = (
                f"Duplicate natural_key '{config.natural_key}': '{key}' "
                f"within window {config.duplicate_window}"
            )
            if config.edge_case_mode == "fail_fast":
                raise FailFastError(reason, row)
            dirty_records.append((row, reason))
        else:
            seen_keys.add(key)
            clean_rows.append(row)

    if clean_rows:
        clean_df = pl.DataFrame(clean_rows, infer_schema_length=len(clean_rows))
    else:
        clean_df = df.clear()

    return clean_df, dirty_records


def check_late_arrivals(
    df:     pl.DataFrame,
    config: EdgeCaseConfig,
) -> tuple[pl.DataFrame, list[tuple[dict, str]]]:
    """
    Quarantine records whose timestamp is older than late_arrival_window.
    Only runs if a datetime field is configured.
    """
    if config.timestamp_field is None:
        return df, []

    if config.timestamp_field not in df.columns:
        return df, []

    window    = config.parse_window(config.late_arrival_window)
    cutoff    = datetime.now(timezone.utc) - window
    dirty     = []
    clean_rows = []

    for row in df.iter_rows(named=True):
        ts = row.get(config.timestamp_field)
        if ts is None:
            clean_rows.append(row)
            continue

        # Normalise to UTC-aware datetime
        if isinstance(ts, datetime):
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            if ts < cutoff:
                reason = (
                    f"Late arrival: record timestamp {ts.isoformat()} "
                    f"is older than window {config.late_arrival_window}"
                )
                if config.edge_case_mode == "fail_fast":
                    raise FailFastError(reason, row)
                dirty.append((row, reason))
                continue

        clean_rows.append(row)

    if clean_rows:
        clean_df = pl.DataFrame(clean_rows, infer_schema_length=len(clean_rows))
    else:
        clean_df = df.clear()

    return clean_df, dirty


# ── Main entry point ──────────────────────────────────────────────

def run_edge_case_checks(
    df:        pl.DataFrame,
    config:    EdgeCaseConfig,
    seen_keys: Optional[set] = None,
) -> EdgeCaseResult:
    """
    Run all edge case checks in order:
      1. Null check
      2. Duplicate check
      3. Late arrival check
    """
    if seen_keys is None:
        seen_keys = set()

    all_dirty: list[tuple[dict, str]] = []

    # 1. Null check
    df, null_dirty = check_nulls(df, config)
    all_dirty.extend(null_dirty)

    # 2. Duplicate check
    df, dupe_dirty = check_duplicates(df, config, seen_keys)
    all_dirty.extend(dupe_dirty)

    # 3. Late arrival check
    df, late_dirty = check_late_arrivals(df, config)
    all_dirty.extend(late_dirty)

    return EdgeCaseResult(clean=df, dirty=all_dirty)