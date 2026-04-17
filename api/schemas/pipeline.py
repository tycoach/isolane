"""
isolane/api/schemas/pipeline.py

Pydantic request and response models for pipeline operations.
"""

from __future__ import annotations
from datetime import datetime
from typing import Optional, Any
from uuid import UUID
from pydantic import BaseModel, Field, field_validator, model_validator


# ── Field config sub-models ───────────────────────────────────────

class FieldConfig(BaseModel):
    """
    Type definition for a single pipeline field.

    """
    type:     str  = Field(..., description="string | integer | float | boolean | datetime")
    nullable: bool = Field(default=True)

    @field_validator("type")
    @classmethod
    def type_valid(cls, v: str) -> str:
        allowed = {"string", "integer", "float", "boolean", "datetime"}
        if v not in allowed:
            raise ValueError(f"Field type must be one of: {', '.join(sorted(allowed))}")
        return v


class DBTConfig(BaseModel):
    """
    dbt integration block within a pipeline config.
    """
    snapshot: Optional[str] = Field(
        default=None,
        description="dbt snapshot name. Required when scd_type=2.",
    )
    model:    Optional[str] = Field(
        default=None,
        description="dbt model name for this pipeline.",
    )
    tests:    list[dict[str, Any]] = Field(
        default_factory=list,
        description="dbt test definitions (not_null, unique, accepted_values).",
    )


# ── Request models ────────────────────────────────────────────────

class PipelineCreate(BaseModel):
    """
    Body for POST /{namespace}/pipelines
    Engineer role or above required.
    """
    pipeline_id:       str = Field(
        ...,
        min_length=1,
        max_length=100,
        description="Unique identifier within the namespace.",
        examples=["customers", "transactions"],
    )
    kafka_topic:       str = Field(
        ...,
        description="Must follow {namespace}.{dataset} convention.",
        examples=["analytics.customers"],
    )
    natural_key:       str = Field(
        ...,
        description="Column used as the unique record identifier for SCD.",
        examples=["customer_id"],
    )
    scd_type:          int = Field(
        ...,
        description="SCD type: 1 (overwrite) or 2 (track history via dbt snapshot).",
    )
    edge_case_mode:    str = Field(
        default="quarantine",
        description="quarantine (route dirty records aside) or fail_fast (hard stop).",
    )
    null_threshold:    float = Field(
        default=0.05,
        ge=0.0,
        le=1.0,
        description="Fraction of null values allowed before quarantine is triggered.",
    )
    late_arrival_window: str = Field(
        default="24h",
        description="Window for accepting late-arriving records. e.g. 24h, 6h.",
    )
    duplicate_window:  str = Field(
        default="30m",
        description="Window for deduplication checks. e.g. 30m, 1h.",
    )
    xautoclaim_ms:     int = Field(
        default=45000,
        ge=5000,
        description=(
            "Redis Streams XAUTOCLAIM timeout in milliseconds. "
            "Set to p99 batch processing time + 30s."
        ),
    )
    dbt:    Optional[DBTConfig]             = None
    fields: dict[str, FieldConfig]          = Field(
        default_factory=dict,
        description="Schema definition for pipeline fields.",
    )

    @field_validator("scd_type")
    @classmethod
    def scd_type_valid(cls, v: int) -> int:
        if v not in (1, 2):
            raise ValueError("scd_type must be 1 or 2")
        return v

    @field_validator("edge_case_mode")
    @classmethod
    def edge_case_mode_valid(cls, v: str) -> str:
        if v not in ("quarantine", "fail_fast"):
            raise ValueError("edge_case_mode must be 'quarantine' or 'fail_fast'")
        return v

    @field_validator("kafka_topic")
    @classmethod
    def topic_has_dot(cls, v: str) -> str:
        if "." not in v:
            raise ValueError(
                "kafka_topic must follow {namespace}.{dataset} convention "
                f"— got '{v}' which contains no dot separator"
            )
        return v

    @model_validator(mode="after")
    def snapshot_required_for_scd2(self) -> PipelineCreate:
        if self.scd_type == 2:
            if self.dbt is None or self.dbt.snapshot is None:
                raise ValueError(
                    "scd_type=2 requires dbt.snapshot to be specified. "
                    "dbt snapshots handle SCD Type 2 record open/close."
                )
        return self


class PipelineUpdate(BaseModel):
    """
    Body for PATCH /{namespace}/pipelines/{pipeline_id}
    """
    edge_case_mode:      Optional[str]   = None
    null_threshold:      Optional[float] = Field(default=None, ge=0.0, le=1.0)
    late_arrival_window: Optional[str]   = None
    duplicate_window:    Optional[str]   = None
    xautoclaim_ms:       Optional[int]   = Field(default=None, ge=5000)
    dbt:                 Optional[DBTConfig]           = None
    fields:              Optional[dict[str, FieldConfig]] = None


# ── Response models ───────────────────────────────────────────────

class PipelineResponse(BaseModel):
    """
    Returned by GET /{namespace}/pipelines/{pipeline_id}
    and POST /{namespace}/pipelines.
    """
    pipeline_id:         str
    kafka_topic:         str
    natural_key:         str
    scd_type:            int
    edge_case_mode:      str
    null_threshold:      float
    late_arrival_window: str
    duplicate_window:    str
    xautoclaim_ms:       int
    dbt:                 Optional[DBTConfig]             = None
    fields:              dict[str, FieldConfig]          = Field(default_factory=dict)
    created_at:          datetime
    updated_at:          datetime

    model_config = {"from_attributes": True}


class PipelineListResponse(BaseModel):
    """Returned by GET /{namespace}/pipelines"""
    pipelines: list[PipelineResponse]
    total:     int


class PipelineRunResponse(BaseModel):
    """
    Returned by GET /{namespace}/pipelines/{pipeline_id}/runs
    Shows run history for a specific pipeline.
    """
    id:                  UUID
    pipeline_id:         str
    batch_offset:        str
    stream_message_id:   Optional[str]  = None
    status:              str
    records_in:          Optional[int]  = None
    records_out:         Optional[int]  = None
    records_quarantined: Optional[int]  = None
    started_at:          datetime
    completed_at:        Optional[datetime] = None
    error_message:       Optional[str]  = None

    model_config = {"from_attributes": True}


class PipelineRunListResponse(BaseModel):
    """Returned by GET /{namespace}/pipelines/{pipeline_id}/runs"""
    runs:  list[PipelineRunResponse]
    total: int