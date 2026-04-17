"""
isolane/api/schemas/tenant.py

Pydantic request and response models for tenant operations.
"""

from __future__ import annotations
from datetime import datetime
from typing import Optional
from uuid import UUID
from pydantic import BaseModel, Field, field_validator
import re


# ── Request models ────────────────────────────────────────────────

class TenantCreate(BaseModel):
    """
    Body for POST /tenants
    Platform operator only.
    """
    namespace: str = Field(
        ...,
        min_length=2,
        max_length=31,
        description="Lowercase alphanumeric namespace. Used as prefix for all resources.",
        examples=["analytics", "finance_ops"],
    )
    team_name: str = Field(
        ...,
        min_length=2,
        max_length=100,
        description="Human-readable team name.",
        examples=["Analytics Team", "Finance Ops"],
    )
    mode: str = Field(
        default="standard",
        description="Pipeline failure mode. fail_fast stops on first dirty record.",
    )

    @field_validator("namespace")
    @classmethod
    def namespace_format(cls, v: str) -> str:
        pattern = r'^[a-z][a-z0-9_]{1,30}$'
        if not re.match(pattern, v):
            raise ValueError(
                "Namespace must start with a lowercase letter, "
                "contain only lowercase letters, digits, and underscores, "
                "and be 2-31 characters long."
            )
        return v

    @field_validator("mode")
    @classmethod
    def mode_valid(cls, v: str) -> str:
        if v not in ("standard", "fail_fast"):
            raise ValueError("mode must be 'standard' or 'fail_fast'")
        return v


class TenantUpdate(BaseModel):
    """
    Body for PATCH /tenants/{namespace}
    """
    team_name: Optional[str] = Field(
        default=None,
        min_length=2,
        max_length=100,
    )
    mode: Optional[str] = Field(default=None)

    @field_validator("mode")
    @classmethod
    def mode_valid(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and v not in ("standard", "fail_fast"):
            raise ValueError("mode must be 'standard' or 'fail_fast'")
        return v


# ── Response models ───────────────────────────────────────────────

class TenantResponse(BaseModel):
    """
    Returned by GET /tenants/{namespace} and POST /tenants.
    """
    namespace:        str
    team_name:        str
    mode:             str
    is_active:        bool
    created_at:       datetime
    deprovisioned_at: Optional[datetime] = None

    model_config = {"from_attributes": True}


class TenantListResponse(BaseModel):
    """Returned by GET /tenants (platform operator only)."""
    tenants: list[TenantResponse]
    total:   int


class ProvisioningStepResponse(BaseModel):
    """
    Single provisioning step status.
    """
    step_name:     str
    step_order:    int
    status:        str
    input_hash:    Optional[str] = None
    error_message: Optional[str] = None
    started_at:    Optional[datetime] = None
    completed_at:  Optional[datetime] = None

    model_config = {"from_attributes": True}


class ProvisioningStatusResponse(BaseModel):
    """Full provisioning status for a tenant namespace."""
    namespace: str
    steps:     list[ProvisioningStepResponse]
    complete:  bool    # True when all steps are DONE
    has_error: bool    # True when any step is FAILED