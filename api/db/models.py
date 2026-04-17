"""
isolane/api/db/models.py

Python dataclasses mirroring every table in metadata/init.sql.
"""

from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from uuid import UUID
import asyncpg


# ── users ─────────────────────────────────────────────────────────

@dataclass
class User:
    id:             UUID
    tenant_id:      Optional[UUID]   # None = platform operator
    email:          str
    password_hash:  str
    role:           str              # platform_operator | admin | engineer | analyst
    is_active:      bool
    last_login_at:  Optional[datetime]
    created_at:     datetime
    updated_at:     datetime

    @classmethod
    def from_record(cls, r: asyncpg.Record) -> User:
        return cls(
            id            = r["id"],
            tenant_id     = r["tenant_id"],
            email         = r["email"],
            password_hash = r["password_hash"],
            role          = r["role"],
            is_active     = r["is_active"],
            last_login_at = r["last_login_at"],
            created_at    = r["created_at"],
            updated_at    = r["updated_at"],
        )

    @property
    def is_platform_operator(self) -> bool:
        return self.tenant_id is None and self.role == "platform_operator"


# ── tenants ───────────────────────────────────────────────────────

@dataclass
class Tenant:
    id:               UUID
    namespace:        str
    team_name:        str
    mode:             str            # standard | fail_fast
    created_at:       datetime
    deprovisioned_at: Optional[datetime]

    @classmethod
    def from_record(cls, r: asyncpg.Record) -> Tenant:
        return cls(
            id               = r["id"],
            namespace        = r["namespace"],
            team_name        = r["team_name"],
            mode             = r["mode"],
            created_at       = r["created_at"],
            deprovisioned_at = r["deprovisioned_at"],
        )

    @property
    def is_active(self) -> bool:
        return self.deprovisioned_at is None


# ── pipelines ─────────────────────────────────────────────────────

@dataclass
class Pipeline:
    id:               UUID
    tenant_id:        UUID
    pipeline_id:      str
    kafka_topic:      str
    natural_key:      str
    scd_type:         int            # 1 or 2
    config_yaml:      str
    xautoclaim_ms:    int
    created_at:       datetime
    updated_at:       datetime

    @classmethod
    def from_record(cls, r: asyncpg.Record) -> Pipeline:
        return cls(
            id            = r["id"],
            tenant_id     = r["tenant_id"],
            pipeline_id   = r["pipeline_id"],
            kafka_topic   = r["kafka_topic"],
            natural_key   = r["natural_key"],
            scd_type      = r["scd_type"],
            config_yaml   = r["config_yaml"],
            xautoclaim_ms = r["xautoclaim_ms"],
            created_at    = r["created_at"],
            updated_at    = r["updated_at"],
        )


# ── provisioning_log ──────────────────────────────────────────────

@dataclass
class ProvisioningStep:
    id:            UUID
    tenant_id:     UUID
    step_name:     str
    step_order:    int
    status:        str              # PENDING | DONE | FAILED
    input_hash:    Optional[str]
    error_message: Optional[str]
    started_at:    Optional[datetime]
    completed_at:  Optional[datetime]
    created_at:    datetime

    @classmethod
    def from_record(cls, r: asyncpg.Record) -> ProvisioningStep:
        return cls(
            id            = r["id"],
            tenant_id     = r["tenant_id"],
            step_name     = r["step_name"],
            step_order    = r["step_order"],
            status        = r["status"],
            input_hash    = r["input_hash"],
            error_message = r["error_message"],
            started_at    = r["started_at"],
            completed_at  = r["completed_at"],
            created_at    = r["created_at"],
        )

    @property
    def is_done(self) -> bool:
        return self.status == "DONE"

    @property
    def is_failed(self) -> bool:
        return self.status == "FAILED"


# ── refresh_tokens ────────────────────────────────────────────────

@dataclass
class RefreshToken:
    id:         UUID
    session_id: UUID
    user_id:    UUID
    tenant_id:  Optional[UUID]      # None for platform operator sessions
    revoked:    bool
    revoked_at: Optional[datetime]
    expires_at: datetime
    created_at: datetime

    @classmethod
    def from_record(cls, r: asyncpg.Record) -> RefreshToken:
        return cls(
            id         = r["id"],
            session_id = r["session_id"],
            user_id    = r["user_id"],
            tenant_id  = r["tenant_id"],
            revoked    = r["revoked"],
            revoked_at = r["revoked_at"],
            expires_at = r["expires_at"],
            created_at = r["created_at"],
        )

    @property
    def is_valid(self) -> bool:
        from datetime import timezone
        return (
            not self.revoked
            and self.expires_at > datetime.now(timezone.utc)
        )


# ── pipeline_runs ─────────────────────────────────────────────────

@dataclass
class PipelineRun:
    id:                  UUID
    tenant_id:           UUID
    pipeline_id:         str
    batch_offset:        str
    stream_message_id:   Optional[str]   # Redis Streams message ID
    status:              str             # running | done | failed | quarantined
    records_in:          Optional[int]
    records_out:         Optional[int]
    records_quarantined: Optional[int]
    started_at:          datetime
    completed_at:        Optional[datetime]
    error_message:       Optional[str]

    @classmethod
    def from_record(cls, r: asyncpg.Record) -> PipelineRun:
        return cls(
            id                  = r["id"],
            tenant_id           = r["tenant_id"],
            pipeline_id         = r["pipeline_id"],
            batch_offset        = r["batch_offset"],
            stream_message_id   = r["stream_message_id"],
            status              = r["status"],
            records_in          = r["records_in"],
            records_out         = r["records_out"],
            records_quarantined = r["records_quarantined"],
            started_at          = r["started_at"],
            completed_at        = r["completed_at"],
            error_message       = r["error_message"],
        )


# ── revocation_audit ──────────────────────────────────────────────

@dataclass
class RevocationAudit:
    id:               UUID
    user_id:          Optional[UUID]
    actor_email:      str
    session_id:       UUID
    tenant_namespace: str
    reason:           str
    triggered_by:     str           # user_logout | admin_forced | auto_expiry
    ip_address:       Optional[str]
    token_expiry:     Optional[datetime]
    created_at:       datetime

    @classmethod
    def from_record(cls, r: asyncpg.Record) -> RevocationAudit:
        return cls(
            id               = r["id"],
            user_id          = r["user_id"],
            actor_email      = r["actor_email"],
            session_id       = r["session_id"],
            tenant_namespace = r["tenant_namespace"],
            reason           = r["reason"],
            triggered_by     = r["triggered_by"],
            ip_address       = str(r["ip_address"]) if r["ip_address"] else None,
            token_expiry     = r["token_expiry"],
            created_at       = r["created_at"],
        )