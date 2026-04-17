"""
isolane/api/routers/audit.py

Revocation audit log endpoints.
Records are written exclusively by revocation.py.
"""

from fastapi import APIRouter, Depends, Query
from typing import Optional

import asyncpg

from api.middleware.auth import require_platform_operator
from api.middleware.namespace_guard import require_namespace_admin
from api.auth.jwt import TokenPayload
from api.auth.revocation import get_audit_log
from api.schemas.auth import AuditLogResponse, RevocationAuditResponse
from api.db.connection import get_pool

router = APIRouter(tags=["audit"])


# ── Platform operator: all namespaces ─────────────────────────────

@router.get("/audit", response_model=AuditLogResponse)
async def get_global_audit_log(
    namespace: Optional[str] = Query(
        default=None,
        description="Filter by namespace. Omit to see all.",
    ),
    triggered_by: Optional[str] = Query(
        default=None,
        description="Filter by trigger type: user_logout | admin_forced | auto_expiry",
    ),
    limit:  int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    payload: TokenPayload = Depends(require_platform_operator),
):
    """
    Global audit log. Platform operator only.
    Optionally filter by namespace or trigger type.
    """
    pool = get_pool()

    async with pool.acquire() as conn:
        # Build query dynamically based on filters
        conditions = []
        params     = []
        idx        = 1

        if namespace:
            conditions.append(f"tenant_namespace = ${idx}")
            params.append(namespace)
            idx += 1

        if triggered_by:
            conditions.append(f"triggered_by = ${idx}")
            params.append(triggered_by)
            idx += 1

        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

        records = await conn.fetch(
            f"""
            SELECT
                id,
                actor_email,
                session_id,
                tenant_namespace,
                reason,
                triggered_by,
                ip_address::text,
                token_expiry,
                created_at
            FROM revocation_audit
            {where}
            ORDER BY created_at DESC
            LIMIT ${idx} OFFSET ${idx + 1}
            """,
            *params, limit, offset,
        )

        total = await conn.fetchval(
            f"SELECT COUNT(*) FROM revocation_audit {where}",
            *params,
        )

    return AuditLogResponse(
        records=[RevocationAuditResponse(**dict(r)) for r in records],
        total=total,
    )


# ── Namespace-scoped: own namespace only ──────────────────────────

@router.get("/{namespace}/audit", response_model=AuditLogResponse)
async def get_namespace_audit_log(
    namespace: str,
    triggered_by: Optional[str] = Query(default=None),
    limit:  int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    payload: TokenPayload = Depends(require_namespace_admin),
):
    """
    Audit log scoped to a single namespace.
    Admin or platform_operator only.
    """
    pool = get_pool()

    async with pool.acquire() as conn:
        params     = [namespace]
        conditions = ["tenant_namespace = $1"]
        idx        = 2

        if triggered_by:
            conditions.append(f"triggered_by = ${idx}")
            params.append(triggered_by)
            idx += 1

        where = f"WHERE {' AND '.join(conditions)}"

        records = await conn.fetch(
            f"""
            SELECT
                id,
                actor_email,
                session_id,
                tenant_namespace,
                reason,
                triggered_by,
                ip_address::text,
                token_expiry,
                created_at
            FROM revocation_audit
            {where}
            ORDER BY created_at DESC
            LIMIT ${idx} OFFSET ${idx + 1}
            """,
            *params, limit, offset,
        )

        total = await conn.fetchval(
            f"SELECT COUNT(*) FROM revocation_audit {where}",
            *params,
        )

    return AuditLogResponse(
        records=[RevocationAuditResponse(**dict(r)) for r in records],
        total=total,
    )
