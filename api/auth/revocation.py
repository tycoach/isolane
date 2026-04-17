"""
isolane/api/auth/revocation.py

Server-side refresh token revocation.
"""

import uuid
from datetime import datetime, timezone
from typing import Optional
import asyncpg

from api.db.connection import get_pool


# ── Prometheus counter (incremented on every revocation) ──────────
try:
    from prometheus_client import Counter
    revocation_total = Counter(
        "isolane_token_revocations_total",
        "Total refresh token revocations",
        ["triggered_by", "tenant_namespace"],
    )
    _prometheus_available = True
except ImportError:
    _prometheus_available = False


# ── Core revocation function ──────────────────────────────────────

async def revoke_session(
    session_id:       str,
    actor_user_id:    str,
    actor_email:      str,
    tenant_namespace: str,
    reason:           str,
    triggered_by:     str,              # user_logout | admin_forced | auto_expiry
    ip_address:       Optional[str] = None,
) -> None:
    """
    Revoke a refresh token by session_id and write the audit record.
    """
    pool = get_pool()

    async with pool.acquire() as conn:
        async with conn.transaction():

            # ── Fetch the token ───────────────────────────────────
            row = await conn.fetchrow(
                """
                SELECT id, user_id, tenant_id, revoked, expires_at
                FROM refresh_tokens
                WHERE session_id = $1
                """,
                uuid.UUID(session_id),
            )

            if row is None:
                raise SessionNotFoundError(
                    f"No session found for session_id: {session_id}"
                )

            if row["revoked"]:
                raise AlreadyRevokedError(
                    f"Session {session_id} is already revoked"
                )

            token_expiry = row["expires_at"]

            # ── Mark token as revoked ─────────────────────────────
            await conn.execute(
                """
                UPDATE refresh_tokens
                SET revoked    = TRUE,
                    revoked_at = NOW()
                WHERE session_id = $1
                """,
                uuid.UUID(session_id),
            )

            # ── Write immutable audit record ──────────────────────
            await conn.execute(
                """
                INSERT INTO revocation_audit (
                    user_id,
                    actor_email,
                    session_id,
                    tenant_namespace,
                    reason,
                    triggered_by,
                    ip_address,
                    token_expiry
                ) VALUES ($1, $2, $3, $4, $5, $6, $7::inet, $8)
                """,
                uuid.UUID(actor_user_id),
                actor_email,
                uuid.UUID(session_id),
                tenant_namespace,
                reason,
                triggered_by,
                ip_address,
                token_expiry,
            )

    # ── Prometheus counter (outside transaction) ──────────────────
    if _prometheus_available:
        revocation_total.labels(
            triggered_by=triggered_by,
            tenant_namespace=tenant_namespace,
        ).inc()


# ── Token validation ──────────────────────────────────────────────

async def get_valid_session(session_id: str) -> asyncpg.Record:
  
    pool = get_pool()

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT
                rt.id,
                rt.session_id,
                rt.user_id,
                rt.tenant_id,
                rt.revoked,
                rt.expires_at,
                u.email,
                u.role,
                u.is_active,
                t.namespace AS tenant_namespace
            FROM refresh_tokens rt
            JOIN users u ON u.id = rt.user_id
            LEFT JOIN tenants t ON t.id = rt.tenant_id
            WHERE rt.session_id = $1
            """,
            uuid.UUID(session_id),
        )

    if row is None:
        raise SessionNotFoundError(
            f"No session found for session_id: {session_id}"
        )

    if row["revoked"]:
        raise AlreadyRevokedError(
            f"Session {session_id} has been revoked"
        )

    if row["expires_at"] < datetime.now(timezone.utc):
        raise SessionExpiredError(
            f"Session {session_id} has expired"
        )

    if not row["is_active"]:
        raise SessionNotFoundError(
            f"User account is inactive"
        )

    return row


async def store_refresh_token(
    session_id:    str,
    user_id:       str,
    tenant_id:     Optional[str],
    expires_at:    datetime,
) -> None:
    """
    Persist a new refresh token to the database.

    """
    pool = get_pool()

    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO refresh_tokens
                (session_id, user_id, tenant_id, expires_at)
            VALUES ($1, $2, $3, $4)
            """,
            uuid.UUID(session_id),
            uuid.UUID(user_id),
            uuid.UUID(tenant_id) if tenant_id else None,
            expires_at,
        )


async def get_audit_log(
    tenant_namespace: str,
    limit: int = 50,
    offset: int = 0,
) -> list[asyncpg.Record]:
    """
    Fetch revocation audit records for a given tenant namespace.
    """
    pool = get_pool()

    async with pool.acquire() as conn:
        return await conn.fetch(
            """
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
            WHERE tenant_namespace = $1
            ORDER BY created_at DESC
            LIMIT $2 OFFSET $3
            """,
            tenant_namespace,
            limit,
            offset,
        )


# ── Custom exceptions ───────────────────────

class SessionNotFoundError(Exception):
    """Raised when the session_id does not exist in refresh_tokens."""
    pass


class AlreadyRevokedError(Exception):
    """Raised when attempting to revoke an already-revoked session."""
    pass


class SessionExpiredError(Exception):
    """Raised when the session's expires_at is in the past."""
    pass