"""
isolane/api/routers/tenants.py

Tenant management endpoints. Platform operator only.

POST   /tenants                          — create + provision
GET    /tenants                          — list all
GET    /tenants/{namespace}              — get one
PATCH  /tenants/{namespace}             — update mutable fields
DELETE /tenants/{namespace}             — deprovision
GET    /tenants/{namespace}/provisioning — step-by-step status

Auth endpoints scoped to namespaces:
POST   /auth/login                       — login (no auth required)
POST   /auth/refresh                     — refresh access token
POST   /auth/logout                      — revoke session
GET    /auth/jwks                        — public keys for verification
POST   /{namespace}/auth/revoke          — admin force-revoke
"""

import uuid
import subprocess
import sys
from datetime import datetime, timezone, timedelta
from typing import Optional

import bcrypt
from fastapi import APIRouter, Depends, HTTPException, Request, status

from api.middleware.auth import require_platform_operator, require_auth
from api.middleware.namespace_guard import require_namespace_admin
from api.auth.jwt import TokenPayload, create_token_pair
from api.auth.jwks import get_jwks
from api.auth.revocation import (
    revoke_session,
    get_valid_session,
    store_refresh_token,
    SessionNotFoundError,
    AlreadyRevokedError,
    SessionExpiredError,
)
from api.db.connection import get_pool, get_conn, set_tenant_context
from api.db.models import Tenant, User, ProvisioningStep
from api.schemas.tenant import (
    TenantCreate,
    TenantUpdate,
    TenantResponse,
    TenantListResponse,
    ProvisioningStepResponse,
    ProvisioningStatusResponse,
)
from api.schemas.auth import (
    LoginRequest,
    TokenResponse,
    RefreshRequest,
    LogoutRequest,
    ForceRevokeRequest,
    RevocationResponse,
    JWKSResponse,
    UserCreate,
    UserResponse,
)

router = APIRouter(tags=["tenants", "auth"])

REFRESH_TOKEN_EXPIRE_HOURS = 8


# ══════════════════════════════════════════════════════════════════
# AUTH — no namespace required
# ══════════════════════════════════════════════════════════════════

@router.get("/auth/jwks", response_model=JWKSResponse)
async def jwks():
    """Public key set for RS256 token verification. No auth required."""
    return get_jwks()


@router.post("/auth/login", response_model=TokenResponse)
async def login(body: LoginRequest, request: Request):
    """
    Authenticate with email + password.
    Returns a token pair: RS256 access token (15 min) + opaque refresh token (8 hr).
    """
    pool = get_pool()

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT
                u.id, u.email, u.password_hash, u.role,
                u.is_active, u.tenant_id,
                t.namespace AS tenant_namespace
            FROM users u
            LEFT JOIN tenants t ON t.id = u.tenant_id
            WHERE u.email = $1
            """,
            body.email,
        )

    if row is None:
        _raise_invalid_credentials()

    # Verify password using bcrypt
    if not bcrypt.checkpw(
        body.password.encode(),
        row["password_hash"].encode(),
    ):
        _raise_invalid_credentials()

    if not row["is_active"]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Account is inactive. Contact your platform operator.",
        )

    # Create token pair
    pair = create_token_pair(
        user_id=str(row["id"]),
        email=row["email"],
        role=row["role"],
        tenant_namespace=row["tenant_namespace"],
    )

    # Persist refresh token
    expires_at = datetime.now(timezone.utc) + timedelta(hours=REFRESH_TOKEN_EXPIRE_HOURS)
    await store_refresh_token(
        session_id=pair.session_id,
        user_id=str(row["id"]),
        tenant_id=str(row["tenant_id"]) if row["tenant_id"] else None,
        expires_at=expires_at,
    )

    # Update last_login_at
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE users SET last_login_at = NOW() WHERE id = $1",
            row["id"],
        )

    return TokenResponse(
        access_token=pair.access_token,
        refresh_token=pair.refresh_token,
    )


@router.post("/auth/refresh", response_model=TokenResponse)
async def refresh_token(body: RefreshRequest):
    """
    Exchange a valid refresh token for a new access token.
    """
    try:
        session = await get_valid_session(body.refresh_token)
    except SessionNotFoundError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,
                            detail="Invalid refresh token")
    except AlreadyRevokedError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,
                            detail="Refresh token has been revoked")
    except SessionExpiredError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,
                            detail="Refresh token has expired")

    # Issue new access token using the same session_id
    new_access_token = create_token_pair(
        user_id=str(session["user_id"]),
        email=session["email"],
        role=session["role"],
        tenant_namespace=session["tenant_namespace"],
    ).access_token

    return TokenResponse(
        access_token=new_access_token,
        refresh_token=body.refresh_token,   # unchanged
    )


@router.post("/auth/logout", response_model=RevocationResponse)
async def logout(
    body:    LogoutRequest,
    request: Request,
    payload: TokenPayload = Depends(require_auth),
):
    """
    Revoke the current session.
    """
    try:
        session = await get_valid_session(body.refresh_token)
    except (SessionNotFoundError, AlreadyRevokedError, SessionExpiredError) as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )

    await revoke_session(
        session_id=str(session["session_id"]),
        actor_user_id=payload.user_id,
        actor_email=payload.email,
        tenant_namespace=payload.tenant_namespace or "platform",
        reason=body.reason,
        triggered_by="user_logout",
        ip_address=_get_client_ip(request),
    )

    return RevocationResponse(session_id=str(session["session_id"]))


# ══════════════════════════════════════════════════════════════════
# TENANT CRUD — platform operator only
# ══════════════════════════════════════════════════════════════════

@router.post("/tenants", response_model=TenantResponse,
             status_code=status.HTTP_201_CREATED)
async def create_tenant(
    body:    TenantCreate,
    payload: TokenPayload = Depends(require_platform_operator),
):
    """
    Create and provision a new tenant namespace.
    """
    pool = get_pool()

    # Check namespace not already taken
    async with pool.acquire() as conn:
        existing = await conn.fetchval(
            "SELECT id FROM tenants WHERE namespace = $1",
            body.namespace,
        )
        if existing:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Namespace '{body.namespace}' already exists",
            )

        # Insert tenant record
        row = await conn.fetchrow(
            """
            INSERT INTO tenants (namespace, team_name, mode)
            VALUES ($1, $2, $3)
            RETURNING id, namespace, team_name, mode, created_at, deprovisioned_at
            """,
            body.namespace,
            body.team_name,
            body.mode,
        )

    # Kick off provisioner subprocess (non-blocking)
    # The provisioner writes step status to provisioning_log
    # and can be resumed if interrupted
    _run_provisioner_async(body.namespace, body.team_name, body.mode)

    return TenantResponse(
        namespace=row["namespace"],
        team_name=row["team_name"],
        mode=row["mode"],
        is_active=True,
        created_at=row["created_at"],
        deprovisioned_at=None,
    )


@router.get("/tenants", response_model=TenantListResponse)
async def list_tenants(
    payload: TokenPayload = Depends(require_platform_operator),
    conn=Depends(get_conn),
):
    """List all tenants. Platform operator only."""
    rows = await conn.fetch(
        """
        SELECT namespace, team_name, mode, created_at, deprovisioned_at
        FROM tenants
        ORDER BY created_at DESC
        """
    )
    tenants = [
        TenantResponse(
            namespace=r["namespace"],
            team_name=r["team_name"],
            mode=r["mode"],
            is_active=r["deprovisioned_at"] is None,
            created_at=r["created_at"],
            deprovisioned_at=r["deprovisioned_at"],
        )
        for r in rows
    ]
    return TenantListResponse(tenants=tenants, total=len(tenants))


@router.get("/tenants/{namespace}", response_model=TenantResponse)
async def get_tenant(
    namespace: str,
    payload:   TokenPayload = Depends(require_platform_operator),
    conn=Depends(get_conn),
):
    """Get a single tenant by namespace."""
    row = await conn.fetchrow(
        """
        SELECT namespace, team_name, mode, created_at, deprovisioned_at
        FROM tenants WHERE namespace = $1
        """,
        namespace,
    )
    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Namespace '{namespace}' not found",
        )
    return TenantResponse(
        namespace=row["namespace"],
        team_name=row["team_name"],
        mode=row["mode"],
        is_active=row["deprovisioned_at"] is None,
        created_at=row["created_at"],
        deprovisioned_at=row["deprovisioned_at"],
    )


@router.patch("/tenants/{namespace}", response_model=TenantResponse)
async def update_tenant(
    namespace: str,
    body:      TenantUpdate,
    payload:   TokenPayload = Depends(require_platform_operator),
    conn=Depends(get_conn),
):
    """Update mutable tenant fields. Namespace cannot be changed."""
    updates = {}
    if body.team_name is not None:
        updates["team_name"] = body.team_name
    if body.mode is not None:
        updates["mode"] = body.mode

    if not updates:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No fields to update",
        )

    set_clause = ", ".join(
        f"{col} = ${i+2}" for i, col in enumerate(updates.keys())
    )
    row = await conn.fetchrow(
        f"""
        UPDATE tenants SET {set_clause}
        WHERE namespace = $1
        RETURNING namespace, team_name, mode, created_at, deprovisioned_at
        """,
        namespace, *updates.values(),
    )
    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Namespace '{namespace}' not found",
        )
    return TenantResponse(
        namespace=row["namespace"],
        team_name=row["team_name"],
        mode=row["mode"],
        is_active=row["deprovisioned_at"] is None,
        created_at=row["created_at"],
        deprovisioned_at=row["deprovisioned_at"],
    )


@router.delete("/tenants/{namespace}", status_code=status.HTTP_204_NO_CONTENT)
async def deprovision_tenant(
    namespace: str,
    payload:   TokenPayload = Depends(require_platform_operator),
    conn=Depends(get_conn),
):
    """
    Mark a tenant as deprovisioned and run the deprovisioner.
    This is irreversible from the API — re-provisioning requires
    a new namespace.
    """
    row = await conn.fetchrow(
        "SELECT id, deprovisioned_at FROM tenants WHERE namespace = $1",
        namespace,
    )
    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Namespace '{namespace}' not found",
        )
    if row["deprovisioned_at"] is not None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Namespace '{namespace}' is already deprovisioned",
        )

    await conn.execute(
        "UPDATE tenants SET deprovisioned_at = NOW() WHERE namespace = $1",
        namespace,
    )
    _run_deprovisioner_async(namespace)


@router.get(
    "/tenants/{namespace}/provisioning",
    response_model=ProvisioningStatusResponse,
)
async def get_provisioning_status(
    namespace: str,
    payload:   TokenPayload = Depends(require_platform_operator),
    conn=Depends(get_conn),
):
    """
    Return the step-by-step provisioning status for a namespace.
    """
    tenant_id = await conn.fetchval(
        "SELECT id FROM tenants WHERE namespace = $1", namespace
    )
    if tenant_id is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Namespace '{namespace}' not found",
        )

    rows = await conn.fetch(
        """
        SELECT step_name, step_order, status, input_hash,
               error_message, started_at, completed_at
        FROM provisioning_log
        WHERE tenant_id = $1
        ORDER BY step_order ASC
        """,
        tenant_id,
    )

    steps = [ProvisioningStepResponse(**dict(r)) for r in rows]
    return ProvisioningStatusResponse(
        namespace=namespace,
        steps=steps,
        complete=all(s.status == "DONE" for s in steps) and len(steps) > 0,
        has_error=any(s.status == "FAILED" for s in steps),
    )


# ══════════════════════════════════════════════════════════════════
# NAMESPACE-SCOPED AUTH — force revoke
# ══════════════════════════════════════════════════════════════════

@router.post(
    "/{namespace}/auth/revoke",
    response_model=RevocationResponse,
)
async def force_revoke(
    namespace: str,
    body:      ForceRevokeRequest,
    request:   Request,
    payload:   TokenPayload = Depends(require_namespace_admin),
):
    """
    Admin force-revoke a session within the namespace.
    """
    try:
        await revoke_session(
            session_id=body.session_id,
            actor_user_id=payload.user_id,
            actor_email=payload.email,
            tenant_namespace=namespace,
            reason=body.reason,
            triggered_by="admin_forced",
            ip_address=_get_client_ip(request),
        )
    except SessionNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Session '{body.session_id}' not found",
        )
    except AlreadyRevokedError:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Session '{body.session_id}' is already revoked",
        )

    return RevocationResponse(session_id=body.session_id)


# ══════════════════════════════════════════════════════════════════
# USER MANAGEMENT
# ══════════════════════════════════════════════════════════════════

@router.post("/users", response_model=UserResponse,
             status_code=status.HTTP_201_CREATED)
async def create_user_platform(
    body:    UserCreate,
    payload: TokenPayload = Depends(require_platform_operator),
    conn=Depends(get_conn),
):
    """
    Create a user. Platform operator only.
    """
    return await _create_user(conn, body)


@router.post("/{namespace}/users", response_model=UserResponse,
             status_code=status.HTTP_201_CREATED)
async def create_user_namespace(
    namespace: str,
    body:      UserCreate,
    payload:   TokenPayload = Depends(require_namespace_admin),
    conn=Depends(get_conn),
):
    """
    Create a user within a namespace. Admin role required.
    Can only create users with role: admin | engineer | analyst.
    Cannot create platform_operator accounts.
    """
    if body.role == "platform_operator":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Namespace admins cannot create platform_operator accounts",
        )
    if body.tenant_namespace and body.tenant_namespace != namespace:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                f"tenant_namespace '{body.tenant_namespace}' does not match "
                f"the URL namespace '{namespace}'"
            ),
        )
    body.tenant_namespace = namespace
    return await _create_user(conn, body)


# ══════════════════════════════════════════════════════════════════
# Internal helpers
# ══════════════════════════════════════════════════════════════════

async def _create_user(conn, body: UserCreate) -> UserResponse:
    """Shared user creation logic."""
    import bcrypt

    # Resolve tenant_id from namespace if provided
    tenant_id = None
    if body.tenant_namespace:
        tenant_id = await conn.fetchval(
            "SELECT id FROM tenants WHERE namespace = $1",
            body.tenant_namespace,
        )
        if tenant_id is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Namespace '{body.tenant_namespace}' not found",
            )

    # Check email uniqueness
    existing = await conn.fetchval(
        "SELECT id FROM users WHERE email = $1", body.email
    )
    if existing:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Email '{body.email}' is already registered",
        )

    # Hash password
    password_hash = bcrypt.hashpw(
        body.password.encode(), bcrypt.gensalt(rounds=12)
    ).decode()

    row = await conn.fetchrow(
        """
        INSERT INTO users
            (tenant_id, email, password_hash, role)
        VALUES ($1, $2, $3, $4)
        RETURNING id, email, role, is_active, last_login_at, created_at, tenant_id
        """,
        tenant_id,
        body.email,
        password_hash,
        body.role,
    )

    return UserResponse(
        id=row["id"],
        email=row["email"],
        role=row["role"],
        tenant_namespace=body.tenant_namespace,
        is_active=row["is_active"],
        last_login_at=row["last_login_at"],
        created_at=row["created_at"],
    )


def _get_client_ip(request: Request) -> Optional[str]:
    """Extract client IP, respecting X-Forwarded-For if behind a proxy."""
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    if request.client:
        return request.client.host
    return None


def _raise_invalid_credentials():
    """
    Always raise the same error for wrong email or wrong password.
    """
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid email or password",
        headers={"WWW-Authenticate": "Bearer"},
    )


def _run_provisioner_async(namespace: str, team: str, mode: str) -> None:
    """Fire-and-forget provisioner subprocess."""
    subprocess.Popen(
        [
            sys.executable,
            "provisioner/provision.py",
            "--ns", namespace,
            "--team", team,
            "--mode", mode,
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def _run_deprovisioner_async(namespace: str) -> None:
    """Fire-and-forget deprovisioner subprocess."""
    subprocess.Popen(
        [
            sys.executable,
            "provisioner/deprovision.py",
            "--ns", namespace,
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )