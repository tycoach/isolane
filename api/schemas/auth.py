"""
isolane/api/schemas/auth.py

Pydantic request and response models for auth operations.

"""

from __future__ import annotations
from datetime import datetime
from typing import Optional
from uuid import UUID
from pydantic import BaseModel, Field, EmailStr, field_validator


# ── Login ─────────────────────────────────────────────────────────

class LoginRequest(BaseModel):
    """Body for POST /auth/login"""
    email:    EmailStr = Field(..., description="User email address")
    password: str      = Field(..., min_length=1, description="User password")


class TokenResponse(BaseModel):
    """
    Returned by POST /auth/login and POST /auth/refresh.
    """
    access_token:  str = Field(..., description="RS256 JWT. Valid for 15 minutes.")
    refresh_token: str = Field(..., description="Opaque UUID. Valid for 8 hours.")
    token_type:    str = Field(default="bearer")
    expires_in:    int = Field(default=900, description="Access token lifetime in seconds.")


# ── Token refresh ─────────────────────────────────────────────────

class RefreshRequest(BaseModel):
    """Body for POST /auth/refresh"""
    refresh_token: str = Field(
        ...,
        description="The opaque refresh token issued at login.",
    )


# ── Logout / revocation ───────────────────────────────────────────

class LogoutRequest(BaseModel):
    """
    Body for POST /auth/logout
    """
    refresh_token: str = Field(..., description="The refresh token to revoke.")
    reason:        str = Field(
        default="User-initiated logout",
        max_length=200,
    )


class ForceRevokeRequest(BaseModel):
    """
    Body for POST /{namespace}/auth/revoke
    """
    session_id: str  = Field(..., description="The session_id to revoke.")
    reason:     str  = Field(
        ...,
        min_length=5,
        max_length=200,
        description="Reason for forced revocation. Required for audit trail.",
    )


class RevocationResponse(BaseModel):
    """Returned by logout and force-revoke endpoints."""
    revoked:    bool     = True
    session_id: str
    message:    str      = "Session revoked. Access token expires within 15 minutes."


# ── User management ───────────────────────────────────────────────

class UserCreate(BaseModel):
    """
    Body for POST /users (platform operator creates any user)
    or POST /{namespace}/users (admin creates user in their tenant).
    """
    email:     EmailStr = Field(..., description="Must be unique across all users.")
    password:  str      = Field(
        ...,
        min_length=12,
        description="Minimum 12 characters.",
    )
    role:      str      = Field(
        ...,
        description="platform_operator | admin | engineer | analyst",
    )
    tenant_namespace: Optional[str] = Field(
        default=None,
        description=(
            "Namespace to assign this user to. "
            "Omit for platform_operator role. "
            "Required for all other roles."
        ),
    )

    @field_validator("role")
    @classmethod
    def role_valid(cls, v: str) -> str:
        allowed = {"platform_operator", "admin", "engineer", "analyst"}
        if v not in allowed:
            raise ValueError(
                f"role must be one of: {', '.join(sorted(allowed))}"
            )
        return v

    @field_validator("password")
    @classmethod
    def password_strength(cls, v: str) -> str:
        if len(v) < 12:
            raise ValueError("Password must be at least 12 characters")
        has_upper  = any(c.isupper() for c in v)
        has_lower  = any(c.islower() for c in v)
        has_digit  = any(c.isdigit() for c in v)
        if not (has_upper and has_lower and has_digit):
            raise ValueError(
                "Password must contain at least one uppercase letter, "
                "one lowercase letter, and one digit"
            )
        return v


class UserResponse(BaseModel):
    """
    Returned by user creation and GET /users endpoints.
    Never exposes password_hash.
    """
    id:               UUID
    email:            str
    role:             str
    tenant_namespace: Optional[str] = None
    is_active:        bool
    last_login_at:    Optional[datetime] = None
    created_at:       datetime

    model_config = {"from_attributes": True}


class UserListResponse(BaseModel):
    """Returned by GET /{namespace}/users"""
    users: list[UserResponse]
    total: int


# ── JWKS ──────────────────────────────────────────────────────────

class JWKResponse(BaseModel):
    """Single JWK entry in the JWKS response."""
    kty: str   # Key type — always "RSA"
    use: str   # Key use — always "sig"
    alg: str   # Algorithm — always "RS256"
    kid: str   # Key ID derived from SHA-256 of public key
    n:   str   # RSA modulus (base64url)
    e:   str   # RSA public exponent (base64url)


class JWKSResponse(BaseModel):
    """Returned by GET /auth/jwks"""
    keys: list[JWKResponse]


# ── Audit log ─────────────────────────────────────────────────────

class RevocationAuditResponse(BaseModel):
    """Single revocation audit record."""
    id:               UUID
    actor_email:      str
    session_id:       UUID
    tenant_namespace: str
    reason:           str
    triggered_by:     str
    ip_address:       Optional[str]      = None
    token_expiry:     Optional[datetime] = None
    created_at:       datetime

    model_config = {"from_attributes": True}


class AuditLogResponse(BaseModel):
    """Returned by GET /audit or GET /{namespace}/audit"""
    records: list[RevocationAuditResponse]
    total:   int