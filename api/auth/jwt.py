"""
isolane/api/auth/jwt.py

RS256 JWT implementation.
- Access tokens: 15-minute expiry, signed with private key
- Payload carries: user_id, email, role, tenant_namespace, session_id
- Verification uses the public key only — private key never leaves this module
- JWKS rotation is handled in jwks.py
"""

import os
import uuid
from datetime import datetime, timezone, timedelta
from typing import Optional
from pathlib import Path

import jwt as pyjwt
from jwt.exceptions import (
    ExpiredSignatureError,
    InvalidTokenError,
    DecodeError,
)


# ── Key loading ───────────────────────────────────────────────────

def _load_private_key() -> str:
    path = os.environ.get("JWT_PRIVATE_KEY_PATH", "./keys/private.pem")
    return Path(path).read_text()


def _load_public_key() -> str:
    path = os.environ.get("JWT_PUBLIC_KEY_PATH", "./keys/public.pem")
    return Path(path).read_text()


# ── Constants ─────────────────────────────────────────────────────

ACCESS_TOKEN_EXPIRE_MINUTES = 15
ALGORITHM = "RS256"
ISSUER    = "isolane"


# ── Token models ──────────────────────────────────────────────────

class TokenPayload:
    """
    Typed wrapper around the decoded JWT payload.
    All fields are validated at decode time.
    """
    def __init__(self, raw: dict):
        self.user_id:          str           = raw["sub"]
        self.email:            str           = raw["email"]
        self.role:             str           = raw["role"]
        self.tenant_namespace: Optional[str] = raw.get("ns")   # None = platform operator
        self.session_id:       str           = raw["sid"]
        self.issued_at:        datetime      = datetime.fromtimestamp(raw["iat"], tz=timezone.utc)
        self.expires_at:       datetime      = datetime.fromtimestamp(raw["exp"], tz=timezone.utc)

    @property
    def is_platform_operator(self) -> bool:
        return self.tenant_namespace is None and self.role == "platform_operator"

    @property
    def is_expired(self) -> bool:
        return datetime.now(timezone.utc) > self.expires_at


class TokenPair:
    """Returned by create_token_pair() — both tokens together."""
    def __init__(self, access_token: str, refresh_token: str, session_id: str):
        self.access_token  = access_token
        self.refresh_token = refresh_token
        self.session_id    = session_id


# ── Token creation ────────────────────────────────────────────────

def create_access_token(
    user_id:          str,
    email:            str,
    role:             str,
    session_id:       str,
    tenant_namespace: Optional[str] = None,
) -> str:
    """
    Create a signed RS256 access token.
    Expires in ACCESS_TOKEN_EXPIRE_MINUTES (15).

    tenant_namespace is None for platform operators.
    """
    now     = datetime.now(timezone.utc)
    expires = now + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)

    payload = {
        "iss": ISSUER,
        "sub": user_id,
        "email": email,
        "role": role,
        "sid": session_id,
        "iat": int(now.timestamp()),
        "exp": int(expires.timestamp()),
    }

    # Only include namespace claim for tenant users
    if tenant_namespace is not None:
        payload["ns"] = tenant_namespace

    return pyjwt.encode(
        payload,
        _load_private_key(),
        algorithm=ALGORITHM,
    )


def create_token_pair(
    user_id:          str,
    email:            str,
    role:             str,
    tenant_namespace: Optional[str] = None,
) -> TokenPair:
    """
    Create a matched access + refresh token pair.
    Both share the same session_id — this links them for revocation.

    The refresh token is an opaque UUID stored server-side in
    refresh_tokens table. It is NOT a JWT.
    """
    session_id    = str(uuid.uuid4())
    access_token  = create_access_token(
        user_id=user_id,
        email=email,
        role=role,
        session_id=session_id,
        tenant_namespace=tenant_namespace,
    )
    refresh_token = str(uuid.uuid4())   # opaque, stored in DB

    return TokenPair(
        access_token=access_token,
        refresh_token=refresh_token,
        session_id=session_id,
    )


# ── Token verification ────────────────────────────────────────────

def verify_access_token(token: str) -> TokenPayload:
    """
    Verify and decode an RS256 access token.

    Raises:
        ExpiredTokenError   — token has expired (15-minute window passed)
        InvalidTokenError   — signature invalid, wrong issuer, malformed
    """
    try:
        raw = pyjwt.decode(
            token,
            _load_public_key(),
            algorithms=[ALGORITHM],
            issuer=ISSUER,
        )
        return TokenPayload(raw)

    except ExpiredSignatureError:
        raise ExpiredTokenError("Access token has expired")
    except (DecodeError, InvalidTokenError) as e:
        raise InvalidAccessTokenError(f"Access token is invalid: {e}")


def decode_token_unverified(token: str) -> dict:
    """
    Decode without verification — used only during JWKS key rotation
    grace window to read the kid header and determine which key to use.
    Never use this for authorization decisions.
    """
    return pyjwt.decode(
        token,
        options={"verify_signature": False},
        algorithms=[ALGORITHM],
    )


# ── Custom exceptions ─────────────────────────────────────────────

class ExpiredTokenError(Exception):
    """Raised when a token's exp claim is in the past."""
    pass


class InvalidAccessTokenError(Exception):
    """Raised when a token fails signature or structural validation."""
    pass