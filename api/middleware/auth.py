"""
isolane/api/middleware/auth.py

Bearer token extraction and validation middleware.

Responsibilities:
  - Extract the Bearer token from the Authorization header
  - Verify the RS256 signature via jwt.py
  - Handle the JWKS grace window (tries current key, then previous)
  - Attach the decoded TokenPayload to the request state
  - Return 401 with a clear error for every failure mode
"""

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

import jwt as pyjwt

from api.auth.jwt import (
    verify_access_token,
    TokenPayload,
    ExpiredTokenError,
    InvalidAccessTokenError,
)
from api.auth.jwks import get_public_keys_for_verification


# ── Bearer scheme — FastAPI security helper ───────────────────────
# auto_error=False so we can return a custom 401 instead of FastAPI's
_bearer = HTTPBearer(auto_error=False)


# ── Core dependency ───────────────────────────────────────────────

async def require_auth(
    credentials: HTTPAuthorizationCredentials = Depends(_bearer),
) -> TokenPayload:
    """
    FastAPI dependency. Validates the Bearer token and returns
    """
    if credentials is None:
        _raise_401("Missing Authorization header")

    if credentials.scheme.lower() != "bearer":
        _raise_401("Authorization scheme must be Bearer")

    token = credentials.credentials

    # Try current key, then previous key (JWKS rotation grace window)
    public_keys = get_public_keys_for_verification()
    last_error: Exception = InvalidAccessTokenError("No public keys available")

    for public_key_pem in public_keys:
        try:
            payload = _verify_with_key(token, public_key_pem)
            return payload
        except ExpiredTokenError as e:
            # Expiry is definitive — no point trying the other key
            _raise_401(str(e), expired=True)
        except InvalidAccessTokenError as e:
            last_error = e
            continue

    # All keys exhausted
    _raise_401(str(last_error))


# ── Role-specific dependencies ────────────────────────────────────

async def require_platform_operator(
    payload: TokenPayload = Depends(require_auth),
) -> TokenPayload:
    """
    Extends require_auth. Rejects non-platform-operator tokens.
    """
    if not payload.is_platform_operator:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="This endpoint requires platform operator access",
        )
    return payload


async def require_engineer_or_above(
    payload: TokenPayload = Depends(require_auth),
) -> TokenPayload:
    """
    Rejects analyst-role tokens.
    """
    if payload.role == "analyst":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="This action requires engineer role or above",
        )
    return payload


async def require_admin_or_above(
    payload: TokenPayload = Depends(require_auth),
) -> TokenPayload:
    """
    Allows only admin and platform_operator roles.
    """
    if payload.role not in ("admin", "platform_operator"):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="This action requires admin role or above",
        )
    return payload


# ── Internal helpers ──────────────────────────────────────────────

def _verify_with_key(token: str, public_key_pem: bytes) -> TokenPayload:
    """
    Attempt to verify token with a specific public key.
    """
    try:
        raw = pyjwt.decode(
            token,
            public_key_pem.decode(),
            algorithms=["RS256"],
            issuer="isolane",
        )
        return TokenPayload(raw)
    except pyjwt.ExpiredSignatureError:
        raise ExpiredTokenError("Access token has expired")
    except (pyjwt.DecodeError, pyjwt.InvalidTokenError) as e:
        raise InvalidAccessTokenError(str(e))


def _raise_401(detail: str, expired: bool = False) -> None:
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail=detail,
        headers={
            "WWW-Authenticate": "Bearer",
            "X-Token-Expired": "true" if expired else "false",
        },
    )