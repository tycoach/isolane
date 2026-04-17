"""
isolane/api/middleware/namespace_guard.py

Namespace claim validation — the second layer of access control.

auth.py proves the token is valid and not expired.
namespace_guard.py proves the token is allowed to access
the specific namespace in the URL path.

The guard enforces three rules:
  1. Platform operators can access any namespace
  2. Tenant users can only access their own namespace
  3. A valid token for namespace "analytics" cannot read
     or write to namespace "finance" — ever
"""

from typing import Callable
from fastapi import Depends, HTTPException, Path, status

from api.auth.jwt import TokenPayload
from api.middleware.auth import require_auth


# ── Primary guard dependency ──────────────────────────────────────

def guard_namespace(namespace: str = Path(...)) -> Callable:
    """
    Returns a FastAPI dependency that validates the namespace
    """
    async def _guard(
        payload: TokenPayload = Depends(require_auth),
    ) -> TokenPayload:
        _check_namespace_access(payload, namespace)
        return payload

    return _guard


# ── Flat dependency (for routes with namespace in path) ───────────

async def require_namespace_match(
    namespace: str = Path(...),
    payload: TokenPayload = Depends(require_auth),
) -> TokenPayload:
    """
    Flat dependency alternative to guard_namespace().
    """
    _check_namespace_access(payload, namespace)
    return payload


# ── Admin guard — namespace scoped ───────────────────────────────

async def require_namespace_admin(
    namespace: str = Path(...),
    payload: TokenPayload = Depends(require_auth),
) -> TokenPayload:
    """
    Requires admin or platform_operator role AND namespace match.
    """
    _check_namespace_access(payload, namespace)

    if payload.role not in ("admin", "platform_operator"):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=(
                f"Admin role required for namespace '{namespace}'. "
                f"Your role is '{payload.role}'."
            ),
        )
    return payload


# ── Engineer guard — namespace scoped ────────────────────────────

async def require_namespace_engineer(
    namespace: str = Path(...),
    payload: TokenPayload = Depends(require_auth),
) -> TokenPayload:
    """
    Requires engineer or above AND namespace match.
    """
    _check_namespace_access(payload, namespace)

    if payload.role == "analyst":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=(
                f"Engineer role or above required for namespace '{namespace}'. "
                f"Your role is '{payload.role}'."
            ),
        )
    return payload


# ── Internal enforcement logic ────────────────────────────────────

def _check_namespace_access(payload: TokenPayload, namespace: str) -> None:
    """
    Core namespace validation logic.
    """
    # Platform operators have no namespace restriction
    if payload.is_platform_operator:
        return

    # Tenant user — namespace claim must match exactly
    if payload.tenant_namespace != namespace:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=(
                f"Access denied: your token is not authorised "
                f"for namespace '{namespace}'"
            ),
        )

    # Sanity check — should never happen if provisioner ran correctly
    if payload.tenant_namespace is None:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Token is missing namespace claim",
        )


# ── Utility: extract namespace from validated payload ─────────────

def get_namespace(payload: TokenPayload) -> str:
    """
    Extract the effective namespace from a validated payload.
    """
    if payload.tenant_namespace is None:
        raise ValueError(
            "get_namespace() called on a platform operator payload "
            "without a URL namespace context. Use the path parameter directly."
        )
    return payload.tenant_namespace