"""
isolane/tests/unit/test_auth.py

Unit tests for JWT auth — signing, verification, expiry, revocation.
"""

import pytest
import time
import os
from pathlib import Path


def get_key_paths():
    """Find test key paths."""
    keys_dir = Path(__file__).parent.parent.parent / "keys"
    return (
        str(keys_dir / "private.pem"),
        str(keys_dir / "public.pem"),
    )


class TestJWTSigning:

    def test_create_and_verify_token_pair(self):
        """Access token can be created and verified."""
        try:
            private_path, public_path = get_key_paths()
            if not Path(private_path).exists():
                pytest.skip("JWT keys not found — run make keys first")
        except Exception:
            pytest.skip("JWT keys not accessible")

        os.environ["JWT_PRIVATE_KEY_PATH"] = private_path
        os.environ["JWT_PUBLIC_KEY_PATH"]  = public_path

        from api.auth.jwt import create_token_pair, verify_access_token

        pair = create_token_pair(
            user_id          = "test-user-id",
            email            = "test@isolane.dev",
            role             = "platform_operator",
            tenant_namespace = None,
        )

        assert pair.access_token
        assert pair.refresh_token
        assert pair.session_id

        payload = verify_access_token(pair.access_token)
        assert payload.user_id == "test-user-id"
        assert payload.email == "test@isolane.dev"
        assert payload.role == "platform_operator"
        assert payload.tenant_namespace is None
        assert payload.is_platform_operator

    def test_tenant_token_has_namespace(self):
        """Tenant token contains namespace claim."""
        try:
            private_path, public_path = get_key_paths()
            if not Path(private_path).exists():
                pytest.skip("JWT keys not found")
        except Exception:
            pytest.skip("JWT keys not accessible")

        os.environ["JWT_PRIVATE_KEY_PATH"] = private_path
        os.environ["JWT_PUBLIC_KEY_PATH"]  = public_path

        from api.auth.jwt import create_token_pair

        pair = create_token_pair(
            user_id          = "tenant-user-id",
            email            = "engineer@analytics.isolane.dev",
            role             = "engineer",
            tenant_namespace = "analytics",
        )

        from api.auth.jwt import verify_access_token
        payload = verify_access_token(pair.access_token)
        assert payload.tenant_namespace == "analytics"
        assert not payload.is_platform_operator

    def test_expired_token_raises(self):
        """Expired token raises ExpiredTokenError."""
        try:
            private_path, public_path = get_key_paths()
            if not Path(private_path).exists():
                pytest.skip("JWT keys not found")
        except Exception:
            pytest.skip("JWT keys not accessible")

        os.environ["JWT_PRIVATE_KEY_PATH"] = private_path
        os.environ["JWT_PUBLIC_KEY_PATH"]  = public_path

        import jwt as pyjwt
        from cryptography.hazmat.primitives import serialization
        from api.auth.jwt import ExpiredTokenError

        with open(private_path, "rb") as f:
            private_key = serialization.load_pem_private_key(f.read(), password=None)

        # Create token that expired 1 hour ago
        payload = {
            "sub":  "test-user",
            "exp":  int(time.time()) - 3600,
            "iat":  int(time.time()) - 4500,
            "iss":  "isolane",
            "role": "engineer",
        }
        token = pyjwt.encode(payload, private_key, algorithm="RS256")

        from api.auth.jwt import verify_access_token
        with pytest.raises(ExpiredTokenError):
            verify_access_token(token)