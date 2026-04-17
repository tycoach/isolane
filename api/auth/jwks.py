"""
isolane/api/auth/jwks.py

JWKS (JSON Web Key Set) endpoint and key rotation logic.

Key rotation flow:
  1. Generate new RS256 key pair (make keys-rotate)
  2. New key is written to keys/private.pem + keys/public.pem
  3. Old public key is kept in keys/public.pem.prev for one TTL window
  4. During the grace window (= ACCESS_TOKEN_EXPIRE_MINUTES = 15 min),
     both old and new public keys are valid for verification
  5. After the grace window, old key is removed

The JWKS endpoint at GET /auth/jwks returns both keys during rotation
so any verifier (future microservice, external tool) can always find
the right key by kid (key ID).

For the current single-service design, verify_access_token() in jwt.py
tries the current key first, then falls back to the previous key if
present and within the grace window.
"""

import os
import json
import base64
import hashlib
from pathlib import Path
from typing import Optional
from datetime import datetime, timezone, timedelta
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend


# ── Key paths ─────────────────────────────────────────────────────

def _current_public_key_path() -> Path:
    return Path(os.environ.get("JWT_PUBLIC_KEY_PATH", "./keys/public.pem"))

def _previous_public_key_path() -> Path:
    return _current_public_key_path().with_suffix(".pem.prev")

def _rotation_timestamp_path() -> Path:
    return _current_public_key_path().with_suffix(".pem.rotated_at")


# ── Key ID (kid) ──────────────────────────────────────────────────

def _key_id(pem_bytes: bytes) -> str:
    """
    Derive a stable key ID from the public key bytes.
    Uses the first 16 hex chars of the SHA-256 digest.
    Same key always produces the same kid.
    """
    return hashlib.sha256(pem_bytes).hexdigest()[:16]


# ── JWKS construction ─────────────────────────────────────────────

def _pem_to_jwk(pem_bytes: bytes) -> Optional[dict]:
    """
    Convert a PEM-encoded RSA public key to JWK format.
    Returns None if the file doesn't exist or can't be parsed.
    """
    try:
        pub_key = serialization.load_pem_public_key(
            pem_bytes,
            backend=default_backend(),
        )
        pub_numbers = pub_key.public_key().public_numbers()

        def int_to_base64url(n: int) -> str:
            length = (n.bit_length() + 7) // 8
            raw    = n.to_bytes(length, "big")
            return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()

        return {
            "kty": "RSA",
            "use": "sig",
            "alg": "RS256",
            "kid": _key_id(pem_bytes),
            "n":   int_to_base64url(pub_numbers.n),
            "e":   int_to_base64url(pub_numbers.e),
        }
    except Exception:
        return None


def get_jwks() -> dict:
    """
    Return the current JWKS payload.
    Includes the previous key during the rotation grace window.

    Response shape:
        { "keys": [ { kty, use, alg, kid, n, e }, ... ] }
    """
    keys = []

    # Current key — always included
    current_path = _current_public_key_path()
    if current_path.exists():
        jwk = _pem_to_jwk(current_path.read_bytes())
        if jwk:
            keys.append(jwk)

    # Previous key — included only within the grace window
    prev_path = _previous_public_key_path()
    rotated_at_path = _rotation_timestamp_path()

    if prev_path.exists() and rotated_at_path.exists():
        try:
            rotated_at = datetime.fromisoformat(
                rotated_at_path.read_text().strip()
            )
            grace_window = timedelta(minutes=15)   # matches ACCESS_TOKEN_EXPIRE_MINUTES
            if datetime.now(timezone.utc) < rotated_at + grace_window:
                jwk = _pem_to_jwk(prev_path.read_bytes())
                if jwk:
                    keys.append(jwk)
            else:
                # Grace window expired — clean up old key files
                prev_path.unlink(missing_ok=True)
                rotated_at_path.unlink(missing_ok=True)
        except (ValueError, OSError):
            pass

    return {"keys": keys}


def get_public_keys_for_verification() -> list[bytes]:
    """
    Return a list of public key PEM bytes to try when verifying a token.
    The current key is always first. The previous key is appended
    if still within the grace window.

    Used by verify_access_token() in jwt.py to try keys in order
    without needing to inspect the JWT header.
    """
    keys = []

    current_path = _current_public_key_path()
    if current_path.exists():
        keys.append(current_path.read_bytes())

    prev_path       = _previous_public_key_path()
    rotated_at_path = _rotation_timestamp_path()

    if prev_path.exists() and rotated_at_path.exists():
        try:
            rotated_at   = datetime.fromisoformat(rotated_at_path.read_text().strip())
            grace_window = timedelta(minutes=15)
            if datetime.now(timezone.utc) < rotated_at + grace_window:
                keys.append(prev_path.read_bytes())
        except (ValueError, OSError):
            pass

    return keys


def rotate_keys(new_private_pem: bytes, new_public_pem: bytes) -> None:
    """
    Perform a key rotation:
      1. Move current public key to .prev
      2. Write new public key as current
      3. Write new private key as current
      4. Record rotation timestamp for grace window tracking

    Called by: make keys-rotate (which generates the new key pair
    and calls this function with the new PEM bytes).
    """
    current_pub  = _current_public_key_path()
    prev_pub     = _previous_public_key_path()
    rotated_at   = _rotation_timestamp_path()
    private_path = Path(os.environ.get(
        "JWT_PRIVATE_KEY_PATH", "./keys/private.pem"
    ))

    # Archive current public key
    if current_pub.exists():
        current_pub.rename(prev_pub)

    # Write new keys
    current_pub.write_bytes(new_public_pem)
    private_path.write_bytes(new_private_pem)
    private_path.chmod(0o600)

    # Record rotation time for grace window
    rotated_at.write_text(datetime.now(timezone.utc).isoformat())