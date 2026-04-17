"""
isolane/scripts/create_admin.py

Create or update a platform operator user.
"""

import argparse
import asyncio
import os
import sys

import bcrypt
import asyncpg


async def run(email: str, password: str) -> None:
    # Validate password strength
    if len(password) < 12:
        print("Error: Password must be at least 12 characters")
        sys.exit(1)

    # Generate bcrypt hash
    h = bcrypt.hashpw(password.encode(), bcrypt.gensalt(rounds=12)).decode()

    # Verify immediately
    assert bcrypt.checkpw(password.encode(), h.encode()), "Hash verification failed"

    # Connect to DB
    conn = await asyncpg.connect(
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        port=int(os.environ.get("POSTGRES_PORT", 5432)),
        user=os.environ.get("POSTGRES_USER", "mtpif"),
        password=os.environ.get("POSTGRES_PASSWORD", "mtpif_dev"),
        database=os.environ.get("POSTGRES_DB", "mtpif"),
    )

    try:
        existing = await conn.fetchval(
            "SELECT id FROM users WHERE email = $1", email
        )

        if existing:
            await conn.execute(
                "UPDATE users SET password_hash = $1, role = 'platform_operator', "
                "tenant_id = NULL WHERE email = $2",
                h, email,
            )
            print(f"Updated platform operator: {email}")
        else:
            await conn.execute(
                "INSERT INTO users (email, password_hash, role, tenant_id) "
                "VALUES ($1, $2, 'platform_operator', NULL)",
                email, h,
            )
            print(f"Created platform operator: {email}")

        # Confirm
        row = await conn.fetchrow(
            "SELECT id, email, role, is_active FROM users WHERE email = $1", email
        )
        print(f"  ID:     {row['id']}")
        print(f"  Email:  {row['email']}")
        print(f"  Role:   {row['role']}")
        print(f"  Active: {row['is_active']}")

    finally:
        await conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create isolane platform operator")
    parser.add_argument("--email",    required=True)
    parser.add_argument("--password", required=True)
    args = parser.parse_args()
    asyncio.run(run(args.email, args.password))