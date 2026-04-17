"""
isolane/provisioner/steps/pg_role.py

Create a scoped warehouse role for dbt runs.
"""

import os
import asyncpg
import asyncio
from typing import Any


def run(ns: str, cfg: dict[str, Any]) -> None:
    asyncio.run(_run_async(ns, cfg))


async def _run_async(ns: str, cfg: dict[str, Any]) -> None:
    conn  = await asyncpg.connect(dsn=_dsn())
    role  = f"{ns}_role"
    pwd   = os.environ.get("TENANT_ROLE_PASSWORD", "change_me_dev")

    schemas = [
        f"{ns}_active",
        f"{ns}_history",
        f"{ns}_snapshots",
        f"{ns}_staging",
        f"quarantine_{ns}",
    ]

    try:
        # Create role if not exists
        exists = await conn.fetchval(
            "SELECT 1 FROM pg_roles WHERE rolname = $1", role
        )
        if not exists:
            await conn.execute(
                f"CREATE ROLE {role} LOGIN PASSWORD '{pwd}'"
            )
            print(f"  [pg_role] Role {role} created")
        else:
            print(f"  [pg_role] Role {role} already exists — updating grants")

        db = os.environ.get("POSTGRES_DB", "mtpif")
        await conn.execute(f"GRANT CONNECT ON DATABASE {db} TO {role}")

        for schema in schemas:
            await conn.execute(f"GRANT USAGE ON SCHEMA {schema} TO {role}")
            await conn.execute(f"GRANT CREATE ON SCHEMA {schema} TO {role}")
            await conn.execute(
                f"GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA {schema} TO {role}"
            )
            await conn.execute(
                f"ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} "
                f"GRANT ALL ON TABLES TO {role}"
            )
            print(f"  [pg_role] {role} granted full access to {schema}")

    finally:
        await conn.close()


def _dsn() -> str:
    return (
        f"postgresql://{os.environ.get('POSTGRES_USER','mtpif')}"
        f":{os.environ.get('POSTGRES_PASSWORD','mtpif_dev')}"
        f"@{os.environ.get('POSTGRES_HOST','localhost')}"
        f":{os.environ.get('POSTGRES_PORT','5432')}"
        f"/{os.environ.get('POSTGRES_DB','mtpif')}"
    )