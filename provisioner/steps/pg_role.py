"""
isolane/provisioner/steps/pg_role.py

Create scoped warehouse role for dbt runs.
Uses psycopg2 to avoid nested asyncio.run() issues.
"""

import os
from typing import Any


def run(ns: str, cfg: dict[str, Any]) -> None:
    import psycopg2

    conn = psycopg2.connect(
        host     = os.environ.get("POSTGRES_HOST", "postgres"),
        port     = int(os.environ.get("POSTGRES_PORT", 5432)),
        user     = os.environ.get("POSTGRES_USER", "mtpif"),
        password = os.environ.get("POSTGRES_PASSWORD", "mtpif_dev"),
        dbname   = os.environ.get("POSTGRES_DB", "mtpif"),
    )
    conn.autocommit = True

    role = f"{ns}_role"
    pwd  = os.environ.get("TENANT_ROLE_PASSWORD", "change_me_dev")
    db   = os.environ.get("POSTGRES_DB", "mtpif")

    schemas = [
        f"{ns}_active",
        f"{ns}_history",
        f"{ns}_snapshots",
        f"{ns}_staging",
        f"quarantine_{ns}",
    ]

    try:
        with conn.cursor() as cur:
            # Create role if not exists
            cur.execute(
                "SELECT 1 FROM pg_roles WHERE rolname = %s", (role,)
            )
            if not cur.fetchone():
                cur.execute(
                    f"CREATE ROLE {role} LOGIN PASSWORD %s", (pwd,)
                )
                print(f"  [pg_role] Role {role} created")
            else:
                print(f"  [pg_role] Role {role} already exists — updating grants")

            cur.execute(f"GRANT CONNECT ON DATABASE {db} TO {role}")

            for schema in schemas:
                cur.execute(f"GRANT USAGE ON SCHEMA {schema} TO {role}")
                cur.execute(f"GRANT CREATE ON SCHEMA {schema} TO {role}")
                cur.execute(
                    f"GRANT ALL PRIVILEGES ON ALL TABLES "
                    f"IN SCHEMA {schema} TO {role}"
                )
                cur.execute(
                    f"ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} "
                    f"GRANT ALL ON TABLES TO {role}"
                )
                print(f"  [pg_role] {role} granted access to {schema}")
    finally:
        conn.close()