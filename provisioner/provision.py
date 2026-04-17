"""
isolane/provisioner/provision.py

State-machine provisioner entry point.
"""

import argparse
import asyncio
import os
import sys
from uuid import UUID

import asyncpg

from provisioner.state import StepState
from provisioner.steps import (
    kafka_acl,
    pg_schema,
    pg_role,
    rocksdb_cf,
    redis_group,
    grafana_org,
    prometheus_scrape,
)

# ── Step registry — order is the execution order ──────────────────
STEPS = [
    ("kafka_acl",          kafka_acl),
    ("pg_schema",          pg_schema),
    ("pg_role",            pg_role),
    ("rocksdb_cf",         rocksdb_cf),
    ("redis_consumer_group", redis_group),
    ("grafana_org",        grafana_org),
    ("prometheus_scrape",  prometheus_scrape),
]


async def provision(ns: str, team: str, mode: str) -> bool:
    """
    Run the full provisioning sequence for a namespace.
    Returns True if all steps completed successfully.
    """
    print(f"\n{'='*60}")
    print(f"  Provisioning: {ns}")
    print(f"  Team: {team}  |  Mode: {mode}")
    print(f"{'='*60}\n")

    conn = await _connect()
    try:
        async with conn.transaction():
            # Ensure tenant record exists
            tenant_id = await _ensure_tenant(conn, ns, team, mode)
            print(f"  Tenant ID: {tenant_id}\n")

            state = StepState(tenant_id, conn)
            cfg   = {
                "ns":        ns,
                "team":      team,
                "mode":      mode,
                "tenant_id": str(tenant_id),
            }

            for order, (step_name, step_module) in enumerate(STEPS):
                inputs            = {**cfg, "step": step_name}
                decision, h = await state.should_run(step_name, inputs)

                label = f"[{order+1}/{len(STEPS)}] {step_name}"

                if decision == "skip":
                    print(f"  {label}: SKIP (done, inputs unchanged)")
                    continue

                tag = {
                    "run":    "RUN",
                    "rerun":  "RE-RUN (drift detected)",
                    "retry":  "RETRY (previously failed)",
                }[decision]
                print(f"  {label}: {tag}")

                await state.mark_pending(step_name, order, h)

                try:
                    step_module.run(ns, cfg)
                    await state.mark_done(step_name, h)
                    print(f"  {label}: DONE\n")
                except Exception as e:
                    await state.mark_failed(step_name, str(e))
                    print(f"  {label}: FAILED — {e}\n")
                    print(f"  Provisioning stopped. Re-run to resume.\n")
                    return False

    finally:
        await conn.close()

    print(f"\n  Provisioning complete for namespace: {ns}")
    print(f"  Run: make seed ns={ns}  to push test data\n")
    return True


async def _ensure_tenant(
    conn: asyncpg.Connection,
    ns:   str,
    team: str,
    mode: str,
) -> UUID:
    """Upsert tenant record and return its UUID."""
    row = await conn.fetchrow(
        """
        INSERT INTO tenants (namespace, team_name, mode)
        VALUES ($1, $2, $3)
        ON CONFLICT (namespace) DO UPDATE
            SET team_name = EXCLUDED.team_name
        RETURNING id
        """,
        ns, team, mode,
    )
    return row["id"]


async def _connect() -> asyncpg.Connection:
    host     = os.environ.get("POSTGRES_HOST", "localhost")
    port     = os.environ.get("POSTGRES_PORT", "5432")
    user     = os.environ.get("POSTGRES_USER", "mtpif")
    password = os.environ.get("POSTGRES_PASSWORD", "mtpif_dev")
    dbname   = os.environ.get("POSTGRES_DB", "mtpif")
    return await asyncpg.connect(
        f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="isolane namespace provisioner")
    parser.add_argument("--ns",   required=True, help="Namespace e.g. analytics")
    parser.add_argument("--team", required=True, help="Team name e.g. analytics-team")
    parser.add_argument("--mode", default="standard",
                        choices=["standard", "fail_fast"])
    args = parser.parse_args()

    success = asyncio.run(provision(args.ns, args.team, args.mode))
    sys.exit(0 if success else 1)