"""
isolane/provisioner/deprovision.py

Reverse the provisioning steps for a namespace.

"""

import argparse
import asyncio
import base64
import json
import os
import sys
import urllib.request
import urllib.error
from pathlib import Path

import asyncpg
import redis as redis_lib


async def deprovision(ns: str) -> None:
    print(f"\n{'='*60}")
    print(f"  Deprovisioning namespace: {ns}")
    print(f"{'='*60}\n")

    _remove_prometheus_scrape(ns)
    _remove_grafana_org(ns)
    _remove_redis_streams(ns)
    await _drop_pg_role(ns)
    await _drop_pg_schemas(ns)
    _remove_kafka_acl(ns)
    await _clear_provisioning_log(ns)

    print(f"\n  Namespace '{ns}' fully deprovisioned.\n")


# ── Step removals ─────────────────────────────────────────────────

def _remove_prometheus_scrape(ns: str) -> None:
    scrape_dir  = Path(os.environ.get(
        "PROMETHEUS_SCRAPE_DIR",
        "./monitoring/prometheus/scrape_configs",
    ))
    config_file = scrape_dir / f"{ns}.yml"
    if config_file.exists():
        config_file.unlink()
        print(f"  [prometheus] Scrape config removed: {config_file}")
    else:
        print(f"  [prometheus] No scrape config found — skipping")


def _remove_grafana_org(ns: str) -> None:
    grafana_url  = os.environ.get("GRAFANA_URL", "http://localhost:3000")
    grafana_user = os.environ.get("GRAFANA_USER", "admin")
    grafana_pass = os.environ.get("GRAFANA_PASSWORD", "admin")
    auth = base64.b64encode(f"{grafana_user}:{grafana_pass}".encode()).decode()
    headers = {"Authorization": f"Basic {auth}"}
    org_name = f"{ns}-org"

    try:
        req = urllib.request.Request(
            f"{grafana_url}/api/orgs/name/{urllib.request.quote(org_name)}",
            headers=headers,
        )
        with urllib.request.urlopen(req) as resp:
            org_id = json.loads(resp.read())["id"]

        del_req = urllib.request.Request(
            f"{grafana_url}/api/orgs/{org_id}",
            headers=headers,
            method="DELETE",
        )
        urllib.request.urlopen(del_req)
        print(f"  [grafana] Org '{org_name}' deleted")
    except urllib.error.HTTPError as e:
        if e.code == 404:
            print(f"  [grafana] Org '{org_name}' not found — skipping")
        else:
            print(f"  [grafana] Warning: {e}")


def _remove_redis_streams(ns: str) -> None:
    r = _redis_client()
    for key in [f"{ns}.work-queue", f"{ns}.dlq"]:
        deleted = r.delete(key)
        status  = "deleted" if deleted else "not found"
        print(f"  [redis] Stream '{key}' {status}")
    r.close()


async def _drop_pg_role(ns: str) -> None:
    conn = await _pg_connect()
    role = f"{ns}_role"
    try:
        schemas = [
            f"{ns}_active", f"{ns}_history",
            f"{ns}_snapshots", f"{ns}_staging",
            f"quarantine_{ns}",
        ]
        for schema in schemas:
            await conn.execute(
                f"REVOKE ALL ON ALL TABLES IN SCHEMA {schema} FROM {role}"
            )
        await conn.execute(f"DROP ROLE IF EXISTS {role}")
        print(f"  [pg_role] Role '{role}' dropped")
    except Exception as e:
        print(f"  [pg_role] Warning: {e}")
    finally:
        await conn.close()


async def _drop_pg_schemas(ns: str) -> None:
    conn = await _pg_connect()
    schemas = [
        f"{ns}_active",
        f"{ns}_history",
        f"{ns}_snapshots",
        f"{ns}_staging",
        f"quarantine_{ns}",
    ]
    try:
        for schema in schemas:
            await conn.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
            print(f"  [pg_schema] Schema '{schema}' dropped")
    finally:
        await conn.close()


def _remove_kafka_acl(ns: str) -> None:
    import subprocess
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
    principal = f"User:{ns}-svc"

    for operation in ("Write", "Read"):
        cmd = [
            "kafka-acls",
            "--bootstrap-server", bootstrap,
            "--remove", "--force",
            "--allow-principal", principal,
            "--operation", operation,
            "--topic", f"{ns}.",
            "--resource-pattern-type", "PREFIXED",
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            print(f"  [kafka_acl] Removed {operation} ACL for {principal}")
        else:
            print(f"  [kafka_acl] Warning removing {operation}: {result.stderr.strip()}")

    # Consumer group ACL
    cmd = [
        "kafka-acls",
        "--bootstrap-server", bootstrap,
        "--remove", "--force",
        "--allow-principal", principal,
        "--operation", "Describe",
        "--group", f"{ns}-consumer-group",
    ]
    subprocess.run(cmd, capture_output=True)
    print(f"  [kafka_acl] Consumer group ACL removed")


async def _clear_provisioning_log(ns: str) -> None:
    conn = await _pg_connect()
    try:
        await conn.execute(
            """
            DELETE FROM provisioning_log
            WHERE tenant_id = (
                SELECT id FROM tenants WHERE namespace = $1
            )
            """,
            ns,
        )
        print(f"  [state] Provisioning log cleared for '{ns}'")
    finally:
        await conn.close()


# ── Helpers ───────────────────────────────────────────────────────

async def _pg_connect() -> asyncpg.Connection:
    return await asyncpg.connect(
        f"postgresql://"
        f"{os.environ.get('POSTGRES_USER','mtpif')}"
        f":{os.environ.get('POSTGRES_PASSWORD','mtpif_dev')}"
        f"@{os.environ.get('POSTGRES_HOST','localhost')}"
        f":{os.environ.get('POSTGRES_PORT','5432')}"
        f"/{os.environ.get('POSTGRES_DB','mtpif')}"
    )


def _redis_client() -> redis_lib.Redis:
    return redis_lib.Redis(
        host=os.environ.get("REDIS_HOST", "localhost"),
        port=int(os.environ.get("REDIS_PORT", 6379)),
        decode_responses=True,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="isolane namespace deprovisioner")
    parser.add_argument("--ns", required=True, help="Namespace to deprovision")
    args = parser.parse_args()
    asyncio.run(deprovision(args.ns))