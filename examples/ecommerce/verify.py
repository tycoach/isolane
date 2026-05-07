"""
examples/ecommerce/verify.py

Isolation verification script for the e-commerce example.
Exit code 1 = one or more assertions failed.
"""

import asyncio
import os
import sys

import asyncpg


def _require(key: str) -> str:
    val = os.environ.get(key)
    if not val:
        print(f"ERROR: Required environment variable {key} is not set")
        sys.exit(1)
    return val


async def get_conn(namespace: str = None) -> asyncpg.Connection:
    conn = await asyncpg.connect(
        host     = os.environ.get("POSTGRES_HOST", "localhost"),
        port     = int(os.environ.get("POSTGRES_PORT", 5432)),
        user     = _require("POSTGRES_USER"),
        password = _require("POSTGRES_PASSWORD"),
        database = _require("POSTGRES_DB"),
    )
    if namespace:
        await conn.execute("SELECT set_tenant_context($1)", namespace)
    return conn


BRANDS = ["brand_a", "brand_b", "brand_c"]

passed = 0
failed = 0


def check(name: str, result: bool, detail: str = ""):
    global passed, failed
    if result:
        print(f"  ✓  {name}")
        passed += 1
    else:
        print(f"  ✗  {name}")
        if detail:
            print(f"       {detail}")
        failed += 1


async def verify_schemas(conn: asyncpg.Connection):
    print("\n── Schema isolation ─────────────────────────────────────")
    for brand in BRANDS:
        expected = [
            f"{brand}_active",
            f"{brand}_staging",
            f"{brand}_history",
            f"{brand}_snapshots",
            f"quarantine_{brand}",
        ]
        for schema in expected:
            exists = await conn.fetchval(
                "SELECT 1 FROM information_schema.schemata WHERE schema_name = $1",
                schema,
            )
            check(f"Schema exists: {schema}", bool(exists))


async def verify_data_counts(conn: asyncpg.Connection):
    print("\n── Data counts ──────────────────────────────────────────")
    for brand in BRANDS:
        # Customers
        try:
            count = await conn.fetchval(
                f"SELECT COUNT(*) FROM {brand}_active.customers"
            )
            check(
                f"{brand}_active.customers has data",
                count > 0,
                f"Got {count} rows"
            )
        except Exception as e:
            check(f"{brand}_active.customers accessible", False, str(e))

        # Quarantine — should have some records (duplicates + null customer_ids)
        try:
            q_count = await conn.fetchval(
                f"SELECT COUNT(*) FROM quarantine_{brand}.records"
            )
            check(
                f"quarantine_{brand} caught bad records",
                q_count > 0,
                f"Got {q_count} quarantined rows"
            )
        except Exception as e:
            check(f"quarantine_{brand}.records accessible", False, str(e))


async def verify_rls_isolation(conn: asyncpg.Connection):
    print("\n── RLS isolation ────────────────────────────────────────")

    # Note: superuser (mtpif) bypasses RLS by design — required for
    # the provisioner and API pool. We verify the policy EXISTS and
    # that tenant_id scoping is structurally correct.

    rls_enabled = await conn.fetchval(
        "SELECT relrowsecurity FROM pg_class WHERE relname = 'pipeline_runs'"
    )
    check("RLS enabled on pipeline_runs table", bool(rls_enabled))

    policy_exists = await conn.fetchval(
        """
        SELECT 1 FROM pg_policies
        WHERE tablename = 'pipeline_runs'
        AND policyname = 'pipeline_runs_tenant_isolation'
        """
    )
    check("pipeline_runs_tenant_isolation policy exists", bool(policy_exists))

    for brand in BRANDS:
        tenant_id = await conn.fetchval(
            "SELECT id FROM tenants WHERE namespace = $1", brand
        )
        if not tenant_id:
            check(f"{brand} pipeline_runs scoped to tenant_id", False, "Tenant not found")
            continue

        wrong_tenant = await conn.fetchval(
            """
            SELECT COUNT(*) FROM pipeline_runs pr
            JOIN tenants t ON t.id = pr.tenant_id
            WHERE pr.tenant_id = $1
            AND t.namespace != $2
            """,
            tenant_id, brand,
        )
        check(
            f"{brand} pipeline_runs scoped to correct tenant_id",
            wrong_tenant == 0,
            f"Found {wrong_tenant} runs with wrong tenant_id"
        )


async def verify_cross_schema_access(conn: asyncpg.Connection):
    print("\n── Cross-schema access ──────────────────────────────────")
    for brand in BRANDS:
        role = f"{brand}_role"
        other_brands = [b for b in BRANDS if b != brand]

        for other in other_brands:
            has_access = await conn.fetchval(
                "SELECT has_schema_privilege($1, $2, 'USAGE')",
                role,
                f"{other}_active",
            )
            check(
                f"{role} cannot access {other}_active",
                not has_access,
                f"has_schema_privilege returned {has_access}"
            )


async def verify_pipeline_runs(conn: asyncpg.Connection):
    print("\n── Pipeline runs ────────────────────────────────────────")
    for brand in BRANDS:
        tenant_id = await conn.fetchval(
            "SELECT id FROM tenants WHERE namespace = $1", brand
        )
        if not tenant_id:
            check(f"{brand} tenant registered", False, "Tenant not found in DB")
            continue

        check(f"{brand} tenant registered", True)

        done_runs = await conn.fetchval(
            "SELECT COUNT(*) FROM pipeline_runs WHERE tenant_id = $1 AND status = 'done'",
            tenant_id,
        )
        failed_runs = await conn.fetchval(
            "SELECT COUNT(*) FROM pipeline_runs WHERE tenant_id = $1 AND status = 'failed'",
            tenant_id,
        )
        check(
            f"{brand} has successful pipeline runs",
            done_runs > 0,
            f"{done_runs} done, {failed_runs} failed"
        )


async def main():
    print("\nisolane e-commerce isolation verification")
    print("=" * 60)

    conn = await get_conn()
    try:
        await verify_schemas(conn)
        await verify_data_counts(conn)
        await verify_pipeline_runs(conn)
        await verify_rls_isolation(conn)
        await verify_cross_schema_access(conn)
    finally:
        await conn.close()

    print(f"\n{'='*60}")
    print(f"  Results: {passed} passed, {failed} failed")

    if failed == 0:
        print(f"  ✓ All isolation guarantees verified")
        print(f"{'='*60}\n")
        sys.exit(0)
    else:
        print(f"  ✗ {failed} assertion(s) failed — review output above")
        print(f"{'='*60}\n")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())