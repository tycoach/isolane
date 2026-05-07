# isolane — E-Commerce Example

End-to-end demonstration of isolane using a multi-brand e-commerce scenario.

Three brands share one isolane deployment. Their data — customers, orders,
and products — is completely isolated at every layer: Postgres schemas, RLS
policies, Redis Streams, RocksDB column families, and Grafana dashboards.

## Brands

| Namespace | Brand Name    | Focus        |
|-----------|---------------|--------------|
| brand_a   | Apex Store    | Electronics  |
| brand_b   | Blue Label    | Fashion      |
| brand_c   | Cedar Market  | Groceries    |

## Pipelines per brand

| Pipeline  | Natural Key | Quarantine triggers                  |
|-----------|-------------|--------------------------------------|
| customers | customer_id | Duplicate customer_id (~5% of data)  |
| orders    | order_id    | Null customer_id (~3% of data)       |
| products  | product_id  | Clean data — no intentional errors   |

## Prerequisites

isolane stack must be running:
```bash
docker compose up -d
make create-admin EMAIL=admin@isolane.dev PASSWORD=Isolane2026!
```

## Quick start

### 1. Provision all three brands

```bash
make tenant NS=brand_a TEAM=brand-a-team
make tenant NS=brand_b TEAM=brand-b-team
make tenant NS=brand_c TEAM=brand-c-team
```

### 2. Copy the dbt models into the project

```bash
cp examples/ecommerce/dbt/models/orders.sql dbt/models/
cp examples/ecommerce/dbt/models/products.sql dbt/models/
```

Then rebuild the worker images:
```bash
docker compose build engine-worker go-worker
docker compose up -d engine-worker go-worker
```

### 3. Seed all three brands

```bash
# Seed with default counts (200 customers, 500 orders, 100 products per brand)
POSTGRES_USER=mtpif POSTGRES_PASSWORD=postgres POSTGRES_DB=mtpif \
python3 examples/ecommerce/seed.py

# Or seed a specific brand
python3 examples/ecommerce/seed.py --brand brand_a

# Dry run first to see what would be pushed
python3 examples/ecommerce/seed.py --dry-run

# Larger dataset
python3 examples/ecommerce/seed.py --customers 1000 --orders 5000 --products 500
```

### 4. Watch the workers process

```bash
docker compose logs go-worker -f
```

You should see:
```
[dispatcher] brand_a.customers | done | in=210 out=200 quarantined=10 duration=8s
[dispatcher] brand_b.customers | done | in=210 out=200 quarantined=10 duration=9s
[dispatcher] brand_c.customers | done | in=210 out=200 quarantined=10 duration=8s
```

### 5. Verify isolation held

```bash
POSTGRES_USER=mtpif POSTGRES_PASSWORD=postgres POSTGRES_DB=mtpif \
python3 examples/ecommerce/verify.py
```

Expected output:
```
── Schema isolation ─────────────────────────────────────
  ✓  Schema exists: brand_a_active
  ✓  Schema exists: brand_a_staging
  ... (all 15 schemas)

── Data counts ──────────────────────────────────────────
  ✓  brand_a_active.customers has data
  ✓  quarantine_brand_a caught bad records
  ... 

── RLS isolation ────────────────────────────────────────
  ✓  brand_a context sees only its own pipeline_runs
  ✓  brand_b context sees only its own pipeline_runs
  ✓  brand_c context sees only its own pipeline_runs

── Cross-schema access ──────────────────────────────────
  ✓  brand_a_role cannot access brand_b_active
  ✓  brand_a_role cannot access brand_c_active
  ... (all 6 cross-schema checks)

Results: 30 passed, 0 failed
✓ All isolation guarantees verified
```

### 6. Open Grafana

`http://localhost:3000` → Dashboards → isolane — Engine Overview

Select a brand from the Namespace dropdown to see its metrics.

## What the data demonstrates

### Quarantine
Each batch of customer records contains ~5% duplicate `customer_id` entries.
These are caught by the edge case checker and written to `quarantine_{brand}.records`
instead of `{brand}_active.customers`. The clean records flow through normally.

### Isolation
Brand A's `analytics_role` has no `USAGE` privilege on `brand_b_active` or
`brand_c_active`. This is enforced at the Postgres role level — not by application
logic. Even if a bug in the application tried to cross namespaces, the DB would
reject the query.

### Observability
Each brand gets its own Grafana org (provisioned by `make tenant`). The Engine
Overview dashboard shows per-pipeline metrics scoped to the selected namespace.
The Prometheus scrape config injects `tenant={brand}` so metrics never mix.

## Resetting

To tear down and start fresh:
```bash
# Remove all brand data (keeps the stack running)
make shell-db
```
```sql
DROP SCHEMA brand_a_active CASCADE;
DROP SCHEMA brand_a_staging CASCADE;
DROP SCHEMA brand_a_history CASCADE;
DROP SCHEMA brand_a_snapshots CASCADE;
DROP SCHEMA quarantine_brand_a CASCADE;
-- repeat for brand_b, brand_c
DELETE FROM tenants WHERE namespace LIKE 'brand_%';
DELETE FROM provisioning_log WHERE tenant_id IN (
  SELECT id FROM tenants WHERE namespace LIKE 'brand_%'
);
```

Then re-provision and re-seed.