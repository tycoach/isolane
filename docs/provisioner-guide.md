# isolane Provisioner Guide

The provisioner is a 7-step idempotent state machine that creates all
infrastructure for a new tenant namespace. Each step is tracked in the
`provisioning_log` table with its status, input hash, and completion time.

---

## Running the provisioner

```bash
make tenant NS=analytics TEAM=analytics-team
```

This is equivalent to:

```bash
docker compose exec api python3 -m provisioner.provision \
  --namespace analytics \
  --team analytics-team \
  --mode standard
```

### Idempotency

The provisioner is safe to re-run at any time:

- **SKIP** — step was completed previously and inputs haven't changed
- **RETRY** — step failed previously; will attempt again
- **RERUN** — step completed but inputs changed (e.g. team name updated)
- **RUN** — step has never been attempted

```
[1/7] kafka_acl: SKIP (done, inputs unchanged)
[2/7] pg_schema: RETRY (previously failed)
[2/7] pg_schema: DONE
[3/7] pg_role: RUN
[3/7] pg_role: DONE
```

If provisioning stops mid-way (crash, network error, Ctrl+C), re-run
the same command. It resumes from the failed step.

---

## The 7 steps

### Step 1 — kafka_acl

Creates Kafka ACLs scoping the namespace to its own topic prefix.

In local dev, the Kafka CLI is not available inside the API container,
so this step skips gracefully:

```
[kafka_acl] kafka-acls CLI not found — skipping ACL provisioning (local dev mode)
[kafka_acl] In production, run this step from inside the Kafka container
```

**In production:** Run from inside the Kafka container:

```bash
docker compose exec kafka kafka-acls.sh \
  --bootstrap-server localhost:9092 \
  --add \
  --allow-principal User:analytics \
  --operation All \
  --topic analytics
```

---

### Step 2 — pg_schema

Creates 5 Postgres schemas for the namespace:

| Schema | Purpose |
|---|---|
| `{ns}_active` | Current clean data — queryable by the team |
| `{ns}_staging` | Records written by the engine before dbt runs |
| `{ns}_history` | SCD Type 2 history (via dbt snapshots) |
| `{ns}_snapshots` | Point-in-time snapshots |
| `quarantine_{ns}` | Records that failed validation |

Also creates the `quarantine_{ns}.records` table with columns:
`id`, `pipeline_id`, `record`, `reason`, `batch_offset`, `quarantined_at`.

---

### Step 3 — pg_role

Creates a Postgres role `{ns}_role` with USAGE + CREATE on the 5 schemas.

The role has no access to any other namespace's schemas. This is verified
by the isolation test suite:

```python
has_access = await conn.fetchval(
    "SELECT has_schema_privilege('analytics_role', 'finance_active', 'USAGE')"
)
assert not has_access
```

In production, the role would also have a password set via
`TENANT_ROLE_PASSWORD` and be used by the dbt profiles for that namespace.

---

### Step 4 — rocksdb_cf

Registers a RocksDB column family `ns_{namespace}` for the namespace.

RocksDB is used by the engine for:
- Deduplication tracking (seen natural keys per pipeline)
- Schema snapshot caching (last known schema per pipeline)
- Batch offset tracking (last processed stream ID per pipeline)

In local dev (Python 3.11+ / Windows), the RocksDB package is not
installable. The engine falls back to an in-memory dict automatically:

```
[rocksdb] rocksdb package not found — using in-memory fallback (dev/test only)
```

Data is not persisted between worker restarts in fallback mode.

---

### Step 5 — redis_consumer_group

Creates two Redis Streams resources:

```
{namespace}.work-queue          Main work queue (XREADGROUP)
{namespace}.dlq                 Dead letter queue
{namespace}-consumer-group      Consumer group on the work queue
```

The consumer group is created with `MKSTREAM` so the stream is
initialised even before the first message arrives.

**DLQ behaviour:** Messages that fail 3 times (configurable via
`MAX_CLAIM_ATTEMPTS`) are routed to the DLQ by XAUTOCLAIM. They are
ACK'd from the work queue so they don't retry further.

---

### Step 6 — grafana_org

Creates a Grafana organisation `{namespace}-org` and adds a Prometheus
datasource pointing to `http://prometheus:9090` with the namespace
pre-selected.

Requires `GRAFANA_URL`, `GRAFANA_USER`, and `GRAFANA_PASSWORD` in `.env`.

The platform operator's Grafana credentials are used to create the org.
Each tenant org is independent — a user in `analytics-org` cannot see
`finance-org` dashboards.

---

### Step 7 — prometheus_scrape

Writes a per-namespace scrape config to
`monitoring/prometheus/scrape_configs/{namespace}.yml`:

```yaml
scrape_configs:
  - job_name: 'engine-analytics'
    honor_labels: false
    metrics_path: '/metrics'
    scrape_interval: 30s
    static_configs:
      - targets: ['engine-worker:9090']
        labels:
          tenant: 'analytics'
```

`honor_labels: false` ensures the worker cannot override the injected
`tenant=` label — preventing a compromised worker from spoofing another
tenant's metrics.

Prometheus picks up new scrape configs via the `scrape_config_files`
directive in `prometheus.yml`. Reload Prometheus after provisioning:

```bash
curl -X POST http://localhost:9090/-/reload
```

---

## State machine internals

The state machine is implemented in `provisioner/state.py`.

### Input hashing

Each step hashes its inputs using SHA-256 canonical JSON:

```python
def _hash(inputs: dict) -> str:
    canonical = json.dumps(inputs, sort_keys=True, ensure_ascii=True)
    return hashlib.sha256(canonical.encode()).hexdigest()
```

If the inputs change (e.g. team name updated), the hash changes and
the step is re-run with `RERUN` status.

### State transitions

```
None → PENDING → DONE
None → PENDING → FAILED → RETRY → DONE
DONE (same hash) → SKIP
DONE (different hash) → RERUN → DONE
```

### Provisioning log schema

```sql
CREATE TABLE provisioning_log (
    id          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id   UUID NOT NULL REFERENCES tenants(id),
    step_name   TEXT NOT NULL,
    step_order  INT NOT NULL,
    status      TEXT NOT NULL,          -- PENDING | DONE | FAILED
    input_hash  TEXT,
    error_message TEXT,
    completed_at TIMESTAMPTZ,
    UNIQUE (tenant_id, step_name)
);
```

---

## Adding a custom step

Steps are registered in `provisioner/provision.py`:

```python
from provisioner.steps import (
    kafka_acl, pg_schema, pg_role, rocksdb_cf,
    redis_consumer_group, grafana_org, prometheus_scrape,
    my_custom_step,          # ← add here
)

STEPS = [
    ("kafka_acl",            kafka_acl),
    ("pg_schema",            pg_schema),
    ("pg_role",              pg_role),
    ("rocksdb_cf",           rocksdb_cf),
    ("redis_consumer_group", redis_consumer_group),
    ("grafana_org",          grafana_org),
    ("prometheus_scrape",    prometheus_scrape),
    ("my_custom_step",       my_custom_step),   # ← add here
]
```

Each step module must expose a `run(namespace, team_name, mode, conn)` function:

```python
# provisioner/steps/my_custom_step.py

def run(namespace: str, team_name: str, mode: str, conn) -> None:
    """
    conn is a psycopg2 connection (not asyncpg).
    Raise an exception to mark the step as FAILED.
    Print progress with [my_custom_step] prefix.
    """
    print(f"  [my_custom_step] Setting up {namespace}...")
    # ... do work ...
    print(f"  [my_custom_step] Done for {namespace}")
```

**Important:** Steps use `psycopg2` (synchronous), not `asyncpg`.
The provisioner runs in a synchronous subprocess to avoid nested event
loop issues with FastAPI's async runtime.

---

## Deprovisioning

```bash
docker compose exec api python3 -m provisioner.deprovision --namespace analytics
```

This reverses the provisioning steps in order:
1. Remove Prometheus scrape config
2. Remove Grafana org
3. Delete Redis consumer group and DLQ
4. Deregister RocksDB column family
5. Drop Postgres role
6. Drop Postgres schemas (CASCADE)
7. Remove Kafka ACLs

**Warning:** Schema drop is destructive. Back up `{ns}_active` before
deprovisioning a production namespace.
