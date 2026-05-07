# isolane

[![CI](https://github.com/tycoach/isolane/actions/workflows/ci.yml/badge.svg)](https://github.com/tycoach/isolane/actions/workflows/ci.yml)
[![Isolation Tests](https://github.com/tycoach/isolane/actions/workflows/isolation-tests.yml/badge.svg)](https://github.com/tycoach/isolane/actions/workflows/isolation-tests.yml)
[![API Image](https://ghcr.io/v2/tycoach/isolane-api/tags/list)](https://github.com/tycoach/isolane/pkgs/container/isolane-api)

**Multi-tenant pipeline isolation framework.**

isolane lets multiple teams share one data pipeline infrastructure without their data, credentials, or observability bleeding into each other. Every tenant gets scoped Postgres schemas, a scoped Redis Stream, a scoped RocksDB column family, and a scoped Grafana org — all provisioned in a single command.

```bash
make tenant NS=analytics TEAM=analytics-team
```

---

## Why isolane

Shared pipeline infrastructure is cheaper to operate than per-tenant deployments. But sharing creates risk — a bug in one team's pipeline can corrupt another team's data, a noisy query can starve a neighbour's writes, and a single Grafana dashboard shows everyone's metrics mixed together.

isolane solves this with structural isolation. The separation is enforced at the database role level, the Redis consumer group level, and the Prometheus label level — not by application logic. Even if there's a bug in the application, the database will reject any cross-tenant query.

---

## Architecture

```
Upstream system
      │
      ▼
{namespace}.work-queue         Redis Streams — one stream per namespace
      │
      ▼
Go worker (XREADGROUP)         High-throughput consumer, one per stream
      │
      ▼
Python engine                  Schema validation, edge case checks, quarantine
      │
      ├──▶ {ns}_staging.*      Validated records written here first
      │
      ▼
dbt run --target {namespace}   Transform staging → active
      │
      ▼
{ns}_active.*                  Clean, queryable data
{ns}_quarantine.*              Rejected records with reason
```

### Isolation layers

| Layer | Mechanism |
|---|---|
| Postgres | Row-level security + scoped roles (`{ns}_role`) |
| Redis | Separate streams (`{ns}.work-queue`) and consumer groups |
| RocksDB | Column families (`ns_{namespace}`) |
| Prometheus | `tenant=` label injected at scrape time (`honor_labels: false`) |
| Grafana | Separate org per namespace |
| dbt | `--target {namespace}` scopes all writes to `{ns}_active` |

---

## Stack

| Component | Purpose |
|---|---|
| PostgreSQL 16 | Tenant data store with RLS |
| Redis 7 (Sentinel) | Work queue and DLQ |
| Apache Kafka | Event streaming (optional in local dev) |
| Go worker | High-throughput Redis Streams consumer |
| Python engine | Polars + dbt data transformation |
| Prometheus | Metrics collection |
| Grafana | Dashboards (one org per tenant) |
| FastAPI | Management API with RS256 JWT auth |

---

---

## Docker images

isolane publishes three Docker images to GitHub Container Registry on every
release. You can pull and run them without building from source.

| Image | Description | Pull |
|---|---|---|
| `isolane-api` | FastAPI management API | `ghcr.io/tycoach/isolane-api:latest` |
| `isolane-worker` | Python engine worker | `ghcr.io/tycoach/isolane-worker:latest` |
| `isolane-go-worker` | Go Redis Streams consumer | `ghcr.io/tycoach/isolane-go-worker:latest` |

### Pull the images

```bash
docker pull ghcr.io/tycoach/isolane-api:latest
docker pull ghcr.io/tycoach/isolane-worker:latest
docker pull ghcr.io/tycoach/isolane-go-worker:latest
```

### Use a specific version

```bash
docker pull ghcr.io/tycoach/isolane-api:v1.0.0
docker pull ghcr.io/tycoach/isolane-worker:v1.0.0
docker pull ghcr.io/tycoach/isolane-go-worker:v1.0.0
```

### Run with pre-built images

Update your `docker-compose.yml` to use the published images instead of
building locally:

```yaml
services:
  api:
    image: ghcr.io/tycoach/isolane-api:latest
    # remove the build: section

  engine-worker:
    image: ghcr.io/tycoach/isolane-worker:latest
    # remove the build: section

  go-worker:
    image: ghcr.io/tycoach/isolane-go-worker:latest
    # remove the build: section
```

This is the recommended approach for production deployments — pull
versioned images rather than building from source on the server.


## Quick start

### Prerequisites

- Docker Desktop (or Docker + Docker Compose)
- Make
- 8GB RAM recommended

### 1. Clone and configure

```bash
git clone https://github.com/tycoach/isolane
cd isolane
cp .env.example .env
# Edit .env — set POSTGRES_PASSWORD, GRAFANA_PASSWORD
```

### 2. Start the stack

```bash
docker compose up -d
```

All 11 services start: Postgres, Redis (master + replica + sentinel), Kafka, Zookeeper, API, Go worker, Python worker, Prometheus, Grafana, Alertmanager, Schema Registry.

### 3. Create the platform operator

```bash
make create-admin EMAIL=admin@yourcompany.com PASSWORD=yourpassword
```

### 4. Provision a namespace

```bash
make tenant NS=analytics TEAM=analytics-team
```

This runs 7 provisioning steps:
1. Kafka ACLs (skipped in local dev)
2. Postgres schemas (`analytics_active`, `analytics_staging`, `analytics_history`, `analytics_snapshots`, `quarantine_analytics`)
3. Postgres role (`analytics_role`) scoped to those schemas
4. RocksDB column family (`ns_analytics`)
5. Redis consumer group (`analytics-consumer-group`)
6. Grafana org (`analytics-org`)
7. Prometheus scrape config (`scrape_configs/analytics.yml`)

The provisioner is idempotent — re-run it any time to retry failed steps or update config.

### 5. Push data

Any upstream system pushes records to the work queue:

```python
import redis, json

r = redis.Redis(host='localhost', port=6379)
r.xadd('analytics.work-queue', {
    'pipeline_id':  'customers',
    'batch_offset': 'etl-run-20260422',
    'records_json': json.dumps([
        {'customer_id': 'C001', 'email': 'user@example.com', 'city': 'Lagos'}
    ])
})
```

### 6. Watch it process

```bash
docker compose logs go-worker -f
```

```
[dispatcher] analytics.customers | done | in=21 out=20 quarantined=1 duration=8s
```

### 7. Query clean data

```bash
make shell-db
```

```sql
SELECT * FROM analytics_active.customers LIMIT 5;
SELECT * FROM quarantine_analytics.records LIMIT 5;
```

### 8. Open Grafana

`http://localhost:3000` → Dashboards → isolane — Engine Overview

Select `analytics` from the Namespace dropdown.

---

## Pipeline configuration

Each pipeline is defined by a YAML file in `config/{namespace}/pipelines/`:

```yaml
# config/analytics/pipelines/customers.yml
pipeline_id: customers
natural_key: customer_id
null_threshold: 0.05
nullable_fields:
  - phone
  - loyalty_tier
dbt:
  model: customers
edge_case_mode: quarantine
```

| Field | Description |
|---|---|
| `natural_key` | Column used for deduplication |
| `null_threshold` | Max fraction of null natural keys before quarantine (0.05 = 5%) |
| `nullable_fields` | Fields allowed to be null |
| `dbt.model` | dbt model name to run after staging |
| `edge_case_mode` | `quarantine` (default) or `fail_fast` |

---

## Edge case handling

The engine checks every batch for:

| Check | What it catches | Action |
|---|---|---|
| Null natural key | Records with no `customer_id` | Quarantine |
| Duplicate natural key | Same `customer_id` appearing twice | Quarantine second occurrence |
| Late arrivals | Records with timestamps far in the past | Flag (configurable) |
| Schema drift | New or removed columns vs previous batch | Log warning |

Quarantined records go to `quarantine_{namespace}.records` with a `reason` column explaining why they were rejected.

---

## Make targets

```bash
make create-admin EMAIL=... PASSWORD=...   # Create platform operator
make tenant NS=... TEAM=...               # Provision a namespace
make seed NS=...                          # Push test data to a namespace
make shell-db                             # psql into the metadata DB
make shell-redis                          # redis-cli into Redis master
make apply-rls                            # Apply RLS policies (after fresh volume)
make check-tables                         # Verify DB tables exist
```

---

## Running tests

```bash
# Install test dependencies (first time only)
docker compose exec api pip install pytest pytest-asyncio httpx

# Unit tests (no external dependencies, ~3 seconds)
docker compose exec api python3 -m pytest tests/unit/ -v

# Isolation tests (require running stack)
docker compose exec \
  -e TEST_ADMIN_EMAIL=admin@yourcompany.com \
  -e TEST_ADMIN_PASSWORD=yourpassword \
  api python3 -m pytest tests/isolation/ -v
```

Expected: **42 passed, 3 skipped** (skipped tests need a second tenant provisioned).

---

## E-commerce example

A full end-to-end example with three brands sharing one isolane deployment:

```bash
# Provision three brands
make tenant NS=brand_a TEAM=brand-a-team
make tenant NS=brand_b TEAM=brand-b-team
make tenant NS=brand_c TEAM=brand-c-team

# Seed realistic data (customers, orders, products)
python3 examples/ecommerce/seed.py

# Verify isolation held
python3 examples/ecommerce/verify.py
```

See [examples/ecommerce/README.md](examples/ecommerce/README.md) for full details.

---

## Project structure

```
isolane/
  api/                    FastAPI management API
    auth/                 RS256 JWT auth, JWKS, token revocation
    routers/              Tenants, pipelines, audit, quarantine
    middleware/           Auth guard, namespace guard
  engine/                 Python data engine
    batch.py              Orchestrates schema → edge cases → staging → dbt
    edge_cases.py         Null, dedup, late arrival checks
    schema.py             Polars schema inference and drift detection
    quarantine.py         Quarantine writer
    rocksdb_client.py     RocksDB CF isolation (in-memory fallback for dev)
  worker/
    go/                   Go consumer (production)
    python/               Python consumer (development)
  provisioner/            7-step idempotent tenant provisioner
    steps/                kafka_acl, pg_schema, pg_role, rocksdb_cf,
                          redis_consumer_group, grafana_org, prometheus_scrape
  dbt/                    dbt project (models, snapshots, macros)
  config/                 Per-namespace pipeline configs
  metadata/               Postgres DDL (init.sql, rls.sql)
  monitoring/             Prometheus, Grafana, Alertmanager config
  tests/
    unit/                 Engine, auth, provisioner unit tests
    isolation/            Cross-tenant isolation test suite
  examples/
    ecommerce/            Three-brand e-commerce demo
```

---

## Environment variables

| Variable | Description | Default |
|---|---|---|
| `POSTGRES_PASSWORD` | Postgres superuser password | — |
| `POSTGRES_USER` | Postgres superuser username | `mtpif` |
| `POSTGRES_DB` | Database name | `mtpif` |
| `GRAFANA_PASSWORD` | Grafana admin password | — |
| `GRAFANA_URL` | Grafana URL (internal) | `http://grafana:3000` |
| `TENANT_ROLE_PASSWORD` | Password for scoped tenant roles | — |
| `JWT_PRIVATE_KEY_PATH` | Path to RS256 private key | `/run/secrets/private.pem` |
| `JWT_PUBLIC_KEY_PATH` | Path to RS256 public key | `/run/secrets/public.pem` |

---

## Ports

| Port | Service |
|---|---|
| 8000 | Management API |
| 3000 | Grafana |
| 9090 | Prometheus |
| 9091 | Python worker metrics |
| 9095 | Go worker metrics |
| 9093 | Alertmanager |
| 5432 | Postgres |
| 6379 | Redis |
| 9092 | Kafka |
| 8081 | Schema Registry |

---

## Production considerations

isolane v1 is designed for single-region deployment on Docker Compose or a single VM. Before running in production:

1. **Secret management** — replace `.env` with Vault, AWS Secrets Manager, or Kubernetes secrets
2. **Kafka ACLs** — the `kafka_acl` provisioner step skips in local dev; run it from inside the Kafka container in production
3. **TLS** — put a reverse proxy (nginx, Caddy) in front of the API and Grafana
4. **Alertmanager receivers** — configure Slack/PagerDuty webhooks in `monitoring/alertmanager/alertmanager.yml`
5. **Backup** — snapshot the Postgres volume and Redis RDB file

---

## License

MIT