# isolane Operator Runbook

Day-to-day operations, troubleshooting, and recovery procedures.

---

## Daily checks

```bash
# All containers healthy?
docker compose ps

# Any pipeline failures in the last hour?
make shell-db
```
```sql
SELECT t.namespace, pipeline_id, COUNT(*) as failures
FROM pipeline_runs pr
JOIN tenants t ON t.id = pr.tenant_id
WHERE status = 'failed'
AND started_at > NOW() - INTERVAL '1 hour'
GROUP BY t.namespace, pipeline_id;
```

```bash
# Any DLQ messages?
make shell-redis
```
```
KEYS *.dlq
XLEN analytics.dlq
```

```bash
# Prometheus targets all up?
curl -s http://localhost:9090/api/v1/targets \
  | python3 -m json.tool | grep health
```

---

## Starting and stopping

```bash
# Start everything
docker compose up -d

# Stop everything (preserves volumes)
docker compose down

# Stop and wipe all data (fresh start)
docker compose down --volumes

# Restart a single service
docker compose restart api
docker compose restart go-worker
docker compose restart prometheus
```

**After `docker compose down --volumes`:**
You must re-apply RLS policies and re-provision all tenants:

```bash
docker compose up -d
make apply-rls
make create-admin EMAIL=admin@isolane.dev PASSWORD=Isolane2026!
make tenant NS=analytics TEAM=analytics-team
```

---

## Worker operations

### Check worker status

```bash
# Python worker
docker compose logs engine-worker --tail=20

# Go worker
docker compose logs go-worker --tail=20

# How many namespaces are active?
curl -s http://localhost:9091/metrics | grep ude_active_namespaces
curl -s http://localhost:9095/metrics | grep ude_active_namespaces
```

### Restart a worker

```bash
docker compose restart engine-worker
docker compose restart go-worker
```

### Check queue depths

```bash
make shell-redis
```
```
# Check all work queues
KEYS *.work-queue
XLEN analytics.work-queue
XLEN finance.work-queue

# Check pending (delivered but not ACK'd)
XPENDING analytics.work-queue analytics-consumer-group - + 10
```

### Clear a stuck queue

If a queue has many pending messages that keep failing:

```bash
make shell-redis
```
```
# Option 1: Delete and recreate (loses all messages)
DEL analytics.work-queue
XGROUP CREATE analytics.work-queue analytics-consumer-group 0 MKSTREAM

# Option 2: ACK all pending messages without processing
XACK analytics.work-queue analytics-consumer-group <message-id>
```

---

## Dead letter queue

### Inspect DLQ

```bash
make shell-redis
```
```
XRANGE analytics.dlq - + COUNT 10
```

Each DLQ entry contains:
- `original_stream_id` — the original message ID
- `pipeline_id` — which pipeline failed
- `reason` — why it was routed to DLQ (`max_claims_exceeded`)
- `claim_count` — how many times it was attempted
- `routed_at` — when it hit the DLQ

### Replay a DLQ message

```bash
# Get the original records from the message (if still in work queue)
make shell-redis
```
```
XRANGE analytics.work-queue <original-stream-id> <original-stream-id>
```

Then re-push to the work queue with a fresh message:

```python
import redis, json

r = redis.Redis(host='localhost', port=6379, decode_responses=True)
r.xadd('analytics.work-queue', {
    'pipeline_id':  'customers',
    'batch_offset': 'replay-2026-04-22',
    'records_json': json.dumps([...]),  # your records here
    '_claim_count': '0',
})
```

### Clear the DLQ

```bash
make shell-redis
```
```
DEL analytics.dlq
XGROUP CREATE analytics.dlq analytics-consumer-group 0 MKSTREAM
```

---

## Postgres operations

### Connect to the database

```bash
make shell-db
```

### Check tenant data

```sql
-- List all tenants
SELECT namespace, team_name, created_at FROM tenants ORDER BY created_at;

-- Check provisioning status
SELECT step_name, status, completed_at
FROM provisioning_log
WHERE tenant_id = (SELECT id FROM tenants WHERE namespace = 'analytics')
ORDER BY step_order;

-- Count records per namespace
SELECT t.namespace, COUNT(*) as pipeline_runs
FROM pipeline_runs pr
JOIN tenants t ON t.id = pr.tenant_id
GROUP BY t.namespace;
```

### Re-apply RLS policies

RLS policies must be re-applied after a fresh volume or after adding new tables:

```bash
make apply-rls
```

### Backup

```bash
# Backup Postgres
docker compose exec postgres pg_dump -U mtpif mtpif > backup_$(date +%Y%m%d).sql

# Restore
docker compose exec -i postgres psql -U mtpif mtpif < backup_20260422.sql
```

---

## Redis operations

### Check sentinel status

```bash
docker compose exec redis-sentinel redis-cli -p 26379 SENTINEL masters
docker compose exec redis-sentinel redis-cli -p 26379 SENTINEL slaves mtpif-master
```

### Check replication

```bash
docker compose exec redis-master redis-cli INFO replication
```

### Redis memory usage

```bash
make shell-redis
```
```
INFO memory
DBSIZE
```

---

## Prometheus and Grafana

### Reload Prometheus config

```bash
# After adding new scrape configs or changing alerts
curl -X POST http://localhost:9090/-/reload
```

### Check alert status

```bash
curl -s http://localhost:9090/api/v1/alerts | python3 -m json.tool
```

### Reset Grafana admin password

```bash
docker compose exec grafana grafana cli admin reset-admin-password newpassword
```

### Grafana datasource not connected

1. Open `http://localhost:3000/connections/datasources`
2. Click **Add data source** → Prometheus
3. URL: `http://prometheus:9090`
4. Click **Save & test**

If Prometheus itself is down:

```bash
docker compose restart prometheus
curl -s http://localhost:9090/-/healthy
```

---

## Troubleshooting

### Worker not processing messages

**Symptom:** Queue has messages but `pipeline_runs` has no new rows.

**Check:**
```bash
docker compose logs go-worker --tail=30
docker compose logs engine-worker --tail=30
```

**Common causes:**

1. **dbt target missing** — namespace not in `dbt/profiles.yml`
   ```bash
   docker compose exec go-worker dbt debug --project-dir /app/dbt --profiles-dir /app/dbt --target analytics
   ```

2. **Postgres connection failed** — check credentials in `.env`
   ```bash
   docker compose exec go-worker python3 -c "import asyncpg, asyncio; asyncio.run(asyncpg.connect(host='postgres', user='mtpif', password='postgres', database='mtpif'))"
   ```

3. **Consumer group missing** — stream was deleted without recreating the group
   ```bash
   make shell-redis
   XINFO GROUPS analytics.work-queue
   ```

4. **dbt not found** — PATH issue in subprocess
   ```bash
   docker compose exec go-worker which dbt
   ```

---

### dbt run fails

**Symptom:** `"error_message": "dbt run failed"` in pipeline_runs.

**Diagnose:**
```bash
docker compose exec go-worker dbt run \
  --project-dir /app/dbt \
  --profiles-dir /app/dbt \
  --vars '{"target_schema": "analytics_active"}' \
  --target analytics \
  --select customers \
  --no-partial-parse
```

**Common causes:**

1. **Missing staging table** — engine didn't write to staging (check edge case threshold)
2. **Wrong column names** — dbt model references a column that doesn't exist in staging
3. **Missing dbt target** — namespace not in `dbt/profiles.yml`
4. **Schema drift** — column type changed between runs

---

### Provisioner step fails

**Symptom:** `make tenant` stops at a step with FAILED.

**Re-run to resume:**
```bash
make tenant NS=analytics TEAM=analytics-team
```

**If the same step keeps failing:**

```bash
# Check error in provisioning_log
make shell-db
```
```sql
SELECT step_name, status, error_message
FROM provisioning_log
WHERE tenant_id = (SELECT id FROM tenants WHERE namespace = 'analytics')
ORDER BY step_order;
```

**Manually mark a step as done (use with caution):**
```sql
UPDATE provisioning_log
SET status = 'DONE', completed_at = NOW()
WHERE tenant_id = (SELECT id FROM tenants WHERE namespace = 'analytics')
AND step_name = 'grafana_org';
```

---

### Prometheus not scraping workers

**Symptom:** Grafana dashboards show "No data".

**Check:**
```bash
# Are targets registered?
curl -s http://localhost:9090/api/v1/targets | python3 -m json.tool | grep health

# Can Prometheus reach the worker?
docker compose exec prometheus wget -qO- http://engine-worker:9090/metrics | head -5

# Is the scrape config valid?
docker compose exec prometheus promtool check config /etc/prometheus/prometheus.yml
```

**Fix scrape config format:**

Each file in `scrape_configs/` must start with `scrape_configs:` (mapping),
not with `-` (list):

```yaml
# Correct
scrape_configs:
  - job_name: 'engine-analytics'
    ...

# Wrong
- job_name: 'engine-analytics'
  ...
```

---

### Kafka cluster ID mismatch

**Symptom:** Kafka container keeps restarting with `InconsistentClusterIdException`.

**Fix:**
```bash
docker compose down --volumes
docker compose up -d
```

This wipes all Kafka data. Re-provision all tenants afterwards.

---

## Performance tuning

### Worker throughput

Adjust in `docker-compose.yml` under the go-worker environment:

| Variable | Default | Effect |
|---|---|---|
| `BATCH_COUNT` | 10 | Records per XREADGROUP call |
| `BLOCK_MS` | 2000 | How long to wait for messages (ms) |
| `CLAIM_TIMEOUT_MS` | 45000 | When XAUTOCLAIM reclaims a message (ms) |
| `MAX_CLAIM_ATTEMPTS` | 3 | Attempts before DLQ routing |

### dbt performance

For large datasets, increase dbt threads in `dbt/profiles.yml`:

```yaml
analytics:
  threads: 8  # default: 4
```

### Postgres connection pool

Adjust in `api/db/connection.py`:

```python
pool = await asyncpg.create_pool(
    min_size=2,   # default
    max_size=10,  # default
)
```

---

## Log locations

| Service | Log command |
|---|---|
| API | `docker compose logs api` |
| Go worker | `docker compose logs go-worker` |
| Python worker | `docker compose logs engine-worker` |
| Postgres | `docker compose logs postgres` |
| Redis | `docker compose logs redis-master` |
| Prometheus | `docker compose logs prometheus` |
| Grafana | `docker compose logs grafana` |

All logs are JSON-file format, max 10MB per file, 3 files per service
(configured in `docker-compose.yml` logging section).
