# Architecture Decision Records

Key design decisions made during the development of isolane v1,
with the reasoning behind each choice.

---

## ADR-001 — Redis Streams over Kafka for the work queue

**Decision:** Use Redis Streams (`XREADGROUP`, `XAUTOCLAIM`) as the primary
work queue instead of Kafka topics.

**Context:** The stack already includes Kafka for event streaming. The work
queue needs at-least-once delivery, consumer group semantics, and dead letter
queue support.

**Reasons for Redis Streams:**
- Kafka requires a broker, Zookeeper, and schema registry — Redis is already
  in the stack for other purposes
- Redis Streams has native consumer group support (`XREADGROUP`, `XACK`,
  `XAUTOCLAIM`) that matches our exactly-once processing model
- XAUTOCLAIM provides built-in re-delivery of timed-out messages without
  external orchestration
- Local dev is simpler — no Kafka CLI needed for basic testing
- Message payloads are small (JSON records) and don't need Kafka's partition
  model for this throughput level

**Trade-offs accepted:**
- No message replay beyond the stream retention window
- No partition-based parallelism (a single stream is consumed by one group)
- Redis memory limits message retention (Kafka can retain indefinitely on disk)

**Kafka remains in the stack** for event streaming from upstream systems.
The Redis work queue is the internal pipeline dispatch mechanism.

---

## ADR-002 — Go worker calling Python engine as a subprocess

**Decision:** The Go worker dispatches to the Python engine via subprocess
(`python3 -m engine.dispatch`), communicating via stdin/stdout JSON.

**Context:** We needed high-throughput Redis consumer loop semantics (Go's
goroutines, XREADGROUP, XAUTOCLAIM) but didn't want to rewrite the data
transformation engine (Polars, dbt) in Go.

**Reasons:**
- Go provides significantly better concurrency for the consumer loop than
  Python's GIL-constrained threading model
- The Python engine (Polars + dbt) is mature, well-tested, and would take
  weeks to rewrite in Go
- Subprocess isolation means a Python crash doesn't take down the Go worker
- The interface is simple: JSON in via stdin, JSON out via stdout, exit code
  signals success/failure
- This pattern is used in production by many systems (e.g. Hadoop streaming,
  AWS Lambda custom runtimes)

**Trade-offs accepted:**
- Process startup overhead (~200-500ms per batch) limits minimum latency
- No shared memory between batches — RocksDB is re-opened each call in
  the subprocess (mitigated by in-memory fallback in dev)
- The Go worker cannot inspect the engine's internal state

**v2 direction:** Rewrite the engine as a native Go library or gRPC service
to eliminate subprocess overhead and enable sub-second batch latency.

---

## ADR-003 — Postgres RLS over application-level filtering

**Decision:** Use Postgres Row-Level Security policies enforced by
`set_tenant_context()` rather than adding `WHERE tenant_id = $1` to every
query.

**Context:** The API pool connects as a single superuser (`mtpif`). We need
to ensure one tenant's API calls cannot read another tenant's data.

**Reasons:**
- RLS is enforced at the database level — a bug in the application cannot
  accidentally bypass it (unlike application-level WHERE clauses)
- The isolation is structural, not policy. Even if we add a new query
  endpoint and forget to add the WHERE clause, RLS prevents data leakage
- `set_tenant_context()` sets a session variable that RLS policies read —
  one call at connection checkout, zero per-query overhead
- RLS policies are version-controlled alongside the rest of the schema

**Trade-offs accepted:**
- The superuser (`mtpif`) bypasses RLS by design — this is required for
  the provisioner. The API pool uses a non-superuser role in production
- RLS adds a small query planning overhead (mitigated by GiST indexes on
  `tenant_id` columns)
- `FORCE ROW LEVEL SECURITY` was considered but rejected — it would prevent
  the provisioner from reading across tenants during multi-tenant operations

---

## ADR-004 — Idempotent provisioner state machine

**Decision:** Track each provisioning step in a `provisioning_log` table
with input hashing, enabling safe re-runs and partial failure recovery.

**Context:** Provisioning involves 7 steps across 5 different systems
(Postgres, Redis, RocksDB, Grafana, Prometheus). Any step can fail due to
network issues, temporary unavailability, or configuration errors.

**Reasons:**
- Without state tracking, a failed provisioning run leaves partial
  infrastructure that's hard to clean up manually
- Input hashing (`SHA-256` of canonical JSON inputs) enables the state
  machine to detect when inputs change and re-run the affected step
- The PENDING → DONE / FAILED → RETRY cycle is simple to reason about
  and easy to debug via the `provisioning_log` table
- Idempotency means provisioning can be put in CI/CD pipelines safely

**Alternative considered:** Terraform for provisioning. Rejected because:
- Terraform state files add operational complexity
- The provisioner needs to call Python code (psycopg2, redis-py, requests)
  that doesn't have clean Terraform providers
- The provisioner is part of the application, not the infrastructure layer

---

## ADR-005 — dbt for staging-to-active transformation

**Decision:** Use dbt Core (not dbt Cloud) as the transformation layer
between `{ns}_staging` and `{ns}_active`.

**Context:** Records are validated and written to staging by the Python
engine. We need a way to transform, deduplicate, and materialize them into
the queryable active schema.

**Reasons:**
- dbt's incremental models handle deduplication via `unique_key` without
  custom SQL
- dbt's `--target` flag maps cleanly to our namespace isolation model —
  one target per namespace in `profiles.yml`
- dbt provides automatic documentation, lineage, and testing
- dbt is the industry standard for SQL transformations — any data engineer
  joining the team will know it

**Trade-offs accepted:**
- dbt adds 5-15 seconds to each batch's processing time (compile + run)
- dbt is a Python subprocess inside a Python subprocess (engine → dbt)
  when called from the Go worker
- dbt's parse step validates all models even when `--select` targets one
  — fixed by removing `source()` macros and using direct schema interpolation

**v2 direction:** Compile dbt models to SQL at startup and execute them
directly via asyncpg, eliminating the dbt subprocess overhead.

---

## ADR-006 — RS256 JWT over symmetric tokens

**Decision:** Use RS256 (RSA asymmetric) JWT signing rather than HS256
(HMAC symmetric).

**Context:** The API issues JWTs that may be verified by downstream services
(other microservices, Grafana, external tools).

**Reasons:**
- RS256 allows downstream services to verify tokens using only the public key,
  without ever seeing the private key
- The JWKS endpoint (`/auth/jwks`) exposes the public key in standard format —
  any JWT library can verify tokens without calling the isolane API
- Key rotation is explicit — the `kid` (key ID) claim identifies which key
  signed the token
- HS256 requires sharing the secret with every service that needs to verify
  tokens, which is a security risk at scale

**Trade-offs accepted:**
- RSA key generation and management add operational complexity vs a simple
  shared secret
- RS256 signature verification is slightly slower than HS256 (mitigated by
  caching the public key in memory)

---

## ADR-007 — Prometheus label injection over per-tenant endpoints

**Decision:** Inject `tenant=` labels via Prometheus scrape config
(`honor_labels: false`) rather than having each worker expose
per-tenant metric endpoints.

**Context:** Workers handle multiple namespaces simultaneously. Grafana
dashboards need to filter metrics by namespace.

**Reasons:**
- `honor_labels: false` guarantees the injected label cannot be overridden
  by the worker — a compromised worker cannot spoof another tenant's metrics
- Workers don't need to know which tenant's perspective they're reporting
  from — they just report aggregate metrics
- Per-tenant Prometheus endpoints would require dynamic endpoint discovery
  and complex scrape config generation
- The provisioner writes one scrape config file per namespace — adding a
  namespace adds one file, no code changes

**Trade-offs accepted:**
- A single worker scrape exposes all tenants' metrics at once — the label
  provides logical separation but not physical separation
- In a high-security environment, per-tenant Prometheus instances would
  provide stronger isolation

---

## ADR-008 — Single Docker Compose for the full stack

**Decision:** Run the entire stack (11 services) in Docker Compose rather
than Kubernetes for v1.

**Context:** isolane needs to be deployable by a single engineer on a
laptop for development and on a single VM for small production deployments.

**Reasons:**
- Docker Compose requires no cluster setup, no kubectl, no Helm charts
- The entire stack starts with one command: `docker compose up -d`
- Development and production environments are identical (same images)
- Docker Compose volume mounts make local file changes immediately visible
  without rebuilds (useful for config files, dbt models)
- The target user for v1 is a platform team at a startup, not an enterprise
  with a dedicated Kubernetes cluster

**v2 direction:** Ship a Kubernetes operator that watches the `tenants` table
and provisions namespaces as custom resources, with KEDA for auto-scaling
workers based on queue depth.
