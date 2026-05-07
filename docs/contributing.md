# Contributing to isolane

---

## Development setup

### Prerequisites

- Docker Desktop
- Python 3.12+
- Go 1.22+
- Make

### Clone and start

```bash
git clone https://github.com/tycoach/isolane
cd isolane
cp .env.example .env
docker compose up -d
make create-admin EMAIL=admin@isolane.dev PASSWORD=Isolane2026!
make tenant NS=analytics TEAM=analytics-team
```

### Run the tests

```bash
# Unit tests — run these before every commit
docker compose exec api python3 -m pytest tests/unit/ -v

# Isolation tests — run before every PR
docker compose exec \
  -e TEST_ADMIN_EMAIL=admin@isolane.dev \
  -e TEST_ADMIN_PASSWORD=Isolane2026! \
  api python3 -m pytest tests/isolation/ -v
```

All unit tests must pass. Isolation tests must pass for any change that
touches the engine, provisioner, worker, or API auth.

---

## Project layout

```
api/            FastAPI app — auth, routers, middleware, DB connection
engine/         Data engine — schema, edge cases, batch, quarantine
worker/go/      Go Redis consumer
worker/python/  Python Redis consumer (dev/fallback)
provisioner/    7-step tenant provisioner
dbt/            dbt project
metadata/       Postgres DDL
monitoring/     Prometheus, Grafana, Alertmanager
tests/unit/     Unit tests (no external deps)
tests/isolation/ Cross-tenant isolation tests
examples/       Usage examples
docs/           Documentation
```

---

## Coding standards

### Python

- Type hints on all function signatures
- Docstrings on all public functions and classes
- No hardcoded credentials — read from environment only
- `asyncpg` for async Postgres, `psycopg2` in provisioner steps (sync only)
- Print logging with `[module]` prefix — no `logging` module in workers

### Go

- `gofmt` before committing
- Error wrapping with `fmt.Errorf("context: %w", err)`
- No global state — pass config through function arguments
- All exported types and functions documented

### SQL

- Snake case for all identifiers
- `TIMESTAMPTZ` for all timestamps (not `TIMESTAMP`)
- `UUID` primary keys with `DEFAULT uuid_generate_v4()`
- Explicit column lists in INSERT statements

---

## Testing requirements

### Unit tests (`tests/unit/`)

Every new function in `engine/`, `provisioner/`, or `api/auth/` must have
a corresponding unit test. Unit tests must:
- Run without external services (no Postgres, Redis, or dbt)
- Complete in under 5 seconds total
- Use `pytest.fixture` for shared setup, not module-level globals

### Isolation tests (`tests/isolation/`)

Any change that affects tenant isolation must include an isolation test.
Isolation tests must:
- Test the actual isolation guarantee, not the implementation
- Skip gracefully when the required tenant is not provisioned
- Never hardcode credentials — read from `TEST_ADMIN_EMAIL` / `TEST_ADMIN_PASSWORD`

### Test naming

```python
def test_{what_it_does}():              # Unit test
def test_{what_it_proves}():            # Isolation test
class Test{Component}:                  # Test class grouping
```

---

## Pull request checklist

Before opening a PR:

- [ ] `tests/unit/` all pass
- [ ] `tests/isolation/` all pass (with `analytics` tenant provisioned)
- [ ] No hardcoded credentials in any file
- [ ] `.env.example` updated if new env vars were added
- [ ] `docs/` updated if behaviour changed
- [ ] Provisioner guide updated if a step was added or changed
- [ ] API reference updated if endpoints were added or changed

---

## Versioning

isolane follows semantic versioning:

- **Patch** (`v1.0.x`) — bug fixes, no API or behaviour changes
- **Minor** (`v1.x.0`) — new features, backward compatible
- **Major** (`vx.0.0`) — breaking changes to API, provisioner steps, or
  DB schema

Breaking changes require a migration guide in `docs/migrations/`.

---

## Reporting issues

Open a GitHub issue with:

1. isolane version (or commit hash)
2. Docker Compose version (`docker compose version`)
3. OS and architecture
4. Steps to reproduce
5. Expected vs actual behaviour
6. Relevant logs (`docker compose logs <service> --tail=50`)

For security issues, do not open a public issue.
Email the maintainer directly.
