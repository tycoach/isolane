# ─────────────────────────────────────────────────────────────────
# isolane — Makefile
# ─────────────────────────────────────────────────────────────────

.PHONY: up down build ps logs shell-db shell-redis shell-api \
        keys keys-if-missing create-admin \
        tenant deprovision seed dbt-run \
        test test-isolation test-all \
        go-build go-test go-run help

-include .env
export

COMPOSE  = docker compose
PYTHON   = python3
NS       ?=
TEAM     ?=
MODE     ?= standard
EMAIL    ?=
PASSWORD ?=

# ── Lifecycle ─────────────────────────────────────────────────────

## up: Start all services
up: keys-if-missing
	@echo "Starting isolane stack..."
	@$(COMPOSE) up -d --build
	@echo ""
	@echo "  API:          http://localhost:8000/docs"
	@echo "  Grafana:      http://localhost:3000"
	@echo "  Prometheus:   http://localhost:9090"
	@echo "  Alertmanager: http://localhost:9093"
	@echo ""

## down: Stop all services (preserves data volumes)
down:
	@$(COMPOSE) down

## down-clean: Stop all services and wipe all data volumes
down-clean:
	@echo "WARNING: This will delete all data volumes."
	@read -p "  Continue? (yes/no): " confirm && \
		[ "$$confirm" = "yes" ] || (echo "Aborted." && exit 1)
	@$(COMPOSE) down --volumes

## build: Rebuild all images without cache
build:
	@$(COMPOSE) build --no-cache

## ps: Show service status
ps:
	@$(COMPOSE) ps

## logs: Tail logs (all services, or filter by NS)
##   Usage: make logs
##   Usage: make logs NS=analytics
logs:
	@if [ -z "$(NS)" ]; then \
		$(COMPOSE) logs -f; \
	else \
		$(COMPOSE) logs -f | grep "$(NS)"; \
	fi

# ── Keys ──────────────────────────────────────────────────────────

## keys: Generate RS256 key pair for JWT signing
keys:
	@echo "Generating RS256 key pair..."
	@mkdir -p keys
	@openssl genrsa -out keys/private.pem 4096 2>/dev/null
	@openssl rsa -in keys/private.pem -pubout -out keys/public.pem 2>/dev/null
	@chmod 600 keys/private.pem
	@echo "  Keys written to ./keys/ — never commit this directory"

keys-if-missing:
	@if [ ! -f keys/private.pem ]; then $(MAKE) keys; fi

## keys-rotate: Rotate RS256 key pair (old key valid for 15 min grace window)
keys-rotate:
	@echo "Rotating RS256 key pair..."
	@[ -f keys/public.pem ] && cp keys/public.pem keys/public.pem.prev || true
	@[ -f keys/private.pem ] && cp keys/private.pem keys/private.pem.prev || true
	@openssl genrsa -out keys/private.pem 4096 2>/dev/null
	@openssl rsa -in keys/private.pem -pubout -out keys/public.pem 2>/dev/null
	@chmod 600 keys/private.pem
	@date -u +"%Y-%m-%dT%H:%M:%SZ" > keys/public.pem.rotated_at
	@echo "  New keys generated. Old key valid for 15-minute grace window."

# ── Admin user ────────────────────────────────────────────────────

## create-admin: Create or update a platform operator user
##   Usage: make create-admin EMAIL=admin@isolane.dev PASSWORD=YourPassword123!
create-admin:
	@if [ -z "$(EMAIL)" ]; then \
		echo "Error: EMAIL is required."; \
		echo "Usage: make create-admin EMAIL=admin@isolane.dev PASSWORD=YourPassword123!"; \
		exit 1; \
	fi
	@if [ -z "$(PASSWORD)" ]; then \
		echo "Error: PASSWORD is required."; \
		exit 1; \
	fi
	@$(COMPOSE) exec api $(PYTHON) scripts/create_admin.py \
		--email $(EMAIL) \
		--password $(PASSWORD)

# ── Tenant management ─────────────────────────────────────────────

## tenant: Provision a new tenant namespace
##   Usage: make tenant NS=analytics TEAM=analytics-team
##   Usage: make tenant NS=finance TEAM=finance-ops MODE=fail_fast
tenant:
	@if [ -z "$(NS)" ]; then \
		echo "Error: NS is required."; \
		echo "Usage: make tenant NS=analytics TEAM=analytics-team"; \
		exit 1; \
	fi
	@if [ -z "$(TEAM)" ]; then \
		echo "Error: TEAM is required."; \
		exit 1; \
	fi
	@echo "Provisioning namespace: $(NS) for team: $(TEAM)"
	@$(COMPOSE) exec api $(PYTHON) provisioner/provision.py \
		--ns $(NS) --team $(TEAM) --mode $(MODE)

## deprovision: Remove a tenant namespace (destructive — requires confirmation)
##   Usage: make deprovision NS=analytics
deprovision:
	@if [ -z "$(NS)" ]; then echo "Error: NS is required."; exit 1; fi
	@echo ""
	@echo "  WARNING: This will permanently remove namespace '$(NS)'."
	@echo "  All schemas, ACLs, streams, and dashboards will be deleted."
	@echo ""
	@read -p "  Type the namespace to confirm: " confirm && \
		[ "$$confirm" = "$(NS)" ] || (echo "Aborted." && exit 1)
	@$(COMPOSE) exec api $(PYTHON) provisioner/deprovision.py --ns $(NS)

# ── Data operations ───────────────────────────────────────────────

## seed: Push synthetic test data for a tenant
##   Usage: make seed NS=analytics
seed:
	@if [ -z "$(NS)" ]; then echo "Error: NS is required."; exit 1; fi
	@$(COMPOSE) exec api $(PYTHON) scripts/seed.py --ns $(NS)

## dbt-run: Manually trigger dbt run for a tenant
##   Usage: make dbt-run NS=analytics
dbt-run:
	@if [ -z "$(NS)" ]; then echo "Error: NS is required."; exit 1; fi
	@echo "Running dbt for namespace: $(NS)"
	@$(COMPOSE) exec api dbt run \
		--project-dir dbt/ \
		--vars '{"target_schema": "$(NS)_active"}' \
		--target $(NS)

## reset: Wipe all state for one tenant and start fresh
##   Usage: make reset NS=analytics
reset:
	@if [ -z "$(NS)" ]; then echo "Error: NS is required."; exit 1; fi
	@echo ""
	@echo "  WARNING: This will wipe all state for namespace '$(NS)'."
	@read -p "  Confirm? (yes/no): " confirm && \
		[ "$$confirm" = "yes" ] || (echo "Aborted." && exit 1)
	@$(COMPOSE) exec api $(PYTHON) scripts/reset_tenant.py --ns $(NS)

# ── Testing ───────────────────────────────────────────────────────

## test: Run unit + integration tests
##   Usage: make test
##   Usage: make test NS=analytics
test:
	@if [ -z "$(NS)" ]; then \
		$(COMPOSE) exec api $(PYTHON) -m pytest tests/unit tests/integration -v; \
	else \
		$(COMPOSE) exec api $(PYTHON) -m pytest tests/unit tests/integration -k "$(NS)" -v; \
	fi

## test-isolation: Run full cross-tenant isolation test matrix (merge-blocking)
test-isolation:
	@echo "Running isolation test matrix..."
	@$(COMPOSE) exec api $(PYTHON) -m pytest tests/isolation/ -v --tb=short
	@echo "Isolation tests complete."

## test-all: Run all tests
test-all:
	@$(COMPOSE) exec api $(PYTHON) -m pytest tests/ -v

# ── Go worker ─────────────────────────────────────────────────────

## go-build: Build the Go worker binary
go-build:
	@echo "Building Go worker..."
	@cd worker/go && go build -o ../../bin/worker ./cmd/worker
	@echo "  Binary: ./bin/worker"

## go-test: Run Go worker tests with race detector
go-test:
	@cd worker/go && go test ./... -race -v

## go-run: Run Go worker locally (outside docker)
go-run:
	@REDIS_ADDR=localhost:6379 \
	 METRICS_PORT=9090 \
	 ROCKSDB_PATH=./data/rocksdb \
	 DBT_PROJECT_DIR=./dbt \
	 ./bin/worker

# ── Shells ────────────────────────────────────────────────────────

## shell-db: Open psql shell
shell-db:
	@$(COMPOSE) exec postgres psql \
		-U $${POSTGRES_USER:-mtpif} \
		$${POSTGRES_DB:-mtpif}

## shell-redis: Open redis-cli
shell-redis:
	@$(COMPOSE) exec redis-master redis-cli

## shell-api: Open shell in API container
shell-api:
	@$(COMPOSE) exec api /bin/bash

## shell-kafka: Open kafka-topics shell
shell-kafka:
	@$(COMPOSE) exec kafka bash

# ── Verification helpers ──────────────────────────────────────────

## check-health: Check all service health endpoints
check-health:
	@echo "Checking service health..."
	@echo -n "  API:        " && \
		curl -sf http://localhost:8000/health/ready > /dev/null && \
		echo "healthy" || echo "UNHEALTHY"
	@echo -n "  Prometheus: " && \
		curl -sf http://localhost:9090/-/healthy > /dev/null && \
		echo "healthy" || echo "UNHEALTHY"
	@echo -n "  Grafana:    " && \
		curl -sf http://localhost:3000/api/health > /dev/null && \
		echo "healthy" || echo "UNHEALTHY"
	@echo -n "  Schema Reg: " && \
		curl -sf http://localhost:8081/subjects > /dev/null && \
		echo "healthy" || echo "UNHEALTHY"

## check-tables: Verify all 7 DB tables exist with RLS status
check-tables:
	@$(COMPOSE) exec postgres psql \
		-U $${POSTGRES_USER:-mtpif} \
		$${POSTGRES_DB:-mtpif} \
		-c "SELECT relname AS table, relrowsecurity AS rls_enabled \
		    FROM pg_class \
		    WHERE relname IN ('users','tenants','pipelines','provisioning_log', \
		                      'refresh_tokens','pipeline_runs','revocation_audit') \
		    ORDER BY relname;"

## check-rls: Verify RLS policies are applied
check-rls:
	@$(COMPOSE) exec postgres psql \
		-U $${POSTGRES_USER:-mtpif} \
		$${POSTGRES_DB:-mtpif} \
		-c "SELECT tablename, policyname, cmd \
		    FROM pg_policies \
		    WHERE schemaname = 'public' \
		    ORDER BY tablename;"

## apply-rls: Manually apply RLS policies (run after fresh volume)
apply-rls:
	@echo "Applying RLS policies..."
	@$(COMPOSE) exec -T postgres psql \
		-U $${POSTGRES_USER:-mtpif} \
		$${POSTGRES_DB:-mtpif} < metadata/rls.sql
	@echo "Done."

## help: Show this help
help:
	@grep -E '^##' Makefile | sed 's/## //' | column -t -s ':'

.DEFAULT_GOAL := help