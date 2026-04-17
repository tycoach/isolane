-- ─────────────────────────────────────────────────────────────────
-- isolane — Row-level security policies
-- Applied after init.sql
-- ─────────────────────────────────────────────────────────────────

-- ── Helper: set tenant context ────────────────────────────────────
CREATE OR REPLACE FUNCTION set_tenant_context(p_namespace TEXT)
RETURNS VOID AS $$
BEGIN
    PERFORM set_config('app.tenant_namespace', p_namespace, TRUE);
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- ── Helper: get current tenant id ────────────────────────────────
CREATE OR REPLACE FUNCTION current_tenant_id()
RETURNS UUID AS $$
    SELECT id FROM tenants
    WHERE namespace = current_setting('app.tenant_namespace', TRUE)
    LIMIT 1;
$$ LANGUAGE sql STABLE SECURITY DEFINER;

-- ── Grant execute to the API role ─────────────────────────────────
DO $$
BEGIN
    IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'isolane_api') THEN
        GRANT EXECUTE ON FUNCTION set_tenant_context(TEXT) TO isolane_api;
        GRANT EXECUTE ON FUNCTION current_tenant_id() TO isolane_api;
    END IF;
END
$$;

-- ── users ─────────────────────────────────────────────────────────
ALTER TABLE users ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS users_tenant_isolation ON users;
CREATE POLICY users_tenant_isolation ON users
    USING (
        tenant_id IS NULL
        OR
        tenant_id = current_tenant_id()
    );

-- NOTE: We do NOT use FORCE ROW LEVEL SECURITY here
-- because the mtpif superuser role needs unrestricted access
-- for the provisioner and API pool. RLS applies to all
-- non-superuser connections automatically.

-- ── pipelines ─────────────────────────────────────────────────────
ALTER TABLE pipelines ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS pipelines_tenant_isolation ON pipelines;
CREATE POLICY pipelines_tenant_isolation ON pipelines
    USING (tenant_id = current_tenant_id());

-- ── refresh_tokens ────────────────────────────────────────────────
ALTER TABLE refresh_tokens ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS refresh_tokens_tenant_isolation ON refresh_tokens;
CREATE POLICY refresh_tokens_tenant_isolation ON refresh_tokens
    USING (
        tenant_id IS NULL
        OR
        tenant_id = current_tenant_id()
    );

-- ── pipeline_runs ─────────────────────────────────────────────────
ALTER TABLE pipeline_runs ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS pipeline_runs_tenant_isolation ON pipeline_runs;
CREATE POLICY pipeline_runs_tenant_isolation ON pipeline_runs
    USING (tenant_id = current_tenant_id());

-- Confirm RLS applied
DO $$
DECLARE
    tbl TEXT;
    enabled BOOLEAN;
BEGIN
    FOR tbl, enabled IN
        SELECT relname, relrowsecurity
        FROM pg_class
        WHERE relname IN ('users','pipelines','pipeline_runs','refresh_tokens')
        ORDER BY relname
    LOOP
        RAISE NOTICE 'RLS on %: %', tbl, enabled;
    END LOOP;
END
$$;