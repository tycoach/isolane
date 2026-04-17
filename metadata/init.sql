-- ─────────────────────────────────────────────────────────────────
-- isolane — metadata store schema
-- Runs once on first postgres container start
-- ─────────────────────────────────────────────────────────────────

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ── users ─────────────────────────────────────────────────────────
-- tenant_id is nullable — NULL means platform operator
CREATE TABLE users (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id       UUID,                                   -- FK added after tenants table
    email           TEXT NOT NULL UNIQUE,
    password_hash   TEXT NOT NULL,
    role            TEXT NOT NULL CHECK (role IN (
                        'platform_operator',
                        'admin',
                        'engineer',
                        'analyst'
                    )),
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    last_login_at   TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT platform_operator_no_tenant CHECK (
        (role = 'platform_operator' AND tenant_id IS NULL)
        OR
        (role != 'platform_operator' AND tenant_id IS NOT NULL)
    )
);

-- ── tenants ───────────────────────────────────────────────────────
CREATE TABLE tenants (
    id               UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    namespace        TEXT NOT NULL UNIQUE,
    team_name        TEXT NOT NULL,
    mode             TEXT NOT NULL DEFAULT 'standard' CHECK (
                         mode IN ('standard', 'fail_fast')
                     ),
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deprovisioned_at TIMESTAMPTZ,
    CONSTRAINT namespace_format CHECK (
        namespace ~ '^[a-z][a-z0-9_]{1,30}$'
    )
);

--  add the FK from users to tenants
ALTER TABLE users
    ADD CONSTRAINT users_tenant_id_fkey
    FOREIGN KEY (tenant_id) REFERENCES tenants(id);

-- ── pipelines ─────────────────────────────────────────────────────
CREATE TABLE pipelines (
    id            UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id     UUID NOT NULL REFERENCES tenants(id),
    pipeline_id   TEXT NOT NULL,
    kafka_topic   TEXT NOT NULL,
    natural_key   TEXT NOT NULL,
    scd_type      INT NOT NULL CHECK (scd_type IN (1, 2)),
    config_yaml   TEXT NOT NULL,
    xautoclaim_ms INT NOT NULL DEFAULT 45000,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, pipeline_id)
);

-- ── provisioning_log ──────────────────────────────────────────────
CREATE TABLE provisioning_log (
    id            UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id     UUID NOT NULL REFERENCES tenants(id),
    step_name     TEXT NOT NULL,
    step_order    INT NOT NULL,
    status        TEXT NOT NULL DEFAULT 'PENDING' CHECK (
                      status IN ('PENDING', 'DONE', 'FAILED')
                  ),
    input_hash    TEXT,
    error_message TEXT,
    started_at    TIMESTAMPTZ,
    completed_at  TIMESTAMPTZ,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, step_name)
);

-- ── refresh_tokens ────────────────────────────────────────────────
CREATE TABLE refresh_tokens (
    id         UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    session_id UUID NOT NULL UNIQUE,
    user_id    UUID NOT NULL REFERENCES users(id),
    tenant_id  UUID REFERENCES tenants(id),           -- NULL for platform operator sessions
    revoked    BOOLEAN NOT NULL DEFAULT FALSE,
    revoked_at TIMESTAMPTZ,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── pipeline_runs ─────────────────────────────────────────────────
-- pipeline_id is a logical reference only — no FK to pipelines.
-- Deleting a pipeline must never cascade-delete its run history.
CREATE TABLE pipeline_runs (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id           UUID NOT NULL REFERENCES tenants(id),
    pipeline_id         TEXT NOT NULL,
    batch_offset        TEXT NOT NULL,
    stream_message_id   TEXT,                          -- Redis Streams message ID e.g. 1712345678901-0
    status              TEXT NOT NULL CHECK (
                            status IN ('running', 'done', 'failed', 'quarantined')
                        ),
    records_in          INT,
    records_out         INT,
    records_quarantined INT,
    started_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at        TIMESTAMPTZ,
    error_message       TEXT
);

-- ── revocation_audit ──────────────────────────────────────────────
-- Immutable append-only table. No UPDATE or DELETE ever.
-- actor_email is denormalised intentionally — audit records must
-- survive user deletion with full actor information intact.
CREATE TABLE revocation_audit (
    id               UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id          UUID REFERENCES users(id),        -- soft ref, nullable on user deletion
    actor_email      TEXT NOT NULL,                    -- denormalised — survives user deletion
    session_id       UUID NOT NULL,
    tenant_namespace TEXT NOT NULL,
    reason           TEXT NOT NULL,
    triggered_by     TEXT NOT NULL CHECK (
                         triggered_by IN (
                             'user_logout',
                             'admin_forced',
                             'auto_expiry'
                         )
                     ),
    ip_address       INET,
    token_expiry     TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── Indexes ───────────────────────────────────────────────────────
CREATE INDEX idx_users_tenant          ON users(tenant_id);
CREATE INDEX idx_users_email           ON users(email);
CREATE INDEX idx_tenants_namespace     ON tenants(namespace);
CREATE INDEX idx_pipelines_tenant      ON pipelines(tenant_id);
CREATE INDEX idx_prov_log_tenant       ON provisioning_log(tenant_id);
CREATE INDEX idx_prov_log_status       ON provisioning_log(tenant_id, status);
CREATE INDEX idx_refresh_session       ON refresh_tokens(session_id);
CREATE INDEX idx_refresh_user          ON refresh_tokens(user_id);
CREATE INDEX idx_pipeline_runs_tenant  ON pipeline_runs(tenant_id, started_at DESC);
CREATE INDEX idx_revocation_session    ON revocation_audit(session_id);
CREATE INDEX idx_revocation_namespace  ON revocation_audit(tenant_namespace);

-- ── Immutability trigger for revocation_audit ─────────────────────
CREATE OR REPLACE FUNCTION deny_revocation_audit_mutation()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION
        'revocation_audit is immutable — UPDATE and DELETE are not permitted';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER revocation_audit_immutable
    BEFORE UPDATE OR DELETE ON revocation_audit
    FOR EACH ROW EXECUTE FUNCTION deny_revocation_audit_mutation();

-- ── updated_at auto-update trigger ───────────────────────────────
CREATE OR REPLACE FUNCTION touch_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER users_updated_at
    BEFORE UPDATE ON users
    FOR EACH ROW EXECUTE FUNCTION touch_updated_at();

CREATE TRIGGER pipelines_updated_at
    BEFORE UPDATE ON pipelines
    FOR EACH ROW EXECUTE FUNCTION touch_updated_at();

-- ── Platform API role ─────────────────────────────────────────────
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'isolane_api') THEN
        CREATE ROLE isolane_api LOGIN PASSWORD 'change_me_dev';
    END IF;
END
$$;

GRANT CONNECT ON DATABASE mtpif TO isolane_api;
GRANT USAGE ON SCHEMA public TO isolane_api;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO isolane_api;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO isolane_api;