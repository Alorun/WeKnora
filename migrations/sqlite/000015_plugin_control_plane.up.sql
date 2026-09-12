-- Phase A unified plugin control-plane persistence.
-- This branch previously used version 14 for these tables, before upstream
-- assigned it to browser authorization. Reconcile either version-14 history:
-- plugin DDL is idempotent, and the browser baseline is ensured below.
CREATE TABLE IF NOT EXISTS plugin_installations (
    id VARCHAR(36) PRIMARY KEY,
    plugin_id VARCHAR(255) NOT NULL,
    version VARCHAR(64) NOT NULL,
    artifact_digest VARCHAR(128) NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 0,
    active INTEGER NOT NULL DEFAULT 0,
    install_status VARCHAR(32) NOT NULL CHECK (install_status IN ('installed', 'invalid')),
    last_error TEXT NOT NULL DEFAULT '',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_plugin_installations_one_active
    ON plugin_installations (plugin_id) WHERE active = 1;
CREATE INDEX IF NOT EXISTS idx_plugin_installations_status
    ON plugin_installations (install_status);

CREATE TABLE IF NOT EXISTS datasource_plugin_bindings (
    data_source_id VARCHAR(36) PRIMARY KEY REFERENCES data_sources(id) ON DELETE CASCADE,
    installation_id VARCHAR(36) NOT NULL REFERENCES plugin_installations(id) ON DELETE RESTRICT,
    extension_id VARCHAR(255) NOT NULL,
    sandbox_id VARCHAR(255) NOT NULL DEFAULT '',
    generation INTEGER NOT NULL DEFAULT 1 CHECK (generation > 0),
    observed_generation INTEGER NOT NULL DEFAULT 0 CHECK (observed_generation >= 0),
    observed_state VARCHAR(32) NOT NULL DEFAULT 'STOPPED'
        CHECK (observed_state IN ('STARTING', 'READY', 'DEGRADED', 'NOT_READY', 'STOPPED', 'FAILED')),
    last_error TEXT NOT NULL DEFAULT '',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_datasource_plugin_bindings_installation
    ON datasource_plugin_bindings (installation_id);

CREATE TABLE IF NOT EXISTS directory_grants (
    id VARCHAR(36) PRIMARY KEY,
    tenant_id INTEGER NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    data_source_id VARCHAR(36) NOT NULL UNIQUE REFERENCES data_sources(id) ON DELETE CASCADE,
    allow_root_id VARCHAR(255) NOT NULL,
    canonical_host_path TEXT NOT NULL,
    device INTEGER NOT NULL CHECK (device >= 0),
    inode INTEGER NOT NULL CHECK (inode >= 0),
    generation INTEGER NOT NULL DEFAULT 1 CHECK (generation > 0),
    status VARCHAR(32) NOT NULL CHECK (status IN ('active', 'revoked')),
    created_by VARCHAR(255) NOT NULL,
    revoked_at DATETIME NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_directory_grants_tenant_status
    ON directory_grants (tenant_id, status);

CREATE TABLE IF NOT EXISTS datasource_plugin_revisions (
    data_source_id VARCHAR(36) NOT NULL REFERENCES data_sources(id) ON DELETE CASCADE,
    external_id VARCHAR(1024) NOT NULL,
    revision VARCHAR(255) NOT NULL,
    knowledge_id VARCHAR(36) NULL REFERENCES knowledges(id) ON DELETE SET NULL,
    state VARCHAR(32) NOT NULL CHECK (state IN ('pending', 'active', 'failed', 'superseded')),
    last_error TEXT NOT NULL DEFAULT '',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    activated_at DATETIME NULL,
    PRIMARY KEY (data_source_id, external_id, revision)
);

CREATE INDEX IF NOT EXISTS idx_datasource_plugin_revisions_pending
    ON datasource_plugin_revisions (state, updated_at) WHERE state = 'pending';
CREATE UNIQUE INDEX IF NOT EXISTS idx_datasource_plugin_revisions_one_active
    ON datasource_plugin_revisions (data_source_id, external_id) WHERE state = 'active';

-- Backfill the upstream baseline for databases that applied the old plugin 14.
CREATE TABLE IF NOT EXISTS browser_devices (
 scope_key VARCHAR(32) PRIMARY KEY,
 id VARCHAR(32) NOT NULL UNIQUE,
 tenant BIGINT NOT NULL,
 "user" VARCHAR(36) NOT NULL,
 label VARCHAR(100) NOT NULL,
 token_hash VARCHAR(64) NOT NULL UNIQUE,
 previous_hash VARCHAR(64) NOT NULL DEFAULT '',
 previous_until DATETIME NOT NULL,
 expires_at DATETIME NOT NULL,
 renew_after DATETIME NOT NULL,
 created_at DATETIME NOT NULL,
 last_seen_at DATETIME NOT NULL,
 revoked_at DATETIME,
 owner VARCHAR(32) NOT NULL DEFAULT '',
 owner_url VARCHAR(500) NOT NULL DEFAULT '',
 lease_key VARCHAR(32) NOT NULL DEFAULT '',
 lease_until DATETIME NOT NULL
);
CREATE TABLE IF NOT EXISTS browser_pairings (
 scope_key VARCHAR(32) PRIMARY KEY,
 token_hash VARCHAR(64) NOT NULL UNIQUE,
 tenant BIGINT NOT NULL,
 "user" VARCHAR(36) NOT NULL,
 expires_at DATETIME NOT NULL
);
CREATE INDEX IF NOT EXISTS browser_pairings_expiry ON browser_pairings(expires_at);
CREATE TABLE IF NOT EXISTS browser_task_interruptions (
 scope_key VARCHAR(32) NOT NULL,
 session VARCHAR(36) NOT NULL,
 PRIMARY KEY (scope_key, session)
);
