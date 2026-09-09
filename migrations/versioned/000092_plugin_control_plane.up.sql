-- Phase A unified plugin control-plane persistence.
CREATE TABLE IF NOT EXISTS plugin_installations (
    id VARCHAR(36) PRIMARY KEY,
    plugin_id VARCHAR(255) NOT NULL,
    version VARCHAR(64) NOT NULL,
    artifact_digest VARCHAR(128) NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT FALSE,
    active BOOLEAN NOT NULL DEFAULT FALSE,
    install_status VARCHAR(32) NOT NULL CHECK (install_status IN ('installed', 'invalid')),
    last_error TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_plugin_installations_one_active
    ON plugin_installations (plugin_id) WHERE active = TRUE;
CREATE INDEX IF NOT EXISTS idx_plugin_installations_status
    ON plugin_installations (install_status);

CREATE TABLE IF NOT EXISTS datasource_plugin_bindings (
    data_source_id VARCHAR(36) PRIMARY KEY REFERENCES data_sources(id) ON DELETE CASCADE,
    installation_id VARCHAR(36) NOT NULL REFERENCES plugin_installations(id) ON DELETE RESTRICT,
    extension_id VARCHAR(255) NOT NULL,
    sandbox_id VARCHAR(255) NOT NULL DEFAULT '',
    generation BIGINT NOT NULL DEFAULT 1 CHECK (generation > 0),
    observed_generation BIGINT NOT NULL DEFAULT 0 CHECK (observed_generation >= 0),
    observed_state VARCHAR(32) NOT NULL DEFAULT 'STOPPED'
        CHECK (observed_state IN ('STARTING', 'READY', 'DEGRADED', 'NOT_READY', 'STOPPED', 'FAILED')),
    last_error TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_datasource_plugin_bindings_installation
    ON datasource_plugin_bindings (installation_id);

CREATE TABLE IF NOT EXISTS directory_grants (
    id VARCHAR(36) PRIMARY KEY,
    tenant_id BIGINT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    data_source_id VARCHAR(36) NOT NULL UNIQUE REFERENCES data_sources(id) ON DELETE CASCADE,
    allow_root_id VARCHAR(255) NOT NULL,
    canonical_host_path TEXT NOT NULL,
    device BIGINT NOT NULL CHECK (device >= 0),
    inode BIGINT NOT NULL CHECK (inode >= 0),
    generation BIGINT NOT NULL DEFAULT 1 CHECK (generation > 0),
    status VARCHAR(32) NOT NULL CHECK (status IN ('active', 'revoked')),
    created_by VARCHAR(255) NOT NULL,
    revoked_at TIMESTAMP NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
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
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    activated_at TIMESTAMP NULL,
    PRIMARY KEY (data_source_id, external_id, revision)
);

CREATE INDEX IF NOT EXISTS idx_datasource_plugin_revisions_pending
    ON datasource_plugin_revisions (state, updated_at) WHERE state = 'pending';
CREATE UNIQUE INDEX IF NOT EXISTS idx_datasource_plugin_revisions_one_active
    ON datasource_plugin_revisions (data_source_id, external_id) WHERE state = 'active';
