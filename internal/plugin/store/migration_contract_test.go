package store

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPostgreSQLAndSQLitePluginMigrationStructuresMatch(t *testing.T) {
	root := filepath.Clean(filepath.Join("..", "..", ".."))
	postgres := readMigration(t, filepath.Join(root, "migrations", "versioned", "000091_plugin_control_plane.up.sql"))
	sqlite := readMigration(t, filepath.Join(root, "migrations", "sqlite", "000013_plugin_control_plane.up.sql"))

	tables := map[string][]string{
		"plugin_installations": {
			"id", "plugin_id", "version", "artifact_digest", "enabled", "active", "install_status", "last_error", "created_at", "updated_at",
		},
		"datasource_plugin_bindings": {
			"data_source_id", "installation_id", "extension_id", "sandbox_id", "generation", "observed_generation", "observed_state", "last_error", "created_at", "updated_at",
		},
		"directory_grants": {
			"id", "tenant_id", "data_source_id", "allow_root_id", "canonical_host_path", "device", "inode", "generation", "status", "created_by", "revoked_at", "created_at", "updated_at",
		},
		"datasource_plugin_revisions": {
			"data_source_id", "external_id", "revision", "knowledge_id", "state", "last_error", "created_at", "updated_at", "activated_at",
		},
	}
	for table, columns := range tables {
		for _, migration := range []struct {
			name string
			sql  string
		}{{"postgresql", postgres}, {"sqlite", sqlite}} {
			section := tableSection(migration.sql, table)
			if section == "" {
				t.Errorf("%s migration missing table %s", migration.name, table)
				continue
			}
			for _, column := range columns {
				if !strings.Contains(section, "\n    "+column+" ") {
					t.Errorf("%s migration missing %s.%s", migration.name, table, column)
				}
			}
		}
	}
	for _, index := range []string{
		"idx_plugin_installations_one_active",
		"idx_datasource_plugin_bindings_installation",
		"idx_directory_grants_tenant_status",
		"idx_datasource_plugin_revisions_pending",
		"idx_datasource_plugin_revisions_one_active",
	} {
		if !strings.Contains(postgres, index) || !strings.Contains(sqlite, index) {
			t.Errorf("index %s is not present in both migrations", index)
		}
	}
}

func readMigration(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return strings.ToLower(string(data))
}

func tableSection(sql, table string) string {
	start := strings.Index(sql, "create table if not exists "+table)
	if start < 0 {
		return ""
	}
	rest := sql[start:]
	end := strings.Index(rest, ");")
	if end < 0 {
		return ""
	}
	return rest[:end]
}
