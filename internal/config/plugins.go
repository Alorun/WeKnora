package config

import (
	"fmt"
	"os"

	"github.com/Tencent/WeKnora/internal/plugin/control"
	"github.com/Tencent/WeKnora/internal/plugin/sandbox/docker/paths"
)

// ExternalPluginsConfig is the administrator-only YAML input to the existing
// Docker Config and prepare.Builder. No request may override these values.
type ExternalPluginsConfig struct {
	Enabled         bool                         `yaml:"enabled" json:"enabled"`
	DeploymentID    string                       `yaml:"deployment_id" json:"-"`
	BackendReplicas int                          `yaml:"backend_replicas" json:"-"`
	DockerHost      string                       `yaml:"docker_host" json:"-"`
	Image           string                       `yaml:"image" json:"-"`
	GateAppPath     string                       `yaml:"gate_app_path" json:"-"`
	GateHostPath    string                       `yaml:"gate_host_path" json:"-"`
	Packages        paths.PathMapping            `yaml:"packages" json:"-"`
	RuntimeRoot     paths.PathMapping            `yaml:"runtime_root" json:"-"`
	ArtifactRoot    paths.PathMapping            `yaml:"artifact_root" json:"-"`
	AllowRoots      map[string]paths.PathMapping `yaml:"allow_roots" json:"-"`
	AdminUID        uint32                       `yaml:"admin_uid" json:"-"`
	PluginUID       uint32                       `yaml:"plugin_uid" json:"-"`
	PluginGID       uint32                       `yaml:"plugin_gid" json:"-"`
	MaxInstances    int                          `yaml:"max_instances" json:"-"`
	MaxResources    control.ManifestResources    `yaml:"max_resources" json:"-"`
}

func (c ExternalPluginsConfig) Validate() error {
	if !c.Enabled {
		return nil
	}
	if c.BackendReplicas != 1 || c.DeploymentID == "" || os.Getenv("REDIS_ADDR") == "" {
		return fmt.Errorf("external_plugins requires backend_replicas=1, stable deployment_id and REDIS_ADDR")
	}
	if c.Image == "" || c.GateAppPath == "" || c.GateHostPath == "" || len(c.AllowRoots) == 0 || c.MaxInstances < 1 {
		return fmt.Errorf("external_plugins requires fixed image, gate paths, allow_roots and max_instances")
	}
	return nil
}
