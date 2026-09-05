// Package control defines the stable, runtime-independent plugin control-plane model.
package control

import (
	"encoding/json"
	"fmt"
	"regexp"
	"time"

	"golang.org/x/mod/semver"
)

type PluginID string
type ExtensionID string

type ExtensionType string

const (
	ExtensionDataSource      ExtensionType = "datasource"
	ExtensionDocumentParser  ExtensionType = "document_parser"
	ExtensionWebSearch       ExtensionType = "web_search"
	ExtensionModelProvider   ExtensionType = "model_provider"
	ExtensionRetrievalEngine ExtensionType = "retrieval_engine"
)

type SourceType string

const (
	SourceBuiltin  SourceType = "builtin"
	SourceExternal SourceType = "external"
)

type PluginState string

const (
	StateStarting PluginState = "STARTING"
	StateReady    PluginState = "READY"
	StateDegraded PluginState = "DEGRADED"
	StateNotReady PluginState = "NOT_READY"
	StateStopped  PluginState = "STOPPED"
	StateFailed   PluginState = "FAILED"
)

const (
	InstallStatusInstalled = "installed"
	InstallStatusInvalid   = "invalid"
	GrantStatusActive      = "active"
	GrantStatusRevoked     = "revoked"
	RevisionPending        = "pending"
	RevisionActive         = "active"
	RevisionFailed         = "failed"
	RevisionSuperseded     = "superseded"
)

var pluginIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]*\.[a-z0-9][a-z0-9_-]*$`)
var extensionIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]*$`)

type PluginDefinition struct {
	ID              PluginID
	Name            string
	Version         string
	Source          SourceType
	ExtensionID     ExtensionID
	ExtensionType   ExtensionType
	ProtocolVersion string
	ContractVersion string
	Capabilities    []string
	Manifest        *Manifest
}

func (d PluginDefinition) Validate() error {
	if !pluginIDPattern.MatchString(string(d.ID)) {
		return fmt.Errorf("invalid plugin id %q: expected publisher.name", d.ID)
	}
	if d.Name == "" {
		return fmt.Errorf("plugin name is required")
	}
	if !extensionIDPattern.MatchString(string(d.ExtensionID)) {
		return fmt.Errorf("invalid extension id %q", d.ExtensionID)
	}
	if !validVersion(d.Version) {
		return fmt.Errorf("invalid plugin version %q", d.Version)
	}
	if d.Source != SourceBuiltin && d.Source != SourceExternal {
		return fmt.Errorf("invalid plugin source %q", d.Source)
	}
	if !d.ExtensionType.Valid() {
		return fmt.Errorf("unsupported extension type %q", d.ExtensionType)
	}
	if d.Source == SourceExternal && d.ExtensionType != ExtensionDataSource {
		return fmt.Errorf("external plugins only support extension type %q", ExtensionDataSource)
	}
	return nil
}

func (t ExtensionType) Valid() bool {
	switch t {
	case ExtensionDataSource, ExtensionDocumentParser, ExtensionWebSearch, ExtensionModelProvider, ExtensionRetrievalEngine:
		return true
	default:
		return false
	}
}

func validVersion(version string) bool {
	return semver.IsValid("v" + version)
}

func CloneDefinition(d PluginDefinition) PluginDefinition {
	out := d
	out.Capabilities = append([]string(nil), d.Capabilities...)
	if d.Manifest != nil {
		cloned := d.Manifest.Clone()
		out.Manifest = &cloned
	}
	return out
}

type PluginInstallation struct {
	ID             string    `json:"id" gorm:"type:varchar(36);primaryKey"`
	PluginID       PluginID  `json:"plugin_id" gorm:"type:varchar(255);not null;index"`
	Version        string    `json:"version" gorm:"type:varchar(64);not null"`
	ArtifactDigest string    `json:"artifact_digest" gorm:"type:varchar(128);not null"`
	Enabled        bool      `json:"enabled" gorm:"not null;default:false"`
	Active         bool      `json:"active" gorm:"not null;default:false"`
	InstallStatus  string    `json:"install_status" gorm:"type:varchar(32);not null"`
	LastError      string    `json:"last_error" gorm:"type:text"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

func (PluginInstallation) TableName() string { return "plugin_installations" }

type DataSourcePluginBinding struct {
	DataSourceID       string      `json:"data_source_id" gorm:"type:varchar(36);primaryKey"`
	InstallationID     string      `json:"installation_id" gorm:"type:varchar(36);not null;index"`
	ExtensionID        ExtensionID `json:"extension_id" gorm:"type:varchar(255);not null"`
	SandboxID          string      `json:"sandbox_id" gorm:"type:varchar(255)"`
	Generation         uint64      `json:"generation" gorm:"not null;default:1"`
	ObservedGeneration uint64      `json:"observed_generation" gorm:"not null;default:0"`
	ObservedState      PluginState `json:"observed_state" gorm:"type:varchar(32);not null"`
	LastError          string      `json:"last_error" gorm:"type:text"`
	CreatedAt          time.Time   `json:"created_at"`
	UpdatedAt          time.Time   `json:"updated_at"`
}

func (DataSourcePluginBinding) TableName() string { return "datasource_plugin_bindings" }

type DirectoryGrant struct {
	ID                string     `json:"id" gorm:"type:varchar(36);primaryKey"`
	TenantID          uint64     `json:"tenant_id" gorm:"not null;index"`
	DataSourceID      string     `json:"data_source_id" gorm:"type:varchar(36);not null;uniqueIndex"`
	AllowRootID       string     `json:"allow_root_id" gorm:"type:varchar(255);not null"`
	CanonicalHostPath string     `json:"-" gorm:"type:text;not null"`
	Device            uint64     `json:"device" gorm:"not null"`
	Inode             uint64     `json:"inode" gorm:"not null"`
	Generation        uint64     `json:"generation" gorm:"not null;default:1"`
	Status            string     `json:"status" gorm:"type:varchar(32);not null"`
	CreatedBy         string     `json:"created_by" gorm:"type:varchar(255);not null"`
	RevokedAt         *time.Time `json:"revoked_at"`
	CreatedAt         time.Time  `json:"created_at"`
	UpdatedAt         time.Time  `json:"updated_at"`
}

func (DirectoryGrant) TableName() string { return "directory_grants" }

type DataSourceRevision struct {
	DataSourceID string     `json:"data_source_id" gorm:"type:varchar(36);primaryKey"`
	ExternalID   string     `json:"external_id" gorm:"type:varchar(1024);primaryKey"`
	Revision     string     `json:"revision" gorm:"type:varchar(255);primaryKey"`
	KnowledgeID  *string    `json:"knowledge_id" gorm:"type:varchar(36);index"`
	State        string     `json:"state" gorm:"type:varchar(32);not null;index"`
	LastError    string     `json:"last_error" gorm:"type:text"`
	CreatedAt    time.Time  `json:"created_at"`
	UpdatedAt    time.Time  `json:"updated_at"`
	ActivatedAt  *time.Time `json:"activated_at"`
}

func (DataSourceRevision) TableName() string { return "datasource_plugin_revisions" }

type PluginStatus struct {
	PluginID  PluginID
	State     PluginState
	LastError string
	UpdatedAt time.Time
}

func cloneJSONMap(value map[string]any) map[string]any {
	if value == nil {
		return nil
	}
	data, _ := json.Marshal(value)
	var out map[string]any
	_ = json.Unmarshal(data, &out)
	return out
}
