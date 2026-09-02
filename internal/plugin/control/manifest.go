package control

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	jsonschema "github.com/santhosh-tekuri/jsonschema/v6"
	"golang.org/x/mod/semver"
	"gopkg.in/yaml.v3"
)

const (
	ManifestAPIVersion = "plugins.weknora.io/v1alpha1"
	ManifestKind       = "Plugin"
	RuntimeSandbox     = "sandbox_service"
	TransportUDS       = "uds"
	NetworkNone        = "none"
	FilesystemReadOnly = "selected_directory_readonly"
	ProtocolV1         = "1.0"
)

var protocolVersionPattern = regexp.MustCompile(`^[0-9]+\.[0-9]+$`)

type Manifest struct {
	APIVersion string           `yaml:"apiVersion" json:"apiVersion"`
	Kind       string           `yaml:"kind" json:"kind"`
	Metadata   ManifestMetadata `yaml:"metadata" json:"metadata"`
	Spec       ManifestSpec     `yaml:"spec" json:"spec"`
}

type ManifestMetadata struct {
	ID      PluginID `yaml:"id" json:"id"`
	Name    string   `yaml:"name" json:"name"`
	Version string   `yaml:"version" json:"version"`
}

type ManifestSpec struct {
	ProtocolVersion string                `yaml:"protocolVersion" json:"protocolVersion"`
	Compatibility   ManifestCompatibility `yaml:"compatibility" json:"compatibility"`
	Extension       ManifestExtension     `yaml:"extension" json:"extension"`
	Runtime         ManifestRuntime       `yaml:"runtime" json:"runtime"`
	Permissions     ManifestPermissions   `yaml:"permissions" json:"permissions"`
	Resources       ManifestResources     `yaml:"resources" json:"resources"`
	ConfigSchema    map[string]any        `yaml:"configSchema" json:"configSchema"`
}

type ManifestCompatibility struct {
	WeKnora string `yaml:"weknora" json:"weknora"`
}

type ManifestExtension struct {
	ID              ExtensionID   `yaml:"id" json:"id"`
	Type            ExtensionType `yaml:"type" json:"type"`
	ContractVersion string        `yaml:"contractVersion" json:"contractVersion"`
	Capabilities    []string      `yaml:"capabilities" json:"capabilities"`
}

type ManifestRuntime struct {
	Kind       string `yaml:"kind" json:"kind"`
	Entrypoint string `yaml:"entrypoint" json:"entrypoint"`
	Transport  string `yaml:"transport" json:"transport"`
}

type ManifestPermissions struct {
	Network    string `yaml:"network" json:"network"`
	Filesystem string `yaml:"filesystem" json:"filesystem"`
}

type ManifestResources struct {
	MemoryMiB    int     `yaml:"memoryMiB" json:"memoryMiB"`
	CPUQuota     float64 `yaml:"cpuQuota" json:"cpuQuota"`
	MaxProcesses int64   `yaml:"maxProcesses" json:"maxProcesses"`
}

type ManifestValidationOptions struct {
	WeKnoraVersion string
	BuiltinIDs     map[PluginID]struct{}
	ArtifactRoot   string
}

func ParseManifest(data []byte, opts ManifestValidationOptions) (Manifest, error) {
	var manifest Manifest
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(&manifest); err != nil {
		return Manifest{}, fmt.Errorf("decode plugin manifest: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return Manifest{}, errors.New("decode plugin manifest: multiple YAML documents are not allowed")
		}
		return Manifest{}, fmt.Errorf("decode plugin manifest trailer: %w", err)
	}
	if err := manifest.Validate(opts); err != nil {
		return Manifest{}, err
	}
	return manifest, nil
}

func (m Manifest) Validate(opts ManifestValidationOptions) error {
	if m.APIVersion != ManifestAPIVersion {
		return fmt.Errorf("unsupported apiVersion %q", m.APIVersion)
	}
	if m.Kind != ManifestKind {
		return fmt.Errorf("unsupported kind %q", m.Kind)
	}
	if !pluginIDPattern.MatchString(string(m.Metadata.ID)) {
		return fmt.Errorf("invalid metadata.id %q: expected publisher.name", m.Metadata.ID)
	}
	if strings.TrimSpace(m.Metadata.Name) == "" {
		return errors.New("metadata.name is required")
	}
	if !validVersion(m.Metadata.Version) {
		return fmt.Errorf("invalid metadata.version %q", m.Metadata.Version)
	}
	if _, reserved := opts.BuiltinIDs[m.Metadata.ID]; reserved || strings.HasPrefix(string(m.Metadata.ID), "builtin.") {
		return fmt.Errorf("external plugin id %q conflicts with a builtin plugin", m.Metadata.ID)
	}
	if !protocolVersionPattern.MatchString(m.Spec.ProtocolVersion) || m.Spec.ProtocolVersion != ProtocolV1 {
		return fmt.Errorf("unsupported protocolVersion %q", m.Spec.ProtocolVersion)
	}
	if !protocolVersionPattern.MatchString(m.Spec.Extension.ContractVersion) || m.Spec.Extension.ContractVersion != ProtocolV1 {
		return fmt.Errorf("unsupported contractVersion %q", m.Spec.Extension.ContractVersion)
	}
	if err := validateCompatibility(m.Spec.Compatibility.WeKnora, opts.WeKnoraVersion); err != nil {
		return err
	}
	if !extensionIDPattern.MatchString(string(m.Spec.Extension.ID)) {
		return fmt.Errorf("invalid extension.id %q", m.Spec.Extension.ID)
	}
	if m.Spec.Extension.Type != ExtensionDataSource {
		return fmt.Errorf("unsupported extension type %q: V1 external plugins only support datasource", m.Spec.Extension.Type)
	}
	if len(m.Spec.Extension.Capabilities) == 0 {
		return errors.New("extension.capabilities must not be empty")
	}
	if err := validateUniqueStrings("extension.capabilities", m.Spec.Extension.Capabilities); err != nil {
		return err
	}
	for _, capability := range m.Spec.Extension.Capabilities {
		switch capability {
		case "resource_listing", "full_sync", "incremental_sync", "deletion_events":
		default:
			return fmt.Errorf("unsupported datasource capability %q", capability)
		}
	}
	if m.Spec.Runtime.Kind != RuntimeSandbox || m.Spec.Runtime.Transport != TransportUDS {
		return fmt.Errorf("unsupported runtime %q/%q: expected sandbox_service/uds", m.Spec.Runtime.Kind, m.Spec.Runtime.Transport)
	}
	if err := ValidateEntrypoint(m.Spec.Runtime.Entrypoint); err != nil {
		return err
	}
	if m.Spec.Permissions.Network != NetworkNone {
		return fmt.Errorf("unsupported_permission: permissions.network=%q", m.Spec.Permissions.Network)
	}
	if m.Spec.Permissions.Filesystem != FilesystemReadOnly {
		return fmt.Errorf("unsupported_permission: permissions.filesystem=%q", m.Spec.Permissions.Filesystem)
	}
	if err := validateResources(m.Spec.Resources); err != nil {
		return err
	}
	if err := validateConfigSchema(m.Spec.ConfigSchema); err != nil {
		return err
	}
	if opts.ArtifactRoot != "" {
		if err := ValidateArtifactEntrypoint(opts.ArtifactRoot, m.Spec.Runtime.Entrypoint); err != nil {
			return err
		}
	}
	return nil
}

func (m Manifest) Definition() PluginDefinition {
	copy := m.Clone()
	return PluginDefinition{
		ID:              m.Metadata.ID,
		Name:            m.Metadata.Name,
		Version:         m.Metadata.Version,
		Source:          SourceExternal,
		ExtensionID:     m.Spec.Extension.ID,
		ExtensionType:   m.Spec.Extension.Type,
		ProtocolVersion: m.Spec.ProtocolVersion,
		ContractVersion: m.Spec.Extension.ContractVersion,
		Capabilities:    append([]string(nil), m.Spec.Extension.Capabilities...),
		Manifest:        &copy,
	}
}

func (m Manifest) Clone() Manifest {
	out := m
	out.Spec.Extension.Capabilities = append([]string(nil), m.Spec.Extension.Capabilities...)
	out.Spec.ConfigSchema = cloneJSONMap(m.Spec.ConfigSchema)
	return out
}

func ValidateEntrypoint(entrypoint string) error {
	if entrypoint == "" {
		return errors.New("runtime.entrypoint is required")
	}
	if filepath.IsAbs(entrypoint) {
		return errors.New("runtime.entrypoint must be a package-relative path")
	}
	clean := filepath.Clean(entrypoint)
	if clean == "." || clean != entrypoint || strings.HasPrefix(clean, ".."+string(filepath.Separator)) || clean == ".." {
		return fmt.Errorf("runtime.entrypoint %q is not a canonical package-relative path", entrypoint)
	}
	return nil
}

// ValidateArtifactEntrypoint rejects links and special files in every component.
func ValidateArtifactEntrypoint(root, entrypoint string) error {
	if err := ValidateEntrypoint(entrypoint); err != nil {
		return err
	}
	rootInfo, err := os.Lstat(root)
	if err != nil {
		return fmt.Errorf("inspect artifact root: %w", err)
	}
	if rootInfo.Mode()&os.ModeSymlink != 0 || !rootInfo.IsDir() {
		return errors.New("artifact root must be a real directory")
	}
	current := root
	for _, component := range strings.Split(filepath.Clean(entrypoint), string(filepath.Separator)) {
		current = filepath.Join(current, component)
		info, statErr := os.Lstat(current)
		if statErr != nil {
			return fmt.Errorf("inspect artifact entrypoint component %q: %w", component, statErr)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("artifact entrypoint contains symlink component %q", component)
		}
	}
	info, err := os.Lstat(current)
	if err != nil {
		return fmt.Errorf("inspect artifact entrypoint: %w", err)
	}
	if !info.Mode().IsRegular() {
		return errors.New("artifact entrypoint must be a regular file")
	}
	if info.Mode().Perm()&0o111 == 0 {
		return errors.New("artifact entrypoint must be executable")
	}
	return nil
}

func validateCompatibility(expression, hostVersion string) error {
	parts := strings.Fields(expression)
	if len(parts) == 0 {
		return errors.New("compatibility.weknora is required")
	}
	for _, part := range parts {
		op, version := splitConstraint(part)
		if op == "" || !semver.IsValid("v"+version) {
			return fmt.Errorf("invalid compatibility.weknora constraint %q", part)
		}
		if hostVersion != "" {
			host := "v" + strings.TrimPrefix(hostVersion, "v")
			if !semver.IsValid(host) {
				return fmt.Errorf("invalid host WeKnora version %q", hostVersion)
			}
			comparison := semver.Compare(host, "v"+version)
			matches := comparison == 0
			switch op {
			case ">":
				matches = comparison > 0
			case ">=":
				matches = comparison >= 0
			case "<":
				matches = comparison < 0
			case "<=":
				matches = comparison <= 0
			case "=":
				matches = comparison == 0
			}
			if !matches {
				return fmt.Errorf("plugin is incompatible with WeKnora %s (requires %s)", hostVersion, expression)
			}
		}
	}
	return nil
}

func splitConstraint(value string) (string, string) {
	for _, op := range []string{">=", "<=", ">", "<", "="} {
		if strings.HasPrefix(value, op) {
			return op, strings.TrimPrefix(value, op)
		}
	}
	return "", ""
}

func validateResources(resources ManifestResources) error {
	if resources.MemoryMiB < 64 || resources.MemoryMiB > 4096 {
		return fmt.Errorf("resources.memoryMiB must be between 64 and 4096")
	}
	if resources.CPUQuota < 0.1 || resources.CPUQuota > 4 {
		return fmt.Errorf("resources.cpuQuota must be between 0.1 and 4")
	}
	if resources.MaxProcesses < 1 || resources.MaxProcesses > 256 {
		return fmt.Errorf("resources.maxProcesses must be between 1 and 256")
	}
	return nil
}

func validateConfigSchema(schema map[string]any) error {
	if len(schema) == 0 {
		return errors.New("configSchema is required")
	}
	if schema["type"] != "object" {
		return errors.New("configSchema.type must be object")
	}
	additional, ok := schema["additionalProperties"]
	if !ok || additional != false {
		return errors.New("configSchema.additionalProperties must be false")
	}
	if properties, ok := schema["properties"]; ok {
		if _, valid := properties.(map[string]any); !valid {
			return errors.New("configSchema.properties must be an object")
		}
	}
	if err := rejectRemoteSchemaReferences(schema); err != nil {
		return err
	}
	compiler := jsonschema.NewCompiler()
	if err := compiler.AddResource("urn:weknora:plugin-config", schema); err != nil {
		return fmt.Errorf("invalid configSchema: %w", err)
	}
	if _, err := compiler.Compile("urn:weknora:plugin-config"); err != nil {
		return fmt.Errorf("invalid configSchema: %w", err)
	}
	return nil
}

func rejectRemoteSchemaReferences(value any) error {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			if key == "$ref" {
				ref, ok := child.(string)
				if !ok || !strings.HasPrefix(ref, "#") {
					return errors.New("configSchema only permits local $ref values")
				}
			}
			if err := rejectRemoteSchemaReferences(child); err != nil {
				return err
			}
		}
	case []any:
		for _, child := range typed {
			if err := rejectRemoteSchemaReferences(child); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateUniqueStrings(field string, values []string) error {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s contains an empty value", field)
		}
		if _, exists := seen[value]; exists {
			return fmt.Errorf("%s contains duplicate value %q", field, value)
		}
		seen[value] = struct{}{}
	}
	return nil
}
