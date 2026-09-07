package prepare

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"

	"github.com/Tencent/WeKnora/internal/plugin/control"
	pluginruntime "github.com/Tencent/WeKnora/internal/plugin/runtime"
	"github.com/Tencent/WeKnora/internal/plugin/sandbox/docker/paths"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	jsonschema "github.com/santhosh-tekuri/jsonschema/v6"
)

type GrantStore interface {
	CreateDirectoryGrant(context.Context, *control.DirectoryGrant) error
	GetActiveDirectoryGrant(context.Context, string) (*control.DirectoryGrant, error)
}

type GrantService struct {
	Store               GrantStore
	AllowRoots          map[string]paths.PathMapping // administrator configuration only
	AdminUID, PluginUID uint32
}

func (s GrantService) Authorize(ctx context.Context, tenant uint64, dsID, rootID, relative, actor string) (*control.DirectoryGrant, error) {
	root, ok := s.AllowRoots[rootID]
	if !ok || tenant == 0 || dsID == "" || actor == "" || s.Store == nil {
		return nil, errors.New("grant ownership/allow-root is required")
	}
	snapshot, err := paths.CreateGrant(root.AppRoot, relative, 1)
	if err != nil {
		return nil, err
	}
	if err := paths.TrustedDirectory(snapshot.CanonicalPath, s.AdminUID, s.PluginUID); err != nil {
		return nil, err
	}
	host, err := root.HostPath(snapshot.CanonicalPath)
	if err != nil {
		return nil, err
	}
	g := &control.DirectoryGrant{ID: uuid.NewString(), TenantID: tenant, DataSourceID: dsID, AllowRootID: rootID, CanonicalHostPath: host,
		Device: snapshot.Device, Inode: snapshot.Inode, Generation: 1, Status: control.GrantStatusActive, CreatedBy: actor}
	return g, s.Store.CreateDirectoryGrant(ctx, g)
}

func (s GrantService) Resolve(g control.DirectoryGrant, ds types.DataSource) (*pluginruntime.DirectoryGrantReference, error) {
	if g.ID == "" || g.TenantID == 0 || g.TenantID != ds.TenantID || g.DataSourceID != ds.ID || g.Status != control.GrantStatusActive || g.RevokedAt != nil {
		return nil, errors.New("grant is revoked or belongs to another tenant/data source")
	}
	m, ok := s.AllowRoots[g.AllowRootID]
	if !ok {
		return nil, errors.New("unknown administrator allow-root")
	}
	reverse := paths.PathMapping{AppRoot: m.HostRoot, HostRoot: m.AppRoot}
	app, err := reverse.HostPath(g.CanonicalHostPath)
	if err != nil {
		return nil, err
	}
	if err := m.ValidatePair(app, g.CanonicalHostPath); err != nil {
		return nil, err
	}
	if err := paths.TrustedDirectory(app, s.AdminUID, s.PluginUID); err != nil {
		return nil, err
	}
	if err := paths.RevalidateGrant(paths.GrantSnapshot{CanonicalPath: app, Device: g.Device, Inode: g.Inode, Generation: g.Generation, Status: g.Status}); err != nil {
		return nil, err
	}
	return &pluginruntime.DirectoryGrantReference{ID: g.ID, Generation: g.Generation, AppPath: app, HostPath: g.CanonicalHostPath, Device: g.Device, Inode: g.Inode}, nil
}

// Builder implements reconcile.SpecBuilder. The supplied loader uses the
// existing tenant-aware repository; Runtime and Backend never query it.
type Builder struct {
	Grants         GrantService
	LoadDataSource func(context.Context, string) (*types.DataSource, error)
	LoadPackage    func(context.Context, control.PluginInstallation) (Package, error)
	RuntimeRoot    paths.PathMapping
	MaxResources   pluginruntime.ResourceLimits
}

func (b Builder) BuildInstanceSpec(ctx context.Context, installation control.PluginInstallation, binding control.DataSourcePluginBinding) (pluginruntime.InstanceSpec, error) {
	var spec pluginruntime.InstanceSpec
	if b.LoadDataSource == nil || b.LoadPackage == nil || b.Grants.Store == nil {
		return spec, errors.New("instance builder is not configured")
	}
	if installation.ID == "" || installation.ID != binding.InstallationID || !installation.Active || !installation.Enabled || installation.InstallStatus != control.InstallStatusInstalled || binding.Generation == 0 {
		return spec, errors.New("installation/binding is inactive or inconsistent")
	}
	ds, err := b.LoadDataSource(ctx, binding.DataSourceID)
	if err != nil {
		return spec, err
	}
	if ds == nil || ds.ID != binding.DataSourceID || ds.TenantID == 0 || ds.DeletedAt.Valid || (ds.Status != types.DataSourceStatusActive && ds.Status != types.DataSourceStatusError) {
		return spec, errors.New("data source is not active")
	}
	p, err := b.LoadPackage(ctx, installation)
	if err != nil {
		return spec, err
	}
	m := p.Manifest
	if m.Metadata.ID != installation.PluginID || m.Metadata.Version != installation.Version || p.Artifact.Digest != installation.ArtifactDigest || m.Spec.Extension.ID != binding.ExtensionID || ds.Type != string(binding.ExtensionID) {
		return spec, errors.New("installation/extension/artifact identity mismatch")
	}
	if err := m.Validate(control.ManifestValidationOptions{ArtifactRoot: p.Artifact.AppPath}); err != nil {
		return spec, err
	}
	if err := VerifyArtifact(p.Artifact, b.Grants.AdminUID, b.Grants.PluginUID); err != nil {
		return spec, err
	}
	g, err := b.Grants.Store.GetActiveDirectoryGrant(ctx, ds.ID)
	if err != nil {
		return spec, err
	}
	if g == nil {
		return spec, errors.New("active directory grant is missing")
	}
	grant, err := b.Grants.Resolve(*g, *ds)
	if err != nil {
		return spec, err
	}
	config, err := ds.ParseConfig()
	if err != nil {
		return spec, errors.New("data source configuration cannot be decoded")
	}
	if config == nil || len(config.Credentials) != 0 {
		return spec, errors.New("C1 does not provision plugin credentials")
	}
	if selected, ok := config.Settings["source_grant"]; ok && selected != g.ID {
		return spec, errors.New("configuration references a different grant")
	}
	compiler := jsonschema.NewCompiler()
	if err := compiler.AddResource("urn:plugin:config", m.Spec.ConfigSchema); err != nil {
		return spec, err
	}
	schema, err := compiler.Compile("urn:plugin:config")
	if err != nil {
		return spec, err
	}
	settings := config.Settings
	if settings == nil {
		settings = map[string]any{}
	}
	if err := schema.Validate(settings); err != nil {
		return spec, errors.New("data source settings do not satisfy configSchema")
	}
	configJSON, err := json.Marshal(config)
	if err != nil {
		return spec, err
	}
	limits := pluginruntime.ResourceLimits{MemoryBytes: int64(m.Spec.Resources.MemoryMiB) << 20, CPUQuota: m.Spec.Resources.CPUQuota, PidsLimit: m.Spec.Resources.MaxProcesses}
	if err := ValidateLimits(limits, b.MaxResources); err != nil {
		return spec, err
	}
	// Data source IDs may not influence paths outside the configured root.
	if _, err := uuid.Parse(ds.ID); err != nil {
		return spec, errors.New("data source ID must be a UUID")
	}
	app := filepath.Join(b.RuntimeRoot.AppRoot, ds.ID, strconv.FormatUint(binding.Generation, 10))
	host, err := b.RuntimeRoot.HostPath(app)
	if err != nil {
		return spec, err
	}
	nonce := make([]byte, 32)
	if _, err := rand.Read(nonce); err != nil {
		return spec, err
	}
	spec = pluginruntime.InstanceSpec{PluginID: string(installation.PluginID), PluginVersion: installation.Version, ExtensionID: string(binding.ExtensionID),
		DataSourceID: ds.ID, Generation: binding.Generation, ProtocolVersion: m.Spec.ProtocolVersion, ContractVersion: m.Spec.Extension.ContractVersion,
		ConfigJSON: configJSON, StartupNonce: nonce, RequiredCapabilities: append([]string(nil), m.Spec.Extension.Capabilities...),
		Artifact: p.Artifact, Grant: grant, RuntimeAppPath: app, RuntimeHostPath: host,
		Permissions: pluginruntime.EffectivePermissions{Network: m.Spec.Permissions.Network, Filesystem: m.Spec.Permissions.Filesystem}, Resources: limits}
	return spec, spec.Validate()
}

// The existing Manifest bounds and the administrator ceiling are both hard
// validation boundaries. Invalid values never silently round up to defaults.
func ValidateLimits(l, max pluginruntime.ResourceLimits) error {
	if max.MemoryBytes == 0 {
		max.MemoryBytes = 4096 << 20
	}
	if max.CPUQuota == 0 {
		max.CPUQuota = 4
	}
	if max.PidsLimit == 0 {
		max.PidsLimit = 256
	}
	if l.MemoryBytes < 64<<20 || l.MemoryBytes > min(max.MemoryBytes, 4096<<20) || !(l.CPUQuota >= .1 && l.CPUQuota <= min(max.CPUQuota, 4)) || l.PidsLimit < 1 || l.PidsLimit > min(max.PidsLimit, 256) {
		return fmt.Errorf("resources outside manifest/administrator bounds")
	}
	return nil
}
