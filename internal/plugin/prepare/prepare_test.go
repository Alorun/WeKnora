package prepare

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/plugin/control"
	"github.com/Tencent/WeKnora/internal/plugin/sandbox/docker/paths"
	pluginstore "github.com/Tencent/WeKnora/internal/plugin/store"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func fixture(t *testing.T) (Discovery, string) {
	t.Helper()
	root := t.TempDir()
	for _, p := range []string{"packages/probe", "snapshots", "allow/source", "runtime"} {
		require.NoError(t, os.MkdirAll(filepath.Join(root, p), 0755))
	}
	source := filepath.Join(root, "packages/probe")
	m, err := os.ReadFile("../sandbox/docker/testdata/plugin.yaml")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(source, "plugin.yaml"), m, 0644))
	require.NoError(t, os.WriteFile(filepath.Join(source, "plugin"), []byte("executable"), 0755))
	mapper := func(p string) paths.PathMapping {
		return paths.PathMapping{AppRoot: filepath.Join(root, p), HostRoot: filepath.Join(root, p)}
	}
	return Discovery{Packages: mapper("packages"), Snapshots: mapper("snapshots"), AdminUID: uint32(os.Geteuid()), PluginUID: 65532, Options: control.ManifestValidationOptions{WeKnoraVersion: "0.7.3"}}, source
}

func TestDiscoverySnapshotAndReplacement(t *testing.T) {
	d, source := fixture(t)
	p, err := d.Load(source)
	require.NoError(t, err)
	p2, err := d.Load(source)
	require.NoError(t, err)
	require.Equal(t, p.Artifact, p2.Artifact)
	previous := []control.PluginInstallation{{ID: uuid.NewString(), PluginID: p.Manifest.Metadata.ID, Version: p.Manifest.Metadata.Version, ArtifactDigest: p.Artifact.Digest, Active: true, InstallStatus: control.InstallStatusInstalled}}
	results, err := d.Scan(previous)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.NotNil(t, results[0].Package)
	require.NoError(t, os.WriteFile(filepath.Join(source, "plugin"), []byte("changed"), 0755))
	results, err = d.Scan(previous)
	require.NoError(t, err)
	require.Nil(t, results[0].Package)
	require.Contains(t, results[0].Installation.LastError, "changed")
	require.NoError(t, VerifyArtifact(p.Artifact, d.AdminUID, d.PluginUID), "active snapshot is preserved")
	require.NoError(t, os.Rename(source, source+"-moved"))
	require.NoError(t, os.RemoveAll(source+"-moved"))
	results, err = d.Scan(previous)
	require.NoError(t, err)
	require.Equal(t, "missing", results[0].Installation.InstallStatus)
}

func TestDiscoveryRejectsUnsafePackages(t *testing.T) {
	for _, kind := range []string{"entry_escape", "version", "permission", "link", "directory_link", "too_many", "duplicate", "modified_snapshot", "special_file", "oversized"} {
		t.Run(kind, func(t *testing.T) {
			d, source := fixture(t)
			switch kind {
			case "entry_escape", "version", "permission":
				m, err := os.ReadFile(filepath.Join(source, "plugin.yaml"))
				require.NoError(t, err)
				old, new := "entrypoint: plugin", "entrypoint: ../plugin"
				if kind == "version" {
					old, new = "version: 1.0.0", "version: latest"
				}
				if kind == "permission" {
					old, new = "network: none", "network: outbound"
				}
				require.NoError(t, os.WriteFile(filepath.Join(source, "plugin.yaml"), []byte(strings.Replace(string(m), old, new, 1)), 0644))
			case "link":
				require.NoError(t, os.Symlink("plugin", filepath.Join(source, "link")))
			case "special_file":
				require.NoError(t, syscall.Mkfifo(filepath.Join(source, "fifo"), 0600))
			case "oversized":
				require.NoError(t, os.Truncate(filepath.Join(source, "plugin"), MaxArtifactBytes+1))
			case "directory_link":
				require.NoError(t, os.Symlink(source, filepath.Join(d.Packages.AppRoot, "linked")))
			case "too_many":
				for i := 0; i < MaxArtifactFiles; i++ {
					require.NoError(t, os.WriteFile(filepath.Join(source, uuid.NewString()), nil, 0600))
				}
			case "duplicate":
				other := filepath.Join(d.Packages.AppRoot, "other")
				require.NoError(t, os.Mkdir(other, 0755))
				for _, name := range []string{"plugin.yaml", "plugin"} {
					data, err := os.ReadFile(filepath.Join(source, name))
					require.NoError(t, err)
					require.NoError(t, os.WriteFile(filepath.Join(other, name), data, 0755))
				}
			case "modified_snapshot":
				p, err := d.Load(source)
				require.NoError(t, err)
				path := filepath.Join(p.Artifact.AppPath, "plugin")
				require.NoError(t, os.Chmod(path, 0600))
				require.NoError(t, os.WriteFile(path, []byte("replacement"), 0600))
				require.Error(t, VerifyArtifact(p.Artifact, d.AdminUID, d.PluginUID))
				return
			}
			results, err := d.Scan(nil)
			require.NoError(t, err)
			invalid := 0
			for _, r := range results {
				if r.Installation.InstallStatus == control.InstallStatusInvalid {
					invalid++
					require.Nil(t, r.Package)
				}
			}
			require.Positive(t, invalid)
		})
	}
}

func TestGrantAndBuilderUseExistingStore(t *testing.T) {
	d, source := fixture(t)
	p, err := d.Load(source)
	require.NoError(t, err)
	root := filepath.Dir(d.Packages.AppRoot)
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "test.db")), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&types.DataSource{}, &control.DirectoryGrant{}, &control.PluginInstallation{}, &control.DataSourcePluginBinding{}))
	store := pluginstore.New(db)
	ctx := context.Background()
	ds := types.DataSource{ID: uuid.NewString(), TenantID: 7, Type: string(p.Manifest.Spec.Extension.ID), Status: types.DataSourceStatusActive, Config: types.JSON(`{"type":"runtime_probe","settings":{}}`)}
	require.NoError(t, db.Create(&ds).Error)
	svc := GrantService{Store: store, AdminUID: d.AdminUID, PluginUID: d.PluginUID, AllowRoots: map[string]paths.PathMapping{"root": {AppRoot: filepath.Join(root, "allow"), HostRoot: filepath.Join(root, "allow")}}}
	_, err = svc.Authorize(ctx, 8, ds.ID, "root", "source", "admin")
	require.Error(t, err)
	grant, err := svc.Authorize(ctx, ds.TenantID, ds.ID, "root", "source", "admin")
	require.NoError(t, err)
	installation := control.PluginInstallation{ID: uuid.NewString(), PluginID: p.Manifest.Metadata.ID, Version: p.Manifest.Metadata.Version, ArtifactDigest: p.Artifact.Digest, Active: true, Enabled: true, InstallStatus: control.InstallStatusInstalled}
	binding := control.DataSourcePluginBinding{DataSourceID: ds.ID, InstallationID: installation.ID, ExtensionID: p.Manifest.Spec.Extension.ID, Generation: 1}
	b := Builder{Grants: svc, LoadDataSource: func(context.Context, string) (*types.DataSource, error) { return &ds, nil }, LoadPackage: func(context.Context, control.PluginInstallation) (Package, error) { return p, nil }, RuntimeRoot: paths.PathMapping{AppRoot: filepath.Join(root, "runtime"), HostRoot: filepath.Join(root, "runtime")}}
	spec, err := b.BuildInstanceSpec(ctx, installation, binding)
	require.NoError(t, err)
	require.Equal(t, grant.ID, spec.Grant.ID)
	require.Len(t, spec.StartupNonce, 32)
	wrong := binding
	wrong.ExtensionID = "other"
	_, err = b.BuildInstanceSpec(ctx, installation, wrong)
	require.Error(t, err)
	bad := *grant
	bad.Inode++
	_, err = svc.Resolve(bad, ds)
	require.ErrorContains(t, err, "inode")
	bad = *grant
	bad.TenantID++
	_, err = svc.Resolve(bad, ds)
	require.ErrorContains(t, err, "another tenant")
	require.NoError(t, os.Rename(grant.CanonicalHostPath, grant.CanonicalHostPath+"-old"))
	require.NoError(t, os.Mkdir(grant.CanonicalHostPath, 0755))
	_, err = b.BuildInstanceSpec(ctx, installation, binding)
	require.ErrorContains(t, err, "inode")
	require.NoError(t, db.Model(&control.DirectoryGrant{}).Where("id = ?", grant.ID).Updates(map[string]any{
		"status":     control.GrantStatusRevoked,
		"generation": 2,
		"revoked_at": time.Now().UTC(),
	}).Error)
	_, err = b.BuildInstanceSpec(ctx, installation, binding)
	require.Error(t, err)
}
