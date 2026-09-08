//go:build integration && localdirectory

package docker

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/plugin/control"
	pluginds "github.com/Tencent/WeKnora/internal/plugin/datasource"
	"github.com/Tencent/WeKnora/internal/plugin/prepare"
	pr "github.com/Tencent/WeKnora/internal/plugin/runtime"
	"github.com/Tencent/WeKnora/internal/plugin/sandbox/docker/paths"
	pluginstore "github.com/Tencent/WeKnora/internal/plugin/store"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// This receiver only observes Connector events and serializes opaque Cursor.
// It is NOT ingestion: no Knowledge, revision, queue or index is created.
type localDirectoryReceiver struct {
	up      []types.FetchedItem
	deleted []string
	cursor  []byte
}

func (r *localDirectoryReceiver) Emit(_ context.Context, item types.FetchedItem) error {
	if item.IsDeleted {
		r.deleted = append(r.deleted, item.ExternalID)
	} else {
		r.up = append(r.up, item)
	}
	return nil
}
func (r *localDirectoryReceiver) Checkpoint(_ context.Context, cursor *types.SyncCursor) error {
	data, err := json.Marshal(cursor)
	if err == nil {
		r.cursor = data
	}
	return err
}

func TestLocalDirectoryRuntime(t *testing.T) {
	// Explicit integration requests fail, not skip, when the artifact/environment
	// is missing. Ordinary tests do not import or require the sibling module.
	artifact := os.Getenv("C2_ARTIFACT")
	require.NotEmpty(t, artifact, "set C2_ARTIFACT and use scripts/test-plugin-backend.sh local-directory")
	require.NotEmpty(t, os.Getenv("C1_APP_ROOT"), "C1 controlled Controller environment is required")
	require.NotEmpty(t, os.Getenv("C1_HOST_ROOT"))
	c := integrationConfig(t) // existing trusted audit sink, mappings and safety limits
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	d := prepare.Discovery{Packages: paths.PathMapping{AppRoot: filepath.Join(os.Getenv("C1_APP_ROOT"), "packages"), HostRoot: filepath.Join(os.Getenv("C1_HOST_ROOT"), "packages")}, Snapshots: c.ArtifactRoot, AdminUID: c.AdminUID, PluginUID: c.PluginUID, Options: control.ManifestValidationOptions{WeKnoraVersion: "0.7.3"}}
	results, err := d.Scan(nil)
	require.NoError(t, err)
	require.Len(t, results, 1)
	candidate := results[0]
	require.Equal(t, artifact, candidate.Path)
	require.NotNil(t, candidate.Package, candidate.Installation.LastError)
	require.Equal(t, control.PluginID("community.local-directory"), candidate.Installation.PluginID)
	t.Logf("DISCOVERY plugin=%s version=%s digest=%s", candidate.Installation.PluginID, candidate.Installation.Version, candidate.Installation.ArtifactDigest)
	// Isolated test database uses existing Store/models only to exercise the real
	// Grant/Builder contract, not C3 application orchestration or migrations.
	db, err := gorm.Open(sqlite.Open(filepath.Join(os.Getenv("C1_APP_ROOT"), "c2.db")), &gorm.Config{})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	defer sqlDB.Close()
	require.NoError(t, db.AutoMigrate(&types.DataSource{}, &control.PluginInstallation{}, &control.DataSourcePluginBinding{}, &control.DirectoryGrant{}))
	store := pluginstore.New(db)
	ds := types.DataSource{ID: uuid.NewString(), TenantID: 7, Type: "local_directory", Status: types.DataSourceStatusActive, Config: types.JSON(`{"type":"local_directory","resource_ids":["grant_root"],"settings":{}}`)}
	require.NoError(t, db.Create(&ds).Error)
	installation := candidate.Installation
	installation.ID = uuid.NewString()
	installation.Active = true
	installation.Enabled = true
	require.NoError(t, store.CreateInstallation(ctx, &installation))
	binding := control.DataSourcePluginBinding{DataSourceID: ds.ID, InstallationID: installation.ID, ExtensionID: "local_directory", Generation: 1, ObservedState: control.StateStopped}
	require.NoError(t, store.CreateBinding(ctx, &binding))
	source := filepath.Join(c.GrantRoots[0].AppRoot, "source")
	require.NoError(t, os.MkdirAll(source, 0755))
	put := func(name, body string) {
		t.Helper()
		p := filepath.Join(source, name)
		require.NoError(t, os.MkdirAll(filepath.Dir(p), 0755))
		require.NoError(t, os.WriteFile(p, []byte(body), 0644))
	}
	put("a.txt", "A")
	put("nested/b.txt", "B")
	put("c.pdf", "%PDF-1.4\n\x00\xffraw")
	grants := prepare.GrantService{Store: store, AllowRoots: map[string]paths.PathMapping{"test-root": c.GrantRoots[0]}, AdminUID: c.AdminUID, PluginUID: c.PluginUID}
	grant, err := grants.Authorize(ctx, ds.TenantID, ds.ID, "test-root", "source", "test-admin")
	require.NoError(t, err)
	builder := prepare.Builder{Grants: grants, RuntimeRoot: c.RuntimeRoot,
		LoadDataSource: func(ctx context.Context, id string) (*types.DataSource, error) {
			var ds types.DataSource
			err := db.WithContext(ctx).First(&ds, "id = ?", id).Error
			return &ds, err
		},
		LoadPackage: func(context.Context, control.PluginInstallation) (prepare.Package, error) { return d.Load(artifact) },
	}
	b, err := New(ctx, c)
	require.NoError(t, err)
	defer func() { require.NoError(t, b.Close()) }()
	routes := pluginds.NewResolver()
	runtime := pr.New(b, routes)
	spec, err := builder.BuildInstanceSpec(ctx, installation, binding)
	require.NoError(t, err)
	handle, err := runtime.Start(ctx, spec)
	require.NoError(t, err)
	defer func() {
		if handle != nil {
			require.NoError(t, runtime.Stop(context.Background(), handle, time.Second))
			assertClean(t, b, spec, handle.InstanceID())
		}
	}()
	t.Logf("REAL READY instance=%s generation=%d app=%s host=%s", handle.InstanceID(), spec.Generation, spec.RuntimeAppPath, spec.RuntimeHostPath)
	// Resolve via the existing Binding-first business router, not a test factory.
	connector, external, err := pluginds.NewConnectorRouter(store, routes).ResolveExternal(ctx, &ds)
	require.NoError(t, err)
	require.True(t, external)
	adapter, ok := connector.(*pluginds.GRPCConnectorAdapter)
	require.True(t, ok)
	config, err := ds.ParseConfig()
	require.NoError(t, err)
	require.NoError(t, adapter.Validate(ctx, config))
	resources, err := adapter.ListResources(ctx, config, "")
	require.NoError(t, err)
	require.Len(t, resources, 1)
	require.Equal(t, "grant_root", resources[0].ExternalID)
	require.Equal(t, "已授权目录", resources[0].Name)
	require.False(t, resources[0].HasChildren)
	children, err := adapter.ListResources(ctx, config, "grant_root")
	require.NoError(t, err)
	require.Empty(t, children)
	ancestors, err := adapter.ResolveResourceAncestors(ctx, config, []string{"grant_root"})
	require.NoError(t, err)
	require.Empty(t, ancestors)
	_, err = adapter.ListResources(ctx, config, "../outside")
	require.Error(t, err)
	scan := func(name string, cursor []byte, up, del int) localDirectoryReceiver {
		t.Helper()
		var previous *types.SyncCursor
		if cursor != nil {
			previous = &types.SyncCursor{}
			require.NoError(t, json.Unmarshal(cursor, previous))
		}
		var receiver localDirectoryReceiver
		_, err := adapter.FetchStream(ctx, config, previous, &receiver)
		require.NoError(t, err)
		require.Len(t, receiver.up, up)
		require.Len(t, receiver.deleted, del)
		require.NotEmpty(t, receiver.cursor)
		t.Logf("EVENTS %s upsert=%d delete=%d checkpoint=1", name, up, del)
		return receiver
	}
	first := scan("first", nil, 3, 0)
	require.Equal(t, []byte("%PDF-1.4\n\x00\xffraw"), first.up[1].Content)
	require.Equal(t, "c.pdf", first.up[1].FileName)
	quiet := scan("unchanged", first.cursor, 0, 0)
	put("a.txt", "B")
	changed := scan("one-change", quiet.cursor, 1, 0)
	replay := scan("old-cursor-replay", quiet.cursor, 1, 0)
	require.Equal(t, changed.up, replay.up)
	require.Equal(t, changed.cursor, replay.cursor)
	put("a.txt", "A")
	again := scan("A-B-A", changed.cursor, 1, 0)
	require.NotEqual(t, first.up[0].Revision, again.up[0].Revision)
	put("d.txt", "D")
	added := scan("added", again.cursor, 1, 0)
	require.NoError(t, os.Remove(filepath.Join(source, "d.txt")))
	deleted := scan("deleted", added.cursor, 0, 1)
	put("d.txt", "D")
	rebuilt := scan("recreated", deleted.cursor, 1, 0)
	require.NotEqual(t, added.up[0].Revision, rebuilt.up[0].Revision)
	require.NoError(t, os.Rename(filepath.Join(source, "d.txt"), filepath.Join(source, "e.txt")))
	renamed := scan("renamed", rebuilt.cursor, 1, 1)
	require.Equal(t, "d.txt", renamed.deleted[0])
	require.Equal(t, "e.txt", renamed.up[0].ExternalID)
	// The old Handle is closed permanently; identical generation/path is not
	// instance identity. Rebuild spec/nonce and complete a fresh handshake.
	old := handle
	require.NoError(t, runtime.Stop(ctx, old, time.Second))
	assertClean(t, b, spec, old.InstanceID())
	_, err = adapter.ListResources(ctx, config, "")
	require.Error(t, err, "stopped route remained callable")
	put("nested/b.txt", "after restart")
	spec, err = builder.BuildInstanceSpec(ctx, installation, binding)
	require.NoError(t, err)
	handle, err = runtime.Start(ctx, spec)
	require.NoError(t, err)
	require.NotEqual(t, old.InstanceID(), handle.InstanceID())
	require.NoError(t, runtime.Stop(ctx, old, 0), "stale Stop must not unpublish the new instance")
	t.Logf("RESTART old=%s new=%s generation=%d", old.InstanceID(), handle.InstanceID(), spec.Generation)
	restarted := scan("after-real-restart", renamed.cursor, 1, 0)
	require.Equal(t, "nested/b.txt", restarted.up[0].ExternalID)
	scan("restart-unchanged", restarted.cursor, 0, 0)
	// A missing old file plus an unsafe entry is an incomplete scan, not Delete.
	require.NoError(t, os.Remove(filepath.Join(source, "a.txt")))
	require.NoError(t, os.Symlink("/etc/passwd", filepath.Join(source, "unsafe")))
	var previous types.SyncCursor
	require.NoError(t, json.Unmarshal(restarted.cursor, &previous))
	var failed localDirectoryReceiver
	_, err = adapter.FetchStream(ctx, config, &previous, &failed)
	require.Error(t, err)
	require.Empty(t, failed.deleted)
	require.Empty(t, failed.cursor)
	t.Logf("INCOMPLETE SCAN error=%v delete=0 checkpoint=0", err)
	require.NoError(t, runtime.Stop(ctx, handle, time.Second))
	assertClean(t, b, spec, handle.InstanceID())
	// Same formal Store/Builder rejects a revoked Grant before a new Start.
	require.NoError(t, store.RevokeDirectoryGrant(ctx, grant.ID, 2))
	_, err = builder.BuildInstanceSpec(ctx, installation, binding)
	require.Error(t, err)
	list, err := b.List(ctx, nil)
	require.NoError(t, err)
	require.Empty(t, list)
	t.Log("CLEAN containers=0 UDS=0 instance-mounts=0 BPF-pins=0 (formal Stop + assertClean)")
}
