package runtime

import (
	"context"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	plugindatasource "github.com/Tencent/WeKnora/internal/plugin/datasource"
	pluginv1 "github.com/Tencent/WeKnora/pkg/plugin/proto/v1"
	pluginsdk "github.com/Tencent/WeKnora/pkg/plugin/sdk"
	"github.com/stretchr/testify/require"
)

type runtimeDataSource struct {
	pluginv1.UnimplementedDataSourcePluginServer
}

func (runtimeDataSource) ListResources(context.Context, *pluginv1.ListResourcesRequest) (*pluginv1.ListResourcesResponse, error) {
	return &pluginv1.ListResourcesResponse{}, nil
}

type fakeBackend struct {
	root         string
	healthStatus pluginv1.HealthStatus
	healthHook   func()
	mu           sync.Mutex
	started      int
	stopped      int
	servers      map[string]*pluginsdk.Server
}

func (b *fakeBackend) StartService(ctx context.Context, spec InstanceSpec) (BackendInstance, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.started++
	id := "instance-" + spec.DataSourceID
	socket := filepath.Join(b.root, spec.DataSourceID, "plugin.sock")
	control, err := pluginsdk.NewControlServer(pluginsdk.Identity{
		PluginID: spec.PluginID, PluginVersion: spec.PluginVersion, ExtensionID: spec.ExtensionID,
		ExtensionType:   pluginv1.ExtensionType_EXTENSION_TYPE_DATASOURCE,
		ProtocolVersion: spec.ProtocolVersion, ContractVersion: spec.ContractVersion,
		StartupNonce: spec.StartupNonce,
	}, pluginsdk.ControlHooks{Health: func(context.Context) (pluginv1.HealthStatus, error) {
		if b.healthHook != nil {
			b.healthHook()
		}
		return b.healthStatus, nil
	}})
	if err != nil {
		return BackendInstance{}, err
	}
	server, err := pluginsdk.ServeUDS(ctx, socket, pluginsdk.Services{Control: control, DataSource: runtimeDataSource{}})
	if err != nil {
		return BackendInstance{}, err
	}
	if b.servers == nil {
		b.servers = make(map[string]*pluginsdk.Server)
	}
	b.servers[id] = server
	return BackendInstance{ID: id, UDSHostPath: socket}, nil
}

func (b *fakeBackend) List(context.Context, map[string]string) ([]BackendInstance, error) {
	return nil, nil
}
func (b *fakeBackend) Inspect(context.Context, string) (BackendInstanceState, error) {
	return BackendInstanceState{}, nil
}
func (b *fakeBackend) Stop(_ context.Context, id string, _ time.Duration) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.stopped++
	if server := b.servers[id]; server != nil {
		server.Stop()
		delete(b.servers, id)
	}
	return nil
}

func runtimeSpec(root string) InstanceSpec {
	return InstanceSpec{
		PluginID: "test.local", PluginVersion: "1.0.0", ExtensionID: "local", DataSourceID: "ds-1", Generation: 1,
		ProtocolVersion: pluginsdk.ProtocolVersion, ContractVersion: pluginsdk.DataSourceContractVersion,
		ConfigJSON: []byte(`{"root":"grant_root"}`), StartupNonce: []byte("nonce"),
		Artifact: ArtifactReference{Digest: "sha256:test", EntryPath: "plugin"}, RuntimeHostPath: root,
	}
}

func TestRuntimePublishesOnlyAfterReadyAndDrainsOnStop(t *testing.T) {
	resolver := plugindatasource.NewResolver()
	backend := &fakeBackend{root: t.TempDir(), healthStatus: pluginv1.HealthStatus_HEALTH_STATUS_READY}
	var publishedDuringHealth atomic.Bool
	backend.healthHook = func() {
		if _, err := resolver.Resolve("ds-1"); err == nil {
			publishedDuringHealth.Store(true)
		}
	}
	runtime := New(backend, resolver)
	handle, err := runtime.Start(context.Background(), runtimeSpec(backend.root))
	require.NoError(t, err)
	require.False(t, publishedDuringHealth.Load(), "handle was published before READY health completed")
	resolved, err := resolver.Resolve("ds-1")
	require.NoError(t, err)
	require.Equal(t, handle.InstanceID(), resolved.Handle.InstanceID())
	require.NoError(t, runtime.Stop(context.Background(), handle, time.Second))
	_, err = resolver.Resolve("ds-1")
	require.ErrorIs(t, err, plugindatasource.ErrHandleNotFound)
	require.Equal(t, 1, backend.stopped)
	// Repeating Stop must not call an already-closed gRPC connection, while
	// still retrying idempotent backend cleanup.
	require.NoError(t, runtime.Stop(context.Background(), handle, 0))
	require.Equal(t, 2, backend.stopped)
}

func TestRuntimeFailureCleansBackendWithoutPublishing(t *testing.T) {
	resolver := plugindatasource.NewResolver()
	backend := &fakeBackend{root: t.TempDir(), healthStatus: pluginv1.HealthStatus_HEALTH_STATUS_NOT_READY}
	runtime := New(backend, resolver)
	_, err := runtime.Start(context.Background(), runtimeSpec(backend.root))
	require.Error(t, err)
	_, resolveErr := resolver.Resolve("ds-1")
	require.ErrorIs(t, resolveErr, plugindatasource.ErrHandleNotFound)
	require.Equal(t, 1, backend.started)
	require.Equal(t, 1, backend.stopped)
}

func TestHandshakeRejectsIdentityVersionAndNonceMismatch(t *testing.T) {
	spec := runtimeSpec(t.TempDir())
	valid := &pluginv1.HandshakeResponse{
		PluginId: spec.PluginID, PluginVersion: spec.PluginVersion, ExtensionId: spec.ExtensionID,
		ExtensionType:   pluginv1.ExtensionType_EXTENSION_TYPE_DATASOURCE,
		ProtocolVersion: spec.ProtocolVersion, ContractVersion: spec.ContractVersion,
		StartupNonce: append([]byte(nil), spec.StartupNonce...),
	}
	require.NoError(t, validateHandshake(spec, valid))
	wrongNonce := *valid
	wrongNonce.StartupNonce = []byte("wrong")
	require.ErrorContains(t, validateHandshake(spec, &wrongNonce), pluginsdk.ErrorNonceMismatch)
	wrongIdentity := *valid
	wrongIdentity.PluginId = "attacker.plugin"
	require.ErrorContains(t, validateHandshake(spec, &wrongIdentity), pluginsdk.ErrorIdentityMismatch)
	wrongVersion := *valid
	wrongVersion.ContractVersion = "2.0"
	require.ErrorContains(t, validateHandshake(spec, &wrongVersion), pluginsdk.ErrorIncompatibleContract)
}
