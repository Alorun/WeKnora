package manager

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/datasource"
	"github.com/Tencent/WeKnora/internal/infrastructure/docparser"
	websearch "github.com/Tencent/WeKnora/internal/infrastructure/web_search"
	"github.com/Tencent/WeKnora/internal/models/provider"
	"github.com/Tencent/WeKnora/internal/plugin/builtin"
	"github.com/Tencent/WeKnora/internal/plugin/catalog"
	"github.com/Tencent/WeKnora/internal/plugin/control"
	pluginruntime "github.com/Tencent/WeKnora/internal/plugin/runtime"
	pluginv1 "github.com/Tencent/WeKnora/pkg/plugin/proto/v1"
	"github.com/stretchr/testify/require"
)

type fakeRegistrar struct {
	name        string
	events      *[]string
	publishErr  error
	healthErr   error
	published   bool
	publishCall int
}

func (r *fakeRegistrar) Publish(context.Context) error {
	*r.events = append(*r.events, r.name+":publish")
	r.publishCall++
	if r.publishErr != nil {
		return r.publishErr
	}
	r.published = true
	return nil
}

func (r *fakeRegistrar) Unpublish(context.Context) error {
	*r.events = append(*r.events, r.name+":unpublish")
	r.published = false
	return nil
}

func (r *fakeRegistrar) Health(context.Context) error {
	*r.events = append(*r.events, r.name+":health")
	return r.healthErr
}

func fakeDescriptor(id string, registrar *fakeRegistrar, allowDisable bool) builtin.BuiltinDescriptor {
	return builtin.BuiltinDescriptor{
		Definition: control.PluginDefinition{
			ID: control.PluginID("builtin." + id), Name: id, Version: "1.0.0",
			Source: control.SourceBuiltin, ExtensionID: control.ExtensionID(id),
			ExtensionType: control.ExtensionDataSource,
		},
		AllowDisable: allowDisable,
		Start: func(context.Context) error {
			*registrar.events = append(*registrar.events, registrar.name+":start")
			return nil
		},
		Stop: func(context.Context) error {
			*registrar.events = append(*registrar.events, registrar.name+":stop")
			return nil
		},
		Registrar: registrar,
	}
}

func TestBuiltinDescriptorsPublishAllFourExtensionTypes(t *testing.T) {
	connectorRegistry := datasource.NewConnectorRegistry()
	webRegistry := websearch.NewRegistry()
	pluginCatalog := catalog.New()
	manager := New(pluginCatalog, nil)
	descriptors := builtin.Descriptors(connectorRegistry, webRegistry)
	if _, err := connectorRegistry.Get("feishu"); err == nil || webRegistry.Has("google") {
		t.Fatal("legacy container path published before PluginManager start")
	}
	if docparser.HasEngine(docparser.BuiltinEngineName) {
		t.Fatal("legacy parser declaration published before PluginManager start")
	}
	if _, exists := provider.Get(provider.ProviderOpenAI); exists {
		t.Fatal("legacy model declaration published before PluginManager start")
	}
	if err := manager.LoadBuiltins(descriptors); err != nil {
		t.Fatal(err)
	}
	if err := manager.StartAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for index := len(descriptors) - 1; index >= 0; index-- {
			_ = descriptors[index].Registrar.Unpublish(context.Background())
		}
	})
	for _, extensionType := range []control.ExtensionType{
		control.ExtensionDataSource, control.ExtensionDocumentParser,
		control.ExtensionWebSearch, control.ExtensionModelProvider,
	} {
		if len(pluginCatalog.List(extensionType)) == 0 {
			t.Fatalf("catalog has no %s definition", extensionType)
		}
	}
	if _, err := connectorRegistry.Get("feishu"); err != nil {
		t.Fatalf("datasource business registry: %v", err)
	}
	if !docparser.HasEngine(docparser.BuiltinEngineName) {
		t.Fatal("parser business registry was not published")
	}
	if !webRegistry.Has("google") {
		t.Fatal("web search business registry was not published")
	}
	if _, exists := provider.Get(provider.ProviderOpenAI); !exists {
		t.Fatal("model metadata business registry was not published")
	}
	connectorCount := len(connectorRegistry.List())
	webCount := len(webRegistry.List())
	modelCount := len(provider.List())
	parserCount := len(docparser.ListAllEngines(true, nil, nil))
	if err := manager.Start(context.Background(), "builtin.datasource_feishu"); err != nil {
		t.Fatalf("idempotent start: %v", err)
	}
	if connectorCount != len(connectorRegistry.List()) || webCount != len(webRegistry.List()) ||
		modelCount != len(provider.List()) || parserCount != len(docparser.ListAllEngines(true, nil, nil)) {
		t.Fatal("idempotent start changed a business registry")
	}
}

func TestStartAllRollsBackEarlierPublication(t *testing.T) {
	var events []string
	first := &fakeRegistrar{name: "a", events: &events}
	second := &fakeRegistrar{name: "b", events: &events, publishErr: errors.New("publish failed")}
	manager := New(catalog.New(), nil)
	if err := manager.LoadBuiltins([]builtin.BuiltinDescriptor{
		fakeDescriptor("a", first, true), fakeDescriptor("b", second, true),
	}); err != nil {
		t.Fatal(err)
	}
	if err := manager.StartAll(context.Background()); err == nil {
		t.Fatal("expected start failure")
	}
	if first.published {
		t.Fatal("earlier registrar remained published")
	}
	status, statusErr := manager.Status("builtin.b")
	if statusErr != nil || status.State != control.StateFailed || status.LastError != "publish failed" {
		t.Fatalf("failed status = %#v, err = %v", status, statusErr)
	}
	wantSuffix := []string{"a:unpublish", "a:stop"}
	if !reflect.DeepEqual(events[len(events)-2:], wantSuffix) {
		t.Fatalf("rollback suffix = %v, want %v", events, wantSuffix)
	}
}

func TestRepeatedStartAndStopOrdering(t *testing.T) {
	var events []string
	registrar := &fakeRegistrar{name: "sample", events: &events}
	manager := New(catalog.New(), nil)
	if err := manager.LoadBuiltins([]builtin.BuiltinDescriptor{fakeDescriptor("sample", registrar, true)}); err != nil {
		t.Fatal(err)
	}
	if err := manager.Start(context.Background(), "builtin.sample"); err != nil {
		t.Fatal(err)
	}
	if err := manager.Start(context.Background(), "builtin.sample"); err != nil {
		t.Fatal(err)
	}
	if registrar.publishCall != 1 {
		t.Fatalf("publish called %d times", registrar.publishCall)
	}
	if err := manager.Stop(context.Background(), "builtin.sample"); err != nil {
		t.Fatal(err)
	}
	if got := events[len(events)-2:]; !reflect.DeepEqual(got, []string{"sample:unpublish", "sample:stop"}) {
		t.Fatalf("stop order = %v", got)
	}
	status, _ := manager.Status("builtin.sample")
	if status.State != control.StateStopped {
		t.Fatalf("state = %s", status.State)
	}
}

type memoryExternalStore struct {
	installations map[string]*control.PluginInstallation
	bindings      map[string]*control.DataSourcePluginBinding
}

func (s *memoryExternalStore) CreateInstallation(_ context.Context, value *control.PluginInstallation) error {
	copy := *value
	s.installations[value.ID] = &copy
	return nil
}
func (s *memoryExternalStore) CreateBinding(_ context.Context, value *control.DataSourcePluginBinding) error {
	copy := *value
	s.bindings[value.DataSourceID] = &copy
	return nil
}
func (s *memoryExternalStore) GetInstallation(_ context.Context, id string) (*control.PluginInstallation, error) {
	value, exists := s.installations[id]
	if !exists {
		return nil, errors.New("not found")
	}
	copy := *value
	return &copy, nil
}
func (s *memoryExternalStore) GetBinding(_ context.Context, dataSourceID string) (*control.DataSourcePluginBinding, error) {
	value, exists := s.bindings[dataSourceID]
	if !exists {
		return nil, errors.New("not found")
	}
	copy := *value
	return &copy, nil
}
func (s *memoryExternalStore) UpdateInstallationState(_ context.Context, id string, enabled bool, status, lastError string) error {
	value := s.installations[id]
	value.Enabled, value.InstallStatus, value.LastError = enabled, status, lastError
	return nil
}

func TestExternalPluginCannotBecomeReadyWithoutRuntime(t *testing.T) {
	store := &memoryExternalStore{installations: map[string]*control.PluginInstallation{}, bindings: map[string]*control.DataSourcePluginBinding{}}
	manager := New(catalog.New(), store)
	manifest := externalManifest()
	installation := &control.PluginInstallation{
		ID: "installation-1", PluginID: manifest.Metadata.ID, Version: manifest.Metadata.Version,
		ArtifactDigest: "sha256:test",
	}
	if err := manager.InstallExternal(context.Background(), manifest, installation); err != nil {
		t.Fatal(err)
	}
	binding := &control.DataSourcePluginBinding{
		DataSourceID: "ds-1", InstallationID: installation.ID,
		ExtensionID: manifest.Spec.Extension.ID, Generation: 1,
	}
	if err := manager.BindDataSource(context.Background(), binding); err != nil {
		t.Fatal(err)
	}
	if err := manager.EnableExternal(context.Background(), installation.ID); !errors.Is(err, ErrRuntimeNotAvailable) {
		t.Fatalf("enable error = %v", err)
	}
	status, err := manager.Status(manifest.Metadata.ID)
	if err != nil {
		t.Fatal(err)
	}
	if status.State != control.StateNotReady || status.State == control.StateReady {
		t.Fatalf("external state = %s", status.State)
	}
	if persisted := store.installations[installation.ID]; persisted.Enabled || persisted.LastError != ErrRuntimeNotAvailable.Error() {
		t.Fatalf("external installation was made runnable: %#v", persisted)
	}
}

func externalManifest() control.Manifest {
	return control.Manifest{
		APIVersion: control.ManifestAPIVersion, Kind: control.ManifestKind,
		Metadata: control.ManifestMetadata{ID: "community.local-files", Name: "Local Files", Version: "0.1.0"},
		Spec: control.ManifestSpec{
			ProtocolVersion: "1.0", Compatibility: control.ManifestCompatibility{WeKnora: ">=0.7.0 <0.8.0"},
			Extension:    control.ManifestExtension{ID: "local_directory", Type: control.ExtensionDataSource, ContractVersion: "1.0", Capabilities: []string{"full_sync"}},
			Runtime:      control.ManifestRuntime{Kind: control.RuntimeSandbox, Entrypoint: "bin/plugin", Transport: control.TransportUDS},
			Permissions:  control.ManifestPermissions{Network: control.NetworkNone, Filesystem: control.FilesystemReadOnly},
			Resources:    control.ManifestResources{MemoryMiB: 256, CPUQuota: 0.5, MaxProcesses: 32},
			ConfigSchema: map[string]any{"type": "object", "additionalProperties": false},
		},
	}
}

type managerRuntimeHandle struct {
	id         string
	dsID       string
	generation uint64
}

func (h *managerRuntimeHandle) InstanceID() string   { return h.id }
func (h *managerRuntimeHandle) DataSourceID() string { return h.dsID }
func (h *managerRuntimeHandle) Generation() uint64   { return h.generation }
func (h *managerRuntimeHandle) BackendInstance() pluginruntime.BackendInstance {
	return pluginruntime.BackendInstance{ID: h.id}
}
func (h *managerRuntimeHandle) ControlClient() pluginv1.PluginControlClient { return nil }
func (h *managerRuntimeHandle) DataSourceClient() pluginv1.DataSourcePluginClient {
	return nil
}

type managerRuntime struct {
	startErr error
	stopErr  error
}

func (r managerRuntime) Start(_ context.Context, spec pluginruntime.InstanceSpec) (pluginruntime.RuntimeHandle, error) {
	if r.startErr != nil {
		return nil, r.startErr
	}
	return &managerRuntimeHandle{id: "runtime-handle", dsID: spec.DataSourceID, generation: spec.Generation}, nil
}
func (managerRuntime) Health(context.Context, pluginruntime.RuntimeHandle) (pluginruntime.HealthResult, error) {
	return pluginruntime.HealthResult{Status: pluginv1.HealthStatus_HEALTH_STATUS_READY}, nil
}
func (r managerRuntime) Stop(context.Context, pluginruntime.RuntimeHandle, time.Duration) error {
	return r.stopErr
}

func TestExternalPluginBecomesReadyOnlyAfterRuntimeStartReturns(t *testing.T) {
	store := &memoryExternalStore{installations: map[string]*control.PluginInstallation{
		"installation-1": {ID: "installation-1", PluginID: "community.local-files", Version: "0.1.0", Active: true},
	}, bindings: map[string]*control.DataSourcePluginBinding{
		"ds-1": {DataSourceID: "ds-1", InstallationID: "installation-1", ExtensionID: "local", Generation: 1},
	}}
	manager := NewWithRuntime(catalog.New(), store, "", managerRuntime{})
	manager.statuses["community.local-files"] = control.PluginStatus{PluginID: "community.local-files", State: control.StateStopped}
	require.NoError(t, manager.EnableExternal(context.Background(), "installation-1"))
	status, err := manager.Status("community.local-files")
	require.NoError(t, err)
	require.Equal(t, control.StateStarting, status.State)
	handle, err := manager.StartExternal(context.Background(), "installation-1", pluginruntime.InstanceSpec{
		PluginID: "community.local-files", PluginVersion: "0.1.0", ExtensionID: "local", DataSourceID: "ds-1", Generation: 1,
	})
	require.NoError(t, err)
	require.NotNil(t, handle)
	status, err = manager.Status("community.local-files")
	require.NoError(t, err)
	require.Equal(t, control.StateReady, status.State)
}

func TestExternalRuntimeRejectsMissingBinding(t *testing.T) {
	store := &memoryExternalStore{installations: map[string]*control.PluginInstallation{
		"installation-1": {ID: "installation-1", PluginID: "community.local-files", Version: "0.1.0", Active: true},
	}, bindings: map[string]*control.DataSourcePluginBinding{}}
	manager := NewWithRuntime(catalog.New(), store, "", managerRuntime{})
	_, err := manager.StartExternal(context.Background(), "installation-1", pluginruntime.InstanceSpec{
		PluginID: "community.local-files", PluginVersion: "0.1.0", ExtensionID: "local", DataSourceID: "ds-unbound", Generation: 1,
	})
	require.ErrorContains(t, err, "load datasource plugin binding")
}

func TestExternalRuntimeFailureKeepsDesiredEnabledForReconcile(t *testing.T) {
	store := &memoryExternalStore{installations: map[string]*control.PluginInstallation{
		"installation-1": {ID: "installation-1", PluginID: "community.local-files", Version: "0.1.0", Active: true, Enabled: true},
	}, bindings: map[string]*control.DataSourcePluginBinding{
		"ds-1": {DataSourceID: "ds-1", InstallationID: "installation-1", ExtensionID: "local", Generation: 1},
	}}
	manager := NewWithRuntime(catalog.New(), store, "", managerRuntime{startErr: errors.New("health not ready")})
	manager.statuses["community.local-files"] = control.PluginStatus{PluginID: "community.local-files", State: control.StateStarting}
	_, err := manager.StartExternal(context.Background(), "installation-1", pluginruntime.InstanceSpec{
		PluginID: "community.local-files", PluginVersion: "0.1.0", ExtensionID: "local", DataSourceID: "ds-1", Generation: 1,
	})
	require.ErrorContains(t, err, "health not ready")
	require.True(t, store.installations["installation-1"].Enabled)
	status, statusErr := manager.Status("community.local-files")
	require.NoError(t, statusErr)
	require.Equal(t, control.StateNotReady, status.State)
}

func TestExternalStopFailureDoesNotRetainUnroutableHandle(t *testing.T) {
	store := &memoryExternalStore{installations: map[string]*control.PluginInstallation{
		"installation-1": {ID: "installation-1", PluginID: "community.local-files", Version: "0.1.0", Active: true, Enabled: true},
	}, bindings: map[string]*control.DataSourcePluginBinding{
		"ds-1": {DataSourceID: "ds-1", InstallationID: "installation-1", ExtensionID: "local", Generation: 1},
	}}
	manager := NewWithRuntime(catalog.New(), store, "", managerRuntime{stopErr: errors.New("backend cleanup failed")})
	manager.statuses["community.local-files"] = control.PluginStatus{PluginID: "community.local-files", State: control.StateStarting}
	spec := pluginruntime.InstanceSpec{PluginID: "community.local-files", PluginVersion: "0.1.0", ExtensionID: "local", DataSourceID: "ds-1", Generation: 1}
	_, err := manager.StartExternal(context.Background(), "installation-1", spec)
	require.NoError(t, err)
	require.ErrorContains(t, manager.StopExternal(context.Background(), "installation-1", "ds-1", time.Second), "backend cleanup failed")
	_, err = manager.StartExternal(context.Background(), "installation-1", spec)
	require.NoError(t, err, "cleanup failure retained an unroutable manager handle")
}
