package reconcile

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/plugin/control"
	plugindatasource "github.com/Tencent/WeKnora/internal/plugin/datasource"
	pluginruntime "github.com/Tencent/WeKnora/internal/plugin/runtime"
	"github.com/Tencent/WeKnora/internal/types"
	pluginv1 "github.com/Tencent/WeKnora/pkg/plugin/proto/v1"
	"github.com/stretchr/testify/require"
)

type reconcileHandle struct {
	id         string
	dsID       string
	generation uint64
}

func (h *reconcileHandle) InstanceID() string   { return h.id }
func (h *reconcileHandle) DataSourceID() string { return h.dsID }
func (h *reconcileHandle) Generation() uint64   { return h.generation }
func (h *reconcileHandle) BackendInstance() pluginruntime.BackendInstance {
	return pluginruntime.BackendInstance{ID: h.id}
}
func (h *reconcileHandle) ControlClient() pluginv1.PluginControlClient       { return nil }
func (h *reconcileHandle) DataSourceClient() pluginv1.DataSourcePluginClient { return nil }

type reconcileStore struct {
	binding      control.DataSourcePluginBinding
	installation control.PluginInstallation
	state        control.PluginState
	sandboxID    string
	lists        int
}

func (s *reconcileStore) ListBindings(context.Context) ([]control.DataSourcePluginBinding, error) {
	s.lists++
	return []control.DataSourcePluginBinding{s.binding}, nil
}
func (s *reconcileStore) GetBinding(context.Context, string) (*control.DataSourcePluginBinding, error) {
	copy := s.binding
	return &copy, nil
}
func (s *reconcileStore) GetInstallation(context.Context, string) (*control.PluginInstallation, error) {
	copy := s.installation
	return &copy, nil
}
func (s *reconcileStore) GetDataSourceStatus(context.Context, string) (string, error) {
	return types.DataSourceStatusActive, nil
}
func (s *reconcileStore) UpdateBindingObserved(_ context.Context, _ string, _ uint64, state control.PluginState, sandboxID, _ string) error {
	s.state, s.sandboxID = state, sandboxID
	return nil
}

type reconcileController struct {
	resolver *plugindatasource.Resolver
	starts   int
	stops    int
}

func (c *reconcileController) StartExternal(_ context.Context, _ string, spec pluginruntime.InstanceSpec) (pluginruntime.RuntimeHandle, error) {
	c.starts++
	handle := &reconcileHandle{id: "new-instance", dsID: spec.DataSourceID, generation: spec.Generation}
	if err := c.resolver.Publish(spec.DataSourceID, spec.Generation, handle); err != nil {
		return nil, err
	}
	return handle, nil
}
func (c *reconcileController) StopExternal(_ context.Context, _ string, dataSourceID string, _ time.Duration) error {
	c.stops++
	resolved, err := c.resolver.Resolve(dataSourceID)
	if err == nil {
		return c.resolver.Unpublish(dataSourceID, resolved.Generation)
	}
	return nil
}

type reconcileRuntime struct {
	pluginruntime.Runtime
	health func(context.Context, pluginruntime.RuntimeHandle) (pluginruntime.HealthResult, error)
}

func (r reconcileRuntime) Health(ctx context.Context, handle pluginruntime.RuntimeHandle) (pluginruntime.HealthResult, error) {
	if r.health != nil {
		return r.health(ctx, handle)
	}
	return pluginruntime.HealthResult{Status: pluginv1.HealthStatus_HEALTH_STATUS_READY}, nil
}

type reconcileBackend struct {
	mu        sync.Mutex
	instances []pluginruntime.BackendInstance
	stopped   []string
	inspected []string
	filters   map[string]string
}

func (b *reconcileBackend) StartService(context.Context, pluginruntime.InstanceSpec) (pluginruntime.BackendInstance, error) {
	return pluginruntime.BackendInstance{}, nil
}

func (b *reconcileBackend) List(_ context.Context, filters map[string]string) ([]pluginruntime.BackendInstance, error) {
	b.filters = filters
	return append([]pluginruntime.BackendInstance(nil), b.instances...), nil
}
func (b *reconcileBackend) Inspect(_ context.Context, id string) (pluginruntime.BackendInstanceState, error) {
	b.inspected = append(b.inspected, id)
	return pluginruntime.BackendInstanceState{Instance: pluginruntime.BackendInstance{ID: id}, Running: true}, nil
}
func (b *reconcileBackend) Stop(_ context.Context, id string, _ time.Duration) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.stopped = append(b.stopped, id)
	return nil
}

type reconcileSpecs struct{}

func (reconcileSpecs) BuildInstanceSpec(_ context.Context, installation control.PluginInstallation, binding control.DataSourcePluginBinding) (pluginruntime.InstanceSpec, error) {
	return pluginruntime.InstanceSpec{PluginID: string(installation.PluginID), PluginVersion: installation.Version,
		ExtensionID: string(binding.ExtensionID), DataSourceID: binding.DataSourceID, Generation: binding.Generation}, nil
}

type revisionRecorder struct{ pending, cleanup int }

type unavailableSpecs struct{ calls int }

func (s *unavailableSpecs) BuildInstanceSpec(context.Context, control.PluginInstallation, control.DataSourcePluginBinding) (pluginruntime.InstanceSpec, error) {
	s.calls++
	return pluginruntime.InstanceSpec{}, errors.New("grant permanently unavailable")
}

func TestPermanentFailureHasBoundedRetryAndGenerationResetsIt(t *testing.T) {
	resolver := plugindatasource.NewResolver()
	st := &reconcileStore{
		binding:      control.DataSourcePluginBinding{DataSourceID: "ds", InstallationID: "install", Generation: 1},
		installation: control.PluginInstallation{ID: "install", Active: true, Enabled: true, InstallStatus: control.InstallStatusInstalled},
	}
	specs := &unavailableSpecs{}
	r := New(st, &reconcileController{resolver: resolver}, reconcileRuntime{}, &reconcileBackend{}, resolver, specs, nil, nil)
	for n := 0; n < 6; n++ {
		_ = r.ReconcileOnce(context.Background())
		entry := r.retries["ds"]
		entry.next = time.Time{} // bounded retry without wall-clock sleeps
		r.retries["ds"] = entry
	}
	require.Equal(t, 3, specs.calls)
	require.Equal(t, control.StateNotReady, st.state)
	st.binding.Generation++
	require.ErrorContains(t, r.ReconcileOnce(context.Background()), "grant permanently unavailable")
	require.Equal(t, 4, specs.calls)
}

func (r *revisionRecorder) ReconcilePending(context.Context, int) error  { r.pending++; return nil }
func (r *revisionRecorder) CleanupSuperseded(context.Context, int) error { r.cleanup++; return nil }

func TestReconcilerReplacesOldGenerationAndCleansOrphan(t *testing.T) {
	resolver := plugindatasource.NewResolver()
	require.NoError(t, resolver.Publish("ds-1", 1, &reconcileHandle{id: "old-handle", dsID: "ds-1", generation: 1}))
	store := &reconcileStore{
		binding:      control.DataSourcePluginBinding{DataSourceID: "ds-1", InstallationID: "install-1", ExtensionID: "local", Generation: 2},
		installation: control.PluginInstallation{ID: "install-1", PluginID: "test.local", Version: "1.0.0", Active: true, Enabled: true, InstallStatus: control.InstallStatusInstalled},
	}
	controller := &reconcileController{resolver: resolver}
	backend := &reconcileBackend{instances: []pluginruntime.BackendInstance{{ID: "orphan", Metadata: map[string]string{"data_source_id": "orphan-ds"}}}}
	revisions := &revisionRecorder{}
	reconciler := New(store, controller, reconcileRuntime{}, backend, resolver, reconcileSpecs{}, revisions, nil)
	require.NoError(t, reconciler.ReconcileOnce(context.Background()))
	require.Equal(t, 1, controller.stops)
	require.Equal(t, 1, controller.starts)
	require.Contains(t, backend.stopped, "orphan")
	require.Equal(t, control.StateReady, store.state)
	require.Equal(t, "new-instance", store.sandboxID)
	require.Equal(t, 1, revisions.pending)
	require.Equal(t, 1, revisions.cleanup)
}

func TestReconcilerInspectsAndSafelyRebuildsAfterControllerRestart(t *testing.T) {
	resolver := plugindatasource.NewResolver()
	store := &reconcileStore{
		binding:      control.DataSourcePluginBinding{DataSourceID: "ds-1", InstallationID: "install-1", ExtensionID: "local", Generation: 1},
		installation: control.PluginInstallation{ID: "install-1", PluginID: "test.local", Version: "1.0.0", Active: true, Enabled: true, InstallStatus: control.InstallStatusInstalled},
	}
	controller := &reconcileController{resolver: resolver}
	backend := &reconcileBackend{instances: []pluginruntime.BackendInstance{{
		ID: "pre-restart", Metadata: map[string]string{"data_source_id": "ds-1", "generation": "1"},
	}}}
	reconciler := New(store, controller, reconcileRuntime{}, backend, resolver, reconcileSpecs{}, nil, nil)
	require.NoError(t, reconciler.ReconcileOnce(context.Background()))
	require.Empty(t, backend.inspected, "Backend.Stop owns the authoritative inspect")
	require.Contains(t, backend.stopped, "pre-restart")
	require.Equal(t, 1, controller.starts)
	require.Equal(t, "true", backend.filters["managed"])
	require.Equal(t, "plugin", backend.filters["workload_kind"])
	require.Equal(t, control.StateReady, store.state)
}

func TestRunWaitsForPeriodicTickAfterCallerInitialPass(t *testing.T) {
	resolver := plugindatasource.NewResolver()
	store := &reconcileStore{}
	r := New(store, &reconcileController{resolver: resolver}, reconcileRuntime{}, &reconcileBackend{}, resolver, reconcileSpecs{}, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, r.Run(ctx, time.Hour), context.Canceled)
	require.Zero(t, store.lists)
}

func TestManualAndPeriodicHealthShareCleanupWithoutSharingRetryBudget(t *testing.T) {
	newFixture := func(cancelSync func(string, uint64, string)) (*Reconciler, *reconcileStore, *reconcileController) {
		resolver := plugindatasource.NewResolver()
		require.NoError(t, resolver.Publish("ds-1", 1, &reconcileHandle{id: "instance-1", dsID: "ds-1", generation: 1}))
		store := &reconcileStore{
			binding:      control.DataSourcePluginBinding{DataSourceID: "ds-1", InstallationID: "install-1", Generation: 1},
			installation: control.PluginInstallation{ID: "install-1", Active: true, Enabled: true, InstallStatus: control.InstallStatusInstalled},
		}
		controller := &reconcileController{resolver: resolver}
		runtime := reconcileRuntime{health: func(context.Context, pluginruntime.RuntimeHandle) (pluginruntime.HealthResult, error) {
			return pluginruntime.HealthResult{Status: pluginv1.HealthStatus_HEALTH_STATUS_NOT_READY}, nil
		}}
		return New(store, controller, runtime, &reconcileBackend{}, resolver, reconcileSpecs{}, nil, cancelSync), store, controller
	}

	canceled := 0
	manual, manualStore, manualController := newFixture(func(string, uint64, string) { canceled++ })
	_, err := manual.Health(context.Background(), "ds-1")
	require.ErrorContains(t, err, "not READY")
	require.Equal(t, 1, canceled)
	require.Equal(t, 1, manualController.stops)
	require.Equal(t, control.StateNotReady, manualStore.state)
	require.Empty(t, manual.retries, "manual health must not consume automatic retry budget")

	periodic, periodicStore, periodicController := newFixture(func(string, uint64, string) { canceled++ })
	err = periodic.ReconcileOnce(context.Background())
	require.ErrorContains(t, err, "not READY")
	require.Equal(t, 2, canceled)
	require.Equal(t, 1, periodicController.stops)
	require.Equal(t, control.StateNotReady, periodicStore.state)
	require.Equal(t, 1, periodic.retries["ds-1"].attempts)
}

func TestLateHealthFailureCannotStopReplacementInstance(t *testing.T) {
	resolver := plugindatasource.NewResolver()
	old := &reconcileHandle{id: "old-instance", dsID: "ds-1", generation: 1}
	require.NoError(t, resolver.Publish("ds-1", 1, old))
	store := &reconcileStore{
		binding:      control.DataSourcePluginBinding{DataSourceID: "ds-1", InstallationID: "install-1", Generation: 1},
		installation: control.PluginInstallation{ID: "install-1", Active: true, Enabled: true, InstallStatus: control.InstallStatusInstalled},
	}
	entered, release := make(chan struct{}), make(chan struct{})
	runtime := reconcileRuntime{health: func(context.Context, pluginruntime.RuntimeHandle) (pluginruntime.HealthResult, error) {
		close(entered)
		<-release
		return pluginruntime.HealthResult{}, errors.New("old instance unhealthy")
	}}
	controller := &reconcileController{resolver: resolver}
	canceled := 0
	r := New(store, controller, runtime, &reconcileBackend{}, resolver, reconcileSpecs{}, nil, func(string, uint64, string) { canceled++ })
	done := make(chan error, 1)
	go func() { _, err := r.Health(context.Background(), "ds-1"); done <- err }()
	<-entered
	_, err := resolver.UnpublishInstance("ds-1", 1, old.InstanceID())
	require.NoError(t, err)
	require.NoError(t, resolver.Publish("ds-1", 1, &reconcileHandle{id: "replacement", dsID: "ds-1", generation: 1}))
	close(release)
	require.ErrorContains(t, <-done, "instance changed")
	require.Zero(t, canceled)
	require.Zero(t, controller.stops)
	current, err := resolver.Resolve("ds-1")
	require.NoError(t, err)
	require.Equal(t, "replacement", current.Handle.InstanceID())
}
