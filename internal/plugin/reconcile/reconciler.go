// Package reconcile contains the single-controller convergence loop for V1
// external datasource instances and pending revision tasks.
package reconcile

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/plugin/control"
	plugindatasource "github.com/Tencent/WeKnora/internal/plugin/datasource"
	pluginruntime "github.com/Tencent/WeKnora/internal/plugin/runtime"
	"github.com/Tencent/WeKnora/internal/types"
	pluginv1 "github.com/Tencent/WeKnora/pkg/plugin/proto/v1"
)

type Store interface {
	ListBindings(context.Context) ([]control.DataSourcePluginBinding, error)
	GetBinding(context.Context, string) (*control.DataSourcePluginBinding, error)
	GetInstallation(context.Context, string) (*control.PluginInstallation, error)
	GetDataSourceStatus(context.Context, string) (string, error)
	UpdateBindingObserved(context.Context, string, uint64, control.PluginState, string, string) error
}

type Controller interface {
	StartExternal(context.Context, string, pluginruntime.InstanceSpec) (pluginruntime.RuntimeHandle, error)
	StopExternal(context.Context, string, string, time.Duration) error
}

type SpecBuilder interface {
	BuildInstanceSpec(context.Context, control.PluginInstallation, control.DataSourcePluginBinding) (pluginruntime.InstanceSpec, error)
}

type RevisionReconciler interface {
	ReconcilePending(context.Context, int) error
	CleanupSuperseded(context.Context, int) error
}

type Reconciler struct {
	mu         sync.Mutex
	retries    map[string]retry
	store      Store
	controller Controller
	runtime    pluginruntime.Runtime
	backend    pluginruntime.PluginSandboxBackend
	resolver   *plugindatasource.Resolver
	specs      SpecBuilder
	revisions  RevisionReconciler
	cancelSync func(string, uint64, string)
	grace      time.Duration
}

type retry struct {
	generation uint64
	attempts   int
	next       time.Time
}

// WithLock serializes administrator desired-state changes with reconciliation.
// It is process-local; V1 has exactly one Controller.
func (r *Reconciler) WithLock(fn func() error) error { r.mu.Lock(); defer r.mu.Unlock(); return fn() }

func New(
	store Store, controller Controller, runtime pluginruntime.Runtime, backend pluginruntime.PluginSandboxBackend,
	resolver *plugindatasource.Resolver, specs SpecBuilder, revisions RevisionReconciler,
	cancelSync func(string, uint64, string),
) *Reconciler {
	return &Reconciler{store: store, controller: controller, runtime: runtime, backend: backend,
		resolver: resolver, specs: specs, revisions: revisions, cancelSync: cancelSync,
		grace: 5 * time.Second, retries: map[string]retry{}}
}

// Run performs periodic passes. The application performs the one initial pass
// synchronously before opening QueuePlugin consumption.
func (r *Reconciler) Run(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		return fmt.Errorf("reconcile interval must be positive")
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := r.ReconcileOnce(ctx); err != nil {
				logger.Warnf(ctx, "[PluginReconciler] pass: %v", err)
			}
		}
	}
}

func (r *Reconciler) ReconcileOnce(ctx context.Context) error {
	if r == nil || r.store == nil || r.controller == nil || r.runtime == nil || r.resolver == nil || r.specs == nil {
		return fmt.Errorf("plugin reconciler is not fully configured")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	bindings, err := r.store.ListBindings(ctx)
	if err != nil {
		return err
	}
	backendInstances, err := r.listManaged(ctx)
	if err != nil {
		return err
	}
	owned := make(map[string]struct{}, len(bindings))
	var result error
	for _, binding := range bindings {
		if err := ctx.Err(); err != nil {
			return errors.Join(result, err)
		}
		owned[binding.DataSourceID] = struct{}{}
		if err := r.reconcileBinding(ctx, binding, backendInstances[binding.DataSourceID]); err != nil {
			result = errors.Join(result, err)
		}
	}
	for dataSourceID, instances := range backendInstances {
		if _, exists := owned[dataSourceID]; exists {
			continue
		}
		for _, instance := range instances {
			result = errors.Join(result, r.backend.Stop(ctx, instance.ID, r.grace))
		}
	}
	if r.revisions != nil {
		result = errors.Join(result, r.revisions.ReconcilePending(ctx, 100))
		result = errors.Join(result, r.revisions.CleanupSuperseded(ctx, 100))
	}
	return result
}

func (r *Reconciler) reconcileBinding(
	ctx context.Context, binding control.DataSourcePluginBinding, backendInstances []pluginruntime.BackendInstance,
) error {
	installation, err := r.store.GetInstallation(ctx, binding.InstallationID)
	if err != nil {
		return r.observe(ctx, binding, control.StateFailed, "", err)
	}
	status, err := r.store.GetDataSourceStatus(ctx, binding.DataSourceID)
	if err != nil {
		return r.observe(ctx, binding, control.StateFailed, "", err)
	}
	desired := installation.Active && installation.Enabled && installation.InstallStatus == control.InstallStatusInstalled &&
		(status == types.DataSourceStatusActive || status == types.DataSourceStatusError)
	resolved, resolveErr := r.resolver.Resolve(binding.DataSourceID)
	if !desired {
		if resolveErr == nil {
			if stopErr := r.controller.StopExternal(ctx, binding.InstallationID, binding.DataSourceID, r.grace); stopErr != nil {
				return r.observe(ctx, binding, control.StateFailed, resolved.Handle.InstanceID(), stopErr)
			}
		}
		for _, instance := range backendInstances {
			if stopErr := r.backend.Stop(ctx, instance.ID, r.grace); stopErr != nil {
				return r.observe(ctx, binding, control.StateFailed, instance.ID, stopErr)
			}
		}
		return r.observe(ctx, binding, control.StateStopped, "", nil)
	}

	if resolveErr == nil && resolved.Generation == binding.Generation {
		runtimeHandle, ok := resolved.Handle.(pluginruntime.RuntimeHandle)
		if !ok {
			return r.observe(ctx, binding, control.StateFailed, resolved.Handle.InstanceID(), errors.New("published handle is not a runtime handle"))
		}
		_, err := r.checkHealth(ctx, binding, runtimeHandle, true)
		return err
	} else if resolveErr == nil {
		if stopErr := r.controller.StopExternal(ctx, binding.InstallationID, binding.DataSourceID, r.grace); stopErr != nil {
			return r.observe(ctx, binding, control.StateFailed, resolved.Handle.InstanceID(), stopErr)
		}
	}

	// A process restart loses the in-memory UDS connection and cgroup policy
	// consumer. Existing backend instances are inspected then safely rebuilt;
	// no fake Recover RPC or implicit READY is used.
	for _, instance := range backendInstances {
		if stopErr := r.backend.Stop(ctx, instance.ID, r.grace); stopErr != nil {
			return r.observe(ctx, binding, control.StateFailed, instance.ID, stopErr)
		}
	}
	previous := r.retries[binding.DataSourceID]
	if previous.generation == binding.Generation && (previous.attempts >= 3 || time.Now().Before(previous.next)) {
		return nil
	}
	spec, err := r.specs.BuildInstanceSpec(ctx, *installation, binding)
	if err != nil {
		return r.observe(ctx, binding, control.StateNotReady, "", err)
	}
	handle, err := r.controller.StartExternal(ctx, installation.ID, spec)
	if err != nil {
		return r.observe(ctx, binding, control.StateNotReady, "", err)
	}
	return r.observe(ctx, binding, control.StateReady, handle.InstanceID(), nil)
}

// Health applies the same identity fence and cleanup order as periodic health
// reconciliation without consuming the automatic restart budget.
func (r *Reconciler) Health(ctx context.Context, dataSourceID string) (pluginruntime.HealthResult, error) {
	if r == nil || r.store == nil || r.controller == nil || r.runtime == nil || r.resolver == nil {
		return pluginruntime.HealthResult{}, fmt.Errorf("plugin reconciler is not fully configured")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	binding, err := r.store.GetBinding(ctx, dataSourceID)
	if err != nil {
		return pluginruntime.HealthResult{}, err
	}
	resolved, err := r.resolver.Resolve(dataSourceID)
	if err != nil {
		return pluginruntime.HealthResult{}, err
	}
	handle, ok := resolved.Handle.(pluginruntime.RuntimeHandle)
	if !ok || resolved.Generation != binding.Generation {
		return pluginruntime.HealthResult{}, errors.New("published handle does not match the current binding")
	}
	return r.checkHealth(ctx, *binding, handle, false)
}

func (r *Reconciler) checkHealth(
	ctx context.Context, binding control.DataSourcePluginBinding, handle pluginruntime.RuntimeHandle, countRetry bool,
) (pluginruntime.HealthResult, error) {
	health, healthErr := r.runtime.Health(ctx, handle)
	current, resolveErr := r.resolver.Resolve(binding.DataSourceID)
	if resolveErr != nil || current.Generation != binding.Generation || current.Handle.InstanceID() != handle.InstanceID() {
		return health, errors.New("plugin instance changed while health check was in flight")
	}
	if healthErr == nil && health.Status == pluginv1.HealthStatus_HEALTH_STATUS_READY {
		if countRetry {
			delete(r.retries, binding.DataSourceID) // survived a full reconciliation interval
		}
		return health, r.record(ctx, binding, control.StateReady, handle.InstanceID(), nil, false)
	}
	if healthErr == nil {
		healthErr = errors.New("plugin health is not READY")
	}
	if r.cancelSync != nil {
		r.cancelSync(binding.DataSourceID, binding.Generation, handle.InstanceID())
	}
	stopErr := r.controller.StopExternal(ctx, binding.InstallationID, binding.DataSourceID, r.grace)
	state, locator := control.StateNotReady, ""
	if stopErr != nil {
		state, locator = control.StateFailed, handle.InstanceID()
	}
	return health, r.record(ctx, binding, state, locator, errors.Join(healthErr, stopErr), countRetry)
}

func (r *Reconciler) listManaged(ctx context.Context) (map[string][]pluginruntime.BackendInstance, error) {
	result := make(map[string][]pluginruntime.BackendInstance)
	if r.backend == nil {
		return result, nil
	}
	instances, err := r.backend.List(ctx, map[string]string{"managed": "true", "workload_kind": "plugin"})
	if err != nil {
		return nil, err
	}
	for _, instance := range instances {
		if instance.Metadata["data_source_id"] == "" {
			continue
		}
		result[instance.Metadata["data_source_id"]] = append(result[instance.Metadata["data_source_id"]], instance)
	}
	return result, nil
}

func (r *Reconciler) observe(
	ctx context.Context, binding control.DataSourcePluginBinding, state control.PluginState, sandboxID string, cause error,
) error {
	return r.record(ctx, binding, state, sandboxID, cause, true)
}

func (r *Reconciler) record(
	ctx context.Context, binding control.DataSourcePluginBinding, state control.PluginState, sandboxID string, cause error, countRetry bool,
) error {
	message := ""
	if cause != nil {
		message = cause.Error()
		if sandboxID == "" {
			sandboxID = binding.SandboxID
		} // retain cleanup location until a successful observation
	}
	if countRetry && state == control.StateNotReady && cause != nil {
		entry := r.retries[binding.DataSourceID]
		if entry.generation != binding.Generation {
			entry = retry{generation: binding.Generation}
		}
		entry.attempts++
		entry.next = time.Now().Add(time.Duration(entry.attempts) * 10 * time.Second)
		r.retries[binding.DataSourceID] = entry
	} else if state == control.StateStopped {
		delete(r.retries, binding.DataSourceID)
	}
	persistCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	updateErr := r.store.UpdateBindingObserved(persistCtx, binding.DataSourceID, binding.Generation, state, sandboxID, message)
	return errors.Join(cause, updateErr)
}
