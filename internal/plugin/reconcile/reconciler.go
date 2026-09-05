// Package reconcile contains the single-controller convergence loop for V1
// external datasource instances and pending revision tasks.
package reconcile

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Tencent/WeKnora/internal/plugin/control"
	plugindatasource "github.com/Tencent/WeKnora/internal/plugin/datasource"
	pluginruntime "github.com/Tencent/WeKnora/internal/plugin/runtime"
	"github.com/Tencent/WeKnora/internal/types"
	pluginv1 "github.com/Tencent/WeKnora/pkg/plugin/proto/v1"
)

type Store interface {
	ListBindings(context.Context) ([]control.DataSourcePluginBinding, error)
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
	store      Store
	controller Controller
	runtime    pluginruntime.Runtime
	backend    pluginruntime.PluginSandboxBackend
	resolver   *plugindatasource.Resolver
	specs      SpecBuilder
	revisions  RevisionReconciler
	grace      time.Duration
}

func New(
	store Store, controller Controller, runtime pluginruntime.Runtime, backend pluginruntime.PluginSandboxBackend,
	resolver *plugindatasource.Resolver, specs SpecBuilder, revisions RevisionReconciler,
) *Reconciler {
	return &Reconciler{store: store, controller: controller, runtime: runtime, backend: backend,
		resolver: resolver, specs: specs, revisions: revisions, grace: 5 * time.Second}
}

func (r *Reconciler) Run(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		return fmt.Errorf("reconcile interval must be positive")
	}
	if err := r.ReconcileOnce(ctx); err != nil {
		return err
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			_ = r.ReconcileOnce(ctx)
		}
	}
}

func (r *Reconciler) ReconcileOnce(ctx context.Context) error {
	if r == nil || r.store == nil || r.controller == nil || r.runtime == nil || r.resolver == nil || r.specs == nil {
		return fmt.Errorf("plugin reconciler is not fully configured")
	}
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
	desired := installation.Active && installation.Enabled &&
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
		health, healthErr := r.runtime.Health(ctx, runtimeHandle)
		if healthErr == nil && health.Status == pluginv1.HealthStatus_HEALTH_STATUS_READY {
			return r.observe(ctx, binding, control.StateReady, runtimeHandle.InstanceID(), nil)
		}
		// One bounded restart per pass. Stop unpublishes before any replacement.
		if stopErr := r.controller.StopExternal(ctx, binding.InstallationID, binding.DataSourceID, r.grace); stopErr != nil {
			return r.observe(ctx, binding, control.StateFailed, runtimeHandle.InstanceID(), errors.Join(healthErr, stopErr))
		}
	} else if resolveErr == nil {
		if stopErr := r.controller.StopExternal(ctx, binding.InstallationID, binding.DataSourceID, r.grace); stopErr != nil {
			return r.observe(ctx, binding, control.StateFailed, resolved.Handle.InstanceID(), stopErr)
		}
	}

	// A process restart loses the in-memory UDS connection and cgroup policy
	// consumer. Existing backend instances are inspected then safely rebuilt;
	// no fake Recover RPC or implicit READY is used.
	for _, instance := range backendInstances {
		state, inspectErr := r.backend.Inspect(ctx, instance.ID)
		if inspectErr != nil {
			return r.observe(ctx, binding, control.StateFailed, instance.ID, inspectErr)
		}
		_ = state
		if stopErr := r.backend.Stop(ctx, instance.ID, r.grace); stopErr != nil {
			return r.observe(ctx, binding, control.StateFailed, instance.ID, stopErr)
		}
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
	message := ""
	if cause != nil {
		message = cause.Error()
	}
	updateErr := r.store.UpdateBindingObserved(ctx, binding.DataSourceID, binding.Generation, state, sandboxID, message)
	return errors.Join(cause, updateErr)
}
