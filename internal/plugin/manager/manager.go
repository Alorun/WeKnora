// Package manager owns plugin state transitions and Registrar publication.
package manager

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/Tencent/WeKnora/internal/plugin/builtin"
	"github.com/Tencent/WeKnora/internal/plugin/catalog"
	"github.com/Tencent/WeKnora/internal/plugin/control"
)

var (
	ErrNotFound            = errors.New("plugin is not managed")
	ErrDisableNotAllowed   = errors.New("plugin cannot be disabled")
	ErrRuntimeNotAvailable = errors.New("runtime_not_available")
)

type ExternalStore interface {
	CreateInstallation(context.Context, *control.PluginInstallation) error
	CreateBinding(context.Context, *control.DataSourcePluginBinding) error
	GetInstallation(context.Context, string) (*control.PluginInstallation, error)
	UpdateInstallationState(context.Context, string, bool, string, string) error
}

type managedBuiltin struct {
	descriptor builtin.BuiltinDescriptor
	status     control.PluginStatus
	published  bool
}

type PluginManager struct {
	mu          sync.Mutex
	catalog     *catalog.Catalog
	external    ExternalStore
	builtins    map[control.PluginID]*managedBuiltin
	statuses    map[control.PluginID]control.PluginStatus
	hostVersion string
	now         func() time.Time
}

func New(pluginCatalog *catalog.Catalog, external ExternalStore) *PluginManager {
	return NewWithVersion(pluginCatalog, external, "")
}

func NewWithVersion(pluginCatalog *catalog.Catalog, external ExternalStore, hostVersion string) *PluginManager {
	return &PluginManager{
		catalog:     pluginCatalog,
		external:    external,
		builtins:    make(map[control.PluginID]*managedBuiltin),
		statuses:    make(map[control.PluginID]control.PluginStatus),
		hostVersion: hostVersion,
		now:         time.Now,
	}
}

func (m *PluginManager) LoadBuiltins(descriptors []builtin.BuiltinDescriptor) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	seen := make(map[control.PluginID]struct{}, len(descriptors))
	for _, descriptor := range descriptors {
		if err := descriptor.Validate(); err != nil {
			return fmt.Errorf("validate builtin %q: %w", descriptor.Definition.ID, err)
		}
		if _, exists := seen[descriptor.Definition.ID]; exists {
			return fmt.Errorf("duplicate builtin descriptor %q", descriptor.Definition.ID)
		}
		if _, exists := m.builtins[descriptor.Definition.ID]; exists {
			return fmt.Errorf("builtin descriptor %q already loaded", descriptor.Definition.ID)
		}
		seen[descriptor.Definition.ID] = struct{}{}
	}

	registered := make([]control.PluginID, 0, len(descriptors))
	for _, descriptor := range descriptors {
		if err := m.catalog.Register(descriptor.Definition); err != nil {
			for _, id := range registered {
				_ = m.catalog.Unregister(id)
			}
			return fmt.Errorf("register builtin %q: %w", descriptor.Definition.ID, err)
		}
		registered = append(registered, descriptor.Definition.ID)
	}
	for _, descriptor := range descriptors {
		status := control.PluginStatus{
			PluginID:  descriptor.Definition.ID,
			State:     control.StateStopped,
			UpdatedAt: m.now(),
		}
		m.builtins[descriptor.Definition.ID] = &managedBuiltin{descriptor: descriptor, status: status}
		m.statuses[descriptor.Definition.ID] = status
	}
	return nil
}

func (m *PluginManager) StartAll(ctx context.Context) error {
	ids := m.builtinIDs()
	started := make([]control.PluginID, 0, len(ids))
	for _, id := range ids {
		if err := m.Start(ctx, id); err != nil {
			startErr := err
			for index := len(started) - 1; index >= 0; index-- {
				startErr = errors.Join(startErr, m.rollbackStarted(ctx, started[index]))
			}
			return fmt.Errorf("start builtin %q: %w", id, startErr)
		}
		started = append(started, id)
	}
	return nil
}

func (m *PluginManager) Start(ctx context.Context, id control.PluginID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	managed, exists := m.builtins[id]
	if !exists {
		return fmt.Errorf("%s: %w", id, ErrNotFound)
	}
	if managed.status.State == control.StateReady {
		return nil
	}
	if managed.published {
		if err := managed.descriptor.Registrar.Health(ctx); err != nil {
			m.setBuiltinStatus(managed, control.StateDegraded, err.Error())
			return err
		}
		m.setBuiltinStatus(managed, control.StateReady, "")
		return nil
	}
	m.setBuiltinStatus(managed, control.StateStarting, "")
	if managed.descriptor.Start != nil {
		if err := managed.descriptor.Start(ctx); err != nil {
			m.setBuiltinStatus(managed, control.StateFailed, err.Error())
			return err
		}
	}
	if err := managed.descriptor.Registrar.Publish(ctx); err != nil {
		startErr := err
		if managed.descriptor.Stop != nil {
			startErr = errors.Join(startErr, managed.descriptor.Stop(ctx))
		}
		m.setBuiltinStatus(managed, control.StateFailed, startErr.Error())
		return startErr
	}
	managed.published = true
	if err := managed.descriptor.Registrar.Health(ctx); err != nil {
		startErr := err
		if unpublishErr := managed.descriptor.Registrar.Unpublish(ctx); unpublishErr != nil {
			startErr = errors.Join(startErr, unpublishErr)
		} else {
			managed.published = false
			if managed.descriptor.Stop != nil {
				startErr = errors.Join(startErr, managed.descriptor.Stop(ctx))
			}
		}
		m.setBuiltinStatus(managed, control.StateFailed, startErr.Error())
		return startErr
	}
	m.setBuiltinStatus(managed, control.StateReady, "")
	return nil
}

func (m *PluginManager) Stop(ctx context.Context, id control.PluginID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	managed, exists := m.builtins[id]
	if !exists {
		return fmt.Errorf("%s: %w", id, ErrNotFound)
	}
	if !managed.descriptor.AllowDisable {
		return fmt.Errorf("%s: %w", id, ErrDisableNotAllowed)
	}
	return m.stopLocked(ctx, managed)
}

func (m *PluginManager) Health(ctx context.Context, id control.PluginID) control.PluginStatus {
	m.mu.Lock()
	defer m.mu.Unlock()
	managed, exists := m.builtins[id]
	if !exists {
		return control.PluginStatus{PluginID: id, State: control.StateNotReady, LastError: ErrNotFound.Error(), UpdatedAt: m.now()}
	}
	if managed.status.State == control.StateReady || managed.status.State == control.StateDegraded {
		if err := managed.descriptor.Registrar.Health(ctx); err != nil {
			m.setBuiltinStatus(managed, control.StateDegraded, err.Error())
		} else {
			m.setBuiltinStatus(managed, control.StateReady, "")
		}
	}
	return managed.status
}

func (m *PluginManager) Status(id control.PluginID) (control.PluginStatus, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	status, exists := m.statuses[id]
	if !exists {
		return control.PluginStatus{}, fmt.Errorf("%s: %w", id, ErrNotFound)
	}
	return status, nil
}

func (m *PluginManager) Statuses() []control.PluginStatus {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]control.PluginStatus, 0, len(m.statuses))
	for _, status := range m.statuses {
		result = append(result, status)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].PluginID < result[j].PluginID })
	return result
}

// InstallExternal records a validated external definition. It does not start a
// runtime or publish a business capability in Phase A.
func (m *PluginManager) InstallExternal(
	ctx context.Context,
	manifest control.Manifest,
	installation *control.PluginInstallation,
) error {
	if installation == nil || installation.ID == "" {
		return errors.New("installation id is required")
	}
	if manifest.Metadata.ID != installation.PluginID || manifest.Metadata.Version != installation.Version {
		return errors.New("installation identity does not match manifest")
	}
	if installation.ArtifactDigest == "" {
		return errors.New("artifact digest is required")
	}
	if err := manifest.Validate(control.ManifestValidationOptions{
		BuiltinIDs: m.catalog.BuiltinIDs(), WeKnoraVersion: m.hostVersion,
	}); err != nil {
		return err
	}
	definition := manifest.Definition()
	if err := definition.Validate(); err != nil {
		return err
	}
	if definition.ExtensionType != control.ExtensionDataSource {
		return errors.New("external plugins only support datasource")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.external == nil {
		return errors.New("external plugin store is not configured")
	}
	if err := m.catalog.Register(definition); err != nil {
		return err
	}
	installation.Enabled = false
	installation.Active = true
	installation.InstallStatus = control.InstallStatusInstalled
	installation.LastError = ""
	if err := m.external.CreateInstallation(ctx, installation); err != nil {
		_ = m.catalog.Unregister(definition.ID)
		return err
	}
	status := control.PluginStatus{PluginID: definition.ID, State: control.StateStopped, UpdatedAt: m.now()}
	m.statuses[definition.ID] = status
	return nil
}

func (m *PluginManager) BindDataSource(ctx context.Context, binding *control.DataSourcePluginBinding) error {
	if binding == nil || binding.DataSourceID == "" || binding.InstallationID == "" || binding.ExtensionID == "" {
		return errors.New("binding data_source_id, installation_id and extension_id are required")
	}
	if binding.Generation == 0 {
		return errors.New("binding generation must be greater than zero")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.external == nil {
		return errors.New("external plugin store is not configured")
	}
	installation, err := m.external.GetInstallation(ctx, binding.InstallationID)
	if err != nil {
		return err
	}
	definition, err := m.catalog.Get(installation.PluginID)
	if err != nil {
		return err
	}
	if definition.Source != control.SourceExternal || definition.ExtensionType != control.ExtensionDataSource || definition.ExtensionID != binding.ExtensionID {
		return errors.New("binding does not match an installed external datasource extension")
	}
	binding.ObservedState = control.StateStopped
	binding.ObservedGeneration = 0
	return m.external.CreateBinding(ctx, binding)
}

func (m *PluginManager) EnableExternal(ctx context.Context, installationID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.external == nil {
		return errors.New("external plugin store is not configured")
	}
	installation, err := m.external.GetInstallation(ctx, installationID)
	if err != nil {
		return err
	}
	pluginID := installation.PluginID
	status := control.PluginStatus{
		PluginID: pluginID, State: control.StateNotReady,
		LastError: ErrRuntimeNotAvailable.Error(), UpdatedAt: m.now(),
	}
	m.statuses[pluginID] = status
	if err := m.external.UpdateInstallationState(ctx, installationID, false, control.InstallStatusInstalled, ErrRuntimeNotAvailable.Error()); err != nil {
		return err
	}
	return ErrRuntimeNotAvailable
}

func (m *PluginManager) builtinIDs() []control.PluginID {
	m.mu.Lock()
	defer m.mu.Unlock()
	ids := make([]control.PluginID, 0, len(m.builtins))
	for id := range m.builtins {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func (m *PluginManager) rollbackStarted(ctx context.Context, id control.PluginID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if managed := m.builtins[id]; managed != nil {
		return m.stopLocked(ctx, managed)
	}
	return nil
}

func (m *PluginManager) stopLocked(ctx context.Context, managed *managedBuiltin) error {
	if managed.status.State == control.StateStopped {
		return nil
	}
	if managed.published {
		if err := managed.descriptor.Registrar.Unpublish(ctx); err != nil {
			m.setBuiltinStatus(managed, control.StateFailed, err.Error())
			return err
		}
		managed.published = false
	}
	if managed.descriptor.Stop != nil {
		if err := managed.descriptor.Stop(ctx); err != nil {
			m.setBuiltinStatus(managed, control.StateFailed, err.Error())
			return err
		}
	}
	m.setBuiltinStatus(managed, control.StateStopped, "")
	return nil
}

func (m *PluginManager) setBuiltinStatus(managed *managedBuiltin, state control.PluginState, lastError string) {
	managed.status.State = state
	managed.status.LastError = lastError
	managed.status.UpdatedAt = m.now()
	m.statuses[managed.status.PluginID] = managed.status
}
