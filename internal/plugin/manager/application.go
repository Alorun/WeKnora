package manager

import (
	"errors"

	"github.com/Tencent/WeKnora/internal/plugin/control"
	pr "github.com/Tencent/WeKnora/internal/plugin/runtime"
)

// ConfigureRuntime is a one-shot application wiring seam, not a fallback.
func (m *PluginManager) ConfigureRuntime(runtime pr.Runtime) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if runtime == nil || m.runtime != nil {
		return errors.New("runtime already configured or nil")
	}
	m.runtime = runtime
	return nil
}

// LoadExternal restores a discovered definition without rewriting enable intent
// or creating another installation. StartAll continues to select builtins only.
func (m *PluginManager) LoadExternal(manifest control.Manifest) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := manifest.Validate(control.ManifestValidationOptions{BuiltinIDs: m.catalog.BuiltinIDs(), WeKnoraVersion: m.hostVersion}); err != nil {
		return err
	}
	if err := m.catalog.Register(manifest.Definition()); err != nil {
		return err
	}
	return nil
}
