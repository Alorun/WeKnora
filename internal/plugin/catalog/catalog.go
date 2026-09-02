// Package catalog stores immutable plugin definitions known to this process.
package catalog

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/Tencent/WeKnora/internal/plugin/control"
)

var (
	ErrNotFound  = errors.New("plugin definition not found")
	ErrDuplicate = errors.New("plugin definition already registered")
)

type Catalog struct {
	mu          sync.RWMutex
	definitions map[control.PluginID]control.PluginDefinition
}

func New() *Catalog {
	return &Catalog{definitions: make(map[control.PluginID]control.PluginDefinition)}
}

func (c *Catalog) Register(definition control.PluginDefinition) error {
	if err := definition.Validate(); err != nil {
		return err
	}
	if definition.Source == control.SourceExternal && strings.HasPrefix(string(definition.ID), "builtin.") {
		return fmt.Errorf("external plugin %q cannot use the builtin namespace", definition.ID)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if existing, ok := c.definitions[definition.ID]; ok {
		if existing.Source == control.SourceBuiltin && definition.Source == control.SourceExternal {
			return fmt.Errorf("external plugin %q cannot replace builtin definition: %w", definition.ID, ErrDuplicate)
		}
		return fmt.Errorf("plugin %q already has active version %s: %w", definition.ID, existing.Version, ErrDuplicate)
	}
	c.definitions[definition.ID] = control.CloneDefinition(definition)
	return nil
}

func (c *Catalog) Get(id control.PluginID) (control.PluginDefinition, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	definition, ok := c.definitions[id]
	if !ok {
		return control.PluginDefinition{}, fmt.Errorf("plugin %q: %w", id, ErrNotFound)
	}
	return control.CloneDefinition(definition), nil
}

// Unregister is used to roll back an atomic manager load. Runtime stop does not
// remove definitions from the Catalog.
func (c *Catalog) Unregister(id control.PluginID) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.definitions[id]; !exists {
		return fmt.Errorf("plugin %q: %w", id, ErrNotFound)
	}
	delete(c.definitions, id)
	return nil
}

func (c *Catalog) List(extensionType control.ExtensionType) []control.PluginDefinition {
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := make([]control.PluginDefinition, 0, len(c.definitions))
	for _, definition := range c.definitions {
		if extensionType == "" || definition.ExtensionType == extensionType {
			result = append(result, control.CloneDefinition(definition))
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result
}

func (c *Catalog) BuiltinIDs() map[control.PluginID]struct{} {
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := make(map[control.PluginID]struct{})
	for id, definition := range c.definitions {
		if definition.Source == control.SourceBuiltin {
			result[id] = struct{}{}
		}
	}
	return result
}
