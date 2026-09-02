// Package builtin adapts in-process extensions to the unified plugin lifecycle.
package builtin

import (
	"context"
	"errors"
	"fmt"

	"github.com/Tencent/WeKnora/internal/datasource"
	"github.com/Tencent/WeKnora/internal/infrastructure/docparser"
	websearch "github.com/Tencent/WeKnora/internal/infrastructure/web_search"
	"github.com/Tencent/WeKnora/internal/models/provider"
	"github.com/Tencent/WeKnora/internal/plugin/control"
)

type Registrar interface {
	Publish(context.Context) error
	Unpublish(context.Context) error
	Health(context.Context) error
}

type Hook func(context.Context) error

type BuiltinDescriptor struct {
	Definition   control.PluginDefinition
	AllowDisable bool
	Start        Hook
	Stop         Hook
	Registrar    Registrar
}

func (d BuiltinDescriptor) Validate() error {
	if d.Definition.Source != control.SourceBuiltin {
		return errors.New("builtin descriptor must have builtin source")
	}
	if err := d.Definition.Validate(); err != nil {
		return err
	}
	if d.Registrar == nil {
		return errors.New("builtin descriptor registrar is required")
	}
	return nil
}

type DataSourceRegistrar struct {
	Registry  *datasource.ConnectorRegistry
	Connector datasource.Connector
}

func (r *DataSourceRegistrar) Publish(context.Context) error {
	if r.Registry == nil || r.Connector == nil {
		return errors.New("datasource registrar is incomplete")
	}
	if err := r.Registry.Register(r.Connector); err != nil {
		return err
	}
	if err := datasource.PublishConnectorMetadata(r.Connector.Type()); err != nil {
		_ = r.Registry.Unregister(r.Connector.Type())
		return err
	}
	return nil
}

func (r *DataSourceRegistrar) Unpublish(context.Context) error {
	if err := r.Registry.Unregister(r.Connector.Type()); err != nil {
		return err
	}
	return datasource.UnpublishConnectorMetadata(r.Connector.Type())
}

func (r *DataSourceRegistrar) Health(context.Context) error {
	registered, err := r.Registry.Get(r.Connector.Type())
	if err != nil {
		return err
	}
	if registered != r.Connector {
		return fmt.Errorf("connector %q is routed to a different implementation", r.Connector.Type())
	}
	if !datasource.IsConnectorMetadataPublished(r.Connector.Type()) {
		return fmt.Errorf("connector metadata %q is not published", r.Connector.Type())
	}
	return nil
}

type ParserRegistrar struct {
	Engine docparser.EngineRegistration
}

func (r *ParserRegistrar) Publish(context.Context) error {
	return docparser.PublishEngine(r.Engine)
}

func (r *ParserRegistrar) Unpublish(context.Context) error {
	return docparser.UnregisterEngine(r.Engine.Name())
}

func (r *ParserRegistrar) Health(context.Context) error {
	if !docparser.HasEngine(r.Engine.Name()) {
		return fmt.Errorf("parser engine %q is not published", r.Engine.Name())
	}
	return nil
}

type WebSearchRegistrar struct {
	Registry *websearch.Registry
	ID       string
	Factory  websearch.ProviderFactory
}

func (r *WebSearchRegistrar) Publish(context.Context) error {
	if r.Registry == nil {
		return errors.New("web search registry is required")
	}
	return r.Registry.Publish(r.ID, r.Factory)
}

func (r *WebSearchRegistrar) Unpublish(context.Context) error {
	return r.Registry.Unregister(r.ID)
}

func (r *WebSearchRegistrar) Health(context.Context) error {
	if !r.Registry.Has(r.ID) {
		return fmt.Errorf("web search provider %q is not published", r.ID)
	}
	return nil
}

// ModelRegistrar controls metadata/config-validation routing. The compiled chat,
// embedding and rerank adapters remain core code, so production descriptors are
// deliberately non-disableable.
type ModelRegistrar struct {
	Provider provider.Provider
}

func (r *ModelRegistrar) Publish(context.Context) error {
	return provider.Publish(r.Provider)
}

func (r *ModelRegistrar) Unpublish(context.Context) error {
	return provider.Unregister(r.Provider.Info().Name)
}

func (r *ModelRegistrar) Health(context.Context) error {
	registered, exists := provider.Get(r.Provider.Info().Name)
	if !exists || registered != r.Provider {
		return fmt.Errorf("model provider %q is not published", r.Provider.Info().Name)
	}
	return nil
}
