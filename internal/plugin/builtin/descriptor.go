// Package builtin adapts in-process extensions to the unified plugin lifecycle.
package builtin

import (
	"context"
	"errors"
	"fmt"

	appretriever "github.com/Tencent/WeKnora/internal/application/service/retriever"
	"github.com/Tencent/WeKnora/internal/datasource"
	"github.com/Tencent/WeKnora/internal/infrastructure/docparser"
	websearch "github.com/Tencent/WeKnora/internal/infrastructure/web_search"
	"github.com/Tencent/WeKnora/internal/models/provider"
	"github.com/Tencent/WeKnora/internal/plugin/control"
	"github.com/Tencent/WeKnora/internal/types"
)

type Registrar interface {
	Publish(context.Context) error
	Unpublish(context.Context) error
	Health(context.Context) error
}

type preflightRegistrar interface {
	Preflight(context.Context) error
}

type Hook func(context.Context) error

type BuiltinDescriptor struct {
	Definition control.PluginDefinition
	Start      Hook
	Stop       Hook
	Registrar  Registrar
}

// Runtime gives in-process extensions the same explicit runtime boundary as
// external plugins without changing their implementation or transport.
type Runtime interface {
	Start(context.Context, BuiltinDescriptor) error
	Health(context.Context, BuiltinDescriptor) error
	Stop(context.Context, BuiltinDescriptor) error
}

type BuiltinRuntime struct{}

func NewBuiltinRuntime() *BuiltinRuntime { return &BuiltinRuntime{} }

func (*BuiltinRuntime) Start(ctx context.Context, descriptor BuiltinDescriptor) error {
	if descriptor.Start == nil {
		return nil
	}
	return descriptor.Start(ctx)
}

func (*BuiltinRuntime) Health(ctx context.Context, descriptor BuiltinDescriptor) error {
	if registrar, ok := descriptor.Registrar.(preflightRegistrar); ok {
		return registrar.Preflight(ctx)
	}
	return nil
}

func (*BuiltinRuntime) Stop(ctx context.Context, descriptor BuiltinDescriptor) error {
	if descriptor.Stop == nil {
		return nil
	}
	return descriptor.Stop(ctx)
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

func (r *DataSourceRegistrar) Preflight(context.Context) error {
	if r.Registry == nil || r.Connector == nil || r.Connector.Type() == "" {
		return errors.New("datasource registrar is incomplete")
	}
	if _, exists := datasource.ConnectorMetadataRegistry[r.Connector.Type()]; !exists {
		return fmt.Errorf("connector metadata %q is not declared", r.Connector.Type())
	}
	return nil
}

func (r *DataSourceRegistrar) Publish(context.Context) error {
	if err := r.Registry.Publish(r.Connector); err != nil {
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

func (r *ParserRegistrar) Preflight(context.Context) error {
	if r.Engine == nil || r.Engine.Name() == "" || !docparser.IsEngineDeclared(r.Engine.Name()) {
		return errors.New("parser registrar is incomplete")
	}
	return nil
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

func (r *WebSearchRegistrar) Preflight(context.Context) error {
	if r.Registry == nil || r.ID == "" || r.Factory == nil {
		return errors.New("web search registrar is incomplete")
	}
	return nil
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

type ModelRegistrar struct {
	Provider provider.Provider
}

func (r *ModelRegistrar) Preflight(context.Context) error {
	if r.Provider == nil || r.Provider.Info().Name == "" || !provider.IsDeclared(r.Provider.Info().Name) {
		return errors.New("model registrar is incomplete")
	}
	return nil
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
	for _, capability := range provider.CapabilitiesFor(r.Provider.Info()) {
		if !provider.HasCapability(r.Provider.Info().Name, capability) {
			return fmt.Errorf("model provider %q capability %q is not published", r.Provider.Info().Name, capability)
		}
	}
	return nil
}

// RetrievalRegistry is the lifecycle surface implemented by the existing
// retrieval engine registry. Business interfaces remain unchanged.
type RetrievalRegistry interface {
	SupportsDriver(types.RetrieverEngineType) bool
	PublishDriver(types.RetrieverEngineType) error
	UnpublishDriver(types.RetrieverEngineType) error
	IsDriverPublished(types.RetrieverEngineType) bool
}

type RetrievalRegistrar struct {
	Registry   RetrievalRegistry
	EngineType types.RetrieverEngineType
}

func (r *RetrievalRegistrar) Preflight(context.Context) error {
	if r.Registry == nil || !r.Registry.SupportsDriver(r.EngineType) {
		return fmt.Errorf("retrieval driver %q is not supported", r.EngineType)
	}
	return nil
}

func (r *RetrievalRegistrar) Publish(context.Context) error {
	return r.Registry.PublishDriver(r.EngineType)
}

func (r *RetrievalRegistrar) Unpublish(context.Context) error {
	return r.Registry.UnpublishDriver(r.EngineType)
}

func (r *RetrievalRegistrar) Health(context.Context) error {
	if !r.Registry.IsDriverPublished(r.EngineType) {
		return fmt.Errorf("retrieval driver %q is not published", r.EngineType)
	}
	return nil
}

var _ RetrievalRegistry = (*appretriever.RetrieveEngineRegistry)(nil)
