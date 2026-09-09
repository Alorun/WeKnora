package builtin

import (
	"fmt"

	appretriever "github.com/Tencent/WeKnora/internal/application/service/retriever"
	"github.com/Tencent/WeKnora/internal/datasource"
	"github.com/Tencent/WeKnora/internal/datasource/connector/feishu/core"
	"github.com/Tencent/WeKnora/internal/datasource/connector/feishu/drive"
	"github.com/Tencent/WeKnora/internal/datasource/connector/feishu/wiki"
	gitlabconnector "github.com/Tencent/WeKnora/internal/datasource/connector/gitlab"
	imaconnector "github.com/Tencent/WeKnora/internal/datasource/connector/ima"
	notionconnector "github.com/Tencent/WeKnora/internal/datasource/connector/notion"
	rssconnector "github.com/Tencent/WeKnora/internal/datasource/connector/rss"
	yuqueconnector "github.com/Tencent/WeKnora/internal/datasource/connector/yuque"
	"github.com/Tencent/WeKnora/internal/infrastructure/docparser"
	websearch "github.com/Tencent/WeKnora/internal/infrastructure/web_search"
	"github.com/Tencent/WeKnora/internal/models/provider"
	"github.com/Tencent/WeKnora/internal/plugin/control"
)

const builtinVersion = "1.0.0"

func Descriptors(
	connectors *datasource.ConnectorRegistry, web *websearch.Registry, retrieval RetrievalRegistry,
) []BuiltinDescriptor {
	result := make([]BuiltinDescriptor, 0, 64)

	datasourceConnectors := []datasource.Connector{
		wiki.NewConnector(core.RegionFeishu),
		wiki.NewConnector(core.RegionLark),
		drive.NewDriveConnector(core.RegionFeishuDrive),
		drive.NewDriveConnector(core.RegionLarkDrive),
		notionconnector.NewConnector(),
		yuqueconnector.NewConnector(),
		imaconnector.NewConnector(),
		rssconnector.NewConnector(),
		gitlabconnector.NewConnector(),
	}
	for _, connector := range datasourceConnectors {
		metadata := datasource.ConnectorMetadataRegistry[connector.Type()]
		result = append(result, descriptor(
			"datasource_"+connector.Type(), connector.Type(), metadata.Name,
			control.ExtensionDataSource, metadata.Capabilities,
			&DataSourceRegistrar{Registry: connectors, Connector: connector},
		))
	}

	for _, engine := range docparser.BuiltinEngineRegistrations() {
		result = append(result, descriptor(
			"document_parser_"+engine.Name(), engine.Name(), engine.Description(),
			control.ExtensionDocumentParser, nil,
			&ParserRegistrar{Engine: engine},
		))
	}

	webFactories := []struct {
		id      string
		factory websearch.ProviderFactory
	}{
		{"duckduckgo", websearch.NewDuckDuckGoProvider},
		{"google", websearch.NewGoogleProvider},
		{"bing", websearch.NewBingProvider},
		{"tavily", websearch.NewTavilyProvider},
		{"ollama", websearch.NewOllamaProvider},
		{"baidu", websearch.NewBaiduProvider},
		{"searxng", websearch.NewSearxngProvider},
		{"keenable", websearch.NewKeenableProvider},
		{"zhipu", websearch.NewZhipuProvider},
		{"exa", websearch.NewExaProvider},
		{"metaso", websearch.NewMetasoProvider},
		{"bocha", websearch.NewBochaProvider},
		{"brave", websearch.NewBraveProvider},
	}
	for _, item := range webFactories {
		result = append(result, descriptor(
			"web_search_"+item.id, item.id, item.id, control.ExtensionWebSearch, nil,
			&WebSearchRegistrar{Registry: web, ID: item.id, Factory: item.factory},
		))
	}

	for _, modelProvider := range provider.BuiltinProviders() {
		info := modelProvider.Info()
		result = append(result, descriptor(
			"model_provider_"+string(info.Name), string(info.Name), info.DisplayName,
			control.ExtensionModelProvider, nil,
			&ModelRegistrar{Provider: modelProvider},
		))
	}

	for _, engineType := range appretriever.BuiltinEngineTypes() {
		id := string(engineType)
		result = append(result, descriptor(
			"retrieval_engine_"+id, id, id,
			control.ExtensionRetrievalEngine, nil,
			&RetrievalRegistrar{Registry: retrieval, EngineType: engineType},
		))
	}
	return result
}

func descriptor(
	pluginName, extensionID, name string,
	extensionType control.ExtensionType,
	capabilities []string,
	registrar Registrar,
) BuiltinDescriptor {
	return BuiltinDescriptor{
		Definition: control.PluginDefinition{
			ID:              control.PluginID(fmt.Sprintf("builtin.%s", pluginName)),
			Name:            name,
			Version:         builtinVersion,
			Source:          control.SourceBuiltin,
			ExtensionID:     control.ExtensionID(extensionID),
			ExtensionType:   extensionType,
			ProtocolVersion: "builtin",
			ContractVersion: "1.0",
			Capabilities:    append([]string(nil), capabilities...),
		},
		Registrar: registrar,
	}
}
