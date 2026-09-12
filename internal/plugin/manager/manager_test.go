package manager

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"reflect"
	"sync"
	"testing"
	"time"

	appretriever "github.com/Tencent/WeKnora/internal/application/service/retriever"
	"github.com/Tencent/WeKnora/internal/datasource"
	"github.com/Tencent/WeKnora/internal/infrastructure/docparser"
	websearch "github.com/Tencent/WeKnora/internal/infrastructure/web_search"
	"github.com/Tencent/WeKnora/internal/models/chat"
	"github.com/Tencent/WeKnora/internal/models/embedding"
	"github.com/Tencent/WeKnora/internal/models/provider"
	"github.com/Tencent/WeKnora/internal/models/rerank"
	"github.com/Tencent/WeKnora/internal/plugin/builtin"
	"github.com/Tencent/WeKnora/internal/plugin/catalog"
	"github.com/Tencent/WeKnora/internal/plugin/control"
	pluginruntime "github.com/Tencent/WeKnora/internal/plugin/runtime"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	pluginv1 "github.com/Tencent/WeKnora/pkg/plugin/proto/v1"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/dns/dnsmessage"
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

func fakeDescriptor(id string, registrar *fakeRegistrar) builtin.BuiltinDescriptor {
	return builtin.BuiltinDescriptor{
		Definition: control.PluginDefinition{
			ID: control.PluginID("builtin." + id), Name: id, Version: "1.0.0",
			Source: control.SourceBuiltin, ExtensionID: control.ExtensionID(id),
			ExtensionType: control.ExtensionDataSource,
		},
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

type fakeRetrieveEngine struct {
	engineType types.RetrieverEngineType
}

func (e *fakeRetrieveEngine) EngineType() types.RetrieverEngineType { return e.engineType }
func (*fakeRetrieveEngine) Retrieve(context.Context, types.RetrieveParams) ([]*types.RetrieveResult, error) {
	return nil, nil
}
func (*fakeRetrieveEngine) Support() []types.RetrieverType { return nil }
func (*fakeRetrieveEngine) Index(context.Context, embedding.Embedder, *types.IndexInfo, []types.RetrieverType) error {
	return nil
}
func (*fakeRetrieveEngine) BatchIndex(context.Context, embedding.Embedder, []*types.IndexInfo, []types.RetrieverType) error {
	return nil
}
func (*fakeRetrieveEngine) EstimateStorageSize(context.Context, embedding.Embedder, []*types.IndexInfo, []types.RetrieverType) int64 {
	return 0
}
func (*fakeRetrieveEngine) CopyIndices(context.Context, string, map[string]string, map[string]string, string, int, string) error {
	return nil
}
func (*fakeRetrieveEngine) DeleteByChunkIDList(context.Context, []string, int, string) error {
	return nil
}
func (*fakeRetrieveEngine) DeleteBySourceIDList(context.Context, []string, int, string) error {
	return nil
}
func (*fakeRetrieveEngine) DeleteByKnowledgeIDList(context.Context, []string, int, string) error {
	return nil
}
func (*fakeRetrieveEngine) BatchUpdateChunkEnabledStatus(context.Context, map[string]bool) error {
	return nil
}
func (*fakeRetrieveEngine) BatchUpdateChunkTagID(context.Context, map[string]string) error {
	return nil
}

type stubDocReader struct{ interfaces.DocReader }

func (*stubDocReader) Read(context.Context, *types.ReadRequest) (*types.ReadResult, error) {
	return &types.ReadResult{}, nil
}

func TestBuiltinDescriptorsPublishAndStopAllFiveExtensionTypes(t *testing.T) {
	ctx := context.Background()
	usePublicFixtureDNS(t)
	connectorRegistry := datasource.NewConnectorRegistry()
	webRegistry := websearch.NewRegistry()
	retrievalRegistry := appretriever.NewRetrieveEngineRegistry(nil, nil).(*appretriever.RetrieveEngineRegistry)
	retrievalEngine := &fakeRetrieveEngine{engineType: types.PostgresRetrieverEngineType}
	require.ErrorIs(t, retrievalRegistry.Register(retrievalEngine), appretriever.ErrDriverNotActive)
	require.NoError(t, retrievalRegistry.Declare(retrievalEngine))
	pluginCatalog := catalog.New()
	manager := New(pluginCatalog, nil)
	descriptors := builtin.Descriptors(connectorRegistry, webRegistry, retrievalRegistry)
	legacyConnectors := datasource.NewConnectorRegistry()
	for _, candidate := range descriptors {
		registrar, ok := candidate.Registrar.(*builtin.DataSourceRegistrar)
		if ok && registrar.Connector.Type() == "feishu" {
			require.NoError(t, legacyConnectors.Register(registrar.Connector))
			break
		}
	}
	if _, err := legacyConnectors.Get("feishu"); err == nil {
		t.Fatal("legacy datasource Register published a business connector")
	}
	legacyWeb := websearch.NewRegistry()
	legacyWeb.Register("legacy", func(types.WebSearchProviderParameters) (interfaces.WebSearchProvider, error) {
		return nil, nil
	})
	if _, err := legacyWeb.CreateProvider("legacy", types.WebSearchProviderParameters{}); err == nil {
		t.Fatal("legacy web search Register published a business factory")
	}
	if _, err := connectorRegistry.Get("feishu"); err == nil || webRegistry.Has("google") {
		t.Fatal("legacy container path published before PluginManager start")
	}
	if docparser.HasEngine(docparser.BuiltinEngineName) {
		t.Fatal("legacy parser declaration published before PluginManager start")
	}
	if _, exists := provider.Get(provider.ProviderOpenAI); exists {
		t.Fatal("legacy model declaration published before PluginManager start")
	}
	if _, err := retrievalRegistry.GetRetrieveEngineService(types.PostgresRetrieverEngineType); err == nil {
		t.Fatal("legacy retrieval declaration published before PluginManager start")
	}
	if _, err := chat.NewRemoteChat(modelChatConfig()); !errors.Is(err, provider.ErrProviderNotActive) {
		t.Fatalf("model chat factory before start: %v", err)
	}
	if err := manager.LoadBuiltins(descriptors); err != nil {
		t.Fatal(err)
	}
	if err := manager.StartAll(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = manager.StopAll(context.Background())
	})
	for _, extensionType := range []control.ExtensionType{
		control.ExtensionDataSource, control.ExtensionDocumentParser,
		control.ExtensionWebSearch, control.ExtensionModelProvider, control.ExtensionRetrievalEngine,
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
	if got, err := retrievalRegistry.GetRetrieveEngineService(types.PostgresRetrieverEngineType); err != nil || got.EngineType() != retrievalEngine.EngineType() {
		t.Fatalf("retrieval business registry = %T, %v", got, err)
	}
	if _, err := webRegistry.CreateProvider("duckduckgo", types.WebSearchProviderParameters{}); err != nil {
		t.Fatalf("web search business factory: %v", err)
	}
	if _, err := docparser.NewReader(ctx, docparser.SimpleEngineName, "md", false, docparser.ReaderDeps{}); err != nil {
		t.Fatalf("parser business factory: %v", err)
	}
	assertModelFactoriesReady(t)
	retainedGenericChat, err := chat.NewRemoteChat(modelChatConfig())
	require.NoError(t, err)
	customChatConfig := modelChatConfig()
	customChatConfig.Provider = "custom-openai-compatible"
	retainedCustomChat, err := chat.NewRemoteChat(customChatConfig)
	require.NoError(t, err)
	nvidiaHeldConfig := modelChatConfig()
	nvidiaHeldConfig.Provider = string(provider.ProviderNvidia)
	retainedNvidiaChat, err := chat.NewRemoteChat(nvidiaHeldConfig)
	require.NoError(t, err)
	anthropicHeldConfig := modelChatConfig()
	anthropicHeldConfig.Provider = string(provider.ProviderAnthropic)
	anthropicHeldConfig.APIKey = "test-key"
	retainedAnthropicChat, err := chat.NewRemoteChat(anthropicHeldConfig)
	require.NoError(t, err)
	retainedParser, err := docparser.NewReader(ctx, docparser.SimpleEngineName, "md", false, docparser.ReaderDeps{})
	require.NoError(t, err)
	parsed, err := retainedParser.Read(ctx, &types.ReadRequest{FileType: "md", FileName: "a.md", FileContent: []byte("# lifecycle")})
	require.NoError(t, err)
	require.Contains(t, parsed.MarkdownContent, "lifecycle")
	retainedRetrieval, err := retrievalRegistry.GetRetrieveEngineService(types.PostgresRetrieverEngineType)
	require.NoError(t, err)
	_, err = retainedRetrieval.Retrieve(ctx, types.RetrieveParams{})
	require.NoError(t, err)

	connectorCount := len(connectorRegistry.List())
	webCount := len(webRegistry.List())
	modelCount := len(provider.List())
	parserCount := len(docparser.ListAllEngines(true, nil, nil))
	retrievalCount := len(retrievalRegistry.GetAllRetrieveEngineServices())
	if err := manager.StartAll(ctx); err != nil {
		t.Fatalf("idempotent StartAll: %v", err)
	}
	if connectorCount != len(connectorRegistry.List()) || webCount != len(webRegistry.List()) ||
		modelCount != len(provider.List()) || parserCount != len(docparser.ListAllEngines(true, nil, nil)) ||
		retrievalCount != len(retrievalRegistry.GetAllRetrieveEngineServices()) {
		t.Fatal("idempotent start changed a business registry")
	}

	require.NoError(t, manager.Stop(ctx, "builtin.datasource_feishu"))
	require.NoError(t, manager.Stop(ctx, "builtin.datasource_feishu"))
	_, err = connectorRegistry.Get("feishu")
	require.Error(t, err)
	require.False(t, datasource.IsConnectorMetadataPublished("feishu"))

	require.NoError(t, manager.Stop(ctx, "builtin.web_search_duckduckgo"))
	_, err = webRegistry.CreateProvider("duckduckgo", types.WebSearchProviderParameters{})
	require.Error(t, err)

	require.NoError(t, manager.Stop(ctx, "builtin.document_parser_simple"))
	_, err = retainedParser.Read(ctx, &types.ReadRequest{FileType: "md", FileContent: []byte("# lifecycle")})
	require.ErrorContains(t, err, "stopped")
	_, err = docparser.NewReader(ctx, docparser.SimpleEngineName, "md", false, docparser.ReaderDeps{Remote: &stubDocReader{}})
	require.ErrorContains(t, err, "stopped")
	reader, err := docparser.NewReader(ctx, "", "md", false, docparser.ReaderDeps{Remote: &stubDocReader{}})
	require.NoError(t, err, "default parsing may fall back only to the still-ready docreader bridge")
	_, err = reader.Read(ctx, &types.ReadRequest{})
	require.NoError(t, err)
	require.NoError(t, manager.Stop(ctx, "builtin.document_parser_builtin"))
	_, err = reader.Read(ctx, &types.ReadRequest{})
	require.ErrorContains(t, err, "stopped", "retained readers must not invoke the stopped bridge")
	_, err = docparser.NewReader(ctx, "markitdown", "docx", false, docparser.ReaderDeps{Remote: &stubDocReader{}})
	require.ErrorContains(t, err, "stopped")
	for _, engine := range docparser.ListAllEngines(true, nil, []types.ParserEngineInfo{{Name: "remote-only"}, {Name: docparser.SimpleEngineName}}) {
		require.NotEqual(t, "remote-only", engine.Name, "stopped DocReader bridge leaked dynamic engines")
		require.NotEqual(t, docparser.SimpleEngineName, engine.Name)
	}
	// A known stopped provider must not take the still-active generic route,
	// including provider-specific embedding and rerank switch branches.
	require.NoError(t, manager.Stop(ctx, "builtin.model_provider_nvidia"))
	_, err = retainedNvidiaChat.Chat(ctx, nil, &chat.ChatOptions{})
	require.ErrorIs(t, err, provider.ErrProviderNotActive, "a retained model must not start a request after Stop")
	nvidiaChat := modelChatConfig()
	nvidiaChat.Provider = string(provider.ProviderNvidia)
	_, err = chat.NewRemoteChat(nvidiaChat)
	require.ErrorIs(t, err, provider.ErrProviderNotActive)
	_, err = embedding.NewEmbedder(embedding.Config{Source: types.ModelSourceRemote, Provider: string(provider.ProviderNvidia)}, nil, nil)
	require.ErrorIs(t, err, provider.ErrProviderNotActive)
	_, err = rerank.NewReranker(&rerank.RerankerConfig{Source: types.ModelSourceRemote, Provider: string(provider.ProviderNvidia)})
	require.ErrorIs(t, err, provider.ErrProviderNotActive)
	require.NoError(t, manager.Stop(ctx, "builtin.model_provider_anthropic"))
	_, err = retainedAnthropicChat.Chat(ctx, nil, &chat.ChatOptions{})
	require.ErrorIs(t, err, provider.ErrProviderNotActive)
	_, err = retainedAnthropicChat.ChatStream(ctx, nil, &chat.ChatOptions{})
	require.ErrorIs(t, err, provider.ErrProviderNotActive)

	require.NoError(t, manager.Stop(ctx, "builtin.model_provider_generic"))
	if _, active := provider.Get(provider.ProviderGeneric); active {
		t.Fatal("stopped model provider metadata remained published")
	}
	for _, retained := range []chat.Chat{retainedGenericChat, retainedCustomChat} {
		_, err = retained.Chat(ctx, nil, &chat.ChatOptions{})
		require.ErrorIs(t, err, provider.ErrProviderNotActive)
		_, err = retained.ChatStream(ctx, nil, &chat.ChatOptions{})
		require.ErrorIs(t, err, provider.ErrProviderNotActive)
	}
	for _, capability := range []provider.Capability{
		provider.CapabilityChat, provider.CapabilityEmbedding, provider.CapabilityRerank,
	} {
		require.False(t, provider.HasCapability(provider.ProviderGeneric, capability))
	}
	assertModelFactoriesStopped(t)

	require.NoError(t, manager.Stop(ctx, "builtin.retrieval_engine_postgres"))
	_, err = retainedRetrieval.Retrieve(ctx, types.RetrieveParams{})
	require.ErrorIs(t, err, appretriever.ErrDriverNotActive)
	_, err = retrievalRegistry.GetRetrieveEngineService(types.PostgresRetrieverEngineType)
	require.ErrorIs(t, err, appretriever.ErrDriverNotActive)

	require.NoError(t, manager.StopAll(ctx))
	require.NoError(t, manager.StopAll(ctx))
	require.Empty(t, connectorRegistry.List())
	require.Empty(t, webRegistry.List())
	require.Empty(t, provider.List())
	require.Empty(t, docparser.ListAllEngines(true, nil, []types.ParserEngineInfo{{Name: "remote-only"}}))
	require.Empty(t, retrievalRegistry.GetAllRetrieveEngineServices())
}

func modelChatConfig() *chat.ChatConfig {
	return &chat.ChatConfig{
		Source: types.ModelSourceRemote, Provider: string(provider.ProviderGeneric),
		BaseURL: "https://lifecycle.test/v1", ModelName: "test-model",
	}
}

// Supply deterministic public DNS answers for constructor-only tests. The
// real SSRF validator still runs; no whitelist or safety policy is changed.
func usePublicFixtureDNS(t *testing.T) {
	t.Helper()
	previous := net.DefaultResolver
	t.Cleanup(func() { net.DefaultResolver = previous })
	net.DefaultResolver = &net.Resolver{PreferGo: true, Dial: func(context.Context, string, string) (net.Conn, error) {
		client, server := net.Pipe()
		go func() {
			defer server.Close()
			_ = server.SetDeadline(time.Now().Add(time.Second))
			var size [2]byte
			if _, err := io.ReadFull(server, size[:]); err != nil {
				return
			}
			packet := make([]byte, binary.BigEndian.Uint16(size[:]))
			if _, err := io.ReadFull(server, packet); err != nil {
				return
			}
			var query dnsmessage.Message
			if query.Unpack(packet) != nil {
				return
			}
			reply := dnsmessage.Message{Header: dnsmessage.Header{ID: query.ID, Response: true, RecursionAvailable: true}, Questions: query.Questions}
			for _, question := range query.Questions {
				if question.Type == dnsmessage.TypeA {
					reply.Answers = append(reply.Answers, dnsmessage.Resource{Header: dnsmessage.ResourceHeader{Name: question.Name, Type: dnsmessage.TypeA, Class: dnsmessage.ClassINET}, Body: &dnsmessage.AResource{A: [4]byte{8, 8, 8, 8}}})
				}
			}
			packet, err := reply.Pack()
			if err != nil {
				return
			}
			binary.BigEndian.PutUint16(size[:], uint16(len(packet)))
			_, _ = server.Write(append(size[:], packet...))
		}()
		return client, nil
	}}
}

func assertModelFactoriesReady(t *testing.T) {
	t.Helper()
	_, err := chat.NewRemoteChat(modelChatConfig())
	require.NoError(t, err)
	customConfig := modelChatConfig()
	customConfig.Provider = "custom-openai-compatible"
	_, err = chat.NewRemoteChat(customConfig)
	require.NoError(t, err, "unknown provider should retain the generic compatibility route")
	_, err = chat.NewRemoteAPIChat(modelChatConfig())
	require.NoError(t, err)
	_, err = embedding.NewEmbedder(embedding.Config{
		Source: types.ModelSourceRemote, Provider: string(provider.ProviderGeneric),
		BaseURL: "https://lifecycle.test/v1", ModelName: "test-model",
	}, nil, nil)
	require.NoError(t, err)
	_, err = rerank.NewReranker(&rerank.RerankerConfig{
		Source: types.ModelSourceRemote, Provider: string(provider.ProviderGeneric),
		BaseURL: "https://lifecycle.test/v1", ModelName: "test-model",
	})
	require.NoError(t, err)
}

func assertModelFactoriesStopped(t *testing.T) {
	t.Helper()
	_, err := chat.NewRemoteChat(modelChatConfig())
	require.ErrorIs(t, err, provider.ErrProviderNotActive)
	customConfig := modelChatConfig()
	customConfig.Provider = "custom-openai-compatible"
	_, err = chat.NewRemoteChat(customConfig)
	require.ErrorIs(t, err, provider.ErrProviderNotActive)
	_, err = chat.NewRemoteAPIChat(modelChatConfig())
	require.ErrorIs(t, err, provider.ErrProviderNotActive)
	_, err = embedding.NewEmbedder(embedding.Config{
		Source: types.ModelSourceRemote, Provider: string(provider.ProviderGeneric),
		BaseURL: "https://lifecycle.test/v1", ModelName: "test-model",
	}, nil, nil)
	require.ErrorIs(t, err, provider.ErrProviderNotActive)
	_, err = rerank.NewReranker(&rerank.RerankerConfig{
		Source: types.ModelSourceRemote, Provider: string(provider.ProviderGeneric),
		BaseURL: "https://lifecycle.test/v1", ModelName: "test-model",
	})
	require.ErrorIs(t, err, provider.ErrProviderNotActive)
}

func TestStartAllRollsBackEarlierPublication(t *testing.T) {
	var events []string
	first := &fakeRegistrar{name: "a", events: &events}
	second := &fakeRegistrar{name: "b", events: &events, publishErr: errors.New("publish failed")}
	manager := New(catalog.New(), nil)
	if err := manager.LoadBuiltins([]builtin.BuiltinDescriptor{
		fakeDescriptor("a", first), fakeDescriptor("b", second),
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

func TestStartHealthFailureLeavesNothingPublished(t *testing.T) {
	var events []string
	registrar := &fakeRegistrar{
		name: "unhealthy", events: &events, healthErr: errors.New("health failed"),
	}
	manager := New(catalog.New(), nil)
	require.NoError(t, manager.LoadBuiltins([]builtin.BuiltinDescriptor{fakeDescriptor("unhealthy", registrar)}))

	err := manager.Start(context.Background(), "builtin.unhealthy")
	require.ErrorContains(t, err, "health failed")
	require.False(t, registrar.published)
	require.Equal(t, []string{
		"unhealthy:start", "unhealthy:publish", "unhealthy:health",
		"unhealthy:unpublish", "unhealthy:stop",
	}, events)
	status, statusErr := manager.Status("builtin.unhealthy")
	require.NoError(t, statusErr)
	require.Equal(t, control.StateFailed, status.State)
	require.Contains(t, status.LastError, "health failed")
}

type unhealthyPreflight struct{ *fakeRegistrar }

func (*unhealthyPreflight) Preflight(context.Context) error {
	return errors.New("runtime preflight failed")
}

func TestBuiltinRuntimeHealthRunsBeforePublication(t *testing.T) {
	var events []string
	r := &fakeRegistrar{name: "preflight", events: &events}
	d := fakeDescriptor("preflight", r)
	d.Registrar = &unhealthyPreflight{r}
	m := New(catalog.New(), nil)
	require.NoError(t, m.LoadBuiltins([]builtin.BuiltinDescriptor{d}))
	require.ErrorContains(t, m.Start(context.Background(), d.Definition.ID), "runtime preflight failed")
	require.Zero(t, r.publishCall)
	require.Equal(t, []string{"preflight:start", "preflight:stop"}, events)
}

func TestRepeatedStartAndStopOrdering(t *testing.T) {
	var events []string
	registrar := &fakeRegistrar{name: "sample", events: &events}
	manager := New(catalog.New(), nil)
	if err := manager.LoadBuiltins([]builtin.BuiltinDescriptor{fakeDescriptor("sample", registrar)}); err != nil {
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

func TestHealthFailureUpdatesUnifiedStatus(t *testing.T) {
	var events []string
	registrar := &fakeRegistrar{name: "health", events: &events}
	manager := New(catalog.New(), nil)
	require.NoError(t, manager.LoadBuiltins([]builtin.BuiltinDescriptor{fakeDescriptor("health", registrar)}))
	require.NoError(t, manager.Start(context.Background(), "builtin.health"))

	registrar.healthErr = errors.New("business registry unhealthy")
	status := manager.Health(context.Background(), "builtin.health")
	require.Equal(t, control.StateDegraded, status.State)
	require.Equal(t, registrar.healthErr.Error(), status.LastError)
}

func TestConcurrentStartStopCatalogAndBusinessRegistry(t *testing.T) {
	connectorRegistry := datasource.NewConnectorRegistry()
	webRegistry := websearch.NewRegistry()
	retrievalRegistry := appretriever.NewRetrieveEngineRegistry(nil, nil).(*appretriever.RetrieveEngineRegistry)
	require.NoError(t, retrievalRegistry.Declare(&fakeRetrieveEngine{engineType: types.PostgresRetrieverEngineType}))
	pluginCatalog := catalog.New()
	manager := New(pluginCatalog, nil)

	wanted := map[control.PluginID]bool{
		"builtin.datasource_feishu":         true,
		"builtin.document_parser_simple":    true,
		"builtin.web_search_duckduckgo":     true,
		"builtin.model_provider_generic":    true,
		"builtin.retrieval_engine_postgres": true,
	}
	var descriptors []builtin.BuiltinDescriptor
	for _, candidate := range builtin.Descriptors(connectorRegistry, webRegistry, retrievalRegistry) {
		if wanted[candidate.Definition.ID] {
			descriptors = append(descriptors, candidate)
		}
	}
	require.Len(t, descriptors, len(wanted))
	require.NoError(t, manager.LoadBuiltins(descriptors))

	const goroutines = 18
	const iterations = 30
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for worker := 0; worker < goroutines; worker++ {
		go func(worker int) {
			defer wg.Done()
			for iteration := 0; iteration < iterations; iteration++ {
				switch worker % 9 {
				case 0:
					_ = manager.StartAll(context.Background())
				case 1:
					_ = manager.StopAll(context.Background())
				case 2:
					_ = pluginCatalog.List(control.ExtensionDataSource)
				case 3:
					_ = connectorRegistry.List()
					_, _ = connectorRegistry.Get("feishu")
				case 4:
					_, _ = retrievalRegistry.GetRetrieveEngineService(types.PostgresRetrieverEngineType)
					_ = retrievalRegistry.GetAllRetrieveEngineServices()
				case 5:
					_ = webRegistry.List()
					_, _ = webRegistry.CreateProvider("duckduckgo", types.WebSearchProviderParameters{})
				case 6:
					_ = docparser.ListAllEngines(false, nil, nil)
					_, _ = docparser.NewReader(context.Background(), docparser.SimpleEngineName, "md", false, docparser.ReaderDeps{})
				case 7:
					_ = provider.List()
					_ = provider.RequireCapability(provider.ProviderGeneric, provider.CapabilityChat)
				case 8:
					_, _ = manager.Status("builtin.datasource_feishu")
				}
			}
		}(worker)
	}
	wg.Wait()
	require.NoError(t, manager.StopAll(context.Background()))
	require.Empty(t, retrievalRegistry.GetAllRetrieveEngineServices())
}

type memoryExternalStore struct {
	installations map[string]*control.PluginInstallation
	bindings      map[string]*control.DataSourcePluginBinding
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
	installation := &control.PluginInstallation{
		ID: "installation-1", PluginID: "community.local-files", Version: "0.1.0",
		ArtifactDigest: "sha256:test", Active: true, InstallStatus: control.InstallStatusInstalled,
	}
	store := &memoryExternalStore{
		installations: map[string]*control.PluginInstallation{installation.ID: installation},
		bindings: map[string]*control.DataSourcePluginBinding{
			"ds-1": {DataSourceID: "ds-1", InstallationID: installation.ID, ExtensionID: "local_directory", Generation: 1},
		},
	}
	manager := New(catalog.New(), store)
	if err := manager.EnableExternal(context.Background(), installation.ID); !errors.Is(err, ErrRuntimeNotAvailable) {
		t.Fatalf("enable error = %v", err)
	}
	_, err := manager.Status(installation.PluginID)
	require.ErrorIs(t, err, ErrNotFound, "external runtime state belongs to its Binding observation")
	if persisted := store.installations[installation.ID]; persisted.Enabled || persisted.LastError != ErrRuntimeNotAvailable.Error() {
		t.Fatalf("external installation was made runnable: %#v", persisted)
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

func managerWithRuntime(t *testing.T, store *memoryExternalStore, runtime pluginruntime.Runtime) *PluginManager {
	t.Helper()
	manager := NewWithVersion(catalog.New(), store, "")
	require.NoError(t, manager.ConfigureRuntime(runtime))
	return manager
}

func TestExternalPluginBecomesReadyOnlyAfterRuntimeStartReturns(t *testing.T) {
	store := &memoryExternalStore{installations: map[string]*control.PluginInstallation{
		"installation-1": {ID: "installation-1", PluginID: "community.local-files", Version: "0.1.0", Active: true},
	}, bindings: map[string]*control.DataSourcePluginBinding{
		"ds-1": {DataSourceID: "ds-1", InstallationID: "installation-1", ExtensionID: "local", Generation: 1},
	}}
	manager := managerWithRuntime(t, store, managerRuntime{})
	require.NoError(t, manager.EnableExternal(context.Background(), "installation-1"))
	handle, err := manager.StartExternal(context.Background(), "installation-1", pluginruntime.InstanceSpec{
		PluginID: "community.local-files", PluginVersion: "0.1.0", ExtensionID: "local", DataSourceID: "ds-1", Generation: 1,
	})
	require.NoError(t, err)
	require.NotNil(t, handle)
	_, err = manager.Status("community.local-files")
	require.ErrorIs(t, err, ErrNotFound)
}

func TestExternalRuntimeFailureKeepsDesiredEnabledForReconcile(t *testing.T) {
	store := &memoryExternalStore{installations: map[string]*control.PluginInstallation{
		"installation-1": {ID: "installation-1", PluginID: "community.local-files", Version: "0.1.0", Active: true, Enabled: true},
	}, bindings: map[string]*control.DataSourcePluginBinding{
		"ds-1": {DataSourceID: "ds-1", InstallationID: "installation-1", ExtensionID: "local", Generation: 1},
	}}
	manager := managerWithRuntime(t, store, managerRuntime{startErr: errors.New("health not ready")})
	_, err := manager.StartExternal(context.Background(), "installation-1", pluginruntime.InstanceSpec{
		PluginID: "community.local-files", PluginVersion: "0.1.0", ExtensionID: "local", DataSourceID: "ds-1", Generation: 1,
	})
	require.ErrorContains(t, err, "health not ready")
	require.True(t, store.installations["installation-1"].Enabled)
	require.Empty(t, store.installations["installation-1"].LastError,
		"instance failure must be recorded on its Binding, not the shared installation")
	_, statusErr := manager.Status("community.local-files")
	require.ErrorIs(t, statusErr, ErrNotFound)
}

func TestExternalBindingsKeepIndependentRuntimeHandlesWithoutPackageStatus(t *testing.T) {
	store := &memoryExternalStore{installations: map[string]*control.PluginInstallation{
		"installation-1": {ID: "installation-1", PluginID: "community.local-files", Version: "0.1.0", Active: true, Enabled: true},
	}, bindings: map[string]*control.DataSourcePluginBinding{
		"ds-1": {DataSourceID: "ds-1", InstallationID: "installation-1", ExtensionID: "local", Generation: 1},
		"ds-2": {DataSourceID: "ds-2", InstallationID: "installation-1", ExtensionID: "local", Generation: 1},
	}}
	manager := managerWithRuntime(t, store, managerRuntime{})
	for _, dataSourceID := range []string{"ds-1", "ds-2"} {
		_, err := manager.StartExternal(context.Background(), "installation-1", pluginruntime.InstanceSpec{
			PluginID: "community.local-files", PluginVersion: "0.1.0", ExtensionID: "local", DataSourceID: dataSourceID, Generation: 1,
		})
		require.NoError(t, err)
	}
	require.Len(t, manager.handles, 2)
	_, err := manager.Status("community.local-files")
	require.ErrorIs(t, err, ErrNotFound)
	require.NoError(t, manager.StopExternal(context.Background(), "installation-1", "ds-1", time.Second))
	require.Nil(t, manager.handles["ds-1"])
	require.NotNil(t, manager.handles["ds-2"])
}

func TestExternalStopFailureDoesNotRetainUnroutableHandle(t *testing.T) {
	store := &memoryExternalStore{installations: map[string]*control.PluginInstallation{
		"installation-1": {ID: "installation-1", PluginID: "community.local-files", Version: "0.1.0", Active: true, Enabled: true},
	}, bindings: map[string]*control.DataSourcePluginBinding{
		"ds-1": {DataSourceID: "ds-1", InstallationID: "installation-1", ExtensionID: "local", Generation: 1},
	}}
	manager := managerWithRuntime(t, store, managerRuntime{stopErr: errors.New("backend cleanup failed")})
	spec := pluginruntime.InstanceSpec{PluginID: "community.local-files", PluginVersion: "0.1.0", ExtensionID: "local", DataSourceID: "ds-1", Generation: 1}
	_, err := manager.StartExternal(context.Background(), "installation-1", spec)
	require.NoError(t, err)
	require.ErrorContains(t, manager.StopExternal(context.Background(), "installation-1", "ds-1", time.Second), "backend cleanup failed")
	_, err = manager.StartExternal(context.Background(), "installation-1", spec)
	require.NoError(t, err, "cleanup failure retained an unroutable manager handle")
}
