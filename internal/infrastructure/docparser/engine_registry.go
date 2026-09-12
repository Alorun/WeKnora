package docparser

import (
	"context"
	"fmt"
	"sync"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

// EngineRegistration is what every locally registered parser engine provides:
// the metadata the engine list shows, and the reader that does the parsing.
// Remote-only engines (e.g. markitdown) live in the Python docreader, are
// discovered through its ListEngines RPC, and never register here — the
// registry routes them to the docreader client by default.
type EngineRegistration interface {
	Name() string
	Description() string
	FileTypes(docreaderConnected bool) []string
	CheckAvailable(docreaderConnected bool, overrides map[string]string) (available bool, reason string)
	// NewReader builds the reader for one parse request. Returning an error
	// means this engine cannot serve the request (missing credentials,
	// unreachable service); the caller reports it rather than silently
	// parsing with something else.
	NewReader(ctx context.Context, deps ReaderDeps) (interfaces.DocReader, error)
}

// ReaderDeps carries everything an engine may need to build its reader but
// cannot construct itself: tenant configuration, tenant credentials, and the
// shared docreader connection.
type ReaderDeps struct {
	// Overrides holds tenant-level engine configuration (service endpoints,
	// API keys), as produced by ParserEngineConfig.ToOverridesMap.
	Overrides map[string]string
	// Remote is the docreader client. Nil when the service is not connected.
	Remote interfaces.DocReader
	// WeKnoraCloudCredentials resolves the tenant's WeKnora Cloud
	// credentials. It is a function rather than a value because resolving
	// them can hit the database, which most engines never need. Nil, or a
	// nil return, means the tenant has not configured them.
	WeKnoraCloudCredentials func(ctx context.Context) *types.WeKnoraCloudCredentials
}

// localEngines holds all locally registered parser engines, in registration
// order — which is also the order the engine list is shown in.
var (
	localEnginesMu      sync.RWMutex
	localEngines        = make(map[string]EngineRegistration)
	localEngineOrder    []string
	declaredEngines     = make(map[string]EngineRegistration)
	declaredEngineOrder []string
)

// RegisterEngine retains the legacy declaration entry point without publishing
// the engine to parser requests. PluginManager is the sole production publisher.
func RegisterEngine(e EngineRegistration) {
	if e == nil || e.Name() == "" {
		panic("parser engine name and implementation are required")
	}
	localEnginesMu.Lock()
	defer localEnginesMu.Unlock()
	if _, exists := declaredEngines[e.Name()]; exists {
		panic(fmt.Sprintf("parser engine %q declared twice", e.Name()))
	}
	declaredEngines[e.Name()] = e
	declaredEngineOrder = append(declaredEngineOrder, e.Name())
}

func BuiltinEngineRegistrations() []EngineRegistration {
	localEnginesMu.RLock()
	defer localEnginesMu.RUnlock()
	result := make([]EngineRegistration, 0, len(declaredEngineOrder))
	for _, name := range declaredEngineOrder {
		result = append(result, declaredEngines[name])
	}
	return result
}

func IsEngineDeclared(name string) bool {
	localEnginesMu.RLock()
	defer localEnginesMu.RUnlock()
	_, exists := declaredEngines[name]
	return exists
}

// PublishEngine makes a declared engine available to new parser requests.
func PublishEngine(e EngineRegistration) error {
	if e == nil || e.Name() == "" {
		return fmt.Errorf("parser engine name and implementation are required")
	}
	localEnginesMu.Lock()
	defer localEnginesMu.Unlock()
	if _, exists := localEngines[e.Name()]; exists {
		return fmt.Errorf("parser engine %q already registered", e.Name())
	}
	localEngines[e.Name()] = e
	localEngineOrder = append(localEngineOrder, e.Name())
	return nil
}

// UnregisterEngine removes an engine from new request routing.
func UnregisterEngine(name string) error {
	localEnginesMu.Lock()
	defer localEnginesMu.Unlock()
	if _, exists := localEngines[name]; !exists {
		return fmt.Errorf("parser engine %q not registered", name)
	}
	delete(localEngines, name)
	for index, registeredName := range localEngineOrder {
		if registeredName == name {
			localEngineOrder = append(localEngineOrder[:index], localEngineOrder[index+1:]...)
			break
		}
	}
	return nil
}

func HasEngine(name string) bool {
	localEnginesMu.RLock()
	defer localEnginesMu.RUnlock()
	_, exists := localEngines[name]
	return exists
}

// lookupEngine returns the locally registered engine with this name.
func lookupEngine(name string) (EngineRegistration, bool) {
	localEnginesMu.RLock()
	defer localEnginesMu.RUnlock()
	engine, exists := localEngines[name]
	return engine, exists
}

// NewReader builds the reader for an engine.
//
// An empty engine name means "no explicit choice": simple formats are handled
// in Go and everything else goes to the docreader service. An unknown name is
// routed to the docreader too, so engines that only exist in the Python
// service keep working without a Go-side registration.
func NewReader(
	ctx context.Context, engine, fileType string, isURL bool, deps ReaderDeps,
) (interfaces.DocReader, error) {
	if registration, ok := lookupEngine(engine); ok {
		reader, err := registration.NewReader(ctx, deps)
		return managedFallbackReader(engine, reader), err
	}
	if engine != "" && IsEngineDeclared(engine) {
		return nil, errEngineUnavailable(engine, "stopped")
	}
	if engine == "" && !isURL && IsSimpleFormat(fileType) {
		if registration, ok := lookupEngine(SimpleEngineName); ok {
			reader, err := registration.NewReader(ctx, deps)
			return managedFallbackReader(SimpleEngineName, reader), err
		}
	}
	return remoteReader(deps)
}

// remoteReader returns the docreader client, or an error when the service is
// not connected — a nil interface value here would panic at the call site.
func remoteReader(deps ReaderDeps) (interfaces.DocReader, error) {
	if !HasEngine(BuiltinEngineName) {
		return nil, errEngineUnavailable(BuiltinEngineName, "stopped")
	}
	if deps.Remote == nil {
		return nil, errNotConnected
	}
	return managedFallbackReader(BuiltinEngineName, deps.Remote), nil
}

// managedFallbackReader keeps an internal parser fallback on the same
// lifecycle gate as direct routing. It prevents a reader created just before
// the DocReader bridge stops from invoking that stopped bridge later.
func managedFallbackReader(engine string, reader interfaces.DocReader) interfaces.DocReader {
	if reader == nil {
		return nil
	}
	if managed, ok := reader.(*activeEngineReader); ok && managed.engine == engine {
		return reader
	}
	return &activeEngineReader{engine: engine, inner: reader}
}

type activeEngineReader struct {
	engine string
	inner  interfaces.DocReader
}

func (r *activeEngineReader) Read(ctx context.Context, req *types.ReadRequest) (*types.ReadResult, error) {
	if !HasEngine(r.engine) {
		return nil, errEngineUnavailable(r.engine, "stopped")
	}
	return r.inner.Read(ctx, req)
}

func (r *activeEngineReader) IsConnected() bool {
	if !HasEngine(r.engine) {
		return false
	}
	if connected, ok := r.inner.(interface{ IsConnected() bool }); ok {
		return connected.IsConnected()
	}
	return true
}

// ListAllEngines returns the merged engine list: locally registered engines
// plus engines discovered from the remote docreader via ListEngines RPC.
//
// Merge rules:
//   - Local engines are always included, with Go-side availability checks.
//   - For a remote engine whose name matches a local one, the remote's
//     file_types and description take precedence (the remote service is
//     authoritative for its own capabilities).
//   - Remote engines not present locally are appended as-is, enabling
//     auto-discovery of newly added docreader engines without Go changes.
func ListAllEngines(
	docreaderConnected bool, overrides map[string]string, remoteEngines []types.ParserEngineInfo,
) []types.ParserEngineInfo {
	localEnginesMu.RLock()
	engines := make([]EngineRegistration, 0, len(localEngineOrder))
	for _, name := range localEngineOrder {
		engines = append(engines, localEngines[name])
	}
	localEnginesMu.RUnlock()

	bridgeReady := HasEngine(BuiltinEngineName)
	remoteMap := make(map[string]types.ParserEngineInfo, len(remoteEngines))
	if bridgeReady {
		for _, re := range remoteEngines {
			remoteMap[re.Name] = re
		}
	}

	seen := make(map[string]bool, len(engines))
	result := make([]types.ParserEngineInfo, 0, len(engines)+len(remoteEngines))

	for _, e := range engines {
		name := e.Name()
		seen[name] = true

		fileTypes := e.FileTypes(docreaderConnected)
		description := e.Description()

		if re, ok := remoteMap[name]; ok {
			if len(re.FileTypes) > 0 {
				fileTypes = re.FileTypes
			}
			if re.Description != "" {
				description = re.Description
			}
		}

		available, reason := e.CheckAvailable(docreaderConnected, overrides)
		result = append(result, types.ParserEngineInfo{
			Name:              name,
			Description:       description,
			FileTypes:         fileTypes,
			Available:         available,
			UnavailableReason: reason,
		})
	}

	if bridgeReady {
		for _, re := range remoteEngines {
			if seen[re.Name] || IsEngineDeclared(re.Name) {
				continue
			}
			result = append(result, re)
		}
	}

	return result
}

// errEngineUnavailable reports an engine that is registered but cannot run for
// this tenant or this build.
func errEngineUnavailable(engine, reason string) error {
	return fmt.Errorf("parser engine %q is unavailable: %s", engine, reason)
}
