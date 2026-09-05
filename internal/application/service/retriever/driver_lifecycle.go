package retriever

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/Tencent/WeKnora/internal/types"
)

var (
	ErrDriverNotActive   = errors.New("retrieval driver is not active")
	ErrDriverUnsupported = errors.New("retrieval driver is not supported")
)

var builtinEngineTypes = []types.RetrieverEngineType{
	types.PostgresRetrieverEngineType,
	types.SQLiteRetrieverEngineType,
	types.ElasticsearchRetrieverEngineType,
	types.OpenSearchRetrieverEngineType,
	types.QdrantRetrieverEngineType,
	types.MilvusRetrieverEngineType,
	types.WeaviateRetrieverEngineType,
	types.DorisRetrieverEngineType,
	types.TencentVectorDBRetrieverEngineType,
}

func BuiltinEngineTypes() []types.RetrieverEngineType {
	return append([]types.RetrieverEngineType(nil), builtinEngineTypes...)
}

// DriverGate is shared by the registry and the DB-store EngineFactory. This
// keeps the existing factory switch while making every business construction
// conditional on PluginManager having published the driver.
type DriverGate struct {
	mu        sync.RWMutex
	supported map[types.RetrieverEngineType]struct{}
	active    map[types.RetrieverEngineType]*driverLease
}

// Each publication owns an epoch. Retained services cannot become callable
// again merely because a stopped driver is started with a new epoch.
type driverLease struct {
	mu     sync.RWMutex
	active atomic.Bool
}

func NewDriverGate() *DriverGate {
	gate := &DriverGate{
		supported: make(map[types.RetrieverEngineType]struct{}, len(builtinEngineTypes)),
		active:    make(map[types.RetrieverEngineType]*driverLease, len(builtinEngineTypes)),
	}
	for _, engineType := range builtinEngineTypes {
		gate.supported[engineType] = struct{}{}
	}
	return gate
}

func (g *DriverGate) Supports(engineType types.RetrieverEngineType) bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	_, exists := g.supported[engineType]
	return exists
}

func (g *DriverGate) IsActive(engineType types.RetrieverEngineType) bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	_, exists := g.active[engineType]
	return exists
}

func (g *DriverGate) RequireActive(engineType types.RetrieverEngineType) error {
	g.mu.RLock()
	defer g.mu.RUnlock()
	if _, exists := g.supported[engineType]; !exists {
		return fmt.Errorf("%s: %w", engineType, ErrDriverUnsupported)
	}
	if _, exists := g.active[engineType]; !exists {
		return fmt.Errorf("%s: %w", engineType, ErrDriverNotActive)
	}
	return nil
}

func (g *DriverGate) activate(engineType types.RetrieverEngineType) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, exists := g.supported[engineType]; !exists {
		return fmt.Errorf("%s: %w", engineType, ErrDriverUnsupported)
	}
	if g.active[engineType] == nil {
		lease := &driverLease{}
		lease.active.Store(true)
		g.active[engineType] = lease
	}
	return nil
}

func (g *DriverGate) deactivate(engineType types.RetrieverEngineType) {
	g.mu.Lock()
	lease := g.active[engineType]
	delete(g.active, engineType)
	if lease != nil {
		lease.active.Store(false)
	}
	g.mu.Unlock()
	if lease != nil {
		lease.mu.Lock() // Drain calls admitted before unpublication.
		lease.mu.Unlock()
	}
}
