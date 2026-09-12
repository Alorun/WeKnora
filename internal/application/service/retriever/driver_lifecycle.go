package retriever

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

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
	draining  map[types.RetrieverEngineType]*driverLease
}

// Each publication owns an epoch. Retained services cannot become callable
// again merely because a stopped driver is started with a new epoch.
type driverLease struct {
	mu       sync.Mutex
	active   bool
	inflight int
	drained  chan struct{}
}

func NewDriverGate() *DriverGate {
	gate := &DriverGate{
		supported: make(map[types.RetrieverEngineType]struct{}, len(builtinEngineTypes)),
		active:    make(map[types.RetrieverEngineType]*driverLease, len(builtinEngineTypes)),
		draining:  make(map[types.RetrieverEngineType]*driverLease, len(builtinEngineTypes)),
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
		if g.draining[engineType] != nil {
			return fmt.Errorf("%s: retrieval driver is still draining", engineType)
		}
		lease := &driverLease{active: true, drained: make(chan struct{})}
		g.active[engineType] = lease
	}
	return nil
}

func (g *DriverGate) deactivate(ctx context.Context, engineType types.RetrieverEngineType) error {
	drainCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	g.mu.Lock()
	lease := g.active[engineType]
	if lease != nil {
		delete(g.active, engineType)
		lease.deactivate()
		g.draining[engineType] = lease
	} else {
		lease = g.draining[engineType]
	}
	g.mu.Unlock()
	if lease == nil {
		return nil
	}
	select {
	case <-lease.drained:
		g.mu.Lock()
		if g.draining[engineType] == lease {
			delete(g.draining, engineType)
		}
		g.mu.Unlock()
		return nil
	case <-drainCtx.Done():
		return fmt.Errorf("drain retrieval driver %s: %w", engineType, drainCtx.Err())
	}
}

func (l *driverLease) acquire() (func(), error) {
	if l == nil {
		return nil, ErrDriverNotActive
	}
	l.mu.Lock()
	if !l.active {
		l.mu.Unlock()
		return nil, ErrDriverNotActive
	}
	l.inflight++
	l.mu.Unlock()
	return func() {
		l.mu.Lock()
		l.inflight--
		if !l.active && l.inflight == 0 {
			select {
			case <-l.drained:
			default:
				close(l.drained)
			}
		}
		l.mu.Unlock()
	}, nil
}

func (l *driverLease) deactivate() {
	l.mu.Lock()
	l.active = false
	if l.inflight == 0 {
		select {
		case <-l.drained:
		default:
			close(l.drained)
		}
	}
	l.mu.Unlock()
}
