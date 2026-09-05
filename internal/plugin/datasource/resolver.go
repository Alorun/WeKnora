// Package datasource contains the Phase-B-facing external connector routing
// seam. It intentionally has no gRPC or Runtime dependency in Phase A.
package datasource

import (
	"context"
	"errors"
	"fmt"
	"sync"

	pluginv1 "github.com/Tencent/WeKnora/pkg/plugin/proto/v1"
)

var (
	ErrHandleNotFound  = errors.New("external connector handle not found")
	ErrStaleGeneration = errors.New("stale connector handle generation")
)

type Handle interface {
	InstanceID() string
}

type RPCHandle interface {
	Handle
	ControlClient() pluginv1.PluginControlClient
	DataSourceClient() pluginv1.DataSourcePluginClient
}

type ResolvedHandle struct {
	Handle     Handle
	Generation uint64
}

type Resolver struct {
	mu      sync.RWMutex
	handles map[string]*entry
}

func NewResolver() *Resolver {
	return &Resolver{handles: make(map[string]*entry)}
}

type entry struct {
	resolved ResolvedHandle
	inflight int
	drained  chan struct{}
	closed   bool
}

type Lease struct {
	ResolvedHandle
	release func()
	once    sync.Once
}

func (l *Lease) Release() {
	if l != nil {
		l.once.Do(l.release)
	}
}

func (r *Resolver) Publish(dataSourceID string, generation uint64, handle Handle) error {
	if dataSourceID == "" || generation == 0 || handle == nil || handle.InstanceID() == "" {
		return errors.New("data source id, positive generation and handle instance id are required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	current, exists := r.handles[dataSourceID]
	if exists && generation < current.resolved.Generation {
		return fmt.Errorf("data source %s generation %d is older than %d: %w", dataSourceID, generation, current.resolved.Generation, ErrStaleGeneration)
	}
	if exists && generation == current.resolved.Generation {
		if current.resolved.Handle.InstanceID() == handle.InstanceID() {
			return nil
		}
		return fmt.Errorf("data source %s generation %d is already published by %s", dataSourceID, generation, current.resolved.Handle.InstanceID())
	}
	if exists {
		r.closeEntryLocked(current)
	}
	r.handles[dataSourceID] = &entry{
		resolved: ResolvedHandle{Handle: handle, Generation: generation},
		drained:  make(chan struct{}),
	}
	return nil
}

func (r *Resolver) Resolve(dataSourceID string) (ResolvedHandle, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	handle, exists := r.handles[dataSourceID]
	if !exists {
		return ResolvedHandle{}, fmt.Errorf("data source %s: %w", dataSourceID, ErrHandleNotFound)
	}
	return handle.resolved, nil
}

// Acquire pins a published handle for one business call. Unpublish removes it
// from new routing immediately; Drain then waits only for leases acquired
// before removal.
func (r *Resolver) Acquire(dataSourceID string) (*Lease, error) {
	r.mu.Lock()
	entry, exists := r.handles[dataSourceID]
	if !exists || entry.closed {
		r.mu.Unlock()
		return nil, fmt.Errorf("data source %s: %w", dataSourceID, ErrHandleNotFound)
	}
	entry.inflight++
	resolved := entry.resolved
	r.mu.Unlock()
	return &Lease{ResolvedHandle: resolved, release: func() { r.release(entry) }}, nil
}

// Unpublish is generation-conditional so a stale stop cannot remove the new
// generation. Once it returns, subsequent Resolve calls cannot acquire it.
func (r *Resolver) Unpublish(dataSourceID string, generation uint64) error {
	_, err := r.unpublish(dataSourceID, generation)
	return err
}

func (r *Resolver) UnpublishAndDrain(ctx context.Context, dataSourceID string, generation uint64) error {
	drained, err := r.unpublish(dataSourceID, generation)
	if err != nil || drained == nil {
		return err
	}
	select {
	case <-drained:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *Resolver) unpublish(dataSourceID string, generation uint64) (<-chan struct{}, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	current, exists := r.handles[dataSourceID]
	if !exists {
		return nil, nil
	}
	if generation < current.resolved.Generation {
		return nil, fmt.Errorf("data source %s generation %d is older than %d: %w", dataSourceID, generation, current.resolved.Generation, ErrStaleGeneration)
	}
	if generation != current.resolved.Generation {
		return nil, fmt.Errorf("data source %s generation %d does not match current %d", dataSourceID, generation, current.resolved.Generation)
	}
	delete(r.handles, dataSourceID)
	r.closeEntryLocked(current)
	return current.drained, nil
}

func (r *Resolver) closeEntryLocked(current *entry) {
	current.closed = true
	if current.inflight == 0 {
		select {
		case <-current.drained:
		default:
			close(current.drained)
		}
	}
}

func (r *Resolver) release(current *entry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if current.inflight > 0 {
		current.inflight--
	}
	if current.closed && current.inflight == 0 {
		select {
		case <-current.drained:
		default:
			close(current.drained)
		}
	}
}
