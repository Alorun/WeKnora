// Package datasource contains the Phase-B-facing external connector routing
// seam. It intentionally has no gRPC or Runtime dependency in Phase A.
package datasource

import (
	"errors"
	"fmt"
	"sync"
)

var (
	ErrHandleNotFound  = errors.New("external connector handle not found")
	ErrStaleGeneration = errors.New("stale connector handle generation")
)

// Handle is deliberately capability-neutral until the datasource gRPC contract
// is defined in Phase B.
type Handle interface {
	InstanceID() string
}

type ResolvedHandle struct {
	Handle     Handle
	Generation uint64
}

type Resolver struct {
	mu      sync.RWMutex
	handles map[string]ResolvedHandle
}

func NewResolver() *Resolver {
	return &Resolver{handles: make(map[string]ResolvedHandle)}
}

func (r *Resolver) Publish(dataSourceID string, generation uint64, handle Handle) error {
	if dataSourceID == "" || generation == 0 || handle == nil || handle.InstanceID() == "" {
		return errors.New("data source id, positive generation and handle instance id are required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	current, exists := r.handles[dataSourceID]
	if exists && generation < current.Generation {
		return fmt.Errorf("data source %s generation %d is older than %d: %w", dataSourceID, generation, current.Generation, ErrStaleGeneration)
	}
	if exists && generation == current.Generation {
		if current.Handle.InstanceID() == handle.InstanceID() {
			return nil
		}
		return fmt.Errorf("data source %s generation %d is already published by %s", dataSourceID, generation, current.Handle.InstanceID())
	}
	r.handles[dataSourceID] = ResolvedHandle{Handle: handle, Generation: generation}
	return nil
}

func (r *Resolver) Resolve(dataSourceID string) (ResolvedHandle, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	handle, exists := r.handles[dataSourceID]
	if !exists {
		return ResolvedHandle{}, fmt.Errorf("data source %s: %w", dataSourceID, ErrHandleNotFound)
	}
	return handle, nil
}

// Unpublish is generation-conditional so a stale stop cannot remove the new
// generation. Once it returns, subsequent Resolve calls cannot acquire it.
func (r *Resolver) Unpublish(dataSourceID string, generation uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	current, exists := r.handles[dataSourceID]
	if !exists {
		return nil
	}
	if generation < current.Generation {
		return fmt.Errorf("data source %s generation %d is older than %d: %w", dataSourceID, generation, current.Generation, ErrStaleGeneration)
	}
	if generation != current.Generation {
		return fmt.Errorf("data source %s generation %d does not match current %d", dataSourceID, generation, current.Generation)
	}
	delete(r.handles, dataSourceID)
	return nil
}
