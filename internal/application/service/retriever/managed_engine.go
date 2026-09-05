package retriever

import (
	"context"

	"github.com/Tencent/WeKnora/internal/models/embedding"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

// Guard preserves the existing service interface while binding it to one
// published driver epoch. Both registry lookups and the dynamic factory use it.
func (g *DriverGate) Guard(service interfaces.RetrieveEngineService) interfaces.RetrieveEngineService {
	if service == nil {
		return nil
	}
	g.mu.RLock()
	lease := g.active[service.EngineType()]
	g.mu.RUnlock()
	if guarded, ok := service.(*managedEngine); ok {
		if guarded.lease == lease {
			return guarded
		}
		service = guarded.RetrieveEngineService
	}
	return &managedEngine{RetrieveEngineService: service, lease: lease}
}

type managedEngine struct {
	interfaces.RetrieveEngineService
	lease *driverLease
}

func (s *managedEngine) acquire() (func(), error) {
	if s.lease == nil || !s.lease.active.Load() {
		return nil, ErrDriverNotActive
	}
	s.lease.mu.RLock()
	if !s.lease.active.Load() {
		s.lease.mu.RUnlock()
		return nil, ErrDriverNotActive
	}
	return s.lease.mu.RUnlock, nil
}

func (s *managedEngine) Retrieve(ctx context.Context, params types.RetrieveParams) ([]*types.RetrieveResult, error) {
	release, err := s.acquire()
	if err != nil {
		return nil, err
	}
	defer release()
	return s.RetrieveEngineService.Retrieve(ctx, params)
}

func (s *managedEngine) Support() []types.RetrieverType {
	release, err := s.acquire()
	if err != nil {
		return nil
	}
	defer release()
	return s.RetrieveEngineService.Support()
}

func (s *managedEngine) Index(ctx context.Context, e embedding.Embedder, item *types.IndexInfo, kinds []types.RetrieverType) error {
	release, err := s.acquire()
	if err != nil {
		return err
	}
	defer release()
	return s.RetrieveEngineService.Index(ctx, e, item, kinds)
}

func (s *managedEngine) BatchIndex(ctx context.Context, e embedding.Embedder, items []*types.IndexInfo, kinds []types.RetrieverType) error {
	release, err := s.acquire()
	if err != nil {
		return err
	}
	defer release()
	return s.RetrieveEngineService.BatchIndex(ctx, e, items, kinds)
}

func (s *managedEngine) EstimateStorageSize(ctx context.Context, e embedding.Embedder, items []*types.IndexInfo, kinds []types.RetrieverType) int64 {
	release, err := s.acquire()
	if err != nil {
		return 0
	}
	defer release()
	return s.RetrieveEngineService.EstimateStorageSize(ctx, e, items, kinds)
}

func (s *managedEngine) CopyIndices(ctx context.Context, source string, kbs, chunks map[string]string, target string, dimension int, kind string) error {
	release, err := s.acquire()
	if err != nil {
		return err
	}
	defer release()
	return s.RetrieveEngineService.CopyIndices(ctx, source, kbs, chunks, target, dimension, kind)
}

func (s *managedEngine) DeleteByChunkIDList(ctx context.Context, ids []string, dimension int, kind string) error {
	release, err := s.acquire()
	if err != nil {
		return err
	}
	defer release()
	return s.RetrieveEngineService.DeleteByChunkIDList(ctx, ids, dimension, kind)
}

func (s *managedEngine) DeleteBySourceIDList(ctx context.Context, ids []string, dimension int, kind string) error {
	release, err := s.acquire()
	if err != nil {
		return err
	}
	defer release()
	return s.RetrieveEngineService.DeleteBySourceIDList(ctx, ids, dimension, kind)
}

func (s *managedEngine) DeleteByKnowledgeIDList(ctx context.Context, ids []string, dimension int, kind string) error {
	release, err := s.acquire()
	if err != nil {
		return err
	}
	defer release()
	return s.RetrieveEngineService.DeleteByKnowledgeIDList(ctx, ids, dimension, kind)
}

func (s *managedEngine) BatchUpdateChunkEnabledStatus(ctx context.Context, values map[string]bool) error {
	release, err := s.acquire()
	if err != nil {
		return err
	}
	defer release()
	return s.RetrieveEngineService.BatchUpdateChunkEnabledStatus(ctx, values)
}

func (s *managedEngine) BatchUpdateChunkTagID(ctx context.Context, values map[string]string) error {
	release, err := s.acquire()
	if err != nil {
		return err
	}
	defer release()
	return s.RetrieveEngineService.BatchUpdateChunkTagID(ctx, values)
}
