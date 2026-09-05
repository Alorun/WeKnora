package retriever

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

type blockingEngine struct {
	mockEngineService
	entered chan struct{}
	finish  chan struct{}
	calls   atomic.Int32
}

func (e *blockingEngine) Retrieve(context.Context, types.RetrieveParams) ([]*types.RetrieveResult, error) {
	e.calls.Add(1)
	if e.entered != nil {
		e.entered <- struct{}{}
		<-e.finish
	}
	return nil, nil
}

func TestDriverStopDrainsAndRejectsRetainedService(t *testing.T) {
	r := NewRetrieveEngineRegistry(nil, nil).(*RetrieveEngineRegistry)
	e := &blockingEngine{mockEngineService: mockEngineService{engineType: types.PostgresRetrieverEngineType}, entered: make(chan struct{}, 1), finish: make(chan struct{})}
	require.NoError(t, r.Declare(e))
	r.DeclareWithStoreID("store", e)
	require.NoError(t, r.PublishDriver(e.EngineType()))
	svc, err := r.GetRetrieveEngineService(e.EngineType())
	require.NoError(t, err)
	dbSvc, err := r.GetByStoreID("store")
	require.NoError(t, err)
	callDone := make(chan error, 1)
	go func() { _, err := svc.Retrieve(context.Background(), types.RetrieveParams{}); callDone <- err }()
	<-e.entered
	stopped := make(chan error, 1)
	go func() { stopped <- r.UnpublishDriver(e.EngineType()) }()
	require.Eventually(t, func() bool { return !r.IsDriverPublished(e.EngineType()) }, time.Second, time.Millisecond)
	select {
	case <-stopped:
		t.Fatal("Stop returned before the in-flight call drained")
	default:
	}
	_, err = dbSvc.Retrieve(context.Background(), types.RetrieveParams{})
	require.ErrorIs(t, err, ErrDriverNotActive)
	close(e.finish)
	require.NoError(t, <-callDone)
	require.NoError(t, <-stopped)
	ctx := context.Background()
	require.ErrorIs(t, svc.Index(ctx, nil, nil, nil), ErrDriverNotActive)
	require.ErrorIs(t, svc.BatchIndex(ctx, nil, nil, nil), ErrDriverNotActive)
	require.ErrorIs(t, svc.CopyIndices(ctx, "", nil, nil, "", 0, ""), ErrDriverNotActive)
	require.ErrorIs(t, svc.DeleteByChunkIDList(ctx, nil, 0, ""), ErrDriverNotActive)
	require.ErrorIs(t, svc.DeleteBySourceIDList(ctx, nil, 0, ""), ErrDriverNotActive)
	require.ErrorIs(t, svc.DeleteByKnowledgeIDList(ctx, nil, 0, ""), ErrDriverNotActive)
	require.ErrorIs(t, svc.BatchUpdateChunkEnabledStatus(ctx, nil), ErrDriverNotActive)
	require.ErrorIs(t, svc.BatchUpdateChunkTagID(ctx, nil), ErrDriverNotActive)
	require.Empty(t, svc.Support())
	require.Zero(t, svc.EstimateStorageSize(ctx, nil, nil, nil))
	require.NoError(t, r.PublishDriver(e.EngineType()))
	_, err = svc.Retrieve(ctx, types.RetrieveParams{})
	require.ErrorIs(t, err, ErrDriverNotActive, "restart must not revive a retained old epoch")
	e.entered = nil
	fresh, err := r.GetByStoreID("store")
	require.NoError(t, err)
	_, err = fresh.Retrieve(ctx, types.RetrieveParams{})
	require.NoError(t, err)
	require.EqualValues(t, 2, e.calls.Load(), "only the admitted old call and fresh epoch call reach the driver")
}
