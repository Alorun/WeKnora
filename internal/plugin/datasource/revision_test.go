package datasource

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"mime/multipart"
	"sync"
	"testing"

	"github.com/Tencent/WeKnora/internal/plugin/control"
	pluginstore "github.com/Tencent/WeKnora/internal/plugin/store"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type revisionKnowledgeRepo struct {
	interfaces.KnowledgeRepository
	db *gorm.DB
}

func (r *revisionKnowledgeRepo) GetKnowledgeByIDOnly(_ context.Context, id string) (*types.Knowledge, error) {
	var knowledge types.Knowledge
	if err := r.db.First(&knowledge, "id = ?", id).Error; err != nil {
		return nil, err
	}
	return &knowledge, nil
}
func (r *revisionKnowledgeRepo) UpdateKnowledge(_ context.Context, knowledge *types.Knowledge) error {
	return r.db.Save(knowledge).Error
}
func (r *revisionKnowledgeRepo) HardDeleteKnowledge(_ context.Context, _ uint64, id string) error {
	return r.db.Unscoped().Delete(&types.Knowledge{}, "id = ?", id).Error
}

type revisionKnowledgeService struct {
	interfaces.KnowledgeService
	repo *revisionKnowledgeRepo
	mu   sync.Mutex
	next int
}

func (s *revisionKnowledgeService) newKnowledge(ctx context.Context, kbID, name, source string, metadata map[string]string) (*types.Knowledge, error) {
	s.mu.Lock()
	s.next++
	id := fmt.Sprintf("knowledge-%d", s.next)
	s.mu.Unlock()
	encoded, _ := json.Marshal(metadata)
	knowledge := &types.Knowledge{ID: id, TenantID: 7, KnowledgeBaseID: kbID, Type: "file", FileName: name,
		FileType: "md", FilePath: "stored/" + id, Source: source, ParseStatus: types.ParseStatusPending,
		EnableStatus: "disabled", Metadata: encoded}
	if source != "" {
		knowledge.Type = "url"
	}
	persister := interfaces.KnowledgeRecordPersisterFromContext(ctx)
	if persister == nil {
		return nil, errors.New("revision persister missing")
	}
	if err := persister.CreateKnowledge(ctx, knowledge); err != nil {
		return nil, err
	}
	return knowledge, nil
}

func (s *revisionKnowledgeService) CreateKnowledgeFromFile(
	ctx context.Context, kbID string, _ *multipart.FileHeader, metadata map[string]string, _ *bool,
	name string, _ []string, _ string, _ *types.KnowledgeProcessOverrides,
) (*types.Knowledge, error) {
	return s.newKnowledge(ctx, kbID, name, "", metadata)
}
func (s *revisionKnowledgeService) CreateKnowledgeFromURL(
	ctx context.Context, kbID, rawURL, fileName, _ string, _ *bool, _ string, _ []string, _ string,
	_ *types.KnowledgeProcessOverrides,
) (*types.Knowledge, error) {
	return s.newKnowledge(ctx, kbID, fileName, rawURL, nil)
}
func (s *revisionKnowledgeService) GetRepository() interfaces.KnowledgeRepository { return s.repo }
func (s *revisionKnowledgeService) DeleteKnowledge(_ context.Context, id string) error {
	return s.repo.db.Delete(&types.Knowledge{}, "id = ?", id).Error
}
func (s *revisionKnowledgeService) BuildDocumentProcessTask(_ context.Context, knowledgeID string) (*asynq.Task, []asynq.Option, error) {
	knowledge, err := s.repo.GetKnowledgeByIDOnly(context.Background(), knowledgeID)
	if err != nil {
		return nil, nil, err
	}
	payload, _ := json.Marshal(types.DocumentProcessPayload{TenantID: knowledge.TenantID, KnowledgeID: knowledge.ID,
		KnowledgeBaseID: knowledge.KnowledgeBaseID, FilePath: knowledge.FilePath, FileName: knowledge.FileName, FileType: knowledge.FileType})
	return asynq.NewTask(types.TypeDocumentProcess, payload), []asynq.Option{asynq.Queue(types.QueueDefault)}, nil
}

type revisionTaskRecorder struct {
	mu       sync.Mutex
	seen     map[string]struct{}
	accepted int
	failNext bool
}

func (r *revisionTaskRecorder) Enqueue(task *asynq.Task, _ ...asynq.Option) (*asynq.TaskInfo, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failNext {
		r.failNext = false
		return nil, errors.New("queue unavailable")
	}
	var payload types.DocumentProcessPayload
	_ = json.Unmarshal(task.Payload(), &payload)
	if _, exists := r.seen[payload.KnowledgeID]; exists {
		return nil, asynq.ErrTaskIDConflict
	}
	r.seen[payload.KnowledgeID] = struct{}{}
	r.accepted++
	return &asynq.TaskInfo{ID: payload.KnowledgeID, Queue: types.QueueDefault}, nil
}

func revisionFixture(t *testing.T) (*gorm.DB, *pluginstore.Store, *RevisionProcessor, *revisionTaskRecorder) {
	t.Helper()
	db, err := gorm.Open(sqlite.Open("file:"+t.Name()+"?mode=memory&cache=shared"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&types.Knowledge{}, &control.DataSourceRevision{}))
	store := pluginstore.New(db)
	repo := &revisionKnowledgeRepo{db: db}
	knowledge := &revisionKnowledgeService{repo: repo}
	queue := &revisionTaskRecorder{seen: make(map[string]struct{})}
	return db, store, NewRevisionProcessor(store, knowledge, queue), queue
}

func externalDS() *types.DataSource {
	return &types.DataSource{ID: "ds-1", TenantID: 7, KnowledgeBaseID: "kb-1", Type: "external"}
}

func externalItem(revision, body string) types.FetchedItem {
	return types.FetchedItem{ExternalID: "a.md", Revision: revision, Title: "A", FileName: "a.md", Content: []byte(body), ContentType: "text/markdown"}
}

func TestRevisionReplayAndFailedReplacementKeepOldActive(t *testing.T) {
	db, store, processor, queue := revisionFixture(t)
	ctx := context.Background()
	result, err := processor.AcceptExternalItem(ctx, externalDS(), externalItem("r1", "one"), nil)
	require.NoError(t, err)
	require.True(t, result.Created)
	_, err = processor.AcceptExternalItem(ctx, externalDS(), externalItem("r1", "one"), nil)
	require.NoError(t, err)
	require.Equal(t, 1, queue.accepted, "same revision must not create a second processing task")

	r1, err := store.GetRevision(ctx, "ds-1", "a.md", "r1")
	require.NoError(t, err)
	require.NoError(t, db.Model(&types.Knowledge{}).Where("id = ?", *r1.KnowledgeID).
		Update("parse_status", types.ParseStatusCompleted).Error)
	require.NoError(t, processor.ReconcilePending(ctx, 10))
	r1, err = store.GetActiveRevision(ctx, "ds-1", "a.md")
	require.NoError(t, err)
	var oldKnowledge types.Knowledge
	require.NoError(t, db.First(&oldKnowledge, "id = ?", *r1.KnowledgeID).Error)
	require.Equal(t, "enabled", oldKnowledge.EnableStatus)
	require.Equal(t, control.RevisionActive, oldKnowledge.GetMetadata()["plugin_revision_state"])

	_, err = processor.AcceptExternalItem(ctx, externalDS(), externalItem("r2", "two"), nil)
	require.NoError(t, err)
	r2, err := store.GetRevision(ctx, "ds-1", "a.md", "r2")
	require.NoError(t, err)
	var pendingKnowledge types.Knowledge
	require.NoError(t, db.First(&pendingKnowledge, "id = ?", *r2.KnowledgeID).Error)
	require.Equal(t, "disabled", pendingKnowledge.EnableStatus)
	require.NoError(t, db.Model(&types.Knowledge{}).Where("id = ?", *r2.KnowledgeID).
		Updates(map[string]any{"parse_status": types.ParseStatusFailed, "error_message": "parse failed"}).Error)
	require.NoError(t, processor.ReconcilePending(ctx, 10))
	r2, err = store.GetRevision(ctx, "ds-1", "a.md", "r2")
	require.NoError(t, err)
	require.Equal(t, control.RevisionFailed, r2.State)
	active, err := store.GetActiveRevision(ctx, "ds-1", "a.md")
	require.NoError(t, err)
	require.Equal(t, "r1", active.Revision)
	require.NoError(t, db.First(&oldKnowledge, "id = ?", *active.KnowledgeID).Error)
	require.Equal(t, "enabled", oldKnowledge.EnableStatus)
}

func TestPendingRevisionRecoversLostEnqueueAndDeleteIsIdempotent(t *testing.T) {
	db, store, processor, queue := revisionFixture(t)
	queue.failNext = true
	ctx := context.Background()
	_, err := processor.AcceptExternalItem(ctx, externalDS(), externalItem("r1", "one"), nil)
	require.NoError(t, err, "durably pending is accepted even when initial enqueue is lost")
	require.Zero(t, queue.accepted)
	_, err = processor.AcceptExternalItem(ctx, externalDS(), externalItem("r1", "one"), nil)
	require.NoError(t, err, "a pending replay is already durably accepted")
	require.Zero(t, queue.accepted)
	require.NoError(t, processor.ReconcilePending(ctx, 10))
	require.Equal(t, 1, queue.accepted)
	revision, err := store.GetRevision(ctx, "ds-1", "a.md", "r1")
	require.NoError(t, err)
	require.NoError(t, db.Model(&types.Knowledge{}).Where("id = ?", *revision.KnowledgeID).
		Update("parse_status", types.ParseStatusCompleted).Error)
	require.NoError(t, processor.ReconcilePending(ctx, 10))

	deleted, err := processor.AcceptExternalItem(ctx, externalDS(), types.FetchedItem{ExternalID: "a.md", IsDeleted: true}, nil)
	require.NoError(t, err)
	require.True(t, deleted.Deleted)
	replayed, err := processor.AcceptExternalItem(ctx, externalDS(), types.FetchedItem{ExternalID: "a.md", IsDeleted: true}, nil)
	require.NoError(t, err)
	require.True(t, replayed.Skipped)
}
