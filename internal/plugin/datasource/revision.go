package datasource

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"mime/multipart"
	"net/textproto"

	core "github.com/Tencent/WeKnora/internal/datasource"
	"github.com/Tencent/WeKnora/internal/plugin/control"
	pluginstore "github.com/Tencent/WeKnora/internal/plugin/store"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	pluginsdk "github.com/Tencent/WeKnora/pkg/plugin/sdk"
	"github.com/hibiken/asynq"
)

type RevisionStore interface {
	GetRevision(context.Context, string, string, string) (*control.DataSourceRevision, error)
	CreatePendingRevisionWithKnowledge(context.Context, *control.DataSourceRevision, *types.Knowledge) error
	ListPendingRevisions(context.Context, int) ([]control.DataSourceRevision, error)
	ListSupersededRevisions(context.Context, int) ([]control.DataSourceRevision, error)
	ActivateRevision(context.Context, string, string, string) (*string, error)
	SupersedeActiveForDelete(context.Context, string, string) (*string, error)
	FailRevision(context.Context, control.DataSourceRevision, string) error
}

type RevisionProcessor struct {
	store      RevisionStore
	knowledge  interfaces.KnowledgeService
	enqueuer   interfaces.TaskEnqueuer
	maxContent int
}

func NewRevisionProcessor(store RevisionStore, knowledge interfaces.KnowledgeService, enqueuer interfaces.TaskEnqueuer) *RevisionProcessor {
	return &RevisionProcessor{store: store, knowledge: knowledge, enqueuer: enqueuer, maxContent: pluginsdk.MaxDocumentBytes}
}

func (p *RevisionProcessor) AcceptExternalItem(
	ctx context.Context, ds *types.DataSource, item types.FetchedItem, tagIDs []string,
) (core.ExternalIngestResult, error) {
	if ds == nil || ds.ID == "" {
		return core.ExternalIngestResult{}, fmt.Errorf("data source is required")
	}
	if item.IsDeleted {
		return p.acceptDelete(ctx, ds, item)
	}
	return p.acceptUpsert(ctx, ds, item, tagIDs)
}

func (p *RevisionProcessor) acceptUpsert(
	ctx context.Context, ds *types.DataSource, item types.FetchedItem, tagIDs []string,
) (core.ExternalIngestResult, error) {
	if p == nil || p.store == nil || p.knowledge == nil || p.enqueuer == nil {
		return core.ExternalIngestResult{}, fmt.Errorf("external revision processor is not configured")
	}
	if item.ExternalID == "" || item.Revision == "" {
		return core.ExternalIngestResult{}, fmt.Errorf("external_id and revision are required")
	}
	if len(item.Content) > p.maxContent {
		return core.ExternalIngestResult{}, fmt.Errorf("%s: document exceeds %d bytes", pluginsdk.ErrorMessageTooLarge, p.maxContent)
	}
	if len(item.Content) == 0 && item.URL == "" {
		return core.ExternalIngestResult{}, fmt.Errorf("upsert has neither content nor URL")
	}
	_, err := p.store.GetRevision(ctx, ds.ID, item.ExternalID, item.Revision)
	if err == nil {
		// The ledger row is the durable acceptance point. A pending replay must
		// not depend on the queue being available; the controller reconciler
		// owns re-arming its deterministic task.
		return core.ExternalIngestResult{Skipped: true}, nil
	}
	if !errors.Is(err, pluginstore.ErrNotFound) {
		return core.ExternalIngestResult{}, err
	}

	revision := &control.DataSourceRevision{DataSourceID: ds.ID, ExternalID: item.ExternalID, Revision: item.Revision, State: control.RevisionPending}
	metadata := revisionMetadata(ds.ID, item)
	persister := &pendingRevisionPersister{store: p.store, revision: revision, metadata: metadata}
	createCtx := interfaces.WithDeferredKnowledgeProcessing(
		interfaces.WithKnowledgeRecordPersister(ctx, persister),
	)
	var knowledge *types.Knowledge
	if len(item.Content) != 0 {
		file, fileErr := fetchedItemFile(item)
		if fileErr != nil {
			return core.ExternalIngestResult{}, fileErr
		}
		knowledge, err = p.knowledge.CreateKnowledgeFromFile(
			createCtx, ds.KnowledgeBaseID, file, metadata, nil, item.FileName, tagIDs, ds.Type, nil,
		)
	} else {
		knowledge, err = p.knowledge.CreateKnowledgeFromURL(
			createCtx, ds.KnowledgeBaseID, item.URL, item.FileName, "", nil, item.Title, tagIDs, ds.Type, nil,
		)
	}
	if err != nil {
		// The transaction may have committed before a later tag attachment failed.
		// Such a revision is durably accepted and recovery owns its processing.
		persisted, lookupErr := p.store.GetRevision(ctx, ds.ID, item.ExternalID, item.Revision)
		if lookupErr != nil || persisted.State != control.RevisionPending {
			return core.ExternalIngestResult{}, err
		}
		knowledge, _ = p.knowledge.GetRepository().GetKnowledgeByIDOnly(ctx, valueOrEmpty(persisted.KnowledgeID))
	}
	if knowledge == nil {
		return core.ExternalIngestResult{}, fmt.Errorf("pending revision has no knowledge")
	}
	// Enqueue is intentionally after the transaction. Failure does not reject
	// the accepted event: the pending ledger is the durable recovery source.
	_ = p.enqueueRevision(ctx, knowledge, revision)
	return core.ExternalIngestResult{Created: true}, nil
}

func (p *RevisionProcessor) acceptDelete(
	ctx context.Context, ds *types.DataSource, item types.FetchedItem,
) (core.ExternalIngestResult, error) {
	if item.ExternalID == "" {
		return core.ExternalIngestResult{}, fmt.Errorf("delete external_id is required")
	}
	knowledgeID, err := p.store.SupersedeActiveForDelete(ctx, ds.ID, item.ExternalID)
	if err != nil {
		return core.ExternalIngestResult{}, err
	}
	if knowledgeID == nil {
		return core.ExternalIngestResult{Skipped: true}, nil
	}
	// The DB transition is the durable acceptance point. Cleanup is retryable
	// from the superseded ledger and therefore cannot make a replay destructive.
	_ = p.cleanupKnowledge(ctx, ds.TenantID, *knowledgeID)
	return core.ExternalIngestResult{Deleted: true}, nil
}

// ReconcilePending re-arms lost parsing tasks and commits completed revisions.
// It is safe to call repeatedly on startup and from a single controller loop.
func (p *RevisionProcessor) ReconcilePending(ctx context.Context, limit int) error {
	revisions, err := p.store.ListPendingRevisions(ctx, limit)
	if err != nil {
		return err
	}
	var result error
	for _, revision := range revisions {
		if revision.KnowledgeID == nil {
			result = errors.Join(result, p.store.FailRevision(ctx, revision, "pending revision has no knowledge"))
			continue
		}
		knowledge, lookupErr := p.knowledge.GetRepository().GetKnowledgeByIDOnly(ctx, *revision.KnowledgeID)
		if lookupErr != nil {
			result = errors.Join(result, p.store.FailRevision(ctx, revision, lookupErr.Error()))
			continue
		}
		switch knowledge.ParseStatus {
		case types.ParseStatusCompleted:
			if metadataErr := p.setRevisionMetadataState(ctx, knowledge, control.RevisionActive); metadataErr != nil {
				result = errors.Join(result, metadataErr)
				continue
			}
			oldKnowledgeID, activateErr := p.store.ActivateRevision(ctx, revision.DataSourceID, revision.ExternalID, revision.Revision)
			result = errors.Join(result, activateErr)
			if activateErr == nil && oldKnowledgeID != nil {
				result = errors.Join(result, p.cleanupKnowledge(ctx, knowledge.TenantID, *oldKnowledgeID))
			}
		case types.ParseStatusFailed:
			if knowledge.ErrorMessage == "Failed to enqueue processing task" {
				result = errors.Join(result, p.enqueueRevision(ctx, knowledge, &revision))
			} else {
				result = errors.Join(result, p.store.FailRevision(ctx, revision, knowledge.ErrorMessage))
			}
		case types.ParseStatusPending:
			result = errors.Join(result, p.enqueueRevision(ctx, knowledge, &revision))
		case types.ParseStatusProcessing, types.ParseStatusFinalizing:
			// An existing worker owns it; no duplicate task is emitted.
		default:
			result = errors.Join(result, p.store.FailRevision(ctx, revision, "unexpected knowledge state: "+knowledge.ParseStatus))
		}
	}
	return result
}

func (p *RevisionProcessor) CleanupSuperseded(ctx context.Context, limit int) error {
	revisions, err := p.store.ListSupersededRevisions(ctx, limit)
	if err != nil {
		return err
	}
	var result error
	for _, revision := range revisions {
		if revision.KnowledgeID == nil {
			continue
		}
		knowledge, lookupErr := p.knowledge.GetRepository().GetKnowledgeByIDOnly(ctx, *revision.KnowledgeID)
		if lookupErr != nil {
			continue
		}
		result = errors.Join(result, p.cleanupKnowledge(ctx, knowledge.TenantID, knowledge.ID))
	}
	return result
}

func (p *RevisionProcessor) setRevisionMetadataState(ctx context.Context, knowledge *types.Knowledge, state string) error {
	current := knowledge.GetMetadata()
	if current == nil {
		current = make(map[string]string)
	}
	if current["plugin_revision_state"] == state {
		return nil
	}
	current["plugin_revision_state"] = state
	encoded, err := json.Marshal(current)
	if err != nil {
		return err
	}
	knowledge.Metadata = encoded
	return p.knowledge.GetRepository().UpdateKnowledge(ctx, knowledge)
}

func (p *RevisionProcessor) enqueueRevision(ctx context.Context, knowledge *types.Knowledge, revision *control.DataSourceRevision) error {
	task, options, err := p.knowledge.BuildDocumentProcessTask(ctx, knowledge.ID)
	if err != nil {
		return err
	}
	taskID := revisionTaskID(revision)
	options = append(options, asynq.TaskID(taskID))
	_, err = p.enqueuer.Enqueue(task, options...)
	if errors.Is(err, asynq.ErrTaskIDConflict) {
		return nil
	}
	return err
}

func (p *RevisionProcessor) cleanupKnowledge(ctx context.Context, tenantID uint64, knowledgeID string) error {
	if err := p.knowledge.DeleteKnowledge(ctx, knowledgeID); err != nil {
		return err
	}
	return p.knowledge.GetRepository().HardDeleteKnowledge(ctx, tenantID, knowledgeID)
}

type pendingRevisionPersister struct {
	store    RevisionStore
	revision *control.DataSourceRevision
	metadata map[string]string
}

func (p *pendingRevisionPersister) CreateKnowledge(ctx context.Context, knowledge *types.Knowledge) error {
	current := knowledge.GetMetadata()
	if current == nil {
		current = make(map[string]string)
	}
	for key, value := range p.metadata {
		current[key] = value
	}
	encoded, err := json.Marshal(current)
	if err != nil {
		return err
	}
	knowledge.Metadata = encoded
	return p.store.CreatePendingRevisionWithKnowledge(ctx, p.revision, knowledge)
}

func revisionMetadata(dataSourceID string, item types.FetchedItem) map[string]string {
	metadata := make(map[string]string, len(item.Metadata)+5)
	for key, value := range item.Metadata {
		metadata[key] = value
	}
	metadata["external_id"] = item.ExternalID
	metadata["datasource_id"] = dataSourceID
	metadata["source_resource_id"] = item.SourceResourceID
	metadata["plugin_revision"] = item.Revision
	metadata["plugin_revision_state"] = control.RevisionPending
	return metadata
}

func fetchedItemFile(item types.FetchedItem) (*multipart.FileHeader, error) {
	fileName := item.FileName
	if fileName == "" {
		fileName = item.Title
	}
	if fileName == "" {
		return nil, fmt.Errorf("file_name is required for document content")
	}
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	header := make(textproto.MIMEHeader)
	header.Set("Content-Disposition", fmt.Sprintf(`form-data; name="file"; filename="%s"`, fileName))
	header.Set("Content-Type", item.ContentType)
	part, err := writer.CreatePart(header)
	if err != nil {
		return nil, err
	}
	if _, err := part.Write(item.Content); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	request := multipart.NewReader(bytes.NewReader(body.Bytes()), writer.Boundary())
	form, err := request.ReadForm(int64(len(item.Content)) + (1 << 20))
	if err != nil {
		return nil, err
	}
	files := form.File["file"]
	if len(files) != 1 {
		return nil, fmt.Errorf("failed to construct fetched item file")
	}
	return files[0], nil
}

func revisionTaskID(revision *control.DataSourceRevision) string {
	sum := sha256.Sum256([]byte(revision.DataSourceID + "\x00" + revision.ExternalID + "\x00" + revision.Revision))
	return "plugin-revision:" + hex.EncodeToString(sum[:])
}

func valueOrEmpty(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
