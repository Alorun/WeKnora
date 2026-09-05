// Package store persists plugin control-plane records using the repository
// conventions already used by WeKnora's GORM-backed application repositories.
package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Tencent/WeKnora/internal/plugin/control"
	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
)

var (
	ErrNotFound       = errors.New("plugin record not found")
	ErrRevisionExists = errors.New("datasource revision already exists")
)

type Store struct {
	db *gorm.DB
}

func New(db *gorm.DB) *Store { return &Store{db: db} }

func (s *Store) CreateInstallation(ctx context.Context, installation *control.PluginInstallation) error {
	if installation == nil {
		return errors.New("installation is required")
	}
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var count int64
		if installation.Active {
			if err := tx.Model(&control.PluginInstallation{}).
				Where("plugin_id = ? AND active = ?", installation.PluginID, true).
				Count(&count).Error; err != nil {
				return err
			}
			if count != 0 {
				return fmt.Errorf("plugin %q already has an active installation", installation.PluginID)
			}
		}
		return tx.Create(installation).Error
	})
}

func (s *Store) GetInstallation(ctx context.Context, id string) (*control.PluginInstallation, error) {
	var installation control.PluginInstallation
	if err := s.db.WithContext(ctx).First(&installation, "id = ?", id).Error; err != nil {
		return nil, normalizeNotFound(err)
	}
	return &installation, nil
}

func (s *Store) ListActiveInstallations(ctx context.Context) ([]control.PluginInstallation, error) {
	var installations []control.PluginInstallation
	err := s.db.WithContext(ctx).Where("active = ?", true).Order("plugin_id ASC").Find(&installations).Error
	return installations, err
}

func (s *Store) UpdateInstallationState(
	ctx context.Context, id string, enabled bool, installStatus, lastError string,
) error {
	result := s.db.WithContext(ctx).Model(&control.PluginInstallation{}).Where("id = ?", id).Updates(map[string]any{
		"enabled": enabled, "install_status": installStatus, "last_error": lastError, "updated_at": time.Now(),
	})
	return affected(result)
}

func (s *Store) CreateBinding(ctx context.Context, binding *control.DataSourcePluginBinding) error {
	if binding == nil {
		return errors.New("binding is required")
	}
	return s.db.WithContext(ctx).Create(binding).Error
}

func (s *Store) GetBinding(ctx context.Context, dataSourceID string) (*control.DataSourcePluginBinding, error) {
	var binding control.DataSourcePluginBinding
	if err := s.db.WithContext(ctx).First(&binding, "data_source_id = ?", dataSourceID).Error; err != nil {
		return nil, normalizeNotFound(err)
	}
	return &binding, nil
}

func (s *Store) ListBindings(ctx context.Context) ([]control.DataSourcePluginBinding, error) {
	var bindings []control.DataSourcePluginBinding
	err := s.db.WithContext(ctx).Order("data_source_id ASC").Find(&bindings).Error
	return bindings, err
}

func (s *Store) UpdateBindingObserved(
	ctx context.Context, dataSourceID string, generation uint64, state control.PluginState, sandboxID, lastError string,
) error {
	result := s.db.WithContext(ctx).Model(&control.DataSourcePluginBinding{}).
		Where("data_source_id = ? AND generation = ?", dataSourceID, generation).
		Updates(map[string]any{
			"observed_generation": generation, "observed_state": state,
			"sandbox_id": sandboxID, "last_error": lastError, "updated_at": time.Now(),
		})
	return affected(result)
}

func (s *Store) GetDataSourceStatus(ctx context.Context, dataSourceID string) (string, error) {
	var status string
	result := s.db.WithContext(ctx).Table("data_sources").Select("status").Where("id = ?", dataSourceID).Scan(&status)
	if result.Error != nil {
		return "", result.Error
	}
	if result.RowsAffected == 0 {
		return "", ErrNotFound
	}
	return status, nil
}

func (s *Store) DeleteBinding(ctx context.Context, dataSourceID string) error {
	result := s.db.WithContext(ctx).Delete(&control.DataSourcePluginBinding{}, "data_source_id = ?", dataSourceID)
	return affected(result)
}

func (s *Store) CreateDirectoryGrant(ctx context.Context, grant *control.DirectoryGrant) error {
	if grant == nil {
		return errors.New("directory grant is required")
	}
	var tenantID uint64
	result := s.db.WithContext(ctx).Table("data_sources").
		Select("tenant_id").Where("id = ?", grant.DataSourceID).Scan(&tenantID)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return ErrNotFound
	}
	if tenantID != grant.TenantID {
		return errors.New("directory grant tenant does not own the data source")
	}
	return s.db.WithContext(ctx).Create(grant).Error
}

func (s *Store) GetActiveDirectoryGrant(ctx context.Context, dataSourceID string) (*control.DirectoryGrant, error) {
	var grant control.DirectoryGrant
	if err := s.db.WithContext(ctx).
		Where("data_source_id = ? AND status = ?", dataSourceID, control.GrantStatusActive).
		First(&grant).Error; err != nil {
		return nil, normalizeNotFound(err)
	}
	return &grant, nil
}

func (s *Store) RevokeDirectoryGrant(ctx context.Context, id string, nextGeneration uint64) error {
	now := time.Now()
	result := s.db.WithContext(ctx).Model(&control.DirectoryGrant{}).
		Where("id = ? AND status = ? AND generation < ?", id, control.GrantStatusActive, nextGeneration).
		Updates(map[string]any{
			"status": control.GrantStatusRevoked, "generation": nextGeneration,
			"revoked_at": now, "updated_at": now,
		})
	return affected(result)
}

func (s *Store) CreateRevision(ctx context.Context, revision *control.DataSourceRevision) error {
	if revision == nil {
		return errors.New("revision is required")
	}
	return s.db.WithContext(ctx).Create(revision).Error
}

// CreateKnowledge writes a pending revision and its disabled Knowledge row in
// one database transaction. It implements interfaces.KnowledgeRecordPersister
// through pendingRevisionPersister in the datasource package rather than being
// used as the default Knowledge repository.
func (s *Store) CreatePendingRevisionWithKnowledge(
	ctx context.Context, revision *control.DataSourceRevision, knowledge *types.Knowledge,
) error {
	if revision == nil || knowledge == nil || revision.DataSourceID == "" || revision.ExternalID == "" || revision.Revision == "" {
		return errors.New("revision and knowledge identity are required")
	}
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var count int64
		if err := tx.Model(&control.DataSourceRevision{}).Where(
			"data_source_id = ? AND external_id = ? AND revision = ?",
			revision.DataSourceID, revision.ExternalID, revision.Revision,
		).Count(&count).Error; err != nil {
			return err
		}
		if count != 0 {
			return ErrRevisionExists
		}
		knowledge.EnableStatus = "disabled"
		if err := tx.Create(knowledge).Error; err != nil {
			return err
		}
		knowledgeID := knowledge.ID
		revision.KnowledgeID = &knowledgeID
		revision.State = control.RevisionPending
		if err := tx.Create(revision).Error; err != nil {
			return err
		}
		return nil
	})
}

func (s *Store) GetRevision(
	ctx context.Context, dataSourceID, externalID, revision string,
) (*control.DataSourceRevision, error) {
	var result control.DataSourceRevision
	err := s.db.WithContext(ctx).
		Where("data_source_id = ? AND external_id = ? AND revision = ?", dataSourceID, externalID, revision).
		First(&result).Error
	if err != nil {
		return nil, normalizeNotFound(err)
	}
	return &result, nil
}

func (s *Store) GetActiveRevision(ctx context.Context, dataSourceID, externalID string) (*control.DataSourceRevision, error) {
	var result control.DataSourceRevision
	err := s.db.WithContext(ctx).Where(
		"data_source_id = ? AND external_id = ? AND state = ?", dataSourceID, externalID, control.RevisionActive,
	).First(&result).Error
	if err != nil {
		return nil, normalizeNotFound(err)
	}
	return &result, nil
}

func (s *Store) ListPendingRevisions(ctx context.Context, limit int) ([]control.DataSourceRevision, error) {
	if limit <= 0 {
		limit = 100
	}
	var revisions []control.DataSourceRevision
	err := s.db.WithContext(ctx).Where("state = ?", control.RevisionPending).
		Order("created_at ASC").Limit(limit).Find(&revisions).Error
	return revisions, err
}

func (s *Store) ListSupersededRevisions(ctx context.Context, limit int) ([]control.DataSourceRevision, error) {
	if limit <= 0 {
		limit = 100
	}
	var revisions []control.DataSourceRevision
	err := s.db.WithContext(ctx).Where("state = ? AND knowledge_id IS NOT NULL", control.RevisionSuperseded).
		Order("updated_at ASC").Limit(limit).Find(&revisions).Error
	return revisions, err
}

// ActivateRevision atomically hides the old Knowledge, promotes the new one,
// and moves the ledger states. Physical cleanup happens after this transaction
// and can be retried without exposing two active documents.
func (s *Store) ActivateRevision(
	ctx context.Context, dataSourceID, externalID, revision string,
) (*string, error) {
	var oldKnowledgeID *string
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var current control.DataSourceRevision
		if err := tx.Where(
			"data_source_id = ? AND external_id = ? AND revision = ?",
			dataSourceID, externalID, revision,
		).First(&current).Error; err != nil {
			return normalizeNotFound(err)
		}
		if current.State == control.RevisionActive {
			return nil
		}
		if current.State != control.RevisionPending || current.KnowledgeID == nil {
			return fmt.Errorf("revision is %s, expected pending", current.State)
		}
		var previous control.DataSourceRevision
		previousResult := tx.Where(
			"data_source_id = ? AND external_id = ? AND state = ?",
			dataSourceID, externalID, control.RevisionActive,
		).First(&previous)
		if previousResult.Error != nil && !errors.Is(previousResult.Error, gorm.ErrRecordNotFound) {
			return previousResult.Error
		}
		now := time.Now()
		if previousResult.Error == nil {
			oldKnowledgeID = previous.KnowledgeID
			if err := tx.Model(&control.DataSourceRevision{}).Where(
				"data_source_id = ? AND external_id = ? AND revision = ? AND state = ?",
				dataSourceID, externalID, previous.Revision, control.RevisionActive,
			).Updates(map[string]any{"state": control.RevisionSuperseded, "updated_at": now}).Error; err != nil {
				return err
			}
			if oldKnowledgeID != nil {
				if err := tx.Model(&types.Knowledge{}).Where("id = ?", *oldKnowledgeID).
					Updates(map[string]any{"enable_status": "disabled", "updated_at": now}).Error; err != nil {
					return err
				}
			}
		}
		result := tx.Model(&control.DataSourceRevision{}).Where(
			"data_source_id = ? AND external_id = ? AND revision = ? AND state = ?",
			dataSourceID, externalID, revision, control.RevisionPending,
		).Updates(map[string]any{"state": control.RevisionActive, "last_error": "", "activated_at": now, "updated_at": now})
		if err := affected(result); err != nil {
			return err
		}
		return tx.Model(&types.Knowledge{}).Where("id = ?", *current.KnowledgeID).
			Updates(map[string]any{"enable_status": "enabled", "updated_at": now}).Error
	})
	return oldKnowledgeID, err
}

// SupersedeActiveForDelete makes a source delete durable and idempotent before
// physical Knowledge cleanup. Replays find no active revision and are no-ops.
func (s *Store) SupersedeActiveForDelete(ctx context.Context, dataSourceID, externalID string) (*string, error) {
	var knowledgeID *string
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var active control.DataSourceRevision
		err := tx.Where("data_source_id = ? AND external_id = ? AND state = ?", dataSourceID, externalID, control.RevisionActive).
			First(&active).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		knowledgeID = active.KnowledgeID
		now := time.Now()
		if err := tx.Model(&control.DataSourceRevision{}).Where(
			"data_source_id = ? AND external_id = ? AND revision = ? AND state = ?",
			dataSourceID, externalID, active.Revision, control.RevisionActive,
		).Updates(map[string]any{"state": control.RevisionSuperseded, "updated_at": now}).Error; err != nil {
			return err
		}
		if knowledgeID != nil {
			return tx.Model(&types.Knowledge{}).Where("id = ?", *knowledgeID).
				Updates(map[string]any{"enable_status": "disabled", "updated_at": now}).Error
		}
		return nil
	})
	return knowledgeID, err
}

func (s *Store) FailRevision(ctx context.Context, revision control.DataSourceRevision, lastError string) error {
	result := s.db.WithContext(ctx).Model(&control.DataSourceRevision{}).Where(
		"data_source_id = ? AND external_id = ? AND revision = ? AND state = ?",
		revision.DataSourceID, revision.ExternalID, revision.Revision, control.RevisionPending,
	).Updates(map[string]any{"state": control.RevisionFailed, "last_error": lastError, "updated_at": time.Now()})
	return affected(result)
}

func (s *Store) TransitionRevision(
	ctx context.Context, dataSourceID, externalID, revision, from, to, lastError string, knowledgeID *string,
) error {
	updates := map[string]any{"state": to, "last_error": lastError, "knowledge_id": knowledgeID, "updated_at": time.Now()}
	if to == control.RevisionActive {
		updates["activated_at"] = time.Now()
	}
	result := s.db.WithContext(ctx).Model(&control.DataSourceRevision{}).
		Where(
			"data_source_id = ? AND external_id = ? AND revision = ? AND state = ?",
			dataSourceID, externalID, revision, from,
		).Updates(updates)
	return affected(result)
}

func normalizeNotFound(err error) error {
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return ErrNotFound
	}
	return err
}

func affected(result *gorm.DB) error {
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return ErrNotFound
	}
	return nil
}
