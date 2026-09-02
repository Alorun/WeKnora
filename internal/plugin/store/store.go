// Package store persists plugin control-plane records using the repository
// conventions already used by WeKnora's GORM-backed application repositories.
package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Tencent/WeKnora/internal/plugin/control"
	"gorm.io/gorm"
)

var ErrNotFound = errors.New("plugin record not found")

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
