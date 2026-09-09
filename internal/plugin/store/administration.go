package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Tencent/WeKnora/internal/plugin/control"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

func (s *Store) ListInstallations(ctx context.Context) ([]control.PluginInstallation, error) {
	var rows []control.PluginInstallation
	err := s.db.WithContext(ctx).Order("plugin_id, created_at").Find(&rows).Error
	return rows, err
}

// SaveDiscovery preserves persisted identity and enable intent. Invalid/missing
// candidates cannot become runnable, even if their prior intent was enabled.
func (s *Store) SaveDiscovery(ctx context.Context, row *control.PluginInstallation) error {
	if row.ID == "" {
		row.ID = uuid.NewString()
		return s.CreateInstallation(ctx, row)
	}
	return affected(s.db.WithContext(ctx).Model(&control.PluginInstallation{}).Where("id = ?", row.ID).
		Updates(map[string]any{"plugin_id": row.PluginID, "version": row.Version, "artifact_digest": row.ArtifactDigest, "active": row.Active,
			"install_status": row.InstallStatus, "last_error": row.LastError, "updated_at": time.Now()}))
}

func (s *Store) CreateBoundDataSource(ctx context.Context, ds *types.DataSource, binding *control.DataSourcePluginBinding) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var kb types.KnowledgeBase
		if err := tx.First(&kb, "id = ? AND tenant_id = ?", ds.KnowledgeBaseID, ds.TenantID).Error; err != nil {
			return err
		}
		if ds.ID == "" {
			ds.ID = uuid.NewString()
		}
		ds.Status = types.DataSourceStatusPaused
		binding.DataSourceID = ds.ID
		binding.Generation = 1
		binding.ObservedState = control.StateNotReady
		if err := tx.Create(ds).Error; err != nil {
			return err
		}
		return tx.Create(binding).Error
	})
}

// DataSource.status is V1's per-Binding desired state; no second enable model.
// The caller serializes this transaction against event acceptance and starts.
func (s *Store) SetDesired(ctx context.Context, id string, enabled bool, revokeGrant string) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var ds types.DataSource
		if err := tx.First(&ds, "id = ?", id).Error; err != nil {
			return err
		}
		var binding control.DataSourcePluginBinding
		if err := tx.First(&binding, "data_source_id = ?", id).Error; err != nil {
			return err
		}
		status := types.DataSourceStatusPaused
		if enabled {
			var grant control.DirectoryGrant
			if err := tx.Where("data_source_id = ? AND status = ? AND revoked_at IS NULL", id, control.GrantStatusActive).First(&grant).Error; err != nil {
				return fmt.Errorf("active grant required: %w", err)
			}
			if grant.TenantID != ds.TenantID {
				return errors.New("grant ownership mismatch")
			}
			var installation control.PluginInstallation
			if err := tx.First(&installation, "id = ?", binding.InstallationID).Error; err != nil {
				return err
			}
			if !installation.Active || installation.InstallStatus != control.InstallStatusInstalled {
				return errors.New("installation unavailable")
			}
			if err := tx.Model(&installation).Update("enabled", true).Error; err != nil {
				return err
			}
			status = types.DataSourceStatusActive
		}
		if revokeGrant != "" {
			if enabled {
				return errors.New("cannot enable while revoking")
			}
			if err := affected(tx.Model(&control.DirectoryGrant{}).Where("id = ? AND data_source_id = ?", revokeGrant, id).
				Updates(map[string]any{"status": control.GrantStatusRevoked, "revoked_at": time.Now(), "generation": gorm.Expr("generation + 1")})); err != nil {
				return err
			}
		}
		// An idempotent enable/disable does not invalidate work a second time.
		if ds.Status != status || revokeGrant != "" {
			if err := tx.Model(&binding).Updates(map[string]any{"generation": gorm.Expr("generation + 1"), "updated_at": time.Now()}).Error; err != nil {
				return err
			}
		}
		return tx.Model(&ds).Updates(map[string]any{"status": status, "error_message": ""}).Error
	})
}

func (s *Store) GetDirectoryGrant(ctx context.Context, dsID string) (*control.DirectoryGrant, error) {
	var g control.DirectoryGrant
	err := s.db.WithContext(ctx).First(&g, "data_source_id = ?", dsID).Error
	return &g, normalizeNotFound(err)
}

// Only non-scope fields are writable here; map updates also allow clearing cron.
func (s *Store) UpdateDataSourcePresentation(ctx context.Context, ds *types.DataSource) error {
	return affected(s.db.WithContext(ctx).Model(&types.DataSource{}).Where("id = ? AND tenant_id = ?", ds.ID, ds.TenantID).
		Updates(map[string]any{"name": ds.Name, "sync_schedule": ds.SyncSchedule, "updated_at": time.Now()}))
}
