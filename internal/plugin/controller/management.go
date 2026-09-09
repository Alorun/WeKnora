package controller

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/Tencent/WeKnora/internal/plugin/control"
	"github.com/Tencent/WeKnora/internal/plugin/manager"
	"github.com/Tencent/WeKnora/internal/plugin/prepare"
	"github.com/Tencent/WeKnora/internal/plugin/store"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/pkg/plugin/sdk"
	"github.com/robfig/cron/v3"
)

type CreateRequest struct {
	KnowledgeBaseID string         `json:"knowledge_base_id"`
	InstallationID  string         `json:"installation_id"`
	Name            string         `json:"name"`
	Settings        map[string]any `json:"settings"`
	ResourceIDs     []string       `json:"resource_ids"`
	SyncSchedule    string         `json:"sync_schedule"`
}

func (c *Controller) Create(ctx context.Context, tenant uint64, req CreateRequest) (*types.DataSource, error) {
	if c.reconciler == nil {
		return nil, manager.ErrRuntimeNotAvailable
	}
	i, err := c.Store.GetInstallation(ctx, req.InstallationID)
	if err != nil {
		return nil, err
	}
	if tenant == 0 || !i.Active || i.InstallStatus != control.InstallStatusInstalled {
		return nil, errors.New("installation unavailable")
	}
	p, err := c.Builder.LoadPackage(ctx, *i)
	if err != nil {
		return nil, err
	}
	if p.Artifact.Digest != i.ArtifactDigest {
		return nil, errors.New("installation artifact changed")
	}
	if err := prepare.ValidateSettings(p.Manifest, req.Settings); err != nil {
		return nil, err
	}
	if req.SyncSchedule != "" {
		if _, err := cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow).Parse(req.SyncSchedule); err != nil {
			return nil, err
		}
	}
	conf := types.DataSourceConfig{Type: string(p.Manifest.Spec.Extension.ID), Settings: req.Settings, ResourceIDs: req.ResourceIDs}
	data, err := json.Marshal(conf)
	if err != nil {
		return nil, err
	}
	if len(data) > sdk.MaxConfigBytes {
		return nil, errors.New("configuration too large")
	}
	ds := &types.DataSource{TenantID: tenant, KnowledgeBaseID: req.KnowledgeBaseID, Name: req.Name, Type: conf.Type, Config: types.JSON(data), SyncMode: types.SyncModeIncremental, SyncSchedule: req.SyncSchedule, SyncDeletions: true}
	binding := &control.DataSourcePluginBinding{InstallationID: i.ID, ExtensionID: p.Manifest.Spec.Extension.ID}
	if err := c.Store.CreateBoundDataSource(ctx, ds, binding); err != nil {
		return nil, err
	}
	return ds, c.RecordManagement(ctx, ds, "plugin.binding_created", nil)
}

func (c *Controller) Grant(ctx context.Context, ds *types.DataSource, root, relative string) (*control.DirectoryGrant, error) {
	if !types.IsSystemAdminFromContext(ctx) {
		return nil, errors.New("system administrator required")
	}
	if c.reconciler == nil {
		return nil, manager.ErrRuntimeNotAvailable
	}
	var grant *control.DirectoryGrant
	err := c.reconciler.WithLock(func() error {
		c.gate.Lock()
		defer c.gate.Unlock()
		if _, err := c.Store.GetBinding(ctx, ds.ID); err != nil {
			return err
		}
		if _, err := c.Store.GetDirectoryGrant(ctx, ds.ID); err == nil {
			return errors.New("grant/sync scope is fixed; create a new data source")
		} else if !errors.Is(err, store.ErrNotFound) {
			return err
		}
		actor, _ := types.UserIDFromContext(ctx)
		var err error
		grant, err = c.Builder.Grants.Authorize(ctx, ds.TenantID, ds.ID, root, relative, actor)
		return err
	})
	if err == nil {
		err = c.RecordManagement(ctx, ds, "plugin.grant_created", nil)
	}
	return grant, err
}

func (c *Controller) Revoke(ctx context.Context, ds *types.DataSource, grantID string) error {
	if !types.IsSystemAdminFromContext(ctx) {
		return errors.New("system administrator required")
	}
	g, err := c.Store.GetDirectoryGrant(ctx, ds.ID)
	if err != nil {
		return err
	}
	if grantID != g.ID {
		return errors.New("grant does not belong to data source")
	}
	err = c.setDesired(ctx, ds.ID, false, g.ID)
	return errors.Join(err, c.RecordManagement(ctx, ds, "plugin.grant_revoked", err))
}

func (c *Controller) RecordManagement(ctx context.Context, ds *types.DataSource, action types.AuditAction, cause error) error {
	actor, _ := types.UserIDFromContext(ctx)
	row := &types.AuditLog{Action: action, ActorUserID: actor, Outcome: types.AuditOutcomeSuccess, CreatedAt: time.Now()}
	if cause != nil {
		row.Outcome = types.AuditOutcomeFailed
	}
	if ds != nil {
		row.TenantID = ds.TenantID
		row.ScopeType = "data_source"
		row.ScopeID = ds.ID
		row.TargetType = "data_source"
		row.TargetID = ds.ID
	}
	return c.audit.Create(ctx, row)
}
