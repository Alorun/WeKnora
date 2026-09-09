// Package controller owns the single-process production composition. It reuses
// Manager, Runtime, Reconciler and Asynq; it is not another service/runtime.
package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/Tencent/WeKnora/internal/config"
	"github.com/Tencent/WeKnora/internal/datasource"
	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/plugin/catalog"
	"github.com/Tencent/WeKnora/internal/plugin/control"
	pluginDS "github.com/Tencent/WeKnora/internal/plugin/datasource"
	"github.com/Tencent/WeKnora/internal/plugin/manager"
	"github.com/Tencent/WeKnora/internal/plugin/prepare"
	"github.com/Tencent/WeKnora/internal/plugin/reconcile"
	pr "github.com/Tencent/WeKnora/internal/plugin/runtime"
	"github.com/Tencent/WeKnora/internal/plugin/sandbox/docker"
	"github.com/Tencent/WeKnora/internal/plugin/sandbox/docker/network"
	"github.com/Tencent/WeKnora/internal/plugin/sandbox/docker/paths"
	"github.com/Tencent/WeKnora/internal/plugin/store"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/google/uuid"
	"github.com/hibiken/asynq"
	"golang.org/x/sys/unix"
	"gorm.io/gorm"
)

type Controller struct {
	Config     config.ExternalPluginsConfig
	Catalog    *catalog.Catalog
	Manager    *manager.PluginManager
	Store      *store.Store
	Builder    prepare.Builder
	backend    *docker.Backend
	reconciler *reconcile.Reconciler
	discovery  prepare.Discovery
	db         *gorm.DB
	dsRepo     interfaces.DataSourceRepository
	audit      interfaces.AuditLogRepository
	revisions  *pluginDS.RevisionProcessor
	scheduler  *datasource.Scheduler
	routes     *pluginDS.Resolver
	lock       *os.File
	cancel     context.CancelFunc
	done       chan struct{}
	worker     *asynq.Server
	gate       sync.RWMutex // desired-state writes vs each durable event/checkpoint
	closing    bool
	syncs      map[string]context.CancelFunc // QueuePlugin concurrency is one
}

func New(cfg *config.Config, cat *catalog.Catalog, mgr *manager.PluginManager, st *store.Store, db *gorm.DB,
	ds interfaces.DataSourceRepository, audit interfaces.AuditLogRepository, revisions *pluginDS.RevisionProcessor,
	scheduler *datasource.Scheduler, routes *pluginDS.Resolver) *Controller {
	return &Controller{Config: cfg.ExternalPlugins, Catalog: cat, Manager: mgr, Store: st, db: db, dsRepo: ds, audit: audit, revisions: revisions, scheduler: scheduler, routes: routes, syncs: map[string]context.CancelFunc{}}
}

func (c *Controller) Start(ctx context.Context, version string) (result error) {
	if !c.Config.Enabled {
		return nil
	}
	if err := c.Config.Validate(); err != nil {
		return err
	}
	cfg := c.Config
	if cfg.PluginUID == 0 {
		cfg.PluginUID = 65532
	}
	if cfg.PluginGID == 0 {
		cfg.PluginGID = 65532
	}
	limits := pr.ResourceLimits{MemoryBytes: int64(cfg.MaxResources.MemoryMiB) << 20, CPUQuota: cfg.MaxResources.CPUQuota, PidsLimit: cfg.MaxResources.MaxProcesses}
	if err := prepare.ValidateLimits(limits, limits); err != nil {
		return err
	}
	bc := docker.Config{DeploymentID: cfg.DeploymentID, DockerHost: cfg.DockerHost, Image: cfg.Image, GateAppPath: cfg.GateAppPath, GateHostPath: cfg.GateHostPath,
		RuntimeRoot: cfg.RuntimeRoot, ArtifactRoot: cfg.ArtifactRoot, AdminUID: cfg.AdminUID, PluginUID: cfg.PluginUID, PluginGID: cfg.PluginGID, MaxInstances: cfg.MaxInstances, MaxResources: limits, Audit: c.RecordNetwork}
	for _, root := range cfg.AllowRoots {
		bc.GrantRoots = append(bc.GrantRoots, root)
	}
	if err := paths.TrustedDirectory(cfg.RuntimeRoot.AppRoot, cfg.AdminUID, cfg.PluginUID); err != nil {
		return err
	}
	lock, err := os.OpenFile(filepath.Join(cfg.RuntimeRoot.AppRoot, ".controller.lock"), os.O_CREATE|os.O_RDWR|unix.O_NOFOLLOW, 0600)
	if err != nil {
		return err
	}
	if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		lock.Close()
		return fmt.Errorf("another plugin Controller owns this runtime root: %w", err)
	}
	c.lock = lock
	defer func() {
		if result != nil {
			result = errors.Join(result, c.Close())
		}
	}()
	b, err := docker.New(ctx, bc)
	if err != nil {
		return err
	}
	c.backend = b
	if err := b.CheckEnvironment(ctx); err != nil {
		return err
	}
	c.discovery = prepare.Discovery{Packages: cfg.Packages, Snapshots: cfg.ArtifactRoot, AdminUID: cfg.AdminUID, PluginUID: cfg.PluginUID, Options: control.ManifestValidationOptions{WeKnoraVersion: version, BuiltinIDs: c.Catalog.BuiltinIDs()}}
	previous, err := c.Store.ListInstallations(ctx)
	if err != nil {
		return err
	}
	results, err := c.discovery.Scan(previous)
	if err != nil {
		return err
	}
	for _, found := range results {
		row := found.Installation
		if found.Package != nil {
			row.InstallStatus = control.InstallStatusInstalled
			row.LastError = ""
		}
		// Existing migrations intentionally have only installed/invalid. A
		// missing package is invalid with a locatable diagnostic, not a new state.
		if row.InstallStatus == "missing" {
			row.InstallStatus = control.InstallStatusInvalid
		}
		if row.ID == "" {
			row.ID = uuid.NewSHA1(uuid.NameSpaceOID, []byte("weknora-plugin:"+found.Path)).String()
			if existing, err := c.Store.GetInstallation(ctx, row.ID); err == nil {
				row.Enabled = existing.Enabled
				row.Active = existing.Active
				if existing.PluginID != "" && (row.PluginID != existing.PluginID || row.Version != existing.Version || row.ArtifactDigest != existing.ArtifactDigest) {
					message := row.LastError
					if message == "" {
						message = "package identity/content at installation path changed"
					}
					row = *existing
					row.InstallStatus = control.InstallStatusInvalid
					row.LastError = message
					found.Package = nil
				}
			} else if !errors.Is(err, store.ErrNotFound) {
				return err
			}
			if found.Package != nil {
				row.Active = true
			}
			// New records go through Create; existing invalid-path records update.
			if _, err := c.Store.GetInstallation(ctx, row.ID); errors.Is(err, store.ErrNotFound) {
				if err := c.Store.CreateInstallation(ctx, &row); err != nil {
					return err
				}
			}
		}
		if err := c.Store.SaveDiscovery(ctx, &row); err != nil {
			return err
		}
		if found.Package != nil {
			if err := c.Manager.LoadExternal(found.Package.Manifest); err != nil {
				return err
			}
		}
	}
	c.Builder = prepare.Builder{Grants: prepare.GrantService{Store: c.Store, AllowRoots: cfg.AllowRoots, AdminUID: cfg.AdminUID, PluginUID: cfg.PluginUID}, LoadDataSource: c.dsRepo.FindByID,
		LoadPackage: func(ctx context.Context, i control.PluginInstallation) (prepare.Package, error) {
			// Load the immutable administrator snapshot, never a mutated package.
			for _, found := range results {
				if found.Package != nil && found.Installation.PluginID == i.PluginID {
					return c.discovery.Load(found.Path)
				}
			}
			return prepare.Package{}, errors.New("installation package unavailable")
		}, RuntimeRoot: cfg.RuntimeRoot, MaxResources: limits}
	runtime := pr.New(b, c.routes)
	if err := c.Manager.ConfigureRuntime(runtime); err != nil {
		return err
	}
	c.reconciler = reconcile.New(c.Store, c.Manager, runtime, b, c.routes, c.Builder, c.revisions)
	if err := c.reconciler.ReconcileOnce(ctx); err != nil {
		logger.Warnf(ctx, "[PluginController] initial instance recovery: %v", err)
	}
	if err := c.audit.Create(ctx, &types.AuditLog{Action: "plugin.controller_started", Outcome: types.AuditOutcomeSuccess, TargetType: "plugin_controller", TargetID: cfg.DeploymentID, CreatedAt: time.Now()}); err != nil {
		return err
	}
	runCtx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	c.done = make(chan struct{})
	go func() {
		defer close(c.done)
		if err := c.reconciler.Run(runCtx, 10*time.Second); err != nil && !errors.Is(err, context.Canceled) {
			logger.Errorf(runCtx, "[PluginController] reconcile stopped: %v", err)
		}
	}()
	return nil
}

func (c *Controller) StartWorker(server *asynq.Server, mux *asynq.ServeMux) error {
	if !c.Config.Enabled {
		return nil
	}
	if c.reconciler == nil {
		return errors.New("plugin Controller not initialized")
	}
	if err := server.Start(mux); err != nil {
		return err
	}
	c.worker = server
	return nil
}

func (c *Controller) Close() error {
	c.gate.Lock()
	c.closing = true
	for _, cancel := range c.syncs {
		cancel()
	}
	c.gate.Unlock()
	if c.cancel != nil {
		c.cancel()
	}
	// The common scheduler may retain builtin entries until its normal cleanup;
	// closing already fences every external enqueue and event acceptance.
	if c.worker != nil {
		c.worker.Stop()
		c.worker.Shutdown()
	}
	if c.done != nil {
		<-c.done
	}
	if c.backend == nil {
		if c.lock != nil {
			err := c.lock.Close()
			c.lock = nil
			return err
		}
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	bindings, err := c.Store.ListBindings(ctx)
	if c.reconciler != nil {
		for _, binding := range bindings {
			err = errors.Join(err, c.Manager.StopExternal(ctx, binding.InstallationID, binding.DataSourceID, 3*time.Second))
		}
	}
	err = errors.Join(err, c.backend.Close())
	if c.lock != nil {
		err = errors.Join(err, c.lock.Close())
		c.lock = nil
	}
	return err
}

// The identity is stamped by Backend, never the plugin; ownership is looked up
// in Host records (including soft-deleted sources during final audit draining).
func (c *Controller) RecordNetwork(ctx context.Context, event network.AuditEvent) error {
	if event.Identity.DeploymentID != c.Config.DeploymentID || event.Identity.InstanceID == "" || !event.Denied {
		return errors.New("invalid trusted plugin audit identity")
	}
	var ds types.DataSource
	if err := c.db.WithContext(ctx).Unscoped().First(&ds, "id = ?", event.Identity.DataSourceID).Error; err != nil {
		return err
	}
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return c.audit.Create(ctx, &types.AuditLog{TenantID: ds.TenantID, Action: "plugin.network_denied", ScopeType: "data_source", ScopeID: ds.ID, TargetType: "plugin_instance", TargetID: event.Identity.InstanceID, Outcome: types.AuditOutcomeDenied, Details: types.JSON(data), CreatedAt: event.ObservedAt})
}
