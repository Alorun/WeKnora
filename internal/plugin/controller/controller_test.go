package controller

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/config"
	"github.com/Tencent/WeKnora/internal/plugin/control"
	pluginDS "github.com/Tencent/WeKnora/internal/plugin/datasource"
	"github.com/Tencent/WeKnora/internal/plugin/sandbox/docker/network"
	"github.com/Tencent/WeKnora/internal/plugin/store"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type testHandle struct{}

func (testHandle) InstanceID() string { return "instance-1" }

func controllerFixture(t *testing.T) (*Controller, *gorm.DB) {
	t.Helper()
	db, err := gorm.Open(sqlite.Open("file:"+t.Name()+"?mode=memory&cache=shared"), &gorm.Config{})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)
	t.Cleanup(func() { sqlDB.Close() })
	require.NoError(t, db.AutoMigrate(&types.DataSource{}, &types.KnowledgeBase{}, &control.PluginInstallation{}, &control.DataSourcePluginBinding{}, &control.DirectoryGrant{}, &types.AuditLog{}))
	require.NoError(t, db.Create(&types.KnowledgeBase{ID: "kb-1", TenantID: 7}).Error)
	require.NoError(t, db.Create(&types.DataSource{ID: "ds-1", TenantID: 7, KnowledgeBaseID: "kb-1", Type: "external", Status: types.DataSourceStatusActive, Config: types.JSON(`{"settings":{}}`)}).Error)
	require.NoError(t, db.Create(&control.PluginInstallation{ID: "i-1", PluginID: "test.directory", Version: "1.0.0", Enabled: true, Active: true, InstallStatus: control.InstallStatusInstalled}).Error)
	require.NoError(t, db.Create(&control.DataSourcePluginBinding{DataSourceID: "ds-1", InstallationID: "i-1", Generation: 1}).Error)
	require.NoError(t, db.Create(&control.DirectoryGrant{ID: "g-1", TenantID: 7, DataSourceID: "ds-1", Status: control.GrantStatusActive, Generation: 1}).Error)
	c := &Controller{Config: config.ExternalPluginsConfig{Enabled: true, DeploymentID: "deployment-1"}, Store: store.New(db), db: db,
		dsRepo: repository.NewDataSourceRepository(db), audit: repository.NewAuditLogRepository(db), routes: pluginDS.NewResolver(), syncs: map[string]context.CancelFunc{}}
	require.NoError(t, c.routes.Publish("ds-1", 1, testHandle{}))
	return c, db
}

func TestDisabledControllerDoesNotRequireInfrastructure(t *testing.T) {
	c := &Controller{}
	require.NoError(t, c.Start(context.Background(), ""))
	require.NoError(t, c.StartWorker(nil, nil))
	require.NoError(t, c.Close())
	c.Config.Enabled = true
	require.Error(t, c.Start(context.Background(), ""), "explicit enable must not silently fall back")
}

func TestRevocationSerializesDurableAcceptanceAndRejectsOldTasks(t *testing.T) {
	c, db := controllerFixture(t)
	ctx := context.Background()
	run, done, err := c.BeginSync(ctx, "ds-1", 1)
	require.NoError(t, err)
	defer done()
	entered, release, revoked := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		require.NoError(t, c.WithSync(run, "ds-1", func() error { close(entered); <-release; return nil }))
	}()
	<-entered
	go func() {
		c.gate.Lock()
		err := c.Store.SetDesired(ctx, "ds-1", false, "g-1")
		c.syncs["ds-1"]()
		c.gate.Unlock()
		revoked <- err
	}()
	select {
	case <-revoked:
		t.Fatal("revocation raced ahead of durable acceptance")
	case <-time.After(25 * time.Millisecond):
	}
	close(release)
	wg.Wait()
	require.NoError(t, <-revoked)
	require.Error(t, c.WithSync(run, "ds-1", func() error { t.Fatal("revoked event accepted"); return nil }))
	_, _, err = c.BeginSync(ctx, "ds-1", 1)
	require.Error(t, err)
	require.Error(t, c.Store.SetDesired(ctx, "ds-1", true, ""), "revoked source cannot be re-enabled")
	var ds types.DataSource
	require.NoError(t, db.First(&ds, "id = ?", "ds-1").Error)
	require.Equal(t, types.DataSourceStatusPaused, ds.Status)
}

type failingAudit struct{ interfaces.AuditLogRepository }

func (failingAudit) Create(context.Context, *types.AuditLog) error {
	return errors.New("audit disk unavailable")
}

func TestTrustedAuditIsDurableTenantScopedAndFailurePropagates(t *testing.T) {
	c, _ := controllerFixture(t)
	ctx := context.Background()
	event := network.AuditEvent{Denied: true, ObservedAt: time.Now(), Protocol: "tcp", DestinationIP: "127.0.0.1", DestinationPort: 9,
		Identity: network.Identity{DeploymentID: "deployment-1", InstanceID: "instance-1", PluginID: "test.directory", DataSourceID: "ds-1", Generation: 1}}
	require.NoError(t, c.RecordNetwork(ctx, event))
	rows, err := c.audit.List(ctx, 7, &interfaces.AuditLogQuery{ScopeType: "data_source", ScopeID: "ds-1"})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Contains(t, string(rows[0].Details), `"instance_id":"instance-1"`)
	rows, err = c.audit.List(ctx, 8, &interfaces.AuditLogQuery{})
	require.NoError(t, err)
	require.Empty(t, rows)
	c.audit = failingAudit{}
	require.ErrorContains(t, c.RecordNetwork(ctx, event), "audit disk unavailable")
	event.Identity.DeploymentID = "another"
	require.ErrorContains(t, c.RecordNetwork(ctx, event), "invalid trusted")
}

func TestScopeAndGrantAuthorizationAreNotUserControlled(t *testing.T) {
	c, _ := controllerFixture(t)
	ctx := context.Background()
	_, err := c.Grant(ctx, &types.DataSource{ID: "ds-1", TenantID: 7}, "root", "source")
	require.ErrorContains(t, err, "system administrator")
	_, err = c.UpdateDataSource(ctx, &types.DataSource{ID: "ds-1", Type: "other"})
	require.ErrorContains(t, err, "immutable")
	_, err = c.UpdateDataSource(ctx, &types.DataSource{ID: "ds-1", Config: types.JSON(`{"settings":{"path":"/etc"}}`)})
	require.ErrorContains(t, err, "scope/config is fixed")
	_, err = c.UpdateDataSource(ctx, &types.DataSource{ID: "ds-1", SyncSchedule: "invalid"})
	require.Error(t, err)
}
