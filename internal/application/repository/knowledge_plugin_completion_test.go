package repository

import (
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/plugin/control"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestPluginWorkerUpdateCannotResurrectOrPublishKnowledge(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&types.Knowledge{}, &types.DataSource{}, &control.DataSourceRevision{}))
	ds := types.DataSource{ID: "ds", TenantID: 1}
	require.NoError(t, db.Create(&ds).Error)
	k := types.Knowledge{ID: "k", TenantID: 1, Metadata: types.JSON(`{"plugin_revision_state":"pending"}`), EnableStatus: "disabled"}
	require.NoError(t, db.Create(&k).Error)
	require.NoError(t, db.Create(&control.DataSourceRevision{DataSourceID: "ds", ExternalID: "file", Revision: "r1", KnowledgeID: &k.ID, State: control.RevisionPending}).Error)
	repo := NewKnowledgeRepository(db)
	ctx := context.Background()
	k.EnableStatus = "enabled"
	k.ParseStatus = types.ParseStatusCompleted
	require.NoError(t, repo.UpdateKnowledge(ctx, &k))
	var current types.Knowledge
	require.NoError(t, db.First(&current, "id = ?", "k").Error)
	require.Equal(t, "disabled", current.EnableStatus)
	require.NoError(t, db.Model(&control.DataSourceRevision{}).Where("knowledge_id = ?", "k").Update("state", control.RevisionSuperseded).Error)
	require.NoError(t, db.Unscoped().Delete(&k).Error)
	require.NoError(t, repo.UpdateKnowledge(ctx, &k))
	var count int64
	require.NoError(t, db.Unscoped().Model(&types.Knowledge{}).Where("id = ?", "k").Count(&count).Error)
	require.Zero(t, count)
}
