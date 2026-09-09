package container

import (
	"github.com/Tencent/WeKnora/internal/config"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

// Register before lifecycle resources: reverse cleanup leaves durable audit/DB
// and queue connections available until Controller, workers and timers stop.
func registerPluginInfrastructureCleanup(cfg *config.Config, db *gorm.DB, redis *redis.Client, cleaner interfaces.ResourceCleaner) error {
	if !cfg.ExternalPlugins.Enabled {
		return nil
	}
	sqlDB, err := db.DB()
	if err != nil {
		return err
	}
	cleaner.RegisterWithName("PluginSharedDatabase", sqlDB.Close)
	if redis != nil {
		cleaner.RegisterWithName("PluginSharedRedis", redis.Close)
	}
	return nil
}

func registerPluginEnqueuerCleanup(cfg *config.Config, queue interfaces.TaskEnqueuer, cleaner interfaces.ResourceCleaner) {
	if cfg.ExternalPlugins.Enabled {
		if client, ok := queue.(*asynq.Client); ok {
			cleaner.RegisterWithName("PluginTaskEnqueuer", client.Close)
		}
	}
}
