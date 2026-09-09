package controller

import (
	"context"
	"encoding/json"
	"time"

	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/hibiken/asynq"
)

// ObserveDocumentCompletion runs AFTER the existing worker (and its dead-letter
// callback). Only the existing ledger may promote a completed Knowledge. A
// failed promotion remains pending for the existing recovery loop.
func (c *Controller) ObserveDocumentCompletion() asynq.MiddlewareFunc {
	return func(next asynq.Handler) asynq.Handler {
		return asynq.HandlerFunc(func(ctx context.Context, task *asynq.Task) error {
			err := next.ProcessTask(ctx, task)
			if !c.Config.Enabled {
				return err
			}
			var payload struct {
				KnowledgeID string `json:"knowledge_id"`
			}
			if json.Unmarshal(task.Payload(), &payload) != nil || payload.KnowledgeID == "" {
				return err
			}
			drain, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if updateErr := c.revisions.ReconcilePending(drain, 100); updateErr != nil {
				logger.Warnf(ctx, "[PluginController] revision completion pending recovery: %v", updateErr)
			}
			return err
		})
	}
}
