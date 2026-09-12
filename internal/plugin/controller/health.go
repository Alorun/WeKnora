package controller

import (
	"context"

	"github.com/Tencent/WeKnora/internal/plugin/manager"
	pr "github.com/Tencent/WeKnora/internal/plugin/runtime"
)

// A manual health query updates the same observed instance state as recovery;
// it never changes the user's enable intent or reports a package-wide READY.
func (c *Controller) Health(ctx context.Context, id string) (pr.HealthResult, error) {
	if c.reconciler == nil {
		return pr.HealthResult{}, manager.ErrRuntimeNotAvailable
	}
	return c.reconciler.Health(ctx, id)
}
