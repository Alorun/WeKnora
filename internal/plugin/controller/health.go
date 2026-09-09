package controller

import (
	"context"
	"errors"
	"time"

	"github.com/Tencent/WeKnora/internal/plugin/control"
	"github.com/Tencent/WeKnora/internal/plugin/manager"
	pr "github.com/Tencent/WeKnora/internal/plugin/runtime"
	pb "github.com/Tencent/WeKnora/pkg/plugin/proto/v1"
)

// A manual health query updates the same observed instance state as recovery;
// it never changes the user's enable intent or reports a package-wide READY.
func (c *Controller) Health(ctx context.Context, id string) (pr.HealthResult, error) {
	if c.reconciler == nil {
		return pr.HealthResult{}, manager.ErrRuntimeNotAvailable
	}
	var health pr.HealthResult
	err := c.reconciler.WithLock(func() error {
		b, err := c.Store.GetBinding(ctx, id)
		if err != nil {
			return err
		}
		health, err = c.Manager.HealthExternal(ctx, id)
		if err == nil && health.Status == pb.HealthStatus_HEALTH_STATUS_READY {
			return c.Store.UpdateBindingObserved(ctx, id, b.Generation, control.StateReady, b.SandboxID, "")
		}
		if err == nil {
			err = errors.New("plugin health is not READY")
		}
		c.gate.Lock()
		if cancel := c.syncs[id]; cancel != nil {
			cancel()
		}
		c.gate.Unlock()
		stopErr := c.Manager.StopExternal(ctx, b.InstallationID, id, 3*time.Second)
		locator := ""
		if stopErr != nil {
			locator = b.SandboxID
		}
		cause := errors.Join(err, stopErr)
		return errors.Join(cause, c.Store.UpdateBindingObserved(ctx, id, b.Generation, control.StateNotReady, locator, cause.Error()))
	})
	return health, err
}
