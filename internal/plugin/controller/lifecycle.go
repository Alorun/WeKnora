package controller

import (
	"context"
	"errors"
	"reflect"
	"time"

	"github.com/Tencent/WeKnora/internal/datasource"
	"github.com/Tencent/WeKnora/internal/plugin/control"
	"github.com/Tencent/WeKnora/internal/plugin/manager"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/robfig/cron/v3"
)

type syncKey struct{}
type syncIdentity struct {
	id         string
	generation uint64
}

func (c *Controller) active(ctx context.Context, id string) (*control.DataSourcePluginBinding, error) {
	if !c.Config.Enabled || c.closing {
		return nil, manager.ErrRuntimeNotAvailable
	}
	b, err := c.Store.GetBinding(ctx, id)
	if err != nil {
		return nil, err
	}
	ds, err := c.dsRepo.FindByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if ds.Status != types.DataSourceStatusActive && ds.Status != types.DataSourceStatusError {
		return nil, errors.New("external data source is paused")
	}
	g, err := c.Store.GetActiveDirectoryGrant(ctx, id)
	if err != nil {
		return nil, errors.New("active directory grant required")
	}
	if g.RevokedAt != nil || g.TenantID != ds.TenantID {
		return nil, errors.New("directory grant unavailable")
	}
	i, err := c.Store.GetInstallation(ctx, b.InstallationID)
	if err != nil {
		return nil, err
	}
	if !i.Active || !i.Enabled || i.InstallStatus != control.InstallStatusInstalled {
		return nil, errors.New("installation unavailable")
	}
	h, err := c.routes.Resolve(id)
	if err != nil {
		return nil, err
	}
	if h.Generation != b.Generation {
		return nil, errors.New("instance generation is outdated")
	}
	return b, nil
}

func (c *Controller) Generation(ctx context.Context, id string) (uint64, error) {
	c.gate.RLock()
	defer c.gate.RUnlock()
	b, err := c.active(ctx, id)
	if err != nil {
		return 0, err
	}
	return b.Generation, nil
}

func (c *Controller) BeginSync(ctx context.Context, id string, generation uint64) (context.Context, func(), error) {
	c.gate.Lock()
	defer c.gate.Unlock()
	b, err := c.active(ctx, id)
	if err != nil {
		return ctx, nil, err
	}
	if generation == 0 || generation != b.Generation {
		return ctx, nil, errors.New("queued plugin generation is stale")
	}
	if _, exists := c.syncs[id]; exists {
		return ctx, nil, errors.New("plugin synchronization already active")
	}
	runCtx, cancel := context.WithCancel(context.WithValue(ctx, syncKey{}, syncIdentity{id, generation}))
	c.syncs[id] = cancel
	return runCtx, func() { cancel(); c.gate.Lock(); delete(c.syncs, id); c.gate.Unlock() }, nil
}

// Linearization point shared with desired-state changes: a revoked generation
// cannot accept an event or save Cursor after the revocation transaction.
func (c *Controller) WithSync(ctx context.Context, id string, fn func() error) error {
	c.gate.RLock()
	defer c.gate.RUnlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	identity, ok := ctx.Value(syncKey{}).(syncIdentity)
	if !ok || identity.id != id {
		return errors.New("plugin sync identity missing")
	}
	b, err := c.active(ctx, id)
	if err != nil {
		return err
	}
	if identity.generation != b.Generation {
		return errors.New("plugin sync generation is stale")
	}
	return fn()
}

func (c *Controller) AcceptExternalItem(ctx context.Context, ds *types.DataSource, item types.FetchedItem, tags []string) (result datasource.ExternalIngestResult, err error) {
	err = c.WithSync(ctx, ds.ID, func() error {
		var err error
		result, err = c.revisions.AcceptExternalItem(ctx, ds, item, tags)
		return err
	})
	return
}

func (c *Controller) SetEnabled(ctx context.Context, id string, enabled bool) error {
	return c.setDesired(ctx, id, enabled, "")
}

func (c *Controller) setDesired(ctx context.Context, id string, enabled bool, revoke string) error {
	if c.reconciler == nil {
		return manager.ErrRuntimeNotAvailable
	}
	return c.reconciler.WithLock(func() error {
		c.gate.Lock()
		if c.closing {
			c.gate.Unlock()
			return errors.New("Controller is closing")
		}
		err := c.Store.SetDesired(ctx, id, enabled, revoke)
		if err == nil && !enabled {
			if cancel := c.syncs[id]; cancel != nil {
				cancel()
			}
		}
		c.gate.Unlock()
		if err != nil {
			return err
		}
		b, err := c.Store.GetBinding(ctx, id)
		if err != nil {
			return err
		}
		observe := func(state control.PluginState, sandbox string, cause error) error {
			persistCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			return errors.Join(cause, c.Store.UpdateBindingObserved(persistCtx, id, b.Generation, state, sandbox, errorText(cause)))
		}
		if !enabled {
			c.scheduler.Remove(id)
			err = c.Manager.StopExternal(ctx, b.InstallationID, id, 3*time.Second)
			// A prior failed cleanup may have dropped the in-memory Handle.
			if b.SandboxID != "" {
				err = errors.Join(err, c.backend.Stop(ctx, b.SandboxID, 3*time.Second))
			}
			state, sandbox := control.StateStopped, ""
			if err != nil {
				state = control.StateFailed
				sandbox = b.SandboxID
			}
			return observe(state, sandbox, err)
		}
		i, err := c.Store.GetInstallation(ctx, b.InstallationID)
		if err != nil {
			return err
		}
		spec, err := c.Builder.BuildInstanceSpec(ctx, *i, *b)
		if err != nil {
			return observe(control.StateNotReady, b.SandboxID, err)
		}
		h, err := c.Manager.StartExternal(ctx, i.ID, spec)
		if err != nil {
			return observe(control.StateNotReady, b.SandboxID, err)
		}
		if err := c.Store.UpdateBindingObserved(ctx, id, b.Generation, control.StateReady, h.InstanceID(), ""); err != nil {
			return err
		}
		ds, err := c.dsRepo.FindByID(ctx, id)
		if err != nil {
			return err
		}
		return c.scheduler.AddOrUpdate(ds)
	})
}

func errorText(err error) string {
	if err != nil {
		return err.Error()
	}
	return ""
}

func (c *Controller) UpdateDataSource(ctx context.Context, next *types.DataSource) (*types.DataSource, error) {
	c.gate.Lock()
	defer c.gate.Unlock()
	old, err := c.dsRepo.FindByID(ctx, next.ID)
	if err != nil {
		return nil, err
	}
	if next.SyncSchedule != "" {
		if _, err := cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow).Parse(next.SyncSchedule); err != nil {
			return nil, err
		}
	}
	if next.Type != "" && next.Type != old.Type || next.Status != "" && next.Status != old.Status || next.SyncMode != "" && next.SyncMode != old.SyncMode {
		return nil, errors.New("plugin scope/type/status is immutable here; use lifecycle API or create a new data source")
	}
	if len(next.Config) > 0 {
		a, ea := old.ParseConfig()
		b, eb := next.ParseConfig()
		if ea != nil || eb != nil || !reflect.DeepEqual(a, b) {
			return nil, errors.New("plugin sync scope/config is fixed; create a new data source")
		}
	}
	if next.Name != "" {
		old.Name = next.Name
	}
	old.SyncSchedule = next.SyncSchedule
	if err := c.Store.UpdateDataSourcePresentation(ctx, old); err != nil {
		return nil, err
	}
	return old, c.scheduler.AddOrUpdate(old)
}
