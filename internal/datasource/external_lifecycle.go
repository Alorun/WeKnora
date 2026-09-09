package datasource

import (
	"context"
	"github.com/Tencent/WeKnora/internal/types"
)

type externalForceFullKey struct{}

// WithExternalForceFull preserves opaque plugin Cursor history while requesting
// a full replay. Builtin connectors retain their existing nil-cursor convention.
func WithExternalForceFull(ctx context.Context, force bool) context.Context {
	return context.WithValue(ctx, externalForceFullKey{}, force)
}
func ExternalForceFull(ctx context.Context) bool {
	v, _ := ctx.Value(externalForceFullKey{}).(bool)
	return v
}

// ExternalLifecycle is the production orchestration seam. Builtin Connector
// interfaces and their sync semantics remain unchanged.
type ExternalLifecycle interface {
	Generation(context.Context, string) (uint64, error)
	BeginSync(context.Context, string, uint64) (context.Context, func(), error)
	WithSync(context.Context, string, func() error) error
	SetEnabled(context.Context, string, bool) error
	UpdateDataSource(context.Context, *types.DataSource) (*types.DataSource, error)
}
