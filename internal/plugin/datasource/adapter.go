package datasource

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	core "github.com/Tencent/WeKnora/internal/datasource"
	"github.com/Tencent/WeKnora/internal/plugin/control"
	pluginstore "github.com/Tencent/WeKnora/internal/plugin/store"
	"github.com/Tencent/WeKnora/internal/types"
	pluginv1 "github.com/Tencent/WeKnora/pkg/plugin/proto/v1"
	pluginsdk "github.com/Tencent/WeKnora/pkg/plugin/sdk"
)

var _ core.StreamingConnector = (*GRPCConnectorAdapter)(nil)
var _ core.ExternalConnector = (*GRPCConnectorAdapter)(nil)

type BindingStore interface {
	GetBinding(context.Context, string) (*control.DataSourcePluginBinding, error)
}

// ConnectorRouter implements the Binding-first rule without teaching the
// legacy Registry about installations, gRPC, or runtime lifecycle.
type ConnectorRouter struct {
	bindings BindingStore
	resolver *Resolver
}

func NewConnectorRouter(bindings BindingStore, resolver *Resolver) *ConnectorRouter {
	return &ConnectorRouter{bindings: bindings, resolver: resolver}
}

func (r *ConnectorRouter) HasExternalBinding(ctx context.Context, dataSourceID string) (bool, error) {
	if r == nil || r.bindings == nil {
		return false, nil
	}
	_, err := r.bindings.GetBinding(ctx, dataSourceID)
	if errors.Is(err, pluginstore.ErrNotFound) {
		return false, nil
	}
	return err == nil, err
}

func (r *ConnectorRouter) ResolveExternal(ctx context.Context, ds *types.DataSource) (core.Connector, bool, error) {
	if r == nil || r.bindings == nil {
		return nil, false, nil
	}
	binding, err := r.bindings.GetBinding(ctx, ds.ID)
	if errors.Is(err, pluginstore.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, true, err
	}
	if binding.Generation == 0 {
		return nil, true, fmt.Errorf("external datasource binding has no generation")
	}
	return NewGRPCConnectorAdapter(ds.Type, ds.ID, binding.Generation, r.resolver), true, nil
}

type GRPCConnectorAdapter struct {
	connectorType string
	dataSourceID  string
	generation    uint64
	resolver      *Resolver
	callTimeout   time.Duration
	syncTimeout   time.Duration
}

func NewGRPCConnectorAdapter(connectorType, dataSourceID string, generation uint64, resolver *Resolver) *GRPCConnectorAdapter {
	return &GRPCConnectorAdapter{connectorType: connectorType, dataSourceID: dataSourceID, generation: generation,
		resolver: resolver, callTimeout: 30 * time.Second, syncTimeout: 2 * time.Hour}
}

func (a *GRPCConnectorAdapter) Type() string     { return a.connectorType }
func (a *GRPCConnectorAdapter) IsExternal() bool { return true }

func (a *GRPCConnectorAdapter) Validate(ctx context.Context, config *types.DataSourceConfig) error {
	configJSON, err := marshalConfig(config)
	if err != nil {
		return err
	}
	lease, err := a.acquire()
	if err != nil {
		return err
	}
	defer lease.Release()
	rpc := lease.Handle.(RPCHandle)
	callCtx, cancel := context.WithTimeout(ctx, a.callTimeout)
	defer cancel()
	response, err := rpc.ControlClient().ValidateConfig(callCtx, &pluginv1.ValidateConfigRequest{ConfigJson: configJSON})
	if err != nil {
		return err
	}
	return protocolError(response.GetError())
}

func (a *GRPCConnectorAdapter) ListResources(ctx context.Context, config *types.DataSourceConfig, parentID string) ([]types.Resource, error) {
	configJSON, err := marshalConfig(config)
	if err != nil {
		return nil, err
	}
	lease, err := a.acquire()
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	rpc := lease.Handle.(RPCHandle)
	callCtx, cancel := context.WithTimeout(ctx, a.callTimeout)
	defer cancel()
	response, err := rpc.DataSourceClient().ListResources(callCtx, &pluginv1.ListResourcesRequest{ConfigJson: configJSON, ParentId: parentID})
	if err != nil {
		return nil, err
	}
	if err := protocolError(response.GetError()); err != nil {
		return nil, err
	}
	result := make([]types.Resource, 0, len(response.GetResources()))
	for _, resource := range response.GetResources() {
		converted, err := resourceFromProto(resource)
		if err != nil {
			return nil, err
		}
		result = append(result, converted)
	}
	return result, nil
}

func (a *GRPCConnectorAdapter) ResolveResourceAncestors(ctx context.Context, config *types.DataSourceConfig, resourceIDs []string) ([]string, error) {
	configJSON, err := marshalConfig(config)
	if err != nil {
		return nil, err
	}
	lease, err := a.acquire()
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	rpc := lease.Handle.(RPCHandle)
	callCtx, cancel := context.WithTimeout(ctx, a.callTimeout)
	defer cancel()
	response, err := rpc.DataSourceClient().ResolveAncestors(callCtx, &pluginv1.ResolveAncestorsRequest{
		ConfigJson: configJSON, ResourceIds: append([]string(nil), resourceIDs...),
	})
	if err != nil {
		return nil, err
	}
	if err := protocolError(response.GetError()); err != nil {
		return nil, err
	}
	return append([]string(nil), response.GetAncestorIds()...), nil
}

func (a *GRPCConnectorAdapter) FetchAll(ctx context.Context, config *types.DataSourceConfig, resourceIDs []string) ([]types.FetchedItem, error) {
	// The legacy full-fetch contract carries the selected resources as a
	// separate argument, while the streaming contract carries them in
	// DataSourceConfig. Preserve the caller's argument in the existing JSON
	// configuration shape instead of adding a second wire representation.
	if config == nil {
		config = &types.DataSourceConfig{}
	} else {
		cloned := *config
		config = &cloned
	}
	config.ResourceIDs = append([]string(nil), resourceIDs...)
	collector := &collectHandler{}
	_, err := a.FetchStream(ctx, config, nil, collector)
	return collector.items, err
}

func (a *GRPCConnectorAdapter) FetchIncremental(ctx context.Context, config *types.DataSourceConfig, cursor *types.SyncCursor) ([]types.FetchedItem, *types.SyncCursor, error) {
	collector := &collectHandler{}
	next, err := a.FetchStream(ctx, config, cursor, collector)
	return collector.items, next, err
}

func (a *GRPCConnectorAdapter) FetchStream(ctx context.Context, config *types.DataSourceConfig, cursor *types.SyncCursor, handler core.StreamHandler) (*types.SyncCursor, error) {
	if handler == nil {
		return nil, fmt.Errorf("stream handler is required")
	}
	configJSON, err := marshalConfig(config)
	if err != nil {
		return nil, err
	}
	var cursorJSON []byte
	if cursor != nil {
		cursorJSON, err = json.Marshal(cursor)
		if err != nil {
			return nil, err
		}
		if len(cursorJSON) > pluginsdk.MaxCursorBytes {
			return nil, fmt.Errorf("%s: cursor exceeds %d bytes", pluginsdk.ErrorMessageTooLarge, pluginsdk.MaxCursorBytes)
		}
	}
	lease, err := a.acquire()
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	rpc := lease.Handle.(RPCHandle)
	callCtx, cancel := context.WithTimeout(ctx, a.syncTimeout)
	defer cancel()
	stream, err := rpc.DataSourceClient().Sync(callCtx, &pluginv1.SyncRequest{
		ConfigJson: configJSON, CursorJson: cursorJSON, ForceFull: cursor == nil,
		MultimodalEnabled: config != nil && config.MultimodalEnabled,
	})
	if err != nil {
		return nil, err
	}
	var lastCheckpoint *types.SyncCursor
	for {
		event, recvErr := stream.Recv()
		if errors.Is(recvErr, context.Canceled) || errors.Is(recvErr, context.DeadlineExceeded) {
			return nil, recvErr
		}
		if recvErr != nil {
			if errors.Is(recvErr, io.EOF) {
				return lastCheckpoint, nil
			}
			return nil, fmt.Errorf("plugin sync stream: %w", recvErr)
		}
		switch payload := event.GetEvent().(type) {
		case *pluginv1.SyncEvent_Upsert:
			item, convertErr := upsertFromProto(payload.Upsert)
			if convertErr != nil {
				return nil, convertErr
			}
			if err := handler.Emit(callCtx, item); err != nil {
				return nil, err
			}
		case *pluginv1.SyncEvent_Delete:
			if payload.Delete.GetExternalId() == "" {
				return nil, fmt.Errorf("delete external_id is required")
			}
			if err := handler.Emit(callCtx, types.FetchedItem{ExternalID: payload.Delete.GetExternalId(), Title: payload.Delete.GetTitle(), IsDeleted: true}); err != nil {
				return nil, err
			}
		case *pluginv1.SyncEvent_ItemError:
			return nil, fmt.Errorf("%s: %s: %s", pluginsdk.ErrorPluginStreamItemFailed, payload.ItemError.GetCode(), payload.ItemError.GetSafeMessage())
		case *pluginv1.SyncEvent_Checkpoint:
			checkpoint, convertErr := cursorFromJSON(payload.Checkpoint.GetCursorJson())
			if convertErr != nil {
				return nil, convertErr
			}
			if err := handler.Checkpoint(callCtx, checkpoint); err != nil {
				return nil, err
			}
			lastCheckpoint = checkpoint
		default:
			return nil, fmt.Errorf("sync event payload is required")
		}
	}
}

func (a *GRPCConnectorAdapter) acquire() (*Lease, error) {
	if a.resolver == nil {
		return nil, ErrHandleNotFound
	}
	lease, err := a.resolver.Acquire(a.dataSourceID)
	if err != nil {
		return nil, err
	}
	if lease.Generation != a.generation {
		lease.Release()
		return nil, fmt.Errorf("data source %s generation changed from %d to %d: %w", a.dataSourceID, a.generation, lease.Generation, ErrStaleGeneration)
	}
	if _, ok := lease.Handle.(RPCHandle); !ok {
		lease.Release()
		return nil, fmt.Errorf("external handle %s has no gRPC clients", lease.Handle.InstanceID())
	}
	return lease, nil
}

type collectHandler struct{ items []types.FetchedItem }

func (h *collectHandler) Emit(_ context.Context, item types.FetchedItem) error {
	h.items = append(h.items, item)
	return nil
}
func (h *collectHandler) Checkpoint(context.Context, *types.SyncCursor) error { return nil }

func marshalConfig(config *types.DataSourceConfig) ([]byte, error) {
	data, err := json.Marshal(config)
	if err != nil {
		return nil, err
	}
	if len(data) > pluginsdk.MaxConfigBytes {
		return nil, fmt.Errorf("%s: configuration exceeds %d bytes", pluginsdk.ErrorMessageTooLarge, pluginsdk.MaxConfigBytes)
	}
	return data, nil
}

func upsertFromProto(upsert *pluginv1.DocumentUpsert) (types.FetchedItem, error) {
	if upsert == nil || upsert.GetExternalId() == "" || upsert.GetRevision() == "" {
		return types.FetchedItem{}, fmt.Errorf("upsert external_id and revision are required")
	}
	if len(upsert.GetContent()) > pluginsdk.MaxDocumentBytes {
		return types.FetchedItem{}, fmt.Errorf("%s: document exceeds %d bytes", pluginsdk.ErrorMessageTooLarge, pluginsdk.MaxDocumentBytes)
	}
	item := types.FetchedItem{
		ExternalID: upsert.GetExternalId(), Revision: upsert.GetRevision(), Title: upsert.GetTitle(),
		Content: append([]byte(nil), upsert.GetContent()...), ContentType: upsert.GetContentType(),
		FileName: upsert.GetFileName(), URL: upsert.GetUrl(), Metadata: cloneStringMap(upsert.GetMetadata()),
		SourceResourceID: upsert.GetSourceResourceId(), ReplacesSubtree: upsert.GetReplacesSubtree(),
		SubtreeKeep: append([]string(nil), upsert.GetSubtreeKeep()...),
	}
	if upsert.GetUpdatedAt() != nil {
		if err := upsert.GetUpdatedAt().CheckValid(); err != nil {
			return types.FetchedItem{}, fmt.Errorf("invalid updated_at: %w", err)
		}
		item.UpdatedAt = upsert.GetUpdatedAt().AsTime()
	}
	return item, nil
}

func resourceFromProto(resource *pluginv1.Resource) (types.Resource, error) {
	if resource == nil || resource.GetExternalId() == "" {
		return types.Resource{}, fmt.Errorf("resource external_id is required")
	}
	var metadata map[string]interface{}
	if len(resource.GetMetadataJson()) != 0 {
		if len(resource.GetMetadataJson()) > pluginsdk.MaxConfigBytes {
			return types.Resource{}, fmt.Errorf("resource metadata exceeds size limit")
		}
		if err := json.Unmarshal(resource.GetMetadataJson(), &metadata); err != nil {
			return types.Resource{}, fmt.Errorf("invalid resource metadata: %w", err)
		}
	}
	result := types.Resource{ExternalID: resource.GetExternalId(), Name: resource.GetName(), Type: resource.GetType(),
		Description: resource.GetDescription(), URL: resource.GetUrl(), ParentID: resource.GetParentId(),
		HasChildren: resource.GetHasChildren(), Metadata: metadata}
	if resource.GetModifiedAt() != nil {
		if err := resource.GetModifiedAt().CheckValid(); err != nil {
			return types.Resource{}, err
		}
		result.ModifiedAt = resource.GetModifiedAt().AsTime()
	}
	return result, nil
}

func cursorFromJSON(data []byte) (*types.SyncCursor, error) {
	if len(data) == 0 {
		return nil, nil
	}
	if len(data) > pluginsdk.MaxCursorBytes {
		return nil, fmt.Errorf("%s: cursor exceeds %d bytes", pluginsdk.ErrorMessageTooLarge, pluginsdk.MaxCursorBytes)
	}
	var cursor types.SyncCursor
	if err := json.Unmarshal(data, &cursor); err != nil {
		return nil, fmt.Errorf("invalid checkpoint cursor: %w", err)
	}
	return &cursor, nil
}

func protocolError(value *pluginv1.PluginError) error {
	if value == nil || value.GetCode() == "" {
		return nil
	}
	return fmt.Errorf("%s: %s", value.GetCode(), value.GetSafeMessage())
}

func cloneStringMap(source map[string]string) map[string]string {
	if source == nil {
		return nil
	}
	result := make(map[string]string, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}
