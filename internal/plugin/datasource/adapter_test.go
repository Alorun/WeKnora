package datasource

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"testing"
	"time"

	core "github.com/Tencent/WeKnora/internal/datasource"
	"github.com/Tencent/WeKnora/internal/types"
	pluginv1 "github.com/Tencent/WeKnora/pkg/plugin/proto/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"
)

type adapterControl struct {
	pluginv1.UnimplementedPluginControlServer
}

func (adapterControl) ValidateConfig(context.Context, *pluginv1.ValidateConfigRequest) (*pluginv1.ValidateConfigResponse, error) {
	return &pluginv1.ValidateConfigResponse{}, nil
}
func (adapterControl) Shutdown(context.Context, *pluginv1.ShutdownRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

type adapterDataSource struct {
	pluginv1.UnimplementedDataSourcePluginServer
}

func (adapterDataSource) ListResources(context.Context, *pluginv1.ListResourcesRequest) (*pluginv1.ListResourcesResponse, error) {
	return &pluginv1.ListResourcesResponse{Resources: []*pluginv1.Resource{{ExternalId: "grant_root", Name: "authorized", Type: "directory"}}}, nil
}
func (adapterDataSource) ResolveAncestors(context.Context, *pluginv1.ResolveAncestorsRequest) (*pluginv1.ResolveAncestorsResponse, error) {
	return &pluginv1.ResolveAncestorsResponse{AncestorIds: []string{"parent"}}, nil
}
func (adapterDataSource) Sync(_ *pluginv1.SyncRequest, stream grpc.ServerStreamingServer[pluginv1.SyncEvent]) error {
	if err := stream.Send(&pluginv1.SyncEvent{Event: &pluginv1.SyncEvent_Upsert{Upsert: &pluginv1.DocumentUpsert{
		ExternalId: "a.md", Revision: "sha256:a", Title: "A", FileName: "a.md", Content: []byte("body"),
	}}}); err != nil {
		return err
	}
	if err := stream.Send(&pluginv1.SyncEvent{Event: &pluginv1.SyncEvent_Delete{Delete: &pluginv1.DocumentDelete{
		ExternalId: "removed.md", Title: "Removed",
	}}}); err != nil {
		return err
	}
	return stream.Send(&pluginv1.SyncEvent{Event: &pluginv1.SyncEvent_Checkpoint{Checkpoint: &pluginv1.Checkpoint{
		CursorJson: []byte(`{"connector_cursor":{"page":1}}`),
	}}})
}

type itemErrorDataSource struct {
	adapterDataSource
}

type forceFullDataSource struct{ adapterDataSource }

func (forceFullDataSource) Sync(req *pluginv1.SyncRequest, stream grpc.ServerStreamingServer[pluginv1.SyncEvent]) error {
	var cursor types.SyncCursor
	if !req.ForceFull || json.Unmarshal(req.CursorJson, &cursor) != nil || cursor.ConnectorCursor["round"] != float64(7) {
		return errors.New("force_full lost the opaque revision round")
	}
	return nil
}

func TestExternalForceFullRetainsOpaqueCursor(t *testing.T) {
	_, adapter := adapterFixtureWithDataSource(t, forceFullDataSource{})
	ctx := core.WithExternalForceFull(context.Background(), true)
	_, err := adapter.FetchStream(ctx, &types.DataSourceConfig{}, &types.SyncCursor{ConnectorCursor: map[string]any{"round": 7}}, &recordingStreamHandler{})
	require.NoError(t, err)
}

func (itemErrorDataSource) Sync(_ *pluginv1.SyncRequest, stream grpc.ServerStreamingServer[pluginv1.SyncEvent]) error {
	if err := stream.Send(&pluginv1.SyncEvent{Event: &pluginv1.SyncEvent_ItemError{ItemError: &pluginv1.ItemError{
		ExternalId: "bad.md", Code: "source_read_failed", SafeMessage: "source item could not be read",
	}}}); err != nil {
		return err
	}
	return stream.Send(&pluginv1.SyncEvent{Event: &pluginv1.SyncEvent_Checkpoint{Checkpoint: &pluginv1.Checkpoint{
		CursorJson: []byte(`{"connector_cursor":{"page":2}}`),
	}}})
}

type rpcTestHandle struct {
	id      string
	control pluginv1.PluginControlClient
	ds      pluginv1.DataSourcePluginClient
}

func (h *rpcTestHandle) InstanceID() string                                { return h.id }
func (h *rpcTestHandle) ControlClient() pluginv1.PluginControlClient       { return h.control }
func (h *rpcTestHandle) DataSourceClient() pluginv1.DataSourcePluginClient { return h.ds }

func adapterFixture(t *testing.T) (*Resolver, *GRPCConnectorAdapter) {
	return adapterFixtureWithDataSource(t, adapterDataSource{})
}

func adapterFixtureWithDataSource(t *testing.T, dataSource pluginv1.DataSourcePluginServer) (*Resolver, *GRPCConnectorAdapter) {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	pluginv1.RegisterPluginControlServer(server, adapterControl{})
	pluginv1.RegisterDataSourcePluginServer(server, dataSource)
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("passthrough:///buf", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	resolver := NewResolver()
	require.NoError(t, resolver.Publish("ds-1", 3, &rpcTestHandle{id: "instance", control: pluginv1.NewPluginControlClient(conn), ds: pluginv1.NewDataSourcePluginClient(conn)}))
	return resolver, NewGRPCConnectorAdapter("external", "ds-1", 3, resolver)
}

type recordingStreamHandler struct {
	items       []types.FetchedItem
	checkpoints int
}

func (h *recordingStreamHandler) Emit(_ context.Context, item types.FetchedItem) error {
	h.items = append(h.items, item)
	return nil
}
func (h *recordingStreamHandler) Checkpoint(context.Context, *types.SyncCursor) error {
	h.checkpoints++
	return nil
}

func TestAdapterMapsListResolveAndSync(t *testing.T) {
	_, adapter := adapterFixture(t)
	resources, err := adapter.ListResources(context.Background(), &types.DataSourceConfig{}, "")
	require.NoError(t, err)
	require.Equal(t, "grant_root", resources[0].ExternalID)
	ancestors, err := adapter.ResolveResourceAncestors(context.Background(), &types.DataSourceConfig{}, []string{"leaf"})
	require.NoError(t, err)
	require.Equal(t, []string{"parent"}, ancestors)
	handler := &recordingStreamHandler{}
	cursor, err := adapter.FetchStream(context.Background(), &types.DataSourceConfig{}, nil, handler)
	require.NoError(t, err)
	require.Equal(t, "sha256:a", handler.items[0].Revision)
	require.True(t, handler.items[1].IsDeleted)
	require.Equal(t, 1, handler.checkpoints)
	require.EqualValues(t, 1, cursor.ConnectorCursor["page"])
}

func TestAdapterTreatsItemErrorAsDirtyAndIgnoresLaterCheckpoint(t *testing.T) {
	_, adapter := adapterFixtureWithDataSource(t, itemErrorDataSource{})
	handler := &recordingStreamHandler{}
	_, err := adapter.FetchStream(context.Background(), &types.DataSourceConfig{}, nil, handler)
	require.ErrorContains(t, err, "source_read_failed")
	require.Zero(t, handler.checkpoints)
}

func TestUnpublishInstanceRejectsNewCallsWhileDrainingLease(t *testing.T) {
	resolver, _ := adapterFixture(t)
	lease, err := resolver.Acquire("ds-1")
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		drained, err := resolver.UnpublishInstance("ds-1", 3, "instance")
		if err != nil {
			done <- err
			return
		}
		select {
		case <-drained:
			done <- nil
		case <-ctx.Done():
			done <- ctx.Err()
		}
	}()
	require.Eventually(t, func() bool {
		probe, err := resolver.Acquire("ds-1")
		if probe != nil {
			probe.Release()
		}
		return errors.Is(err, ErrHandleNotFound)
	}, time.Second, 10*time.Millisecond)
	lease.Release()
	require.NoError(t, <-done)
}
