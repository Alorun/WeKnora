package sdk_test

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	pluginv1 "github.com/Tencent/WeKnora/pkg/plugin/proto/v1"
	"github.com/Tencent/WeKnora/pkg/plugin/sdk"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type minimalDataSource struct {
	pluginv1.UnimplementedDataSourcePluginServer
}

func (minimalDataSource) ListResources(context.Context, *pluginv1.ListResourcesRequest) (*pluginv1.ListResourcesResponse, error) {
	return &pluginv1.ListResourcesResponse{Resources: []*pluginv1.Resource{{ExternalId: "grant_root", Name: "authorized"}}}, nil
}

func TestMinimalPluginServesOverUDS(t *testing.T) {
	control, err := sdk.NewControlServer(sdk.Identity{
		PluginID: "test.local", PluginVersion: "1.0.0", ExtensionID: "local",
		ExtensionType: pluginv1.ExtensionType_EXTENSION_TYPE_DATASOURCE, StartupNonce: []byte("nonce"),
	}, sdk.ControlHooks{})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	socket := filepath.Join(t.TempDir(), "plugin.sock")
	server, err := sdk.ServeUDS(ctx, socket, sdk.Services{Control: control, DataSource: minimalDataSource{}})
	require.NoError(t, err)
	defer server.Stop()

	conn, err := grpc.NewClient("passthrough:///plugin", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", socket)
		}))
	require.NoError(t, err)
	defer conn.Close()
	callCtx, callCancel := context.WithTimeout(context.Background(), time.Second)
	defer callCancel()
	controlClient := pluginv1.NewPluginControlClient(conn)
	handshake, err := controlClient.Handshake(callCtx, &pluginv1.HandshakeRequest{
		PluginId: "test.local", PluginVersion: "1.0.0", ExtensionId: "local",
		ExtensionType:   pluginv1.ExtensionType_EXTENSION_TYPE_DATASOURCE,
		ProtocolVersion: sdk.ProtocolVersion, ContractVersion: sdk.DataSourceContractVersion,
		StartupNonce: []byte("nonce"),
	})
	require.NoError(t, err)
	require.Nil(t, handshake.GetError())
	wrongNonce, err := controlClient.Handshake(callCtx, &pluginv1.HandshakeRequest{
		PluginId: "test.local", PluginVersion: "1.0.0", ExtensionId: "local",
		ExtensionType:   pluginv1.ExtensionType_EXTENSION_TYPE_DATASOURCE,
		ProtocolVersion: sdk.ProtocolVersion, ContractVersion: sdk.DataSourceContractVersion,
		StartupNonce: []byte("stale"),
	})
	require.NoError(t, err)
	require.Equal(t, sdk.ErrorNonceMismatch, wrongNonce.GetError().GetCode())
	response, err := pluginv1.NewDataSourcePluginClient(conn).ListResources(callCtx, &pluginv1.ListResourcesRequest{})
	require.NoError(t, err)
	require.Equal(t, "grant_root", response.GetResources()[0].GetExternalId())
}

func TestVersionCompatibility(t *testing.T) {
	require.True(t, sdk.CompatibleVersion("1.0", "1.2"))
	require.False(t, sdk.CompatibleVersion("1.2", "1.1"))
	require.False(t, sdk.CompatibleVersion("1.0", "2.0"))
}
