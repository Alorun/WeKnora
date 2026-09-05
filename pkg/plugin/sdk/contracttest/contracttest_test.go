package contracttest_test

import (
	"context"
	"fmt"
	"testing"

	pluginv1 "github.com/Tencent/WeKnora/pkg/plugin/proto/v1"
	"github.com/Tencent/WeKnora/pkg/plugin/sdk"
	"github.com/Tencent/WeKnora/pkg/plugin/sdk/contracttest"
)

type dataSource struct {
	pluginv1.UnimplementedDataSourcePluginServer
}

func (dataSource) ListResources(context.Context, *pluginv1.ListResourcesRequest) (*pluginv1.ListResourcesResponse, error) {
	return &pluginv1.ListResourcesResponse{Resources: []*pluginv1.Resource{{ExternalId: "grant_root"}}}, nil
}

func TestRun(t *testing.T) {
	identity := sdk.Identity{
		PluginID: "test.local", PluginVersion: "1.0.0", ExtensionID: "local",
		ExtensionType: pluginv1.ExtensionType_EXTENSION_TYPE_DATASOURCE,
		StartupNonce:  []byte("nonce"), SupportedCapabilities: []string{"full_sync"},
	}
	control, err := sdk.NewControlServer(identity, sdk.ControlHooks{})
	if err != nil {
		t.Fatal(err)
	}
	contracttest.Run(t, contracttest.Options{
		Identity: identity, Services: sdk.Services{Control: control, DataSource: dataSource{}},
		ConfigJSON: []byte(`{"root":"grant_root"}`), RequiredCapabilities: []string{"full_sync"},
		VerifyDataSource: func(ctx context.Context, client pluginv1.DataSourcePluginClient) error {
			resources, err := client.ListResources(ctx, &pluginv1.ListResourcesRequest{})
			if err != nil {
				return err
			}
			if len(resources.GetResources()) != 1 || resources.GetResources()[0].GetExternalId() != "grant_root" {
				return fmt.Errorf("unexpected resources: %v", resources.GetResources())
			}
			return nil
		},
	})
}
