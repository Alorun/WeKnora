// Package contracttest provides the small reusable V1 conformance entry point
// for external Go plugins. It deliberately exercises the public UDS protocol
// only and has no dependency on WeKnora internal packages.
package contracttest

import (
	"bytes"
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	pluginv1 "github.com/Tencent/WeKnora/pkg/plugin/proto/v1"
	"github.com/Tencent/WeKnora/pkg/plugin/sdk"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Options struct {
	Identity             sdk.Identity
	Services             sdk.Services
	ConfigJSON           []byte
	RequiredCapabilities []string
	VerifyDataSource     func(context.Context, pluginv1.DataSourcePluginClient) error
}

// Run verifies the fixed control handshake and optionally lets the plugin's
// own test assert its datasource behavior through the generated client.
func Run(t testing.TB, options Options) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	socket := filepath.Join(t.TempDir(), "plugin.sock")
	server, err := sdk.ServeUDS(ctx, socket, options.Services)
	if err != nil {
		t.Fatalf("serve plugin contract UDS: %v", err)
	}
	defer server.Stop()

	callCtx, callCancel := context.WithTimeout(ctx, 5*time.Second)
	defer callCancel()
	conn, err := grpc.NewClient("passthrough:///plugin-contract",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(sdk.MaxMessageBytes),
			grpc.MaxCallSendMsgSize(sdk.MaxMessageBytes),
		),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", socket)
		}),
	)
	if err != nil {
		t.Fatalf("dial plugin contract UDS: %v", err)
	}
	defer conn.Close()

	protocolVersion := options.Identity.ProtocolVersion
	if protocolVersion == "" {
		protocolVersion = sdk.ProtocolVersion
	}
	contractVersion := options.Identity.ContractVersion
	if contractVersion == "" {
		contractVersion = sdk.DataSourceContractVersion
	}
	control := pluginv1.NewPluginControlClient(conn)
	handshake, err := control.Handshake(callCtx, &pluginv1.HandshakeRequest{
		PluginId: options.Identity.PluginID, PluginVersion: options.Identity.PluginVersion,
		ExtensionId: options.Identity.ExtensionID, ExtensionType: options.Identity.ExtensionType,
		ProtocolVersion: protocolVersion, ContractVersion: contractVersion,
		StartupNonce: options.Identity.StartupNonce, RequiredCapabilities: options.RequiredCapabilities,
	})
	if err != nil {
		t.Fatalf("plugin handshake RPC: %v", err)
	}
	if pluginErr := handshake.GetError(); pluginErr != nil {
		t.Fatalf("plugin handshake: %s: %s", pluginErr.GetCode(), pluginErr.GetSafeMessage())
	}
	if handshake.GetPluginId() != options.Identity.PluginID || handshake.GetPluginVersion() != options.Identity.PluginVersion ||
		handshake.GetExtensionId() != options.Identity.ExtensionID || handshake.GetExtensionType() != options.Identity.ExtensionType {
		t.Fatal("plugin handshake returned a different identity")
	}
	if !bytes.Equal(handshake.GetStartupNonce(), options.Identity.StartupNonce) {
		t.Fatal("plugin handshake returned a different startup nonce")
	}
	if !sdk.CompatibleVersion(protocolVersion, handshake.GetProtocolVersion()) ||
		!sdk.CompatibleVersion(contractVersion, handshake.GetContractVersion()) {
		t.Fatal("plugin handshake returned incompatible protocol or contract versions")
	}
	supported := make(map[string]struct{}, len(handshake.GetSupportedCapabilities()))
	for _, capability := range handshake.GetSupportedCapabilities() {
		supported[capability] = struct{}{}
	}
	for _, required := range options.RequiredCapabilities {
		if _, ok := supported[required]; !ok {
			t.Fatalf("plugin handshake omitted required capability %q", required)
		}
	}
	validated, err := control.ValidateConfig(callCtx, &pluginv1.ValidateConfigRequest{ConfigJson: options.ConfigJSON})
	if err != nil {
		t.Fatalf("validate plugin config RPC: %v", err)
	}
	if pluginErr := validated.GetError(); pluginErr != nil {
		t.Fatalf("validate plugin config: %s: %s", pluginErr.GetCode(), pluginErr.GetSafeMessage())
	}
	health, err := control.Health(callCtx, &pluginv1.HealthRequest{})
	if err != nil {
		t.Fatalf("plugin health RPC: %v", err)
	}
	if health.GetStatus() != pluginv1.HealthStatus_HEALTH_STATUS_READY {
		t.Fatalf("plugin health = %s, want READY", health.GetStatus())
	}
	if options.VerifyDataSource != nil {
		if err := options.VerifyDataSource(callCtx, pluginv1.NewDataSourcePluginClient(conn)); err != nil {
			t.Fatalf("datasource contract: %v", err)
		}
	}
}
