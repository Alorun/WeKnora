// This program runs only inside a WeKnora-managed plugin instance.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	pb "github.com/Tencent/WeKnora/pkg/plugin/proto/v1"
	"github.com/Tencent/WeKnora/pkg/plugin/sdk"
)

func identity(nonce []byte) sdk.Identity {
	// Change these together with plugin.yaml when copying the template.
	return sdk.Identity{PluginID: "example.hello", PluginVersion: "0.1.0", ExtensionID: "hello",
		ExtensionType: pb.ExtensionType_EXTENSION_TYPE_DATASOURCE, StartupNonce: nonce,
		SupportedCapabilities: []string{"resource_listing", "full_sync", "incremental_sync"}}
}

func run() error {
	const bootstrap = "/run/weknora/bootstrap.json"
	if len(os.Args) != 1 || os.Getenv("WEKNORA_PLUGIN_BOOTSTRAP") != bootstrap {
		return errors.New("WeKnora managed bootstrap required")
	}
	f, err := os.Open(bootstrap)
	if err != nil {
		return err
	}
	data, err := io.ReadAll(io.LimitReader(f, 4097))
	f.Close()
	var boot struct{ StartupNonce []byte }
	if err != nil || len(data) > 4096 || json.Unmarshal(data, &boot) != nil || len(boot.StartupNonce) == 0 || len(boot.StartupNonce) > 256 {
		return errors.New("invalid managed bootstrap")
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	control, err := sdk.NewControlServer(identity(boot.StartupNonce), sdk.ControlHooks{
		ValidateConfig: func(_ context.Context, data []byte) error { return validateConfig(data) },
		Health: func(ctx context.Context) (pb.HealthStatus, error) {
			return pb.HealthStatus_HEALTH_STATUS_READY, ctx.Err()
		},
		Shutdown: func(context.Context, time.Duration) error {
			// Allow the response to leave the RPC before shutting down its server.
			time.AfterFunc(20*time.Millisecond, cancel)
			return nil
		},
	})
	if err != nil {
		return err
	}
	server, err := sdk.ServeUDS(ctx, "/run/weknora/plugin.sock", sdk.Services{Control: control, DataSource: &source{}})
	if err != nil {
		return err
	}
	return <-server.Done()
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
