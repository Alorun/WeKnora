package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Tencent/WeKnora/internal/plugin/sandbox/docker/prototype/gate"
)

func main() {
	config := gate.Config{}
	flag.StringVar(&config.ReleasePath, "release", "", "fixed release file")
	flag.StringVar(&config.PluginPath, "plugin", "", "fixed plugin executable")
	flag.StringVar(&config.SocketPath, "socket", "", "fixed plugin UDS")
	flag.StringVar(&config.ResultPath, "result", "", "bounded result file")
	flag.StringVar(&config.StartedPath, "started", "", "plugin-start marker")
	flag.DurationVar(&config.Timeout, "timeout", 30*time.Second, "fail-closed release timeout")
	flag.Parse()
	if flag.NArg() != 0 {
		exit("unexpected positional arguments", 64)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	if err := gate.Wait(ctx, config); err != nil {
		if err == gate.ErrReleaseTimeout {
			exit(err.Error(), 78)
		}
		if ctx.Err() != nil {
			exit(ctx.Err().Error(), 143)
		}
		exit(err.Error(), 70)
	}
	argv := gate.PluginArgv(config)
	if err := syscall.Exec(config.PluginPath, argv, []string{}); err != nil {
		exit(fmt.Sprintf("exec fake plugin: %v", err), 71)
	}
}

func exit(message string, code int) {
	_ = json.NewEncoder(os.Stderr).Encode(map[string]any{
		"component": "gate-runner", "error": message, "exit_code": code,
	})
	os.Exit(code)
}
