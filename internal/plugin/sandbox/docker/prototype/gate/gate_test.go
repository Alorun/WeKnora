package gate

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"
)

func TestWaitFailsClosedOnTimeout(t *testing.T) {
	config := Config{
		ReleasePath: fmt.Sprintf("/run/weknora/gate-test-%d-%d", os.Getpid(), time.Now().UnixNano()),
		PluginPath:  "/opt/weknora/artifact/fake-prototype",
		SocketPath:  "/run/weknora/plugin.sock",
		ResultPath:  "/run/weknora/result.json",
		StartedPath: "/run/weknora/plugin.started",
		Timeout:     time.Second,
	}
	started := time.Now()
	err := Wait(context.Background(), config)
	if !errors.Is(err, ErrReleaseTimeout) {
		t.Fatalf("expected timeout, got %v", err)
	}
	if time.Since(started) < time.Second {
		t.Fatal("gate returned before fail-closed timeout")
	}
}
