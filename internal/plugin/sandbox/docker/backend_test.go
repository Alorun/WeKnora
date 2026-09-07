package docker

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/plugin/sandbox/docker/paths"
	"github.com/stretchr/testify/require"
)

func TestDeploymentLockAndMetadata(t *testing.T) {
	root := t.TempDir()
	b := &Backend{config: Config{DeploymentID: "this-deployment", RuntimeRoot: paths.PathMapping{AppRoot: root, HostRoot: root}}}
	require.NoError(t, os.Mkdir(b.recordsRoot(), 0700))
	var inside atomic.Int32
	var wg sync.WaitGroup
	for n := 0; n < 8; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			unlock, err := b.lock(ctx)
			if err != nil {
				t.Error(err)
				return
			}
			defer unlock()
			if inside.Add(1) != 1 {
				t.Error("concurrent reservation")
			}
			time.Sleep(time.Millisecond)
			inside.Add(-1)
		}()
	}
	wg.Wait()
	labels := map[string]string{"managed": "true", "workload_kind": "plugin", "deployment_id": "this-deployment", "plugin_id": "test.plugin", "data_source_id": "ds", "generation": "1"}
	i, err := b.instance(strings.Repeat("a", 64), labels)
	require.NoError(t, err)
	require.NoError(t, b.saveRecord(i))
	record, err := b.readRecord(i.ID)
	require.NoError(t, err)
	require.Equal(t, i, record)
	for _, k := range []string{"managed", "workload_kind", "deployment_id", "generation", "data_source_id"} {
		old := labels[k]
		labels[k] = "../../foreign"
		_, err = b.instance(i.ID, labels)
		require.Error(t, err)
		labels[k] = old
	}
	require.Error(t, b.unmountRuntime(root))
	require.Error(t, b.unmountRuntime(filepath.Join(root, "..", "other")))
}

func TestDiagnosticsBound(t *testing.T) {
	require.LessOrEqual(t, len(TrimDiagnostics([]byte(strings.Repeat("x", DiagnosticBytes*4)))), DiagnosticBytes)
}
