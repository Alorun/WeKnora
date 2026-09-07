//go:build integration

package docker

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/plugin/control"
	pluginds "github.com/Tencent/WeKnora/internal/plugin/datasource"
	"github.com/Tencent/WeKnora/internal/plugin/prepare"
	pr "github.com/Tencent/WeKnora/internal/plugin/runtime"
	"github.com/Tencent/WeKnora/internal/plugin/sandbox/docker/network"
	"github.com/Tencent/WeKnora/internal/plugin/sandbox/docker/paths"
	pb "github.com/Tencent/WeKnora/pkg/plugin/proto/v1"
	"github.com/cilium/ebpf"
	"github.com/google/uuid"
	"github.com/moby/moby/client"
	"github.com/stretchr/testify/require"
)

//go:embed testdata/plugin.yaml
var probeManifest []byte

func integrationConfig(t *testing.T) Config {
	t.Helper()
	app, host := os.Getenv("C1_APP_ROOT"), os.Getenv("C1_HOST_ROOT")
	if app == "" || host == "" {
		t.Skip("UNVERIFIED: use bash scripts/test-plugin-backend.sh for real Docker/BPF")
	}
	for _, p := range []string{"runtime", "snapshots", "allow", "packages"} {
		require.NoError(t, os.MkdirAll(filepath.Join(app, p), 0755))
	}
	uid, err := strconv.ParseUint(os.Getenv("C1_ADMIN_UID"), 10, 32)
	require.NoError(t, err)
	mapping := func(p string) paths.PathMapping {
		return paths.PathMapping{AppRoot: filepath.Join(app, p), HostRoot: filepath.Join(host, p)}
	}
	return Config{DeploymentID: os.Getenv("C1_DEPLOYMENT"), Image: os.Getenv("C1_IMAGE"), GateAppPath: filepath.Join(app, "gate"), GateHostPath: filepath.Join(host, "gate"),
		RuntimeRoot: mapping("runtime"), ArtifactRoot: mapping("snapshots"), GrantRoots: []paths.PathMapping{mapping("allow")}, AdminUID: uint32(uid), PluginUID: 65532, PluginGID: 65532, MaxInstances: 2,
		Audit: func(_ context.Context, e network.AuditEvent) error { t.Logf("HOST AUDIT %+v", e); return nil }}
}

func integrationSpec(t *testing.T, c Config) pr.InstanceSpec {
	t.Helper()
	app := os.Getenv("C1_APP_ROOT")
	source := filepath.Join(app, "packages", "probe")
	require.NoError(t, os.MkdirAll(source, 0755))
	bin, err := os.ReadFile(filepath.Join(app, "probe"))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(source, "plugin"), bin, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(source, "plugin.yaml"), probeManifest, 0644))
	d := prepare.Discovery{Packages: paths.PathMapping{AppRoot: filepath.Join(app, "packages"), HostRoot: filepath.Join(os.Getenv("C1_HOST_ROOT"), "packages")}, Snapshots: c.ArtifactRoot, AdminUID: c.AdminUID, PluginUID: c.PluginUID, Options: control.ManifestValidationOptions{WeKnoraVersion: "0.7.3"}}
	p, err := d.Load(source)
	require.NoError(t, err)
	grantPath := filepath.Join(c.GrantRoots[0].AppRoot, "source")
	require.NoError(t, os.MkdirAll(grantPath, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(grantPath, "allowed.txt"), []byte("allowed"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(c.GrantRoots[0].AppRoot, "outside-sentinel.txt"), []byte("outside"), 0644))
	g, err := paths.CreateGrant(c.GrantRoots[0].AppRoot, "source", 1)
	require.NoError(t, err)
	hostGrant, err := c.GrantRoots[0].HostPath(grantPath)
	require.NoError(t, err)
	ds := uuid.NewString()
	runtimeApp := filepath.Join(c.RuntimeRoot.AppRoot, ds, "1")
	runtimeHost, err := c.RuntimeRoot.HostPath(runtimeApp)
	require.NoError(t, err)
	return pr.InstanceSpec{PluginID: string(p.Manifest.Metadata.ID), PluginVersion: p.Manifest.Metadata.Version, ExtensionID: string(p.Manifest.Spec.Extension.ID), DataSourceID: ds, Generation: 1,
		ProtocolVersion: "1.0", ContractVersion: "1.0", StartupNonce: []byte(uuid.NewString()), RequiredCapabilities: []string{"full_sync"}, ConfigJSON: []byte(`{}`), Artifact: p.Artifact,
		Grant: &pr.DirectoryGrantReference{ID: uuid.NewString(), Generation: 1, AppPath: grantPath, HostPath: hostGrant, Device: g.Device, Inode: g.Inode}, RuntimeAppPath: runtimeApp, RuntimeHostPath: runtimeHost,
		Permissions: pr.EffectivePermissions{Network: "none", Filesystem: "selected_directory_readonly"}, Resources: pr.ResourceLimits{MemoryBytes: 256 << 20, CPUQuota: .5, PidsLimit: 32}}
}

func syncProbe(ctx context.Context, h pr.RuntimeHandle, mode string) (map[string]any, error) {
	s, err := h.DataSourceClient().Sync(ctx, &pb.SyncRequest{ConfigJson: []byte(fmt.Sprintf(`{"mode":%q}`, mode))})
	if err != nil {
		return nil, err
	}
	e, err := s.Recv()
	if err != nil {
		return nil, err
	}
	var result map[string]any
	err = json.Unmarshal(e.GetUpsert().GetContent(), &result)
	if err != nil {
		return nil, err
	}
	_, err = s.Recv()
	if err != io.EOF {
		return nil, fmt.Errorf("unexpected stream trailer: %v", err)
	}
	return result, nil
}

func TestDockerPluginRuntime(t *testing.T) {
	c := integrationConfig(t)
	var mu sync.Mutex
	var events []network.AuditEvent
	c.Audit = func(_ context.Context, e network.AuditEvent) error {
		mu.Lock()
		defer mu.Unlock()
		if len(events) >= 16 {
			return fmt.Errorf("test audit cap")
		}
		events = append(events, e)
		t.Logf("HOST AUDIT %+v", e)
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	b, err := New(ctx, c)
	require.NoError(t, err)
	defer func() { require.NoError(t, b.Close()) }()
	routes := pluginds.NewResolver()
	r := pr.New(b, routes)
	spec := integrationSpec(t, c)
	h, err := r.Start(ctx, spec)
	require.NoError(t, err)
	t.Logf("REAL READY sandbox=%s app=%s host=%s", h.InstanceID(), spec.RuntimeAppPath, spec.RuntimeHostPath)
	fileInfo, err := os.Stat(spec.RuntimeAppPath)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0700), fileInfo.Mode().Perm())
	marker, err := os.Stat(filepath.Join(spec.RuntimeAppPath, "probe.started"))
	require.NoError(t, err)
	release, err := os.Stat(filepath.Join(spec.RuntimeAppPath, "release"))
	require.NoError(t, err)
	require.False(t, marker.ModTime().Before(release.ModTime()), "plugin ran before gate release")
	for _, mode := range []string{"isolation", "network", "space", "pid", "logs", "cpu"} {
		t.Run(mode, func(t *testing.T) {
			cg, err := network.FindDockerCgroup("/sys/fs/cgroup", h.InstanceID())
			require.NoError(t, err)
			before, _ := os.ReadFile(filepath.Join(cg, "cpu.stat"))
			result, err := syncProbe(ctx, h, mode)
			require.NoError(t, err)
			t.Logf("%s: %+v", mode, result)
			switch mode {
			case "isolation":
				require.Equal(t, "allowed", result["read"])
				for _, name := range []string{"grant", "artifact", "root"} {
					require.Contains(t, result[name], "read-only file system")
				}
				require.Contains(t, result["outside"], "no such file")
			case "network":
				for _, n := range []string{"tcp4", "tcp6", "udp4", "udp6"} {
					require.Contains(t, result[n], "operation not permitted")
				}
				require.Eventually(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(events) == 4 }, 3*time.Second, 20*time.Millisecond)
				mu.Lock()
				for _, e := range events {
					require.True(t, e.Denied)
					require.Equal(t, spec.PluginID, e.Identity.PluginID)
					require.Equal(t, spec.DataSourceID, e.Identity.DataSourceID)
					require.Equal(t, spec.Generation, e.Identity.Generation)
				}
				mu.Unlock()
			case "space":
				for _, p := range []string{"/tmp/fill", "/run/weknora/fill"} {
					require.Contains(t, result[p], "no space left")
				}
			case "pid":
				require.Contains(t, result["pid_error"], "resource temporarily unavailable")
				t.Log("PID limit counts OS threads/tasks, not goroutines")
			case "logs":
				state, err := b.client.ContainerInspect(ctx, h.InstanceID(), client.ContainerInspectOptions{})
				require.NoError(t, err)
				require.Equal(t, LogMaxSize, state.Container.HostConfig.LogConfig.Config["max-size"])
				require.LessOrEqual(t, len(b.diagnostics(ctx, h.InstanceID())), DiagnosticBytes)
				var files []string
				require.Eventually(t, func() bool {
					var err error
					files, err = filepath.Glob(filepath.Join("/docker-logs", h.InstanceID(), h.InstanceID()+"-json.log*"))
					return err == nil && len(files) == 2
				}, 3*time.Second, 20*time.Millisecond, "wait for Docker logging pipe to flush and rotate")
				for _, path := range files {
					info, err := os.Stat(path)
					require.NoError(t, err)
					require.LessOrEqual(t, info.Size(), int64(1<<20)+4096)
					t.Logf("rotated log %s bytes=%d", filepath.Base(path), info.Size())
				}
			case "cpu":
				after, err := os.ReadFile(filepath.Join(cg, "cpu.stat"))
				require.NoError(t, err)
				quota, err := os.ReadFile(filepath.Join(cg, "cpu.max"))
				require.NoError(t, err)
				t.Logf("quota=%s before=%s after=%s", quota, before, after)
				require.Greater(t, counter(after, "nr_throttled"), counter(before, "nr_throttled"), "condition insufficient: CPU throttling not observed")
			}
		})
	}
	require.NoError(t, r.Stop(ctx, h, time.Second))
	require.NoError(t, b.Stop(ctx, h.InstanceID(), 0))
	assertClean(t, b, spec, h.InstanceID())
	for n := 0; n < 2; n++ {
		// Recovery reuses the same generation and directory but has a new ID.
		s := spec
		s.StartupNonce = []byte(uuid.NewString())
		handle, err := r.Start(ctx, s)
		require.NoError(t, err)
		require.NoError(t, b.Stop(ctx, h.InstanceID(), 0), "stale cleanup must not unmount the replacement")
		require.NoError(t, r.Stop(ctx, h, 0), "stale runtime stop must not unpublish the replacement")
		current, err := routes.Resolve(s.DataSourceID)
		require.NoError(t, err)
		require.Equal(t, handle.InstanceID(), current.Handle.InstanceID())
		_, err = syncProbe(ctx, handle, "")
		require.NoError(t, err)
		require.NoError(t, r.Stop(ctx, handle, time.Second))
		assertClean(t, b, s, handle.InstanceID())
		h = handle
	}
	list, err := b.List(ctx, nil)
	require.NoError(t, err)
	require.Empty(t, list)
	// A damaged cleanup locator must remain visible, even after Docker removal.
	recordPath := filepath.Join(b.recordsRoot(), h.InstanceID()+".json")
	require.NoError(t, os.WriteFile(recordPath, []byte("incomplete"), 0600))
	require.ErrorContains(t, b.Stop(ctx, h.InstanceID(), 0), "cleanup locator")
	require.FileExists(t, recordPath)
	require.NoError(t, os.Remove(recordPath)) // only this test's injected locator
	require.NoError(t, b.Stop(ctx, h.InstanceID(), 0))
}

func counter(data []byte, key string) uint64 {
	for _, line := range strings.Split(string(data), "\n") {
		v := strings.Fields(line)
		if len(v) == 2 && v[0] == key {
			n, _ := strconv.ParseUint(v[1], 10, 64)
			return n
		}
	}
	return 0
}
func assertClean(t *testing.T, b *Backend, s pr.InstanceSpec, id string) {
	t.Helper()
	_, err := os.Stat(s.RuntimeAppPath)
	require.True(t, os.IsNotExist(err), "UDS mount/directory remains: %v", err)
	_, err = os.Stat(b.pinRoot(id))
	require.True(t, os.IsNotExist(err), "BPF pins remain: %v", err)
	_, err = b.client.ContainerInspect(context.Background(), id, client.ContainerInspectOptions{})
	require.Error(t, err)
	t.Logf("CLEAN container=%s UDS=%s pins=%s", id, s.RuntimeAppPath, b.pinRoot(id))
}

func TestDockerPluginCleanup(t *testing.T) {
	c := integrationConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	b, err := New(ctx, c)
	require.NoError(t, err)
	require.NoError(t, b.Close())
	list, err := os.ReadDir(filepath.Join(c.RuntimeRoot.AppRoot, ".instances"))
	require.NoError(t, err)
	for _, e := range list {
		require.False(t, strings.HasSuffix(e.Name(), ".json"), "cleanup locator remains")
	}
	if os.Getenv("C1_OPERATION") == "cleanup" {
		// These four directories were created by this test in its unique root;
		// all container mounts/pins have already been checked and removed.
		for _, p := range []string{"runtime", "snapshots", "allow", "packages"} {
			require.NoError(t, os.RemoveAll(filepath.Join(os.Getenv("C1_APP_ROOT"), p)))
		}
	}
}

func TestDockerPluginFailures(t *testing.T) {
	c := integrationConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	b, err := New(ctx, c)
	require.NoError(t, err)
	defer func() { require.NoError(t, b.Close()) }()
	resolver := pluginds.NewResolver()
	r := pr.New(b, resolver)
	for _, mode := range []string{"handshake", "validation", "cancel", "ignore_shutdown", "memory", "sigkill", "crash"} {
		t.Run(mode, func(t *testing.T) {
			s := integrationSpec(t, c)
			if mode == "handshake" {
				s.PluginVersion = "2.0.0"
			}
			if mode == "validation" {
				s.ConfigJSON = []byte(`{"fail_validation":true}`)
			}
			if mode == "cancel" {
				s.ConfigJSON = []byte(`{"slow_validation":true}`)
			}
			if mode == "memory" {
				s.Resources.MemoryBytes = 64 << 20
			}
			startCtx, cancelStart := context.WithTimeout(ctx, 10*time.Second)
			defer cancelStart()
			if mode == "cancel" {
				go func() {
					for startCtx.Err() == nil {
						if _, err := os.Stat(filepath.Join(s.RuntimeAppPath, "validation.started")); err == nil {
							cancelStart()
							return
						}
						time.Sleep(10 * time.Millisecond)
					}
				}()
			}
			h, err := r.Start(startCtx, s)
			if mode == "handshake" || mode == "validation" || mode == "cancel" {
				require.Error(t, err)
				t.Logf("startup failed as required: %v", err)
				_, e := resolver.Resolve(s.DataSourceID)
				require.Error(t, e)
				list, e := b.List(ctx, nil)
				require.NoError(t, e)
				require.Empty(t, list)
				_, e = os.Stat(s.RuntimeAppPath)
				require.True(t, os.IsNotExist(e))
				return
			}
			require.NoError(t, err)
			cg, err := network.FindDockerCgroup("/sys/fs/cgroup", h.InstanceID())
			require.NoError(t, err)
			if mode == "memory" {
				limit, err := os.ReadFile(filepath.Join(cg, "memory.max"))
				require.NoError(t, err)
				swap, err := os.ReadFile(filepath.Join(cg, "memory.swap.max"))
				require.NoError(t, err)
				require.Equal(t, "67108864", strings.TrimSpace(string(limit)))
				require.Equal(t, "0", strings.TrimSpace(string(swap)))
				t.Logf("memory.max=%s swap.max=%s", limit, swap)
			}
			_, syncErr := syncProbe(ctx, h, mode)
			if mode == "ignore_shutdown" {
				require.NoError(t, syncErr)
				require.Error(t, r.Stop(ctx, h, 100*time.Millisecond))
			} else {
				require.Error(t, syncErr)
				require.Eventually(t, func() bool { b.mu.Lock(); _, ok := b.final[h.InstanceID()]; b.mu.Unlock(); return ok }, 10*time.Second, 20*time.Millisecond)
				_ = r.Stop(ctx, h, 0)
			}
			state, err := b.Inspect(ctx, h.InstanceID())
			require.NoError(t, err)
			require.Equal(t, mode == "memory", state.OOMKilled, "137 is not sufficient evidence of OOM")
			if mode == "crash" {
				require.Equal(t, 42, state.ExitCode)
			}
			t.Logf("exit mode=%s exit_code=%d OOMKilled=%v", mode, state.ExitCode, state.OOMKilled)
			require.NoError(t, b.Stop(ctx, h.InstanceID(), 0))
			assertClean(t, b, s, h.InstanceID())
			_, err = resolver.Resolve(s.DataSourceID)
			require.Error(t, err)
		})
	}
}

func TestDockerPluginConcurrentLimitAndRecovery(t *testing.T) {
	c := integrationConfig(t)
	var recoveredAudits atomic.Int32
	c.Audit = func(_ context.Context, e network.AuditEvent) error {
		recoveredAudits.Add(1)
		t.Logf("RECOVERED HOST AUDIT %+v", e)
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	b, err := New(ctx, c)
	require.NoError(t, err)
	defer func() { require.NoError(t, b.Close()) }()
	r := pr.New(b, pluginds.NewResolver())
	anchorSpec := integrationSpec(t, c)
	anchor, err := r.Start(ctx, anchorSpec)
	require.NoError(t, err)
	var wg sync.WaitGroup
	type started struct {
		h   pr.RuntimeHandle
		err error
	}
	out := make(chan started, 2)
	for n := 0; n < 2; n++ {
		spec := integrationSpec(t, c)
		wg.Add(1)
		go func() { defer wg.Done(); h, err := r.Start(ctx, spec); out <- started{h, err} }()
	}
	wg.Wait()
	close(out)
	success, failed := 0, 0
	for item := range out {
		if item.err != nil {
			failed++
			require.Contains(t, item.err.Error(), "instance limit")
		} else {
			success++
			require.NoError(t, r.Stop(ctx, item.h, time.Second))
		}
	}
	require.Equal(t, 1, success)
	require.Equal(t, 1, failed)
	// Start in a different process and let it terminate without Close. Recovery
	// must not rely on this parent's service map or any fake backend.
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run", "^TestDockerPluginRecoveryChild$", "-test.v")
	cmd.Env = append(os.Environ(), "C1_RECOVERY_CHILD=1")
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, string(output))
	t.Log(string(output))
	b2, err := New(ctx, c)
	require.NoError(t, err)
	list, err := b2.List(ctx, nil)
	require.NoError(t, err)
	require.Len(t, list, 2)
	for _, i := range list {
		if i.ID == anchor.InstanceID() {
			continue
		}
		state, err := b2.Inspect(ctx, i.ID)
		require.NoError(t, err)
		require.True(t, state.Running)
		require.NoError(t, b2.Stop(ctx, i.ID, 0))
		require.EqualValues(t, 4, recoveredAudits.Load(), "pending kernel audits must survive Controller replacement")
		require.NoError(t, b2.Stop(ctx, i.ID, 0))
		t.Logf("fresh Backend recovered/cleaned %s", i.ID)
	}
	_, err = syncProbe(ctx, anchor, "")
	require.NoError(t, err, "other instance must remain healthy")
	require.NoError(t, r.Stop(ctx, anchor, time.Second))
	require.NoError(t, b2.Close())
	assertClean(t, b, anchorSpec, anchor.InstanceID())
}

func TestDockerPluginRecoveryChild(t *testing.T) {
	if os.Getenv("C1_RECOVERY_CHILD") != "1" {
		return
	}
	c := integrationConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	b, err := New(ctx, c)
	require.NoError(t, err)
	r := pr.New(b, pluginds.NewResolver())
	h, err := r.Start(ctx, integrationSpec(t, c))
	require.NoError(t, err)
	// Simulate the crash window after the audit consumer has stopped but a
	// final in-flight Sync still issues network attempts. Links/maps stay real.
	b.mu.Lock()
	svc := b.services[h.InstanceID()]
	b.mu.Unlock()
	svc.cancel()
	<-svc.done
	_, err = syncProbe(ctx, h, "network")
	require.NoError(t, err)
	t.Logf("leaving recoverable instance %s", h.InstanceID()) /* deliberately no Close: process exit */
}

func TestDockerPluginPolicyUnavailable(t *testing.T) {
	if os.Getenv("C1_OPERATION") != "no-policy" {
		return
	}
	c := integrationConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	b, err := New(ctx, c)
	require.NoError(t, err)
	defer func() { require.NoError(t, b.Close()) }()
	s := integrationSpec(t, c)
	r := pr.New(b, pluginds.NewResolver())
	_, err = r.Start(ctx, s)
	require.ErrorContains(t, err, "network audit not available")
	t.Logf("FAIL CLOSED: %v", err)
	list, e := b.List(ctx, nil)
	require.NoError(t, e)
	require.Empty(t, list)
	_, e = os.Stat(s.RuntimeAppPath)
	require.True(t, os.IsNotExist(e))
}

func TestDockerPluginAuditFailure(t *testing.T) {
	for _, mode := range []string{"sink", "gap"} {
		t.Run(mode, func(t *testing.T) { testAuditFailure(t, mode) })
	}
}

func testAuditFailure(t *testing.T, mode string) {
	c := integrationConfig(t)
	if mode == "sink" {
		c.Audit = func(context.Context, network.AuditEvent) error { return fmt.Errorf("audit sink unavailable") }
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	b, err := New(ctx, c)
	require.NoError(t, err)
	defer func() { require.NoError(t, b.Close()) }()
	r := pr.New(b, pluginds.NewResolver())
	s := integrationSpec(t, c)
	h, err := r.Start(ctx, s)
	require.NoError(t, err)
	want := "audit sink unavailable"
	if mode == "sink" {
		_, _ = syncProbe(ctx, h, "network")
	} else {
		// Simulate the fixed kernel program's loss counter. This verifies the
		// real consumer fails closed without adding a traffic stress test.
		counter, err := ebpf.LoadPinnedMap(filepath.Join(b.pinRoot(h.InstanceID()), "dropped"), nil)
		require.NoError(t, err)
		require.NoError(t, counter.Update(uint32(0), uint64(1), ebpf.UpdateAny))
		require.NoError(t, counter.Close())
		want = "audit gap: dropped=1"
	}
	require.Eventually(t, func() bool {
		b.mu.Lock()
		defer b.mu.Unlock()
		return strings.Contains(b.final[h.InstanceID()].DiagnosticTail, want)
	}, 5*time.Second, 20*time.Millisecond)
	state, err := b.Inspect(ctx, h.InstanceID())
	require.NoError(t, err)
	require.False(t, state.Running)
	t.Logf("visible audit failure: %s", state.DiagnosticTail)
	_ = r.Stop(ctx, h, 0)
	assertClean(t, b, s, h.InstanceID())
}
