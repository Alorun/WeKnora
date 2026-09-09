package docker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/logger"
	pluginruntime "github.com/Tencent/WeKnora/internal/plugin/runtime"
	"github.com/Tencent/WeKnora/internal/plugin/sandbox/docker/network"
	"github.com/Tencent/WeKnora/internal/plugin/sandbox/docker/paths"
	"github.com/containerd/errdefs"
	"github.com/moby/moby/client"
	"golang.org/x/sys/unix"
)

func (b *Backend) mountRuntime(path string) error {
	parent := filepath.Dir(path)
	if err := os.MkdirAll(parent, 0755); err != nil {
		return err
	}
	if err := paths.TrustedDirectory(parent, b.config.AdminUID, b.config.PluginUID); err != nil {
		return err
	}
	if err := os.Mkdir(path, 0700); err != nil {
		return fmt.Errorf("runtime directory already exists or cannot be created: %w", err)
	}
	options := fmt.Sprintf("size=%d,mode=0700,uid=%d,gid=%d", UDSBytes, b.config.PluginUID, b.config.PluginGID)
	if err := unix.Mount("weknora-plugin", path, "tmpfs", unix.MS_NOSUID|unix.MS_NODEV|unix.MS_NOEXEC, options); err != nil {
		os.Remove(path)
		return fmt.Errorf("bounded UDS tmpfs requires mount capability: %w", err)
	}
	var fs unix.Statfs_t
	if err := unix.Statfs(path, &fs); err != nil || fs.Type != unix.TMPFS_MAGIC || fs.Blocks*uint64(fs.Bsize) > UDSBytes {
		return errors.Join(errors.New("UDS tmpfs size was not applied"), b.unmountRuntime(path))
	}
	return nil
}

func (b *Backend) unmountRuntime(path string) error {
	// Only canonical per-instance paths derived from our trusted metadata are
	// accepted. Never follow plugin-created symlinks or recursively delete them.
	rel, err := filepath.Rel(b.config.RuntimeRoot.AppRoot, path)
	if err != nil || strings.HasPrefix(rel, "..") || len(strings.Split(rel, string(filepath.Separator))) != 2 {
		return errors.New("refusing runtime cleanup outside instance directory")
	}
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("runtime cleanup path replaced")
	}
	if err := unix.Unmount(path, 0); err != nil && !errors.Is(err, unix.EINVAL) && !errors.Is(err, unix.ENOENT) {
		return fmt.Errorf("unmount instance UDS: %w", err)
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.Remove(filepath.Dir(path)); err != nil && !errors.Is(err, unix.ENOTEMPTY) && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func validContainerID(id string) bool {
	return len(id) == 64 && strings.Trim(id, "0123456789abcdef") == ""
}
func (b *Backend) owns(m map[string]string) bool {
	return m["managed"] == "true" && m["workload_kind"] == "plugin" && m["deployment_id"] == b.config.DeploymentID
}

func (b *Backend) instance(id string, labels map[string]string) (pluginruntime.BackendInstance, error) {
	if !validContainerID(id) || !b.owns(labels) || !identifier.MatchString(labels["data_source_id"]) || !identifier.MatchString(labels["plugin_id"]) {
		return pluginruntime.BackendInstance{}, errors.New("container is not owned by this plugin deployment")
	}
	g, err := strconv.ParseUint(labels["generation"], 10, 64)
	if err != nil || g == 0 {
		return pluginruntime.BackendInstance{}, errors.New("invalid instance generation metadata")
	}
	m := map[string]string{}
	for k, v := range labels {
		m[k] = v
	}
	return pluginruntime.BackendInstance{ID: id, UDSHostPath: filepath.Join(b.config.RuntimeRoot.AppRoot, labels["data_source_id"], strconv.FormatUint(g, 10), "plugin.sock"), Metadata: m}, nil
}

// The record is a cleanup locator, not a desired-state store or reservation
// ledger. It survives Docker-side removal until mounts/pins are actually gone.
func (b *Backend) saveRecord(i pluginruntime.BackendInstance) error {
	f, err := os.OpenFile(filepath.Join(b.recordsRoot(), i.ID+".json"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	err = json.NewEncoder(f).Encode(i.Metadata)
	syncErr := f.Sync()
	closeErr := f.Close()
	return errors.Join(err, syncErr, closeErr)
}

func (b *Backend) readRecord(id string) (pluginruntime.BackendInstance, error) {
	if !validContainerID(id) {
		return pluginruntime.BackendInstance{}, errors.New("invalid container ID")
	}
	f, err := os.Open(filepath.Join(b.recordsRoot(), id+".json"))
	if err != nil {
		return pluginruntime.BackendInstance{}, err
	}
	defer f.Close()
	var labels map[string]string
	if err := json.NewDecoder(io.LimitReader(f, 8192)).Decode(&labels); err != nil {
		return pluginruntime.BackendInstance{}, err
	}
	return b.instance(id, labels)
}

func (b *Backend) List(ctx context.Context, metadata map[string]string) ([]pluginruntime.BackendInstance, error) {
	f := client.Filters{}
	f.Add("label", "managed=true", "workload_kind=plugin", "deployment_id="+b.config.DeploymentID)
	for k, v := range metadata {
		f.Add("label", k+"="+v)
	}
	list, err := b.client.ContainerList(ctx, client.ContainerListOptions{All: true, Filters: f})
	if err != nil {
		return nil, err
	}
	var result []pluginruntime.BackendInstance
	seen := map[string]bool{}
	for _, c := range list.Items {
		i, err := b.instance(c.ID, c.Labels)
		if err != nil {
			return nil, err
		}
		result = append(result, i)
		seen[i.ID] = true
	}
	// Include incomplete cleanup after Docker NotFound; Reconciler can retry Stop.
	entries, err := os.ReadDir(b.recordsRoot())
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		id := strings.TrimSuffix(e.Name(), ".json")
		if seen[id] {
			continue
		}
		i, err := b.readRecord(id)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		matches := true
		for k, v := range metadata {
			if i.Metadata[k] != v {
				matches = false
			}
		}
		if matches {
			result = append(result, i)
		}
	}
	return result, nil
}

func (b *Backend) Inspect(ctx context.Context, id string) (pluginruntime.BackendInstanceState, error) {
	return b.inspect(ctx, id, true)
}

func (b *Backend) inspect(ctx context.Context, id string, diagnosticsCache bool) (pluginruntime.BackendInstanceState, error) {
	if !validContainerID(id) {
		return pluginruntime.BackendInstanceState{}, errors.New("invalid container ID")
	}
	r, err := b.client.ContainerInspect(ctx, id, client.ContainerInspectOptions{})
	if errdefs.IsNotFound(err) {
		b.mu.Lock()
		cached, ok := b.final[id]
		b.mu.Unlock()
		if ok && diagnosticsCache {
			cached.Instance.Metadata = maps.Clone(cached.Instance.Metadata)
			return cached, nil
		}
		i, readErr := b.readRecord(id)
		if readErr == nil {
			return pluginruntime.BackendInstanceState{Instance: i}, nil
		}
		if !errors.Is(readErr, os.ErrNotExist) {
			return pluginruntime.BackendInstanceState{}, fmt.Errorf("read cleanup locator: %w", readErr)
		}
		return pluginruntime.BackendInstanceState{}, err
	}
	if err != nil {
		return pluginruntime.BackendInstanceState{}, err
	}
	if r.Container.Config == nil || r.Container.State == nil {
		return pluginruntime.BackendInstanceState{}, errors.New("incomplete Docker inspect")
	}
	i, err := b.instance(id, r.Container.Config.Labels)
	if err != nil {
		return pluginruntime.BackendInstanceState{}, err
	}
	s := r.Container.State
	state := pluginruntime.BackendInstanceState{Instance: i, Running: s.Running, ExitCode: s.ExitCode, OOMKilled: s.OOMKilled}
	if !s.Running {
		state.DiagnosticTail = b.diagnostics(ctx, id)
	}
	return state, nil
}

func (b *Backend) diagnostics(ctx context.Context, id string) string {
	logs, err := b.client.ContainerLogs(ctx, id, client.ContainerLogsOptions{ShowStdout: true, ShowStderr: true, Tail: "100"})
	if err != nil {
		return "diagnostics unavailable"
	}
	defer logs.Close()
	// Docker multiplex headers are harmless diagnostic bytes; avoiding StdCopy
	// also avoids allocating an attacker-controlled frame length on the host.
	data, err := io.ReadAll(io.LimitReader(logs, DiagnosticBytes))
	if err != nil {
		return "diagnostics read failed"
	}
	return TrimDiagnostics(data)
}

func (b *Backend) Stop(ctx context.Context, id string, grace time.Duration) error {
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	unlock, err := b.lock(cleanup)
	if err != nil {
		return err
	}
	defer unlock()
	return b.stop(cleanup, id, grace)
}

func (b *Backend) stop(ctx context.Context, id string, grace time.Duration) error {
	if !validContainerID(id) {
		return errors.New("invalid container ID")
	}
	// A cached exit is diagnostic only, never cleanup authority: the same
	// data source/generation path may now belong to a replacement container.
	state, err := b.inspect(ctx, id, false)
	if errdefs.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	i := state.Instance
	if !b.owns(i.Metadata) {
		return errors.New("refusing foreign container cleanup")
	}
	seconds := int(min(max(grace, 0), 10*time.Second) / time.Second)
	if state.Running {
		_, err = b.client.ContainerStop(ctx, id, client.ContainerStopOptions{Timeout: &seconds})
		if err != nil && !errdefs.IsNotFound(err) {
			return fmt.Errorf("stop plugin before detaching network: %w", err)
		}
	}
	// Prove exit before touching policy. SIGKILL alone is never called an OOM.
	r, err := b.client.ContainerInspect(ctx, id, client.ContainerInspectOptions{})
	if err != nil && !errdefs.IsNotFound(err) {
		return err
	}
	if err == nil {
		if r.Container.State == nil || r.Container.State.Running {
			return errors.New("plugin is still running; policy retained")
		}
		state.ExitCode, state.OOMKilled = r.Container.State.ExitCode, r.Container.State.OOMKilled
		state.DiagnosticTail = b.diagnostics(ctx, id)
	}
	state.Running = false
	b.mu.Lock()
	svc := b.services[id]
	b.mu.Unlock()
	var policy *network.PinnedPolicy
	var auditErr error
	if svc != nil {
		svc.cancel()
		select {
		case <-svc.done:
		case <-ctx.Done():
			return fmt.Errorf("audit consumer drain: %w", ctx.Err())
		}
		policy, auditErr = svc.policy, svc.auditErr
	} else if _, err := os.Stat(b.pinRoot(id)); err == nil {
		// A replacement Backend must not silently discard audit events left
		// in pinned maps while the previous Controller was down.
		policy, auditErr = network.OpenPinned(b.pinRoot(id))
	}
	if policy != nil {
		auditErr = errors.Join(auditErr, b.drainAudit(ctx, i, policy))
		if err := policy.Detach(); err != nil {
			if retryErr := network.RemovePins(b.pinRoot(id)); retryErr != nil {
				return errors.Join(err, retryErr)
			}
		}
	} else if err := network.RemovePins(b.pinRoot(id)); err != nil {
		return err
	}
	if auditErr != nil {
		state.DiagnosticTail = TrimDiagnostics([]byte(state.DiagnosticTail + "\naudit failure/gap: " + auditErr.Error()))
		logger.GetLogger(ctx).WithField("sandbox_id", id).WithError(auditErr).Error("plugin audit gap during exit/recovery cleanup")
	}
	b.mu.Lock()
	delete(b.services, id)
	b.mu.Unlock()
	if err := b.unmountRuntime(filepath.Dir(i.UDSHostPath)); err != nil {
		return err
	}
	if _, err := b.client.ContainerRemove(ctx, id, client.ContainerRemoveOptions{Force: true}); err != nil && !errdefs.IsNotFound(err) {
		return err
	}
	if err := os.Remove(filepath.Join(b.recordsRoot(), id+".json")); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	b.mu.Lock()
	if len(b.final) >= 64 {
		for k := range b.final {
			delete(b.final, k)
			break
		}
	}
	b.final[id] = state
	b.mu.Unlock()
	// Empty deployment pin directory is safe to remove; unrelated deployments
	// and the parent bpffs hierarchy are never touched.
	parent := filepath.Dir(b.pinRoot(id))
	if _, err := os.Lstat(parent); err == nil {
		if err := os.Remove(parent); err != nil && !errors.Is(err, os.ErrNotExist) && !errors.Is(err, unix.ENOTEMPTY) {
			return err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

// Called after process exit and consumer drain: no producer can refill this
// bounded ring, and delivery errors remain visible in the exit diagnostics.
func (b *Backend) drainAudit(ctx context.Context, i pluginruntime.BackendInstance, policy *network.PinnedPolicy) error {
	generation, _ := strconv.ParseUint(i.Metadata["generation"], 10, 64)
	identity := network.Identity{InstanceID: i.ID, DeploymentID: b.config.DeploymentID, PluginID: i.Metadata["plugin_id"], DataSourceID: i.Metadata["data_source_id"], Generation: generation}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		event, err := policy.ReadPending(identity)
		if err != nil || event == nil {
			return err
		}
		if err := b.config.Audit(ctx, *event); err != nil {
			return err
		}
		logger.GetLogger(ctx).WithField("plugin_audit", *event).Info("plugin network denied (exit/recovery drain)")
	}
}

// Close does not abandon live workloads or audit consumers. Crashed processes
// leave pins and cleanup locators, which a new Backend can list and stop.
func (b *Backend) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	unlock, err := b.lock(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	b.mu.Lock()
	alreadyClosed := b.disposed
	b.closed = true
	b.mu.Unlock()
	if alreadyClosed {
		return nil
	}
	list, err := b.List(ctx, nil)
	if err != nil {
		return err
	}
	for _, i := range list {
		err = errors.Join(err, b.stop(ctx, i.ID, 0))
	}
	if err != nil {
		return err
	}
	err = b.client.Close()
	if err == nil {
		b.mu.Lock()
		b.disposed = true
		b.mu.Unlock()
	}
	return err
}
