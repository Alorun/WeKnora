package docker

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Tencent/WeKnora/internal/logger"
	pluginruntime "github.com/Tencent/WeKnora/internal/plugin/runtime"
	"github.com/Tencent/WeKnora/internal/plugin/sandbox/docker/network"
	"github.com/Tencent/WeKnora/internal/plugin/sandbox/docker/paths"
	"github.com/containerd/errdefs"
	ct "github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
	"golang.org/x/sys/unix"
)

var _ pluginruntime.PluginSandboxBackend = (*Backend)(nil)

type Backend struct {
	client              *client.Client
	config              Config
	imageID, gateDigest string
	mu                  sync.Mutex
	services            map[string]*service
	final               map[string]pluginruntime.BackendInstanceState // at most 64 diagnostic tails
	closed              bool
	disposed            bool
}

type service struct {
	policy   *network.PinnedPolicy
	cancel   context.CancelFunc
	done     chan struct{}
	auditErr error // under backend mu
}

func New(ctx context.Context, config Config) (*Backend, error) {
	if runtime.GOOS != "linux" {
		return nil, errors.New("plugins require Linux")
	}
	if err := config.validate(); err != nil {
		return nil, err
	}
	config.GrantRoots = append([]paths.PathMapping(nil), config.GrantRoots...)
	host := config.DockerHost
	if host == "" {
		host = "unix:///var/run/docker.sock"
	}
	cli, err := client.New(client.WithHost(host), client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, err
	}
	fail := func(err error) (*Backend, error) { cli.Close(); return nil, err }
	info, err := cli.Info(ctx, client.InfoOptions{})
	if err != nil {
		return fail(err)
	}
	if info.Info.OSType != "linux" || info.Info.CgroupVersion != "2" || !info.Info.MemoryLimit || !info.Info.SwapLimit || !info.Info.PidsLimit || !info.Info.CPUCfsQuota {
		return fail(errors.New("Linux/cgroup v2 CPU, memory, swap and PID limits required"))
	}
	if !strings.Contains(strings.Join(info.Info.SecurityOptions, ","), "seccomp") {
		return fail(errors.New("Docker seccomp required"))
	}
	image, err := cli.ImageInspect(ctx, config.Image)
	if err != nil {
		return fail(err)
	}
	if len(image.Config.Volumes) != 0 {
		return fail(errors.New("runtime image must not declare writable volumes"))
	}
	for _, value := range image.Config.Env {
		// Docker's builder adds this standard PATH even to FROM scratch.
		if value != "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin" {
			return fail(errors.New("runtime image has nonstandard environment variables; use a minimal administrator image"))
		}
	}
	gate, err := fileDigest(config.GateAppPath)
	if err != nil {
		return fail(err)
	}
	b := &Backend{client: cli, config: config, imageID: image.ID, gateDigest: gate, services: map[string]*service{}, final: map[string]pluginruntime.BackendInstanceState{}}
	if err := os.MkdirAll(b.recordsRoot(), 0700); err != nil {
		return fail(err)
	}
	// List proves daemon access and bounds recovery state without a resource ledger.
	if _, err := b.List(ctx, nil); err != nil {
		return fail(err)
	}
	return b, nil
}

func (b *Backend) StartService(ctx context.Context, spec pluginruntime.InstanceSpec) (instance pluginruntime.BackendInstance, resultErr error) {
	unlock, err := b.lock(ctx)
	if err != nil {
		return instance, err
	}
	defer unlock()
	b.mu.Lock()
	closed := b.closed
	b.mu.Unlock()
	if closed {
		return instance, errors.New("backend is closed")
	}
	if err := b.validateSpec(spec); err != nil {
		return instance, err
	}
	if digest, err := fileDigest(b.config.GateAppPath); err != nil || digest != b.gateDigest {
		return instance, errors.New("trusted gate changed")
	}
	listed, err := b.List(ctx, nil)
	if err != nil {
		return instance, err
	}
	active := 0
	for _, item := range listed {
		state, err := b.client.ContainerInspect(ctx, item.ID, client.ContainerInspectOptions{})
		if errdefs.IsNotFound(err) {
			continue
		}
		if err != nil {
			return instance, err
		}
		if state.Container.State.Running || state.Container.State.Status == "created" {
			active++
		}
		if item.Metadata["data_source_id"] == spec.DataSourceID {
			return instance, errors.New("data source already has an instance; stop/reconcile it first")
		}
		if item.Metadata["plugin_id"] == spec.PluginID && (item.Metadata["artifact_digest"] != spec.Artifact.Digest || item.Metadata["plugin_version"] != spec.PluginVersion) {
			return instance, errors.New("plugin already has another active artifact/version")
		}
	}
	if active >= b.config.MaxInstances {
		return instance, errors.New("node plugin instance limit reached")
	}
	// The deployment lock covers the entire create path: creating instances
	// occupy a slot even before Docker has assigned their ID.
	if err := b.mountRuntime(spec.RuntimeAppPath); err != nil {
		return instance, err
	}
	defer func() {
		if resultErr == nil {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if instance.ID != "" {
			resultErr = errors.Join(resultErr, b.stop(cleanupCtx, instance.ID, 0))
		} else {
			resultErr = errors.Join(resultErr, b.unmountRuntime(spec.RuntimeAppPath))
		}
	}()
	bootstrap, err := json.Marshal(struct {
		PluginID, PluginVersion, ExtensionID, ProtocolVersion, ContractVersion string
		StartupNonce                                                           []byte
		Capabilities                                                           []string
		ArtifactDevice, ArtifactInode, GrantDevice, GrantInode                 uint64
	}{
		spec.PluginID, spec.PluginVersion, spec.ExtensionID, spec.ProtocolVersion, spec.ContractVersion, spec.StartupNonce, spec.RequiredCapabilities,
		spec.Artifact.Device, spec.Artifact.Inode, spec.Grant.Device, spec.Grant.Inode})
	if err != nil {
		return instance, err
	}
	if err := os.WriteFile(filepath.Join(spec.RuntimeAppPath, "bootstrap.json"), bootstrap, 0444); err != nil {
		return instance, err
	}
	labels := map[string]string{"managed": "true", "workload_kind": "plugin", "deployment_id": b.config.DeploymentID,
		"plugin_id": spec.PluginID, "plugin_version": spec.PluginVersion, "data_source_id": spec.DataSourceID, "generation": strconv.FormatUint(spec.Generation, 10),
		"artifact_digest": spec.Artifact.Digest}
	created, err := b.client.ContainerCreate(ctx, b.createOptions(spec, labels))
	if err != nil {
		return instance, err
	}
	instance = pluginruntime.BackendInstance{ID: created.ID, UDSHostPath: filepath.Join(spec.RuntimeAppPath, "plugin.sock"), Metadata: labels}
	if err := b.saveRecord(instance); err != nil {
		return instance, err
	}
	// Verify daemon binds before ANY entrypoint executes, including the gate.
	if err := b.verifyMountedFile(ctx, instance.ID, "/opt/weknora/gate", b.gateDigest, 128<<20); err != nil {
		return instance, err
	}
	bootDigest := sha256.Sum256(bootstrap)
	if err := b.verifyMountedFile(ctx, instance.ID, "/run/weknora/bootstrap.json", hex.EncodeToString(bootDigest[:]), 4096); err != nil {
		return instance, err
	}
	if _, err := b.client.ContainerStart(ctx, instance.ID, client.ContainerStartOptions{}); err != nil {
		return instance, err
	}
	state, err := b.client.ContainerInspect(ctx, instance.ID, client.ContainerInspectOptions{})
	if err != nil {
		return instance, err
	}
	if state.Container.State == nil || !state.Container.State.Running {
		return instance, errors.New("gate exited before policy ready")
	}
	if err := verifyConfiguration(state.Container, spec, fmt.Sprintf("%d:%d", b.config.PluginUID, b.config.PluginGID)); err != nil {
		return instance, err
	}
	gateCtx, gateCancel := context.WithTimeout(ctx, 5*time.Second)
	defer gateCancel()
	for {
		if _, err := os.Stat(filepath.Join(spec.RuntimeAppPath, "gate.ready")); err == nil {
			break
		}
		select {
		case <-gateCtx.Done():
			return instance, fmt.Errorf("trusted gate did not validate mounted identities: %w", gateCtx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
	if _, err := os.Lstat(instance.UDSHostPath); !errors.Is(err, os.ErrNotExist) {
		return instance, errors.New("plugin socket exists before gate release")
	}
	cgroup, err := network.FindDockerCgroup("/sys/fs/cgroup", instance.ID)
	if err != nil {
		return instance, err
	}
	cgroupID, err := network.CgroupID(cgroup)
	if err != nil {
		return instance, err
	}
	policy, err := network.AttachAndPin(cgroup, b.pinRoot(instance.ID))
	if err != nil {
		return instance, fmt.Errorf("network audit not available (fail closed): %w", err)
	}
	consumerCtx, cancel := context.WithCancel(context.Background())
	svc := &service{policy: policy, cancel: cancel, done: make(chan struct{})}
	b.mu.Lock()
	b.services[instance.ID] = svc
	b.mu.Unlock()
	ready := make(chan struct{})
	consumerInstance := instance
	consumerInstance.Metadata = maps.Clone(instance.Metadata)
	go b.consume(consumerCtx, consumerInstance, cgroupID, svc, ready)
	<-ready
	if err := ctx.Err(); err != nil {
		return instance, err
	}
	if err := os.WriteFile(filepath.Join(spec.RuntimeAppPath, "release"), []byte("policy-ready\n"), 0444); err != nil {
		return instance, err
	}
	logger.GetLogger(ctx).WithField("sandbox_id", instance.ID).WithField("deployment_id", b.config.DeploymentID).Info("plugin policy ready; gate released")
	go b.reapExit(instance.ID)
	return instance, nil
}

func (b *Backend) consume(ctx context.Context, i pluginruntime.BackendInstance, cgroupID uint64, svc *service, ready chan struct{}) {
	defer close(svc.done)
	close(ready)
	generation, _ := strconv.ParseUint(i.Metadata["generation"], 10, 64)
	identity := network.Identity{DeploymentID: b.config.DeploymentID, PluginID: i.Metadata["plugin_id"], DataSourceID: i.Metadata["data_source_id"], Generation: generation}
	for {
		if ctx.Err() != nil {
			return
		}
		events, err := svc.policy.ReadEvents(ctx, 1, identity)
		if err != nil && ctx.Err() != nil {
			return
		}
		if err == nil {
			if events[0].CgroupID != cgroupID {
				err = errors.New("audit cgroup identity mismatch")
			} else {
				err = b.config.Audit(ctx, events[0])
				if err == nil {
					logger.GetLogger(ctx).WithField("plugin_audit", events[0]).Info("plugin network denied")
				}
			}
		}
		if err != nil {
			b.mu.Lock()
			svc.auditErr = err
			b.mu.Unlock()
			logger.GetLogger(context.Background()).WithField("sandbox_id", i.ID).WithError(err).Error("plugin audit gap/failure; stopping instance")
			// Stop runs outside this consumer; it waits for done before detach.
			go func() {
				cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				if e := b.Stop(cleanup, i.ID, 0); e != nil {
					logger.Errorf(cleanup, "plugin audit-failure cleanup: %v", e)
				}
			}()
			return
		}
	}
}

func (b *Backend) reapExit(id string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wait := b.client.ContainerWait(ctx, id, client.ContainerWaitOptions{Condition: ct.WaitConditionNotRunning})
	select {
	case <-wait.Result:
	case err := <-wait.Error:
		if err != nil {
			logger.Errorf(ctx, "plugin exit watcher: %v", err)
			return
		}
	}
	cleanup, done := context.WithTimeout(context.Background(), 30*time.Second)
	defer done()
	if err := b.Stop(cleanup, id, 0); err != nil {
		logger.Errorf(cleanup, "plugin exit cleanup %s: %v", id, err)
	}
}

func (b *Backend) recordsRoot() string {
	return filepath.Join(b.config.RuntimeRoot.AppRoot, ".instances")
}
func (b *Backend) pinRoot(id string) string {
	return filepath.Join("/sys/fs/bpf/weknora-plugin", b.config.DeploymentID, id)
}
func fileDigest(path string) (string, error) {
	f, e := os.Open(path)
	if e != nil {
		return "", e
	}
	defer f.Close()
	h := sha256.New()
	n, e := io.Copy(h, io.LimitReader(f, 128<<20))
	if n == 128<<20 {
		return "", errors.New("gate too large")
	}
	return hex.EncodeToString(h.Sum(nil)), e
}

// A filesystem lock prevents concurrent Backend objects/processes for this
// deployment from exceeding the single node limit. It is not a distributed lease.
func (b *Backend) lock(ctx context.Context) (func(), error) {
	f, err := os.OpenFile(filepath.Join(b.recordsRoot(), "lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	for {
		err = unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			return func() { unix.Flock(int(f.Fd()), unix.LOCK_UN); f.Close() }, nil
		}
		if !errors.Is(err, unix.EWOULDBLOCK) {
			f.Close()
			return nil, err
		}
		select {
		case <-ctx.Done():
			f.Close()
			return nil, ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func (b *Backend) verifyMountedFile(ctx context.Context, id, path, digest string, limit int64) error {
	r, err := b.client.CopyFromContainer(ctx, id, client.CopyFromContainerOptions{SourcePath: path})
	if err != nil {
		return fmt.Errorf("verify daemon bind %s: %w", path, err)
	}
	defer r.Content.Close()
	tarReader := tar.NewReader(io.LimitReader(r.Content, limit+4096))
	header, err := tarReader.Next()
	if err != nil {
		return err
	}
	if header.Typeflag != tar.TypeReg || header.Size > limit {
		return errors.New("daemon bind is not the expected bounded regular file")
	}
	h := sha256.New()
	if _, err := io.Copy(h, tarReader); err != nil {
		return err
	}
	if hex.EncodeToString(h.Sum(nil)) != digest {
		return fmt.Errorf("App/Host bind content mismatch: %s", path)
	}
	return nil
}

func verifyConfiguration(c ct.InspectResponse, s pluginruntime.InstanceSpec, user string) error {
	h := c.HostConfig
	if h == nil || c.Config == nil || !h.ReadonlyRootfs || h.Privileged || h.NetworkMode != "none" || h.Memory != s.Resources.MemoryBytes || h.MemorySwap != h.Memory || h.NanoCPUs != int64(s.Resources.CPUQuota*1e9) || h.PidsLimit == nil || *h.PidsLimit != s.Resources.PidsLimit || h.Init == nil || !*h.Init || len(h.CapAdd) != 0 || strings.Join(h.CapDrop, ",") != "ALL" || !strings.Contains(strings.Join(h.SecurityOpt, ","), "no-new-privileges") {
		return errors.New("Docker did not apply mandatory isolation/resource limits")
	}
	if h.LogConfig.Config["max-size"] != LogMaxSize || h.LogConfig.Config["max-file"] != LogMaxFiles {
		return errors.New("Docker log rotation unavailable")
	}
	if c.Config.User != user || strings.Join(c.Config.Entrypoint, ",") != "/opt/weknora/gate" || c.Config.Healthcheck == nil || strings.Join(c.Config.Healthcheck.Test, ",") != "NONE" {
		return errors.New("Docker did not apply the trusted non-root entrypoint")
	}
	return nil
}

// TrimDiagnostics never retains unlimited container output in host memory.
func TrimDiagnostics(p []byte) string {
	if len(p) > DiagnosticBytes {
		p = p[len(p)-DiagnosticBytes:]
	}
	return string(bytes.ToValidUTF8(p, []byte("?")))
}
