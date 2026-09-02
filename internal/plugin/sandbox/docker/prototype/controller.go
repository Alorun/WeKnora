package prototype

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/plugin/sandbox/docker/network"
	"github.com/containerd/errdefs"
	"github.com/moby/moby/api/pkg/stdcopy"
	containertypes "github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	"github.com/moby/moby/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

const (
	gatePath       = ContainerArtifact + "/gate-prototype"
	fakePluginPath = ContainerArtifact + "/fake-prototype"
	releasePath    = "/run/weknora/release"
	resultPath     = "/run/weknora/result.json"
	startedPath    = "/run/weknora/plugin.started"
	helperPath     = "/opt/weknora/controller/controller-prototype"
	maxLogBytes    = 32 * 1024
)

// Controller is deliberately independent of PluginManager, stores, and services.
type Controller struct {
	client *client.Client
}

type InspectEvidence struct {
	Running          bool     `json:"running"`
	Status           string   `json:"status"`
	ExitCode         int      `json:"exit_code"`
	OOMKilled        bool     `json:"oom_killed"`
	User             string   `json:"user"`
	ReadonlyRootfs   bool     `json:"readonly_rootfs"`
	NetworkMode      string   `json:"network_mode"`
	CapDrop          []string `json:"cap_drop"`
	CapAdd           []string `json:"cap_add"`
	SecurityOpt      []string `json:"security_opt"`
	Init             bool     `json:"init"`
	Memory           int64    `json:"memory"`
	NanoCPUs         int64    `json:"nano_cpus"`
	PidsLimit        int64    `json:"pids_limit"`
	Privileged       bool     `json:"privileged"`
	ArtifactReadOnly bool     `json:"artifact_read_only"`
	GrantReadOnly    bool     `json:"grant_read_only"`
	RuntimeReadOnly  bool     `json:"runtime_read_only"`
	Tmpfs            string   `json:"tmpfs"`
}

type FakeAttempt struct {
	Name   string `json:"name"`
	Target string `json:"target"`
	Error  string `json:"error"`
}

type FakeResult struct {
	StartedAt        time.Time     `json:"started_at"`
	AllowedRead      bool          `json:"allowed_read"`
	AllowedContent   string        `json:"allowed_content"`
	WriteDenied      bool          `json:"write_denied"`
	WriteError       string        `json:"write_error"`
	OutsideInvisible bool          `json:"outside_invisible"`
	OutsideError     string        `json:"outside_error"`
	Attempts         []FakeAttempt `json:"network_attempts"`
}

type RunEvidence struct {
	ContainerID           string               `json:"container_id"`
	PluginStartedAtGate   bool                 `json:"plugin_started_at_gate"`
	PluginStartedAtReady  bool                 `json:"plugin_started_at_policy_ready"`
	AuditConsumerReadyAt  time.Time            `json:"audit_consumer_ready_at"`
	PolicyReadyAt         time.Time            `json:"policy_ready_at"`
	ReleasedAt            time.Time            `json:"released_at"`
	HealthServing         bool                 `json:"health_serving"`
	Fake                  FakeResult           `json:"fake_plugin"`
	AuditEvents           []network.AuditEvent `json:"audit_events"`
	InspectRunning        InspectEvidence      `json:"inspect_running"`
	Recovered             bool                 `json:"recovered"`
	ControllerPID         int                  `json:"controller_pid"`
	RecoveryControllerPID int                  `json:"recovery_controller_pid"`
	FinalExitCode         int                  `json:"final_exit_code"`
	FinalOOMKilled        bool                 `json:"final_oom_killed"`
	SecondStopSucceeded   bool                 `json:"second_stop_succeeded"`
}

func NewController() (*Controller, error) {
	cli, err := client.New(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("create Docker client: %w", err)
	}
	return &Controller{client: cli}, nil
}

func (c *Controller) Close() error { return c.client.Close() }

// Run performs the complete stage-1 sequence and always compensates resources.
func (c *Controller) Run(ctx context.Context, spec PrototypeInstanceSpec) (evidence RunEvidence, runErr error) {
	evidence.ControllerPID = os.Getpid()
	if err := spec.Validate(); err != nil {
		return evidence, err
	}
	if err := RevalidateGrant(spec.Grant); err != nil {
		return evidence, fmt.Errorf("pre-create grant check: %w", err)
	}
	if err := PrepareRuntimeDir(spec.RuntimeAppPath, spec.ContainerUser); err != nil {
		return evidence, fmt.Errorf("prepare runtime directory: %w", err)
	}
	defer func() {
		if cleanupErr := cleanupRuntimeDir(spec.RuntimeAppPath); cleanupErr != nil {
			runErr = errors.Join(runErr, cleanupErr)
		}
	}()
	socketPath, err := RuntimeSocketPath(spec.RuntimeAppPath)
	if err != nil {
		return evidence, err
	}
	if err := RemoveStaleSocket(socketPath); err != nil {
		return evidence, fmt.Errorf("remove stale UDS: %w", err)
	}
	digest, err := artifactDigest(spec.ArtifactAppPath)
	if err != nil {
		return evidence, err
	}
	if err := RevalidateGrant(spec.Grant); err != nil {
		return evidence, fmt.Errorf("container-create grant check: %w", err)
	}
	expectedAllowed, err := os.ReadFile(filepath.Join(spec.GrantAppPath, "allowed.txt"))
	if err != nil {
		return evidence, fmt.Errorf("read allowed.txt through Grant App Path: %w", err)
	}

	containerID, err := c.createPlugin(ctx, spec, digest)
	if err != nil {
		return evidence, err
	}
	evidence.ContainerID = containerID
	policyReady := false
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		final, cleanupErr := c.Stop(cleanupCtx, spec, containerID, policyReady)
		evidence.FinalExitCode = final.ExitCode
		evidence.FinalOOMKilled = final.OOMKilled
		if _, secondErr := c.Stop(cleanupCtx, spec, containerID, false); secondErr == nil {
			evidence.SecondStopSucceeded = true
		} else {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("second stop: %w", secondErr))
		}
		if runErr == nil && cleanupErr != nil {
			runErr = cleanupErr
		} else if runErr != nil && cleanupErr != nil {
			runErr = errors.Join(runErr, cleanupErr)
		}
	}()

	if _, err := c.client.ContainerStart(ctx, containerID, client.ContainerStartOptions{}); err != nil {
		return evidence, fmt.Errorf("start gate container: %w", err)
	}
	evidence.PluginStartedAtGate = fileExists(filepath.Join(spec.RuntimeAppPath, filepath.Base(startedPath)))
	inspect, err := c.client.ContainerInspect(ctx, containerID, client.ContainerInspectOptions{})
	if err != nil || inspect.Container.State == nil || !inspect.Container.State.Running {
		return evidence, fmt.Errorf("gate is not running before policy attach: %w", err)
	}

	if _, err := c.runNetworkHelper(ctx, spec, "attach", containerID, 0); err != nil {
		return evidence, fmt.Errorf("attach eBPF policy: %w", err)
	}
	auditHelperID, err := c.startAuditConsumer(ctx, spec, containerID, 4)
	if err != nil {
		return evidence, fmt.Errorf("start eBPF audit consumer: %w", err)
	}
	defer func() {
		_, _ = c.client.ContainerRemove(context.Background(), auditHelperID, client.ContainerRemoveOptions{Force: true})
	}()
	evidence.AuditConsumerReadyAt = time.Now().UTC()
	policyReady = true
	evidence.PolicyReadyAt = time.Now().UTC()
	evidence.PluginStartedAtReady = fileExists(filepath.Join(spec.RuntimeAppPath, filepath.Base(startedPath)))
	if evidence.PluginStartedAtGate || evidence.PluginStartedAtReady {
		return evidence, errors.New("fake plugin started before policy ready")
	}
	if err := os.WriteFile(filepath.Join(spec.RuntimeAppPath, filepath.Base(releasePath)), []byte("ready\n"), 0o600); err != nil {
		return evidence, fmt.Errorf("release gate: %w", err)
	}
	evidence.ReleasedAt = time.Now().UTC()
	if err := waitForHealth(ctx, socketPath, 20*time.Second); err != nil {
		return evidence, err
	}
	evidence.HealthServing = true
	if err := waitJSON(filepath.Join(spec.RuntimeAppPath, filepath.Base(resultPath)), &evidence.Fake, 20*time.Second); err != nil {
		return evidence, err
	}
	if err := validateFakeResult(evidence.Fake, string(expectedAllowed)); err != nil {
		return evidence, err
	}

	raw, err := c.finishAuditConsumer(ctx, auditHelperID)
	if err != nil {
		return evidence, fmt.Errorf("read eBPF audit events: %w", err)
	}
	if err := json.Unmarshal(raw, &evidence.AuditEvents); err != nil {
		return evidence, fmt.Errorf("decode helper audit events: %w", err)
	}
	if err := validateAuditEvents(evidence.AuditEvents, spec); err != nil {
		return evidence, err
	}

	inspect, err = c.client.ContainerInspect(ctx, containerID, client.ContainerInspectOptions{})
	if err != nil {
		return evidence, err
	}
	evidence.InspectRunning = inspectEvidence(inspect.Container)
	if err := validateSecurityEvidence(evidence.InspectRunning, spec); err != nil {
		return evidence, err
	}
	recovered, recoveryPID, err := recoverInFreshProcess(ctx, spec)
	if err != nil {
		return evidence, err
	}
	evidence.Recovered = recovered
	evidence.RecoveryControllerPID = recoveryPID
	if recoveryPID == evidence.ControllerPID {
		return evidence, errors.New("recovery did not run in a fresh Controller process")
	}
	return evidence, nil
}

func (c *Controller) createPlugin(ctx context.Context, spec PrototypeInstanceSpec, digest string) (string, error) {
	init := true
	pids := spec.PidsLimit
	created, err := c.client.ContainerCreate(ctx, client.ContainerCreateOptions{
		Config: &containertypes.Config{
			Image: spec.Image, User: spec.ContainerUser, Labels: labelsForSpec(spec, digest),
			Entrypoint: []string{gatePath}, Cmd: []string{
				"-release", releasePath, "-plugin", fakePluginPath, "-socket", ContainerSocket,
				"-result", resultPath, "-started", startedPath, "-timeout", "30s",
			},
		},
		HostConfig: &containertypes.HostConfig{
			NetworkMode: "none", CapDrop: []string{"ALL"}, SecurityOpt: []string{"no-new-privileges"},
			ReadonlyRootfs: true, Init: &init,
			Tmpfs: map[string]string{"/tmp": "rw,noexec,nosuid,nodev,size=16m"},
			Resources: containertypes.Resources{
				Memory: spec.MemoryBytes, MemorySwap: spec.MemoryBytes,
				NanoCPUs: int64(spec.CPUQuota * 1e9), PidsLimit: &pids,
			},
			Mounts: []mount.Mount{
				{Type: mount.TypeBind, Source: spec.ArtifactHostPath, Target: ContainerArtifact, ReadOnly: true},
				{Type: mount.TypeBind, Source: spec.GrantHostPath, Target: ContainerGrant, ReadOnly: true},
				{Type: mount.TypeBind, Source: spec.RuntimeHostPath, Target: "/run/weknora"},
			},
		},
		Name: "weknora-plugin-prototype-" + spec.RunID,
	})
	if err != nil {
		return "", fmt.Errorf("create plugin container: %w", err)
	}
	return created.ID, nil
}

func (c *Controller) runNetworkHelper(ctx context.Context, spec PrototypeInstanceSpec, operation, targetID string, count int) ([]byte, error) {
	helperID, err := c.createNetworkHelper(ctx, spec, operation, targetID, count)
	if err != nil {
		return nil, err
	}
	defer func() {
		_, _ = c.client.ContainerRemove(context.Background(), helperID, client.ContainerRemoveOptions{Force: true})
	}()
	if _, err := c.client.ContainerStart(ctx, helperID, client.ContainerStartOptions{}); err != nil {
		return nil, err
	}
	return c.waitNetworkHelper(ctx, helperID)
}

func (c *Controller) createNetworkHelper(ctx context.Context, spec PrototypeInstanceSpec, operation, targetID string, count int) (string, error) {
	labels := map[string]string{
		"managed": "true", "workload_kind": ControllerWorkload,
		"prototype_run_id": spec.RunID,
	}
	args := []string{"network-helper", operation, "-container-id", targetID, "-pin-root", spec.BPFPinRoot}
	if operation == "read" {
		args = append(args, "-count", strconv.Itoa(count), "-run-id", spec.RunID, "-plugin-id", spec.PluginID,
			"-data-source-id", spec.DataSourceID, "-generation", strconv.FormatUint(spec.Generation, 10))
	}
	created, err := c.client.ContainerCreate(ctx, client.ContainerCreateOptions{
		Config: &containertypes.Config{Image: spec.Image, User: "0:0", Entrypoint: []string{helperPath}, Cmd: args, Labels: labels},
		HostConfig: &containertypes.HostConfig{
			NetworkMode: "none", CapDrop: []string{"ALL"}, CapAdd: []string{"BPF", "NET_ADMIN", "PERFMON"},
			SecurityOpt: []string{"no-new-privileges"}, ReadonlyRootfs: true,
			CgroupnsMode: containertypes.CgroupnsModeHost,
			Mounts: []mount.Mount{
				{Type: mount.TypeBind, Source: spec.ControllerHostPath, Target: helperPath, ReadOnly: true},
				{Type: mount.TypeBind, Source: "/sys/fs/cgroup", Target: "/sys/fs/cgroup", ReadOnly: true},
				{Type: mount.TypeBind, Source: "/sys/fs/bpf", Target: "/sys/fs/bpf"},
			},
		},
	})
	if err != nil {
		return "", err
	}
	return created.ID, nil
}

func (c *Controller) startAuditConsumer(ctx context.Context, spec PrototypeInstanceSpec, targetID string, count int) (string, error) {
	helperID, err := c.createNetworkHelper(ctx, spec, "read", targetID, count)
	if err != nil {
		return "", err
	}
	if _, err := c.client.ContainerStart(ctx, helperID, client.ContainerStartOptions{}); err != nil {
		_, _ = c.client.ContainerRemove(context.Background(), helperID, client.ContainerRemoveOptions{Force: true})
		return "", err
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		logs, logErr := c.containerLogs(ctx, helperID)
		if logErr == nil && bytes.Contains(logs, []byte(`"status":"audit_consumer_ready"`)) {
			return helperID, nil
		}
		inspect, inspectErr := c.client.ContainerInspect(ctx, helperID, client.ContainerInspectOptions{})
		if inspectErr != nil || inspect.Container.State == nil || !inspect.Container.State.Running {
			_, _ = c.client.ContainerRemove(context.Background(), helperID, client.ContainerRemoveOptions{Force: true})
			return "", fmt.Errorf("audit consumer exited before ready: %s", strings.TrimSpace(string(logs)))
		}
		time.Sleep(50 * time.Millisecond)
	}
	_, _ = c.client.ContainerRemove(context.Background(), helperID, client.ContainerRemoveOptions{Force: true})
	return "", errors.New("audit consumer readiness timeout")
}

func (c *Controller) finishAuditConsumer(ctx context.Context, helperID string) ([]byte, error) {
	logs, err := c.waitNetworkHelper(ctx, helperID)
	if err != nil {
		return nil, err
	}
	lines := bytes.Split(bytes.TrimSpace(logs), []byte("\n"))
	if len(lines) < 2 {
		return nil, fmt.Errorf("audit consumer did not output events: %s", strings.TrimSpace(string(logs)))
	}
	return lines[len(lines)-1], nil
}

func (c *Controller) waitNetworkHelper(ctx context.Context, helperID string) ([]byte, error) {
	wait := c.client.ContainerWait(ctx, helperID, client.ContainerWaitOptions{Condition: containertypes.WaitConditionNotRunning})
	select {
	case err := <-wait.Error:
		return nil, err
	case result := <-wait.Result:
		logs, logErr := c.containerLogs(ctx, helperID)
		if logErr != nil {
			return nil, logErr
		}
		if result.StatusCode != 0 {
			return nil, fmt.Errorf("network helper exit %d: %s", result.StatusCode, strings.TrimSpace(string(logs)))
		}
		return bytes.TrimSpace(logs), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *Controller) containerLogs(ctx context.Context, id string) ([]byte, error) {
	logs, err := c.client.ContainerLogs(ctx, id, client.ContainerLogsOptions{ShowStdout: true, ShowStderr: true, Tail: "200"})
	if err != nil {
		return nil, err
	}
	defer logs.Close()
	var output bytes.Buffer
	limited := io.LimitReader(logs, maxLogBytes)
	if _, err := stdcopy.StdCopy(&output, &output, limited); err != nil {
		return nil, err
	}
	return output.Bytes(), nil
}

// Recover proves List+Inspect+metadata, pinned policy, and UDS health are sufficient.
func (c *Controller) Recover(ctx context.Context, spec PrototypeInstanceSpec) (bool, error) {
	filters := client.Filters{}
	filters.Add("label", "managed=true", "workload_kind="+WorkloadKind, "prototype_run_id="+spec.RunID)
	listed, err := c.client.ContainerList(ctx, client.ContainerListOptions{All: true, Filters: filters})
	if err != nil {
		return false, err
	}
	if len(listed.Items) != 1 {
		return false, fmt.Errorf("recovery expected one container, found %d", len(listed.Items))
	}
	item := listed.Items[0]
	cleanupInvalid := func(reason error) (bool, error) {
		policyReady := false
		if _, inspectErr := c.runNetworkHelper(ctx, spec, "inspect", item.ID, 0); inspectErr == nil {
			policyReady = true
		}
		_, cleanupErr := c.Stop(ctx, spec, item.ID, policyReady)
		return false, errors.Join(reason, cleanupErr)
	}
	if !matchesSpecLabels(spec, item.Labels) {
		return cleanupInvalid(errors.New("recovery metadata mismatch"))
	}
	digest, err := artifactDigest(spec.ArtifactAppPath)
	if err != nil || item.Labels["artifact_digest"] != digest {
		return cleanupInvalid(errors.New("recovery artifact digest mismatch"))
	}
	inspect, err := c.client.ContainerInspect(ctx, item.ID, client.ContainerInspectOptions{})
	if err != nil || inspect.Container.State == nil || !inspect.Container.State.Running {
		return cleanupInvalid(errors.New("recovery inspect is not running"))
	}
	if _, err := c.runNetworkHelper(ctx, spec, "inspect", item.ID, 0); err != nil {
		return cleanupInvalid(fmt.Errorf("recovery policy is not ready: %w", err))
	}
	socketPath, err := RuntimeSocketPath(spec.RuntimeAppPath)
	if err != nil {
		return cleanupInvalid(err)
	}
	if err := healthCheck(ctx, socketPath); err != nil {
		return cleanupInvalid(fmt.Errorf("recovery UDS is unhealthy: %w", err))
	}
	return true, nil
}

func recoverInFreshProcess(ctx context.Context, spec PrototypeInstanceSpec) (bool, int, error) {
	payload, err := json.Marshal(spec)
	if err != nil {
		return false, 0, err
	}
	command := exec.CommandContext(ctx, spec.ControllerAppPath, "recover-stdin")
	command.Stdin = bytes.NewReader(payload)
	output, err := command.CombinedOutput()
	if len(output) > maxLogBytes {
		return false, 0, errors.New("recovery Controller output exceeds limit")
	}
	if err != nil {
		return false, 0, fmt.Errorf("fresh recovery Controller: %w: %s", err, strings.TrimSpace(string(output)))
	}
	var result struct {
		Recovered bool `json:"recovered"`
		PID       int  `json:"pid"`
	}
	if err := json.Unmarshal(output, &result); err != nil {
		return false, 0, fmt.Errorf("decode recovery Controller output: %w", err)
	}
	return result.Recovered, result.PID, nil
}

// Stop is exact-run scoped. Docker NotFound and missing local resources are success.
func (c *Controller) Stop(ctx context.Context, spec PrototypeInstanceSpec, containerID string, policyReady bool) (InspectEvidence, error) {
	var final InspectEvidence
	var errs []error
	inspect, err := c.client.ContainerInspect(ctx, containerID, client.ContainerInspectOptions{})
	if err == nil {
		if !matchesRunLabels(spec.RunID, inspect.Container.Config.Labels) {
			return final, errors.New("refusing to stop container outside current prototype run")
		}
		if inspect.Container.State != nil && inspect.Container.State.Running {
			seconds := 5
			_, err = c.client.ContainerStop(ctx, containerID, client.ContainerStopOptions{Timeout: &seconds})
			if err != nil && !errdefs.IsNotFound(err) {
				errs = append(errs, err)
			}
		}
		if stopped, inspectErr := c.client.ContainerInspect(ctx, containerID, client.ContainerInspectOptions{}); inspectErr == nil {
			final = inspectEvidence(stopped.Container)
		}
		if _, err = c.client.ContainerRemove(ctx, containerID, client.ContainerRemoveOptions{}); err != nil && !errdefs.IsNotFound(err) {
			errs = append(errs, err)
		}
	} else if !errdefs.IsNotFound(err) {
		errs = append(errs, err)
	}
	if policyReady {
		if _, err := c.runNetworkHelper(ctx, spec, "detach", containerID, 0); err != nil {
			errs = append(errs, err)
		}
	}
	if err := cleanupRuntimeDir(spec.RuntimeAppPath); err != nil {
		errs = append(errs, err)
	}
	return final, errors.Join(errs...)
}

func cleanupRuntimeDir(runtimeDir string) error {
	var errs []error
	for _, name := range []string{filepath.Base(ContainerSocket), filepath.Base(releasePath), filepath.Base(resultPath), filepath.Base(startedPath)} {
		if err := os.Remove(filepath.Join(runtimeDir, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
			errs = append(errs, err)
		}
	}
	if err := os.Remove(runtimeDir); err != nil && !errors.Is(err, os.ErrNotExist) {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func matchesRunLabels(runID string, labels map[string]string) bool {
	return labels["managed"] == "true" && labels["workload_kind"] == WorkloadKind && labels["prototype_run_id"] == runID
}

func matchesSpecLabels(spec PrototypeInstanceSpec, labels map[string]string) bool {
	return matchesRunLabels(spec.RunID, labels) && labels["plugin_id"] == spec.PluginID &&
		labels["data_source_id"] == spec.DataSourceID && labels["generation"] == strconv.FormatUint(spec.Generation, 10)
}

func inspectEvidence(value containertypes.InspectResponse) InspectEvidence {
	e := InspectEvidence{}
	if value.State != nil {
		e.Running, e.Status, e.ExitCode, e.OOMKilled = value.State.Running, string(value.State.Status), value.State.ExitCode, value.State.OOMKilled
	}
	if value.Config != nil {
		e.User = value.Config.User
	}
	if value.HostConfig != nil {
		h := value.HostConfig
		e.ReadonlyRootfs, e.NetworkMode, e.CapDrop, e.CapAdd = h.ReadonlyRootfs, string(h.NetworkMode), h.CapDrop, h.CapAdd
		e.SecurityOpt, e.Memory, e.NanoCPUs, e.Privileged = h.SecurityOpt, h.Memory, h.NanoCPUs, h.Privileged
		if h.Init != nil {
			e.Init = *h.Init
		}
		if h.PidsLimit != nil {
			e.PidsLimit = *h.PidsLimit
		}
		e.Tmpfs = h.Tmpfs["/tmp"]
	}
	for _, item := range value.Mounts {
		switch item.Destination {
		case ContainerArtifact:
			e.ArtifactReadOnly = !item.RW
		case ContainerGrant:
			e.GrantReadOnly = !item.RW
		case "/run/weknora":
			e.RuntimeReadOnly = !item.RW
		}
	}
	return e
}

func validateSecurityEvidence(e InspectEvidence, spec PrototypeInstanceSpec) error {
	if !e.Running || e.User != spec.ContainerUser || !e.ReadonlyRootfs || e.NetworkMode != "none" || e.Privileged || !e.Init {
		return fmt.Errorf("inspect security invariant failed: %+v", e)
	}
	if len(e.CapDrop) != 1 || strings.ToUpper(e.CapDrop[0]) != "ALL" || len(e.CapAdd) != 0 {
		return errors.New("plugin capabilities are not drop ALL/add none")
	}
	if !contains(e.SecurityOpt, "no-new-privileges") || e.Memory != spec.MemoryBytes || e.NanoCPUs != int64(spec.CPUQuota*1e9) || e.PidsLimit != spec.PidsLimit {
		return errors.New("plugin security option or resource limits mismatch")
	}
	if e.Tmpfs != "rw,noexec,nosuid,nodev,size=16m" {
		return fmt.Errorf("plugin /tmp configuration mismatch: %q", e.Tmpfs)
	}
	if !e.ArtifactReadOnly || !e.GrantReadOnly || e.RuntimeReadOnly {
		return errors.New("plugin mount access mismatch")
	}
	return nil
}

func validateAuditEvents(events []network.AuditEvent, spec PrototypeInstanceSpec) error {
	want := map[string]bool{"ipv4/tcp/connect4": false, "ipv6/tcp/connect6": false, "ipv4/udp/sendmsg4": false, "ipv6/udp/sendmsg6": false}
	var cgroupID uint64
	for _, event := range events {
		key := event.Family + "/" + event.Protocol + "/" + event.Hook
		if _, ok := want[key]; ok {
			want[key] = true
		}
		if event.Identity.RunID != spec.RunID || event.Identity.PluginID != spec.PluginID || event.Identity.DataSourceID != spec.DataSourceID || event.Identity.Generation != spec.Generation {
			return errors.New("audit identity does not match instance spec")
		}
		if cgroupID == 0 {
			cgroupID = event.CgroupID
		} else if event.CgroupID != cgroupID {
			return errors.New("audit events contain different cgroup IDs")
		}
	}
	for key, seen := range want {
		if !seen {
			return fmt.Errorf("missing audit event %s", key)
		}
	}
	return nil
}

func validateFakeResult(result FakeResult, expectedContent string) error {
	if !result.AllowedRead || result.AllowedContent != expectedContent {
		return errors.New("fake plugin could not read allowed.txt")
	}
	if !result.WriteDenied || result.WriteError == "" {
		return errors.New("fake plugin write was not explicitly denied")
	}
	if !result.OutsideInvisible || result.OutsideError == "" {
		return errors.New("outside sentinel was visible")
	}
	want := map[string]bool{"ipv4_tcp": false, "ipv6_tcp": false, "ipv4_udp": false, "ipv6_udp": false}
	for _, attempt := range result.Attempts {
		if _, ok := want[attempt.Name]; ok && attempt.Error != "" && !strings.Contains(attempt.Error, "unexpected network success") {
			want[attempt.Name] = true
		}
	}
	for name, denied := range want {
		if !denied {
			return fmt.Errorf("network attempt %s was not explicitly denied", name)
		}
	}
	return nil
}

func artifactDigest(dir string) (string, error) {
	hash := sha256.New()
	for _, name := range []string{"gate-prototype", "fake-prototype"} {
		file, err := os.Open(filepath.Join(dir, name))
		if err != nil {
			return "", fmt.Errorf("open artifact %s: %w", name, err)
		}
		_, copyErr := io.Copy(hash, file)
		closeErr := file.Close()
		if copyErr != nil || closeErr != nil {
			return "", errors.Join(copyErr, closeErr)
		}
	}
	return "sha256:" + hex.EncodeToString(hash.Sum(nil)), nil
}

func waitForHealth(ctx context.Context, socketPath string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var last error
	for time.Now().Before(deadline) {
		if err := healthCheck(ctx, socketPath); err == nil {
			return nil
		} else {
			last = err
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("UDS health timeout: %w", last)
}

func healthCheck(ctx context.Context, socketPath string) error {
	callCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	conn, err := grpc.NewClient("passthrough:///plugin", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return DialUnix(socketPath)
	}))
	if err != nil {
		return err
	}
	defer conn.Close()
	response, err := healthpb.NewHealthClient(conn).Check(callCtx, &healthpb.HealthCheckRequest{})
	if err != nil {
		return err
	}
	if response.Status != healthpb.HealthCheckResponse_SERVING {
		return fmt.Errorf("health status %s", response.Status)
	}
	return nil
}

func waitJSON(path string, target any, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var last error
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(path)
		if err == nil {
			if len(data) > maxLogBytes {
				return errors.New("fake plugin result exceeds limit")
			}
			if err = json.Unmarshal(data, target); err == nil {
				return nil
			}
		}
		last = err
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("wait for JSON result: %w", last)
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
