// Package prototype is an isolated stage-1 Docker/UDS/Grant/eBPF feasibility prototype.
package prototype

import (
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

const (
	WorkloadKind       = "plugin_prototype"
	ControllerWorkload = "plugin_prototype_controller"
	ContainerSocket    = "/run/weknora/plugin.sock"
	ContainerGrant     = "/data/source"
	ContainerArtifact  = "/opt/weknora/artifact"
	DefaultMemoryBytes = 256 * 1024 * 1024
	DefaultCPUQuota    = 0.5
	DefaultPidsLimit   = 32
)

var safeIdentifier = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`)

// PrototypeInstanceSpec is deliberately explicit: the prototype never looks
// up artifacts, permissions, grants, or limits from a RuntimeKey.
type PrototypeInstanceSpec struct {
	RunID              string        `json:"run_id"`
	PluginID           string        `json:"plugin_id"`
	DataSourceID       string        `json:"data_source_id"`
	Generation         uint64        `json:"generation"`
	Image              string        `json:"image"`
	ArtifactAppPath    string        `json:"artifact_app_path"`
	ArtifactHostPath   string        `json:"artifact_host_path"`
	GrantAppPath       string        `json:"grant_app_path"`
	GrantHostPath      string        `json:"grant_host_path"`
	RuntimeAppPath     string        `json:"runtime_app_path"`
	RuntimeHostPath    string        `json:"runtime_host_path"`
	ControllerAppPath  string        `json:"controller_app_path"`
	ControllerHostPath string        `json:"controller_host_path"`
	ArtifactMapping    PathMapping   `json:"artifact_mapping"`
	GrantMapping       PathMapping   `json:"grant_mapping"`
	RuntimeMapping     PathMapping   `json:"runtime_mapping"`
	Grant              GrantSnapshot `json:"grant"`
	ContainerUser      string        `json:"container_user"`
	MemoryBytes        int64         `json:"memory_bytes"`
	CPUQuota           float64       `json:"cpu_quota"`
	PidsLimit          int64         `json:"pids_limit"`
	NetworkPolicy      string        `json:"network_policy"`
	BPFPinRoot         string        `json:"bpf_pin_root"`
}

func (s PrototypeInstanceSpec) Validate() error {
	for name, value := range map[string]string{
		"run_id": s.RunID, "plugin_id": s.PluginID, "data_source_id": s.DataSourceID,
	} {
		if !safeIdentifier.MatchString(value) {
			return fmt.Errorf("%s is missing or unsafe", name)
		}
	}
	if s.Generation == 0 {
		return errors.New("generation must be greater than zero")
	}
	if strings.TrimSpace(s.Image) == "" {
		return errors.New("image is required")
	}
	if s.NetworkPolicy != "none" {
		return errors.New("prototype only supports network policy none")
	}
	if s.MemoryBytes != DefaultMemoryBytes || s.CPUQuota != DefaultCPUQuota || s.PidsLimit != DefaultPidsLimit {
		return errors.New("prototype resource limits must be 256MiB, 0.5 CPU, and 32 PIDs")
	}
	uid, gid, err := ParseContainerUser(s.ContainerUser)
	if err != nil {
		return err
	}
	if uid == 0 || gid == 0 {
		return errors.New("plugin container user and group must be non-root")
	}
	for name, value := range map[string]string{
		"artifact_app_path": s.ArtifactAppPath, "artifact_host_path": s.ArtifactHostPath,
		"grant_app_path": s.GrantAppPath, "grant_host_path": s.GrantHostPath,
		"runtime_app_path": s.RuntimeAppPath, "runtime_host_path": s.RuntimeHostPath,
		"controller_app_path": s.ControllerAppPath, "controller_host_path": s.ControllerHostPath, "bpf_pin_root": s.BPFPinRoot,
	} {
		if !filepath.IsAbs(value) || filepath.Clean(value) != value {
			return fmt.Errorf("%s must be a clean absolute path", name)
		}
	}
	if err := s.ArtifactMapping.ValidatePair(s.ArtifactAppPath, s.ArtifactHostPath); err != nil {
		return fmt.Errorf("artifact mapping: %w", err)
	}
	if err := s.GrantMapping.ValidatePair(s.GrantAppPath, s.GrantHostPath); err != nil {
		return fmt.Errorf("grant mapping: %w", err)
	}
	if err := s.RuntimeMapping.ValidatePair(s.RuntimeAppPath, s.RuntimeHostPath); err != nil {
		return fmt.Errorf("runtime mapping: %w", err)
	}
	if s.Grant.CanonicalPath != s.GrantAppPath {
		return errors.New("grant snapshot does not match grant app path")
	}
	if err := RevalidateGrant(s.Grant); err != nil {
		return fmt.Errorf("grant revalidation: %w", err)
	}
	if !strings.HasPrefix(s.BPFPinRoot, "/sys/fs/bpf/") {
		return errors.New("BPF pin root must be below /sys/fs/bpf")
	}
	return nil
}

func ParseContainerUser(value string) (int, int, error) {
	parts := strings.Split(value, ":")
	if len(parts) != 2 {
		return 0, 0, errors.New("container_user must be numeric UID:GID")
	}
	uid, err := strconv.Atoi(parts[0])
	if err != nil || uid < 0 {
		return 0, 0, errors.New("container_user has invalid UID")
	}
	gid, err := strconv.Atoi(parts[1])
	if err != nil || gid < 0 {
		return 0, 0, errors.New("container_user has invalid GID")
	}
	return uid, gid, nil
}

func labelsForSpec(spec PrototypeInstanceSpec, digest string) map[string]string {
	return map[string]string{
		"managed":          "true",
		"workload_kind":    WorkloadKind,
		"prototype_run_id": spec.RunID,
		"plugin_id":        spec.PluginID,
		"data_source_id":   spec.DataSourceID,
		"generation":       strconv.FormatUint(spec.Generation, 10),
		"artifact_digest":  digest,
	}
}
