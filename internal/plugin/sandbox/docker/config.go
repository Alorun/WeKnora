// Package docker implements the single-node Linux Docker plugin backend.
// Application wiring is intentionally separate (C3); no test fallback exists.
package docker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"

	"github.com/Tencent/WeKnora/internal/plugin/prepare"
	pluginruntime "github.com/Tencent/WeKnora/internal/plugin/runtime"
	"github.com/Tencent/WeKnora/internal/plugin/sandbox/docker/network"
	"github.com/Tencent/WeKnora/internal/plugin/sandbox/docker/paths"
	ct "github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	"github.com/moby/moby/client"
)

const (
	UDSBytes        = 4 << 20
	TmpBytes        = 16 << 20
	DiagnosticBytes = 32 << 10
	LogMaxSize      = "1m"
	LogMaxFiles     = "2"
)

var identifier = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,79}$`)

type Config struct {
	DeploymentID                   string
	DockerHost                     string
	Image                          string // administrator-supplied, resolved to a local immutable image ID
	GateAppPath, GateHostPath      string // trusted, administrator-built executable
	RuntimeRoot                    paths.PathMapping
	ArtifactRoot                   paths.PathMapping
	GrantRoots                     []paths.PathMapping
	AdminUID, PluginUID, PluginGID uint32
	MaxInstances                   int
	MaxResources                   pluginruntime.ResourceLimits
	// Synchronous, bounded-latency trusted sink. It must return delivery errors.
	// There is no unbounded userspace queue; kernel ring capacity is 1 MiB.
	Audit func(context.Context, network.AuditEvent) error
}

func (c Config) validate() error {
	if !identifier.MatchString(c.DeploymentID) || c.Image == "" || c.Audit == nil || c.MaxInstances < 1 || c.MaxInstances > 64 {
		return errors.New("deployment, fixed image, audit sink and instance limit (1..64) required")
	}
	if c.PluginUID == 0 || c.PluginGID == 0 || c.AdminUID == c.PluginUID {
		return errors.New("plugin UID/GID must be non-root and distinct from administrator")
	}
	if c.DockerHost != "" && !strings.HasPrefix(c.DockerHost, "unix:///") {
		return errors.New("only a local Unix Docker socket is supported")
	}
	for _, m := range append([]paths.PathMapping{c.RuntimeRoot, c.ArtifactRoot}, c.GrantRoots...) {
		if err := m.ValidatePair(m.AppRoot, m.HostRoot); err != nil {
			return err
		}
		if err := paths.TrustedDirectory(m.AppRoot, c.AdminUID, c.PluginUID); err != nil {
			return err
		}
	}
	if len(c.GrantRoots) == 0 {
		return errors.New("administrator grant roots required")
	}
	if !filepath.IsAbs(c.GateHostPath) || filepath.Clean(c.GateHostPath) != c.GateHostPath {
		return errors.New("gate host path required")
	}
	if err := paths.TrustedDirectory(filepath.Dir(c.GateAppPath), c.AdminUID, c.PluginUID); err != nil {
		return err
	}
	f, err := os.Lstat(c.GateAppPath)
	if err != nil {
		return err
	}
	if !f.Mode().IsRegular() || f.Mode().Perm()&0111 == 0 || f.Mode().Perm()&0022 != 0 {
		return errors.New("gate must be an administrator-controlled executable")
	}
	st, ok := f.Sys().(*syscall.Stat_t)
	if !ok || (st.Uid != 0 && st.Uid != c.AdminUID) {
		return errors.New("gate owner is not trusted")
	}
	return nil
}

func (b *Backend) validateSpec(s pluginruntime.InstanceSpec) error {
	if err := s.Validate(); err != nil {
		return err
	}
	for _, id := range []string{s.PluginID, s.DataSourceID, s.ExtensionID} {
		if !identifier.MatchString(id) {
			return errors.New("unsafe instance identity")
		}
	}
	if s.Permissions.Network != "none" || s.Permissions.Filesystem != "selected_directory_readonly" || s.Grant == nil {
		return errors.New("only network:none and a read-only Directory Grant are supported")
	}
	if err := prepare.ValidateLimits(s.Resources, b.config.MaxResources); err != nil {
		return err
	}
	want := filepath.Join(b.config.RuntimeRoot.AppRoot, s.DataSourceID, strconv.FormatUint(s.Generation, 10))
	if s.RuntimeAppPath != want {
		return errors.New("runtime path does not match instance identity")
	}
	if err := b.config.RuntimeRoot.ValidatePair(s.RuntimeAppPath, s.RuntimeHostPath); err != nil {
		return err
	}
	if len(filepath.Join(s.RuntimeAppPath, "plugin.sock")) > 107 {
		return errors.New("UDS path exceeds Linux 107-byte limit")
	}
	if err := b.config.ArtifactRoot.ValidatePair(s.Artifact.AppPath, s.Artifact.HostPath); err != nil {
		return err
	}
	if err := prepare.VerifyArtifact(s.Artifact, b.config.AdminUID, b.config.PluginUID); err != nil {
		return err
	}
	if s.Grant.ID == "" || s.Grant.Generation == 0 {
		return errors.New("grant identity required")
	}
	matched := false
	for _, root := range b.config.GrantRoots {
		if root.ValidatePair(s.Grant.AppPath, s.Grant.HostPath) == nil {
			matched = true
			break
		}
	}
	if !matched {
		return errors.New("grant path has no explicit administrator mapping")
	}
	if err := paths.TrustedDirectory(s.Grant.AppPath, b.config.AdminUID, b.config.PluginUID); err != nil {
		return err
	}
	return paths.RevalidateGrant(paths.GrantSnapshot{CanonicalPath: s.Grant.AppPath, Device: s.Grant.Device, Inode: s.Grant.Inode, Generation: s.Grant.Generation, Status: "active"})
}

func (b *Backend) createOptions(s pluginruntime.InstanceSpec, labels map[string]string) client.ContainerCreateOptions {
	init, pids := true, s.Resources.PidsLimit
	return client.ContainerCreateOptions{
		Config: &ct.Config{Image: b.imageID, User: fmt.Sprintf("%d:%d", b.config.PluginUID, b.config.PluginGID),
			Entrypoint: []string{"/opt/weknora/gate"}, Cmd: []string{"-entry", s.Artifact.EntryPath}, Labels: labels,
			Env: []string{}, WorkingDir: "/", StopSignal: "SIGTERM", Healthcheck: &ct.HealthConfig{Test: []string{"NONE"}}},
		HostConfig: &ct.HostConfig{NetworkMode: "none", CapDrop: []string{"ALL"}, SecurityOpt: []string{"no-new-privileges"},
			ReadonlyRootfs: true, Init: &init, ShmSize: 1 << 20, CgroupnsMode: ct.CgroupnsModePrivate,
			Tmpfs:     map[string]string{"/tmp": "rw,noexec,nosuid,nodev,size=16m"},
			LogConfig: ct.LogConfig{Type: "json-file", Config: map[string]string{"max-size": LogMaxSize, "max-file": LogMaxFiles}},
			Resources: ct.Resources{Memory: s.Resources.MemoryBytes, MemorySwap: s.Resources.MemoryBytes, NanoCPUs: int64(s.Resources.CPUQuota * 1e9), PidsLimit: &pids},
			Mounts: []mount.Mount{
				{Type: mount.TypeBind, Source: b.config.GateHostPath, Target: "/opt/weknora/gate", ReadOnly: true},
				{Type: mount.TypeBind, Source: s.Artifact.HostPath, Target: "/opt/weknora/artifact", ReadOnly: true, BindOptions: &mount.BindOptions{ReadOnlyForceRecursive: true}},
				{Type: mount.TypeBind, Source: s.Grant.HostPath, Target: "/data/source", ReadOnly: true, BindOptions: &mount.BindOptions{ReadOnlyForceRecursive: true}},
				{Type: mount.TypeBind, Source: s.RuntimeHostPath, Target: "/run/weknora"},
			},
		},
	}
}
