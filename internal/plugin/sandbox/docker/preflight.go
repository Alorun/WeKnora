package docker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/Tencent/WeKnora/internal/plugin/sandbox/docker/network"
	"golang.org/x/sys/unix"
)

// CheckEnvironment checks mandatory privileges before an explicitly enabled
// Controller admits work, even when no Binding exists yet. It never attaches a
// deny program to the application's/another workload's cgroup.
func (b *Backend) CheckEnvironment(ctx context.Context) (result error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	var caps [2]unix.CapUserData
	if err := unix.Capget(&unix.CapUserHeader{Version: unix.LINUX_CAPABILITY_VERSION_3}, &caps[0]); err != nil {
		return err
	}
	for _, cap := range []uint{unix.CAP_SYS_ADMIN, unix.CAP_NET_ADMIN, unix.CAP_BPF, unix.CAP_PERFMON} {
		if caps[cap/32].Effective&(1<<(cap%32)) == 0 {
			return fmt.Errorf("permission_enforcement_unavailable: missing capability %d", cap)
		}
	}
	var fs unix.Statfs_t
	if err := unix.Statfs("/sys/fs/bpf", &fs); err != nil {
		return err
	}
	if fs.Type != unix.BPF_FS_MAGIC {
		return errors.New("bpffs is not mounted")
	}
	if err := unix.Access("/sys/fs/bpf", unix.W_OK); err != nil {
		return err
	}
	if err := network.CheckLoad(); err != nil {
		return fmt.Errorf("permission_enforcement_unavailable: %w", err)
	}
	dir, err := os.MkdirTemp(b.config.RuntimeRoot.AppRoot, ".preflight-")
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, os.Remove(dir)) }()
	if err := unix.Mount("tmpfs", dir, "tmpfs", unix.MS_NOSUID|unix.MS_NODEV|unix.MS_NOEXEC, "size=4m,mode=0700"); err != nil {
		return err
	}
	defer func() { result = errors.Join(result, unix.Unmount(dir, 0)) }()
	// cgroup visibility is required for the existing Docker cgroup resolver.
	data, err := os.ReadFile("/sys/fs/cgroup/cgroup.controllers")
	if err != nil {
		return err
	}
	for _, name := range [][]byte{[]byte("cpu"), []byte("memory"), []byte("pids")} {
		if !bytes.Contains(data, name) {
			return errors.New("required cgroup controller unavailable")
		}
	}
	return nil
}
