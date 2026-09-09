// Package paths contains shared local plugin path and identity checks.
package paths

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

// PathMapping explicitly maps the path visible to a Controller to the source
// path interpreted by the local Docker daemon.
type PathMapping struct {
	AppRoot  string `json:"app_root" yaml:"app_root"`
	HostRoot string `json:"host_root" yaml:"host_root"`
}

func (m PathMapping) ValidatePair(appPath, hostPath string) error {
	if !filepath.IsAbs(appPath) || filepath.Clean(appPath) != appPath || !filepath.IsAbs(hostPath) || filepath.Clean(hostPath) != hostPath {
		return errors.New("mapped paths must be clean and absolute")
	}
	if !filepath.IsAbs(m.AppRoot) || !filepath.IsAbs(m.HostRoot) {
		return errors.New("mapping roots must be absolute")
	}
	if filepath.Clean(m.AppRoot) != m.AppRoot || filepath.Clean(m.HostRoot) != m.HostRoot {
		return errors.New("mapping roots must be clean")
	}
	rel, err := filepath.Rel(m.AppRoot, appPath)
	if err != nil || relEscapes(rel) {
		return errors.New("app path is outside app root")
	}
	want := filepath.Join(m.HostRoot, rel)
	if filepath.Clean(hostPath) != want {
		return fmt.Errorf("host path mismatch: got %q, want %q", hostPath, want)
	}
	return nil
}

func (m PathMapping) HostPath(appPath string) (string, error) {
	if !filepath.IsAbs(m.AppRoot) || !filepath.IsAbs(m.HostRoot) || filepath.Clean(m.AppRoot) != m.AppRoot || filepath.Clean(m.HostRoot) != m.HostRoot {
		return "", errors.New("mapping roots must be clean and absolute")
	}
	if !filepath.IsAbs(appPath) || filepath.Clean(appPath) != appPath {
		return "", errors.New("app path must be absolute")
	}
	rel, err := filepath.Rel(m.AppRoot, filepath.Clean(appPath))
	if err != nil || relEscapes(rel) {
		return "", errors.New("app path is outside app root")
	}
	return filepath.Join(m.HostRoot, rel), nil
}

func relEscapes(rel string) bool {
	return rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel)
}

// GrantSnapshot is the temporary, in-memory Directory Grant used by this prototype.
type GrantSnapshot struct {
	CanonicalPath string `json:"canonical_path"`
	Device        uint64 `json:"device"`
	Inode         uint64 `json:"inode"`
	Generation    uint64 `json:"generation"`
	Status        string `json:"status"`
}

// CreateGrant accepts only a relative SystemAdmin selection beneath allowRoot.
func CreateGrant(allowRoot, selectedRelative string, generation uint64) (GrantSnapshot, error) {
	if !filepath.IsAbs(allowRoot) || filepath.Clean(allowRoot) != allowRoot {
		return GrantSnapshot{}, errors.New("allow-root must be a clean absolute path")
	}
	if selectedRelative == "" || filepath.IsAbs(selectedRelative) {
		return GrantSnapshot{}, errors.New("selected grant path must be relative")
	}
	for _, component := range strings.Split(filepath.ToSlash(selectedRelative), "/") {
		if component == ".." {
			return GrantSnapshot{}, errors.New("selected grant path contains parent traversal")
		}
	}
	selected := filepath.Join(allowRoot, filepath.Clean(selectedRelative))
	rel, err := filepath.Rel(allowRoot, selected)
	if err != nil || relEscapes(rel) {
		return GrantSnapshot{}, errors.New("selected grant path is outside allow-root")
	}
	if err := rejectSymlinkComponents(allowRoot); err != nil {
		return GrantSnapshot{}, err
	}
	if err := rejectSymlinkComponents(selected); err != nil {
		return GrantSnapshot{}, err
	}
	info, err := os.Stat(selected)
	if err != nil {
		return GrantSnapshot{}, fmt.Errorf("stat selected grant path: %w", err)
	}
	if !info.IsDir() {
		return GrantSnapshot{}, errors.New("selected grant path must be a directory")
	}
	device, inode, err := fileIdentity(info)
	if err != nil {
		return GrantSnapshot{}, err
	}
	if generation == 0 {
		return GrantSnapshot{}, errors.New("grant generation must be greater than zero")
	}
	return GrantSnapshot{
		CanonicalPath: selected, Device: device, Inode: inode,
		Generation: generation, Status: "active",
	}, nil
}

// RevalidateGrant rejects revocation, symlink replacement, and device/inode changes.
func RevalidateGrant(grant GrantSnapshot) error {
	if grant.Status != "active" {
		return errors.New("grant is not active")
	}
	if grant.Generation == 0 {
		return errors.New("grant generation is invalid")
	}
	if err := rejectSymlinkComponents(grant.CanonicalPath); err != nil {
		return err
	}
	info, err := os.Stat(grant.CanonicalPath)
	if err != nil {
		return fmt.Errorf("stat grant path: %w", err)
	}
	device, inode, err := fileIdentity(info)
	if err != nil {
		return err
	}
	if device != grant.Device || inode != grant.Inode {
		return errors.New("grant device or inode changed")
	}
	return nil
}

func rejectSymlinkComponents(path string) error {
	clean := filepath.Clean(path)
	if !filepath.IsAbs(clean) {
		return errors.New("path must be absolute")
	}
	current := string(filepath.Separator)
	for _, component := range strings.Split(strings.TrimPrefix(clean, string(filepath.Separator)), string(filepath.Separator)) {
		if component == "" {
			continue
		}
		current = filepath.Join(current, component)
		info, err := os.Lstat(current)
		if err != nil {
			return fmt.Errorf("lstat path component %q: %w", current, err)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("path component %q is a symbolic link", current)
		}
	}
	return nil
}

func fileIdentity(info os.FileInfo) (uint64, uint64, error) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, 0, errors.New("filesystem does not expose device/inode")
	}
	return uint64(stat.Dev), stat.Ino, nil
}

// TrustedDirectory rejects replaceable directory chains. Sticky system temp
// ancestors are allowed only when the next component belongs to the admin.
// A plugin UID must differ from the administrator UID; files inside a Grant
// are intentionally not frozen and may be updated by the source owner.
func TrustedDirectory(path string, adminUID, pluginUID uint32) error {
	if adminUID == pluginUID {
		return errors.New("plugin and administrator UID must differ")
	}
	if filepath.Clean(path) != path {
		return errors.New("directory path is not canonical")
	}
	if err := rejectSymlinkComponents(path); err != nil {
		return err
	}
	for current := path; ; current = filepath.Dir(current) {
		info, err := os.Lstat(current)
		if err != nil {
			return err
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok || !info.IsDir() {
			return fmt.Errorf("not a directory: %s", current)
		}
		if stat.Uid != 0 && stat.Uid != adminUID {
			return fmt.Errorf("untrusted directory owner: %s", current)
		}
		if info.Mode().Perm()&0022 != 0 && !(stat.Uid == 0 && info.Mode()&os.ModeSticky != 0) {
			return fmt.Errorf("replaceable directory: %s", current)
		}
		if current == "/" {
			break
		}
	}
	return nil
}
