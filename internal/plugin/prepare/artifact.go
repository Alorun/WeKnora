// Package prepare builds validated, explicit startup snapshots. It does not
// start services, publish routes, or perform application startup orchestration.
package prepare

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"syscall"

	"github.com/Tencent/WeKnora/internal/plugin/control"
	pluginruntime "github.com/Tencent/WeKnora/internal/plugin/runtime"
	"github.com/Tencent/WeKnora/internal/plugin/sandbox/docker/paths"
)

const (
	MaxArtifactFiles       = 1024
	MaxArtifactBytes int64 = 128 << 20
	MaxManifestBytes int64 = 256 << 10
)

type Package struct {
	Manifest control.Manifest
	Artifact pluginruntime.ArtifactReference
}

type Discovery struct {
	Packages            paths.PathMapping
	Snapshots           paths.PathMapping
	Options             control.ManifestValidationOptions
	AdminUID, PluginUID uint32
}

type ScanResult struct {
	Path         string
	Package      *Package
	Installation control.PluginInstallation
}

// Scan returns candidate records, never enables/publishes a plugin. The C3
// caller persists the existing Installation model and coordinates Manager.
// Existing active IDs/digests are immutable, including on a new Scan object.
func (d Discovery) Scan(previous []control.PluginInstallation) ([]ScanResult, error) {
	if err := paths.TrustedDirectory(d.Packages.AppRoot, d.AdminUID, d.PluginUID); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(d.Packages.AppRoot)
	if err != nil {
		return nil, err
	}
	if len(entries) > 256 {
		return nil, errors.New("plugin directory count exceeds 256")
	}
	var results []ScanResult
	seen := map[control.PluginID]int{}
	for _, e := range entries {
		path := filepath.Join(d.Packages.AppRoot, e.Name())
		p, loadErr := d.Load(path)
		r := ScanResult{Path: path, Installation: control.PluginInstallation{InstallStatus: control.InstallStatusInvalid}}
		if loadErr == nil {
			r.Package = &p
			r.Installation.PluginID, r.Installation.Version, r.Installation.ArtifactDigest = p.Manifest.Metadata.ID, p.Manifest.Metadata.Version, p.Artifact.Digest
			r.Installation.InstallStatus = control.InstallStatusInstalled
			if idx, exists := seen[r.Installation.PluginID]; exists {
				loadErr = fmt.Errorf("duplicate plugin id also at %s", results[idx].Path)
				results[idx].Package = nil
				results[idx].Installation.InstallStatus, results[idx].Installation.LastError = control.InstallStatusInvalid, loadErr.Error()
			} else {
				seen[r.Installation.PluginID] = len(results)
			}
			for _, old := range previous {
				if old.PluginID == r.Installation.PluginID && old.Active {
					r.Installation = old
					if old.Version != p.Manifest.Metadata.Version || old.ArtifactDigest != p.Artifact.Digest {
						loadErr = errors.New("active plugin version/content changed; explicit reinstall required")
					}
				}
			}
		}
		if loadErr != nil {
			r.Package = nil
			r.Installation.InstallStatus = control.InstallStatusInvalid
			r.Installation.LastError = loadErr.Error()
		}
		results = append(results, r)
	}
	for _, old := range previous {
		if _, ok := seen[old.PluginID]; !ok && old.Active {
			old.InstallStatus, old.LastError = "missing", "plugin package missing or invalid in administrator directory"
			results = append(results, ScanResult{Installation: old})
		}
	}
	return results, nil
}

func (d Discovery) Load(source string) (Package, error) {
	var result Package
	if _, err := d.Packages.HostPath(source); err != nil {
		return result, err
	}
	if err := paths.TrustedDirectory(source, d.AdminUID, d.PluginUID); err != nil {
		return result, err
	}
	digest, files, err := digestTree(source)
	if err != nil {
		return result, err
	}
	f, err := os.Open(filepath.Join(source, "plugin.yaml"))
	if err != nil {
		return result, err
	}
	data, err := io.ReadAll(io.LimitReader(f, MaxManifestBytes+1))
	_ = f.Close()
	if err != nil || int64(len(data)) > MaxManifestBytes {
		return result, errors.New("manifest exceeds size limit or is unreadable")
	}
	opts := d.Options
	opts.ArtifactRoot = source
	m, err := control.ParseManifest(data, opts)
	if err != nil {
		return result, err
	}
	if err := paths.TrustedDirectory(d.Snapshots.AppRoot, d.AdminUID, d.PluginUID); err != nil {
		return result, err
	}
	target := filepath.Join(d.Snapshots.AppRoot, digest)
	if _, err := os.Stat(target); errors.Is(err, os.ErrNotExist) {
		tmp, err := os.MkdirTemp(d.Snapshots.AppRoot, ".snapshot-")
		if err != nil {
			return result, err
		}
		defer os.RemoveAll(tmp) // only this uniquely created, admin-owned snapshot
		for _, name := range files {
			in, err := os.Open(filepath.Join(source, name))
			if err != nil {
				return result, err
			}
			info, err := in.Stat()
			if err != nil {
				in.Close()
				return result, err
			}
			path := filepath.Join(tmp, name)
			if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
				in.Close()
				return result, err
			}
			out, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
			if err != nil {
				in.Close()
				return result, err
			}
			_, copyErr := io.Copy(out, io.LimitReader(in, MaxArtifactBytes+1))
			closeErr := out.Close()
			in.Close()
			if err := errors.Join(copyErr, closeErr); err != nil {
				return result, err
			}
			if err := os.Chmod(path, 0444|info.Mode().Perm()&0111); err != nil {
				return result, err
			}
		}
		copied, _, err := digestTree(tmp)
		if err != nil || copied != digest {
			return result, errors.New("artifact changed during snapshot")
		}
		if err := os.Chmod(tmp, 0755); err != nil {
			return result, err
		}
		if err := os.Rename(tmp, target); err != nil {
			return result, err
		}
	}
	info, err := os.Stat(target)
	if err != nil {
		return result, err
	}
	// The parsed manifest must be the one in the hashed snapshot, including
	// when another administrator changed the source during discovery.
	f, err = os.Open(filepath.Join(target, "plugin.yaml"))
	if err != nil {
		return result, err
	}
	frozenManifest, err := io.ReadAll(io.LimitReader(f, MaxManifestBytes+1))
	_ = f.Close()
	if err != nil || !bytes.Equal(data, frozenManifest) {
		return result, errors.New("manifest changed during snapshot")
	}
	st := info.Sys().(*syscall.Stat_t)
	host, err := d.Snapshots.HostPath(target)
	if err != nil {
		return result, err
	}
	result = Package{Manifest: m, Artifact: pluginruntime.ArtifactReference{Digest: digest, EntryPath: m.Spec.Runtime.Entrypoint, AppPath: target, HostPath: host, Device: uint64(st.Dev), Inode: st.Ino}}
	return result, VerifyArtifact(result.Artifact, d.AdminUID, d.PluginUID)
}

func VerifyArtifact(a pluginruntime.ArtifactReference, admin, plugin uint32) error {
	if err := paths.TrustedDirectory(a.AppPath, admin, plugin); err != nil {
		return err
	}
	if err := control.ValidateArtifactEntrypoint(a.AppPath, a.EntryPath); err != nil {
		return err
	}
	info, err := os.Stat(a.AppPath)
	if err != nil {
		return err
	}
	st := info.Sys().(*syscall.Stat_t)
	if uint64(st.Dev) != a.Device || st.Ino != a.Inode {
		return errors.New("artifact identity changed")
	}
	digest, _, err := digestTree(a.AppPath)
	if err != nil {
		return err
	}
	if digest != a.Digest {
		return errors.New("artifact digest changed")
	}
	return filepath.WalkDir(a.AppPath, func(path string, e os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := e.Info()
		if err != nil {
			return err
		}
		owner := info.Sys().(*syscall.Stat_t).Uid
		if (owner != 0 && owner != admin) || info.Mode().Perm()&0022 != 0 {
			return fmt.Errorf("artifact snapshot is not administrator-controlled: %s", path)
		}
		return nil
	})
}

func digestTree(root string) (string, []string, error) {
	var files []string
	var total int64
	entries := 0
	err := filepath.WalkDir(root, func(path string, e os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := e.Info()
		entries++
		if entries > MaxArtifactFiles+1 {
			return errors.New("artifact entry count exceeded")
		}
		if err != nil {
			return err
		}
		if e.IsDir() {
			return nil
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("artifact contains symlink or special file: %s", path)
		}
		total += info.Size()
		if len(files) >= MaxArtifactFiles || total > MaxArtifactBytes {
			return errors.New("artifact file/byte limit exceeded")
		}
		rel, _ := filepath.Rel(root, path)
		files = append(files, rel)
		return nil
	})
	if err != nil {
		return "", nil, err
	}
	sort.Strings(files)
	h := sha256.New()
	for _, name := range files {
		f, err := os.Open(filepath.Join(root, name))
		if err != nil {
			return "", nil, err
		}
		info, err := f.Stat()
		if err != nil {
			f.Close()
			return "", nil, err
		}
		fmt.Fprintf(h, "%d:%s:%d:%d:", len(name), name, info.Size(), info.Mode().Perm()&0111)
		n, err := io.Copy(h, io.LimitReader(f, info.Size()+1))
		f.Close()
		if err != nil || n != info.Size() {
			return "", nil, errors.New("artifact file changed while hashing")
		}
	}
	return hex.EncodeToString(h.Sum(nil)), files, nil
}
