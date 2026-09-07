// Package gate contains the trusted, fail-closed release gate.
package gate

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

var ErrReleaseTimeout = errors.New("gate release timed out")

type Config struct {
	ReleasePath string
	PluginPath  string
	SocketPath  string
	ResultPath  string
	StartedPath string
	Timeout     time.Duration
}

func (c Config) Validate() error {
	for name, path := range map[string]string{
		"release": c.ReleasePath, "plugin": c.PluginPath, "socket": c.SocketPath,
		"result": c.ResultPath, "started": c.StartedPath,
	} {
		if !filepath.IsAbs(path) || filepath.Clean(path) != path {
			return fmt.Errorf("%s path must be clean and absolute", name)
		}
	}
	if !strings.HasPrefix(c.PluginPath, "/opt/weknora/artifact/") {
		return errors.New("plugin path must be in the read-only artifact mount")
	}
	for _, path := range []string{c.ReleasePath, c.SocketPath, c.ResultPath, c.StartedPath} {
		if !strings.HasPrefix(path, "/run/weknora/") {
			return errors.New("gate runtime paths must be in /run/weknora")
		}
	}
	if c.Timeout < time.Second || c.Timeout > time.Minute {
		return errors.New("gate timeout must be between one second and one minute")
	}
	return nil
}

// Wait remains fail-closed until a regular release file appears.
func Wait(ctx context.Context, config Config) error {
	if err := config.Validate(); err != nil {
		return err
	}
	return WaitRelease(ctx, config.ReleasePath, config.Timeout)
}

// WaitRelease never releases on timeout or cancellation.
func WaitRelease(ctx context.Context, release string, timeout time.Duration) error {
	if !filepath.IsAbs(release) || timeout < time.Second || timeout > time.Minute {
		return errors.New("invalid gate release path or timeout")
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return ErrReleaseTimeout
		case <-ticker.C:
			info, err := os.Lstat(release)
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if err != nil {
				return err
			}
			if !info.Mode().IsRegular() {
				return errors.New("gate release path is not a regular file")
			}
			return nil
		}
	}
}

func PluginArgv(config Config) []string {
	return []string{
		config.PluginPath,
		"-socket", config.SocketPath,
		"-result", config.ResultPath,
		"-started", config.StartedPath,
	}
}
