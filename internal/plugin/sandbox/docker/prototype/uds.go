package prototype

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"syscall"
)

const maxUnixSocketPathBytes = 107

func RuntimeSocketPath(runtimeDir string) (string, error) {
	path := filepath.Join(runtimeDir, filepath.Base(ContainerSocket))
	if len([]byte(path)) > maxUnixSocketPathBytes {
		return "", fmt.Errorf("Unix socket path is %d bytes; maximum is %d", len([]byte(path)), maxUnixSocketPathBytes)
	}
	return path, nil
}

func PrepareRuntimeDir(runtimeDir, containerUser string) error {
	uid, gid, err := ParseContainerUser(containerUser)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(runtimeDir, 0o700); err != nil {
		return err
	}
	if err := os.Chmod(runtimeDir, 0o700); err != nil {
		return err
	}
	info, err := os.Stat(runtimeDir)
	if err != nil {
		return err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return errors.New("runtime directory owner is unavailable")
	}
	if int(stat.Uid) != uid || int(stat.Gid) != gid {
		return fmt.Errorf("runtime directory owner is %d:%d, want %d:%d", stat.Uid, stat.Gid, uid, gid)
	}
	return nil
}

func RemoveStaleSocket(socketPath string) error {
	info, err := os.Lstat(socketPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSocket == 0 {
		return errors.New("stale UDS path exists and is not a socket")
	}
	return os.Remove(socketPath)
}

func DialUnix(socketPath string) (net.Conn, error) {
	if len([]byte(socketPath)) > maxUnixSocketPathBytes {
		return nil, errors.New("Unix socket path is too long")
	}
	return net.Dial("unix", socketPath)
}
