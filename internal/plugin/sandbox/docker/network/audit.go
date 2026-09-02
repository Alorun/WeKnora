// Package network contains the fixed cgroup eBPF deny-and-audit prototype.
package network

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/cilium/ebpf"
	"github.com/cilium/ebpf/link"
	"github.com/cilium/ebpf/ringbuf"
)

const (
	eventsPinName  = "events"
	maxAuditEvents = 64
)

var (
	//go:embed audit_bpf.o
	auditObject []byte

	programs = []struct {
		name       string
		attachType ebpf.AttachType
	}{
		{"deny_connect4", ebpf.AttachCGroupInet4Connect},
		{"deny_connect6", ebpf.AttachCGroupInet6Connect},
		{"deny_sendmsg4", ebpf.AttachCGroupUDP4Sendmsg},
		{"deny_sendmsg6", ebpf.AttachCGroupUDP6Sendmsg},
	}
)

// Identity is Host-owned metadata associated with a cgroup ID.
type Identity struct {
	RunID        string `json:"prototype_run_id"`
	PluginID     string `json:"plugin_id"`
	DataSourceID string `json:"data_source_id"`
	Generation   uint64 `json:"generation"`
}

// AuditEvent is the bounded userspace representation of one denied socket operation.
type AuditEvent struct {
	CgroupID        uint64    `json:"cgroup_id"`
	PID             uint32    `json:"pid"`
	UID             uint32    `json:"uid"`
	Family          string    `json:"family"`
	Protocol        string    `json:"protocol"`
	Hook            string    `json:"hook"`
	DestinationIP   string    `json:"destination_ip"`
	DestinationPort uint16    `json:"destination_port"`
	KernelTimeNS    uint64    `json:"kernel_time_ns"`
	ObservedAt      time.Time `json:"observed_at"`
	Identity        Identity  `json:"identity"`
}

type rawAuditEvent struct {
	CgroupID           uint64
	KernelTimeNS       uint64
	PID                uint32
	UID                uint32
	DestinationPort    uint16
	Family             uint8
	Protocol           uint8
	Hook               uint8
	Padding            [3]byte
	DestinationAddress [16]byte
}

// PinnedPolicy owns open handles to links and the ring buffer. Pins preserve
// enforcement across a Controller process restart.
type PinnedPolicy struct {
	pinRoot    string
	links      []link.Link
	eventsMap  *ebpf.Map
	reader     *ringbuf.Reader
	collection *ebpf.Collection
}

// AttachAndPin loads the fixed programs, attaches all four hooks, and pins all
// links plus the audit ring buffer before returning success.
func AttachAndPin(cgroupPath, pinRoot string) (_ *PinnedPolicy, err error) {
	if err := validatePinRoot(pinRoot); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(pinRoot, 0o700); err != nil {
		return nil, fmt.Errorf("create BPF pin root: %w", err)
	}

	spec, err := ebpf.LoadCollectionSpecFromReader(bytes.NewReader(auditObject))
	if err != nil {
		return nil, fmt.Errorf("load embedded BPF object: %w", err)
	}
	collection, err := ebpf.NewCollection(spec)
	if err != nil {
		return nil, fmt.Errorf("load BPF collection: %w", err)
	}
	policy := &PinnedPolicy{pinRoot: pinRoot, collection: collection}
	defer func() {
		if err != nil {
			_ = policy.Detach()
		}
	}()

	policy.eventsMap = collection.Maps["audit_events"]
	if policy.eventsMap == nil {
		return nil, errors.New("BPF object is missing audit_events map")
	}
	if err = policy.eventsMap.Pin(filepath.Join(pinRoot, eventsPinName)); err != nil {
		return nil, fmt.Errorf("pin audit map: %w", err)
	}

	for _, item := range programs {
		program := collection.Programs[item.name]
		if program == nil {
			return nil, fmt.Errorf("BPF object is missing program %s", item.name)
		}
		attached, attachErr := link.AttachCgroup(link.CgroupOptions{
			Path: cgroupPath, Attach: item.attachType, Program: program,
		})
		if attachErr != nil {
			return nil, fmt.Errorf("attach %s: %w", item.name, attachErr)
		}
		policy.links = append(policy.links, attached)
		if err = attached.Pin(filepath.Join(pinRoot, item.name)); err != nil {
			return nil, fmt.Errorf("pin %s: %w", item.name, err)
		}
	}

	policy.reader, err = ringbuf.NewReader(policy.eventsMap)
	if err != nil {
		return nil, fmt.Errorf("open audit ring buffer: %w", err)
	}
	return policy, nil
}

// OpenPinned simulates Controller recovery without reloading or reattaching programs.
func OpenPinned(pinRoot string) (_ *PinnedPolicy, err error) {
	if err := validatePinRoot(pinRoot); err != nil {
		return nil, err
	}
	policy := &PinnedPolicy{pinRoot: pinRoot}
	defer func() {
		if err != nil {
			_ = policy.CloseHandlesKeepPins()
		}
	}()

	policy.eventsMap, err = ebpf.LoadPinnedMap(filepath.Join(pinRoot, eventsPinName), nil)
	if err != nil {
		return nil, fmt.Errorf("open pinned audit map: %w", err)
	}
	for _, item := range programs {
		pinned, openErr := link.LoadPinnedLink(filepath.Join(pinRoot, item.name), nil)
		if openErr != nil {
			return nil, fmt.Errorf("open pinned %s: %w", item.name, openErr)
		}
		policy.links = append(policy.links, pinned)
	}
	policy.reader, err = ringbuf.NewReader(policy.eventsMap)
	if err != nil {
		return nil, fmt.Errorf("reopen audit ring buffer: %w", err)
	}
	return policy, nil
}

// ReadEvents reads exactly count events or returns when the context expires.
func (p *PinnedPolicy) ReadEvents(ctx context.Context, count int, identity Identity) ([]AuditEvent, error) {
	if p == nil || p.reader == nil {
		return nil, errors.New("audit reader is not open")
	}
	if count < 1 || count > maxAuditEvents {
		return nil, fmt.Errorf("audit event count must be between 1 and %d", maxAuditEvents)
	}
	events := make([]AuditEvent, 0, count)
	for len(events) < count {
		deadline := time.Now().Add(200 * time.Millisecond)
		if contextDeadline, ok := ctx.Deadline(); ok && contextDeadline.Before(deadline) {
			deadline = contextDeadline
		}
		p.reader.SetDeadline(deadline)
		record, err := p.reader.Read()
		if err != nil {
			if errors.Is(err, os.ErrDeadlineExceeded) {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				continue
			}
			return nil, fmt.Errorf("read audit ring buffer: %w", err)
		}
		event, err := decodeAuditEvent(record.RawSample, identity)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, nil
}

// CloseHandlesKeepPins models a Controller crash/restart: descriptors close,
// while pinned links keep enforcing the deny policy.
func (p *PinnedPolicy) CloseHandlesKeepPins() error {
	if p == nil {
		return nil
	}
	var errs []error
	if p.reader != nil {
		errs = appendError(errs, p.reader.Close())
		p.reader = nil
	}
	for _, item := range p.links {
		errs = appendError(errs, item.Close())
	}
	p.links = nil
	if p.collection != nil {
		p.collection.Close()
		p.collection = nil
		p.eventsMap = nil
	} else if p.eventsMap != nil {
		errs = appendError(errs, p.eventsMap.Close())
		p.eventsMap = nil
	}
	return errors.Join(errs...)
}

// Detach unpins and closes the fixed policy. It is idempotent for missing pins.
func (p *PinnedPolicy) Detach() error {
	if p == nil {
		return nil
	}
	var errs []error
	if p.reader != nil {
		errs = appendError(errs, p.reader.Close())
		p.reader = nil
	}
	for _, item := range p.links {
		if err := item.Unpin(); err != nil && !errors.Is(err, os.ErrNotExist) {
			errs = append(errs, err)
		}
		errs = appendError(errs, item.Close())
	}
	p.links = nil
	if p.eventsMap != nil {
		if err := p.eventsMap.Unpin(); err != nil && !errors.Is(err, os.ErrNotExist) {
			errs = append(errs, err)
		}
	}
	if p.collection != nil {
		p.collection.Close()
		p.collection = nil
		p.eventsMap = nil
	} else if p.eventsMap != nil {
		errs = appendError(errs, p.eventsMap.Close())
		p.eventsMap = nil
	}
	if err := os.Remove(p.pinRoot); err != nil && !errors.Is(err, os.ErrNotExist) {
		errs = append(errs, err)
	}
	parent := filepath.Dir(p.pinRoot)
	if parent == "/sys/fs/bpf/weknora-plugin-prototype" {
		if err := os.Remove(parent); err != nil && !errors.Is(err, os.ErrNotExist) && !errors.Is(err, syscall.ENOTEMPTY) {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// FindDockerCgroup resolves only a cgroup directory containing the full Docker ID.
func FindDockerCgroup(root, containerID string) (string, error) {
	if len(containerID) != 64 || strings.Trim(containerID, "0123456789abcdef") != "" {
		return "", errors.New("container ID must be 64 lowercase hexadecimal characters")
	}
	root = filepath.Clean(root)
	var found string
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !entry.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if rel != "." && strings.Count(rel, string(filepath.Separator)) >= 8 {
			return filepath.SkipDir
		}
		if strings.Contains(entry.Name(), containerID) {
			found = path
			return filepath.SkipAll
		}
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("walk cgroup v2 tree: %w", err)
	}
	if found == "" {
		return "", fmt.Errorf("cgroup for container %s not found", containerID)
	}
	return found, nil
}

// CurrentCgroupPath resolves the caller's cgroup in a host cgroup namespace.
func CurrentCgroupPath(root string) (string, error) {
	data, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return "", err
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "0::") {
			path := filepath.Join(root, strings.TrimPrefix(line, "0::"))
			if _, err := os.Stat(path); err != nil {
				return "", fmt.Errorf("stat current cgroup: %w", err)
			}
			return path, nil
		}
	}
	return "", errors.New("cgroup v2 entry not found")
}

// CgroupID returns the kernfs inode used by bpf_get_current_cgroup_id.
func CgroupID(path string) (uint64, error) {
	var stat syscall.Stat_t
	if err := syscall.Stat(path, &stat); err != nil {
		return 0, err
	}
	return stat.Ino, nil
}

func decodeAuditEvent(sample []byte, identity Identity) (AuditEvent, error) {
	var raw rawAuditEvent
	if len(sample) != binary.Size(raw) {
		return AuditEvent{}, fmt.Errorf("unexpected audit event size %d", len(sample))
	}
	if err := binary.Read(bytes.NewReader(sample), binary.LittleEndian, &raw); err != nil {
		return AuditEvent{}, fmt.Errorf("decode audit event: %w", err)
	}
	address := net.IP(raw.DestinationAddress[:])
	family := "ipv6"
	if raw.Family == syscall.AF_INET {
		family = "ipv4"
		address = net.IP(raw.DestinationAddress[:4])
	}
	protocol := fmt.Sprintf("ipproto_%d", raw.Protocol)
	if raw.Protocol == syscall.IPPROTO_TCP {
		protocol = "tcp"
	} else if raw.Protocol == syscall.IPPROTO_UDP {
		protocol = "udp"
	}
	hooks := map[uint8]string{1: "connect4", 2: "connect6", 3: "sendmsg4", 4: "sendmsg6"}
	return AuditEvent{
		CgroupID: raw.CgroupID, PID: raw.PID, UID: raw.UID,
		Family: family, Protocol: protocol, Hook: hooks[raw.Hook],
		DestinationIP: address.String(), DestinationPort: raw.DestinationPort,
		KernelTimeNS: raw.KernelTimeNS, ObservedAt: time.Now().UTC(), Identity: identity,
	}, nil
}

func validatePinRoot(path string) error {
	clean := filepath.Clean(path)
	if !filepath.IsAbs(clean) || clean == "/sys/fs/bpf" || !strings.HasPrefix(clean, "/sys/fs/bpf/") {
		return errors.New("BPF pin root must be a child of /sys/fs/bpf")
	}
	return nil
}

func appendError(errs []error, err error) []error {
	if err != nil {
		return append(errs, err)
	}
	return errs
}
