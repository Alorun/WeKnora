//go:build integration

package network

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCgroupDenyAudit(t *testing.T) {
	if os.Getenv("WEKNORA_EBPF_INTEGRATION") != "1" {
		t.Skip("set WEKNORA_EBPF_INTEGRATION=1 inside the controlled helper container")
	}
	runID := os.Getenv("WEKNORA_PROTOTYPE_RUN_ID")
	if runID == "" {
		t.Fatal("WEKNORA_PROTOTYPE_RUN_ID is required")
	}
	cgroupPath, err := CurrentCgroupPath("/sys/fs/cgroup")
	if err != nil {
		t.Fatal(err)
	}
	pinRoot := filepath.Join("/sys/fs/bpf/weknora-plugin-prototype", runID+"-network-test")
	policy, err := AttachAndPin(cgroupPath, pinRoot)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := policy.Detach(); err != nil {
			t.Errorf("detach: %v", err)
		}
	}()

	errorsByAttempt := map[string]string{
		"ipv4/tcp": tcpAttempt("tcp4", "203.0.113.1:443"),
		"ipv6/tcp": tcpAttempt("tcp6", "[2001:db8::1]:443"),
		"ipv4/udp": udpAttempt("udp4", "203.0.113.1:443"),
		"ipv6/udp": udpAttempt("udp6", "[2001:db8::1]:443"),
	}
	for name, result := range errorsByAttempt {
		if len(result) >= len("unexpected success") && result[:len("unexpected success")] == "unexpected success" {
			t.Errorf("%s was not denied", name)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	events, err := policy.ReadEvents(ctx, 4, Identity{RunID: runID, PluginID: "network-test", DataSourceID: "network-test", Generation: 1})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{"ipv4/tcp/connect4": false, "ipv6/tcp/connect6": false, "ipv4/udp/sendmsg4": false, "ipv6/udp/sendmsg6": false}
	for _, event := range events {
		want[event.Family+"/"+event.Protocol+"/"+event.Hook] = true
		if event.Identity.RunID != runID {
			t.Errorf("event identity mismatch: %+v", event.Identity)
		}
	}
	for key, seen := range want {
		if !seen {
			t.Errorf("missing event %s; events=%+v errors=%+v", key, events, errorsByAttempt)
		}
	}
	t.Logf("denials=%+v events=%+v", errorsByAttempt, events)
}

func tcpAttempt(network, target string) string {
	connection, err := net.DialTimeout(network, target, time.Second)
	if err != nil {
		return err.Error()
	}
	_ = connection.Close()
	return "unexpected success"
}

func udpAttempt(network, target string) string {
	address, err := net.ResolveUDPAddr(network, target)
	if err != nil {
		return err.Error()
	}
	connection, err := net.ListenUDP(network, nil)
	if err != nil {
		return err.Error()
	}
	defer connection.Close()
	if _, err = connection.WriteToUDP([]byte("weknora-prototype"), address); err != nil {
		return err.Error()
	}
	return fmt.Sprintf("unexpected success to %s", target)
}
