// Test-only SDK plugin. Compiled by the integration runner, never installed in
// a deployment image or registered in the production Catalog.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	pb "github.com/Tencent/WeKnora/pkg/plugin/proto/v1"
	"github.com/Tencent/WeKnora/pkg/plugin/sdk"
	"google.golang.org/grpc"
)

type probe struct {
	pb.UnimplementedDataSourcePluginServer
	ignore atomic.Bool
}

func main() {
	if os.Getenv("PROBE_CHILD") == "1" {
		time.Sleep(10 * time.Second)
		return
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	data, err := os.ReadFile(os.Getenv("WEKNORA_PLUGIN_BOOTSTRAP"))
	if err != nil {
		panic(err)
	}
	var boot struct {
		PluginID, PluginVersion, ExtensionID, ProtocolVersion, ContractVersion string
		StartupNonce                                                           []byte
		Capabilities                                                           []string
	}
	if err := json.Unmarshal(data, &boot); err != nil {
		panic(err)
	}
	_ = os.WriteFile("/run/weknora/probe.started", []byte(time.Now().UTC().Format(time.RFC3339Nano)), 0600)
	p := &probe{}
	control, err := sdk.NewControlServer(sdk.Identity{PluginID: "test.runtime-probe", PluginVersion: "1.0.0", ExtensionID: "runtime_probe",
		ExtensionType: pb.ExtensionType_EXTENSION_TYPE_DATASOURCE, ProtocolVersion: boot.ProtocolVersion, ContractVersion: boot.ContractVersion, StartupNonce: boot.StartupNonce, SupportedCapabilities: boot.Capabilities}, sdk.ControlHooks{
		ValidateConfig: func(ctx context.Context, b []byte) error {
			if strings.Contains(string(b), "slow_validation") {
				_ = os.WriteFile("/run/weknora/validation.started", []byte("validating"), 0600)
				<-ctx.Done()
				return ctx.Err()
			}
			if strings.Contains(string(b), "fail_validation") {
				return fmt.Errorf("probe validation failure")
			}
			return nil
		},
		Shutdown: func(ctx context.Context, _ time.Duration) error {
			if p.ignore.Load() {
				<-ctx.Done()
				return ctx.Err()
			}
			time.AfterFunc(50*time.Millisecond, cancel)
			return nil
		},
	})
	if err != nil {
		panic(err)
	}
	s, err := sdk.ServeUDS(ctx, "/run/weknora/plugin.sock", sdk.Services{Control: control, DataSource: p})
	if err != nil {
		panic(err)
	}
	<-s.Done()
}

func (p *probe) Sync(req *pb.SyncRequest, stream grpc.ServerStreamingServer[pb.SyncEvent]) error {
	var cfg struct{ Mode string }
	if err := json.Unmarshal(req.ConfigJson, &cfg); err != nil {
		return err
	}
	result := map[string]any{}
	switch cfg.Mode {
	case "network":
		for _, family := range []string{"4", "6"} {
			target := "203.0.113.1:443"
			if family == "6" {
				target = "[2001:db8::1]:443"
			}
			conn, err := net.DialTimeout("tcp"+family, target, time.Second)
			if conn != nil {
				conn.Close()
			}
			result["tcp"+family] = fmt.Sprint(err)
			addr, err := net.ResolveUDPAddr("udp"+family, target)
			if err != nil {
				return err
			}
			udp, err := net.ListenUDP("udp"+family, nil)
			if err != nil {
				return err
			}
			_, err = udp.WriteToUDP([]byte("deny"), addr)
			udp.Close()
			result["udp"+family] = fmt.Sprint(err)
		}
	case "isolation":
		data, err := os.ReadFile("/data/source/allowed.txt")
		result["read"] = string(data)
		result["read_error"] = fmt.Sprint(err)
		for name, path := range map[string]string{"grant": "/data/source/write-denied.txt", "artifact": "/opt/weknora/artifact/write-denied.txt", "root": "/write-denied.txt"} {
			result[name] = fmt.Sprint(os.WriteFile(path, []byte("no"), 0600))
		}
		_, err = os.ReadFile("/data/outside-sentinel.txt")
		result["outside"] = fmt.Sprint(err)
	case "space":
		for _, path := range []string{"/tmp/fill", "/run/weknora/fill"} {
			f, err := os.Create(path)
			if err != nil {
				return err
			}
			buf := make([]byte, 64<<10)
			n := 0
			for n < 400 {
				if _, err = f.Write(buf); err != nil {
					break
				}
				n++
			}
			f.Close()
			os.Remove(path)
			result[path] = fmt.Sprint(err)
			result[path+"_blocks"] = n
		}
	case "cpu":
		var wg sync.WaitGroup
		for n := 0; n < 2; n++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				end := time.Now().Add(3 * time.Second)
				var x uint64
				for time.Now().Before(end) {
					for i := 0; i < 10000; i++ {
						x = x*1664525 + 1013904223
					}
				}
				runtime.KeepAlive(x)
			}()
		}
		wg.Wait()
		result["cpu"] = "done"
	case "memory":
		// Touch pages, at most 256 MiB, under a verified 64 MiB hard limit.
		var pages [][]byte
		for i := 0; i < 256; i++ {
			b := make([]byte, 1<<20)
			for j := 0; j < len(b); j += 4096 {
				b[j] = 1
			}
			pages = append(pages, b)
		}
		runtime.KeepAlive(pages)
		result["memory"] = "allocation survived"
	case "pid":
		var children []*exec.Cmd
		defer func() {
			for _, c := range children {
				c.Process.Kill()
				c.Wait()
			}
		}()
		for n := 0; n < 40; n++ {
			c := exec.Command(os.Args[0])
			c.Env = []string{"PROBE_CHILD=1", "GOMAXPROCS=1"}
			if err := c.Start(); err != nil {
				result["pid_error"] = err.Error()
				break
			}
			children = append(children, c)
		}
		result["children"] = len(children)
	case "logs":
		line := strings.Repeat("x", 1023) + "\n"
		for i := 0; i < 4096; i++ {
			fmt.Print(line)
		}
		result["log_bytes"] = 4 << 20
	case "ignore_shutdown":
		p.ignore.Store(true)
		signal.Ignore(syscall.SIGTERM)
		result["ignoring"] = true
	case "sigkill":
		syscall.Kill(os.Getpid(), syscall.SIGKILL)
	case "crash":
		os.Exit(42)
	default:
		result["sync"] = "ok"
	}
	body, err := json.Marshal(result)
	if err != nil {
		return err
	}
	return stream.Send(&pb.SyncEvent{Event: &pb.SyncEvent_Upsert{Upsert: &pb.DocumentUpsert{ExternalId: "probe", Revision: "1", Content: body, ContentType: "application/json"}}})
}
