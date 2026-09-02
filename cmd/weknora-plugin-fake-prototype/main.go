package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

const (
	allowedPath = "/data/source/allowed.txt"
	writePath   = "/data/source/write-denied.txt"
	outsidePath = "/data/outside-sentinel.txt"
	maxErrorLen = 512
)

type attempt struct {
	Name   string `json:"name"`
	Target string `json:"target"`
	Error  string `json:"error"`
}

type result struct {
	StartedAt        time.Time `json:"started_at"`
	AllowedRead      bool      `json:"allowed_read"`
	AllowedContent   string    `json:"allowed_content"`
	WriteDenied      bool      `json:"write_denied"`
	WriteError       string    `json:"write_error"`
	OutsideInvisible bool      `json:"outside_invisible"`
	OutsideError     string    `json:"outside_error"`
	Attempts         []attempt `json:"network_attempts"`
}

func main() {
	var socketPath, resultPath, startedPath string
	flag.StringVar(&socketPath, "socket", "", "UDS path")
	flag.StringVar(&resultPath, "result", "", "result path")
	flag.StringVar(&startedPath, "started", "", "start marker")
	flag.Parse()
	if socketPath == "" || resultPath == "" || startedPath == "" || flag.NArg() != 0 {
		fatal("required fixed paths are missing", 64)
	}

	startedAt := time.Now().UTC()
	if err := os.WriteFile(startedPath, []byte(startedAt.Format(time.RFC3339Nano)), 0o600); err != nil {
		fatal(fmt.Sprintf("write start marker: %v", err), 70)
	}

	report := runChecks(startedAt)
	if err := writeResult(resultPath, report); err != nil {
		fatal(fmt.Sprintf("write result: %v", err), 70)
	}
	_ = os.Remove(socketPath)
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		fatal(fmt.Sprintf("listen UDS: %v", err), 71)
	}
	if err := os.Chmod(socketPath, 0o600); err != nil {
		_ = listener.Close()
		fatal(fmt.Sprintf("chmod UDS: %v", err), 71)
	}

	server := grpc.NewServer()
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(server, healthServer)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	go func() {
		<-ctx.Done()
		healthServer.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
		server.GracefulStop()
	}()
	if err := server.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		fatal(fmt.Sprintf("serve UDS health: %v", err), 72)
	}
}

func runChecks(startedAt time.Time) result {
	report := result{StartedAt: startedAt}
	content, err := os.ReadFile(allowedPath)
	report.AllowedRead = err == nil
	if err == nil {
		report.AllowedContent = string(content)
	}
	err = os.WriteFile(writePath, []byte("must not be written"), 0o600)
	report.WriteDenied = err != nil
	report.WriteError = boundedError(err)
	_, err = os.ReadFile(outsidePath)
	report.OutsideInvisible = errors.Is(err, os.ErrNotExist)
	report.OutsideError = boundedError(err)
	report.Attempts = []attempt{
		tcpAttempt("ipv4_tcp", "tcp4", "203.0.113.1:443"),
		tcpAttempt("ipv6_tcp", "tcp6", "[2001:db8::1]:443"),
		udpAttempt("ipv4_udp", "udp4", net.IPv4(203, 0, 113, 1), 443),
		udpAttempt("ipv6_udp", "udp6", net.ParseIP("2001:db8::1"), 443),
	}
	return report
}

func tcpAttempt(name, network, target string) attempt {
	conn, err := net.DialTimeout(network, target, 750*time.Millisecond)
	if conn != nil {
		_ = conn.Close()
	}
	if err == nil {
		err = errors.New("unexpected network success")
	}
	return attempt{Name: name, Target: target, Error: boundedError(err)}
}

func udpAttempt(name, network string, ip net.IP, port int) attempt {
	local := &net.UDPAddr{IP: net.IPv4zero, Port: 0}
	if network == "udp6" {
		local.IP = net.IPv6unspecified
	}
	conn, err := net.ListenUDP(network, local)
	if err == nil {
		_, err = conn.WriteToUDP([]byte("weknora-plugin-prototype"), &net.UDPAddr{IP: ip, Port: port})
		_ = conn.Close()
	}
	if err == nil {
		err = errors.New("unexpected network success")
	}
	target := net.JoinHostPort(ip.String(), fmt.Sprint(port))
	return attempt{Name: name, Target: target, Error: boundedError(err)}
}

func writeResult(path string, value result) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if len(data) > 32*1024 {
		return errors.New("result exceeds 32 KiB")
	}
	temporary := path + ".tmp"
	if err := os.WriteFile(temporary, data, 0o600); err != nil {
		return err
	}
	return os.Rename(temporary, path)
}

func boundedError(err error) string {
	if err == nil {
		return ""
	}
	message := err.Error()
	if len(message) > maxErrorLen {
		return message[:maxErrorLen]
	}
	return message
}

func fatal(message string, code int) {
	_ = json.NewEncoder(os.Stderr).Encode(map[string]any{
		"component": "fake-plugin", "error": message, "exit_code": code,
		"base": filepath.Base(os.Args[0]),
	})
	os.Exit(code)
}
