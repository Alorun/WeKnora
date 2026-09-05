package sdk

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"

	pluginv1 "github.com/Tencent/WeKnora/pkg/plugin/proto/v1"
	"google.golang.org/grpc"
)

type Services struct {
	Control    pluginv1.PluginControlServer
	DataSource pluginv1.DataSourcePluginServer
}

type Server struct {
	grpc     *grpc.Server
	listener net.Listener
	path     string
	once     sync.Once
	done     chan error
}

// ServeUDS starts the two fixed V1 services on a Unix socket. It never falls
// back to TCP and refuses to replace anything other than a stale socket.
func ServeUDS(ctx context.Context, socketPath string, services Services, options ...grpc.ServerOption) (*Server, error) {
	if socketPath == "" || !filepath.IsAbs(socketPath) {
		return nil, fmt.Errorf("an absolute UDS path is required")
	}
	if services.Control == nil || services.DataSource == nil {
		return nil, fmt.Errorf("control and datasource services are required")
	}
	if err := os.MkdirAll(filepath.Dir(socketPath), 0o700); err != nil {
		return nil, fmt.Errorf("create UDS directory: %w", err)
	}
	if info, err := os.Lstat(socketPath); err == nil {
		if info.Mode()&os.ModeSocket == 0 {
			return nil, fmt.Errorf("refusing to replace non-socket path %s", socketPath)
		}
		if err := os.Remove(socketPath); err != nil {
			return nil, fmt.Errorf("remove stale UDS: %w", err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("inspect UDS: %w", err)
	}
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		return nil, fmt.Errorf("listen on UDS: %w", err)
	}
	if err := os.Chmod(socketPath, 0o600); err != nil {
		_ = listener.Close()
		_ = os.Remove(socketPath)
		return nil, fmt.Errorf("protect UDS: %w", err)
	}
	grpcServer := grpc.NewServer(append([]grpc.ServerOption{grpc.MaxRecvMsgSize(MaxMessageBytes), grpc.MaxSendMsgSize(MaxMessageBytes)}, options...)...)
	pluginv1.RegisterPluginControlServer(grpcServer, services.Control)
	pluginv1.RegisterDataSourcePluginServer(grpcServer, services.DataSource)
	server := &Server{grpc: grpcServer, listener: listener, path: socketPath, done: make(chan error, 1)}
	go func() { server.done <- grpcServer.Serve(listener) }()
	go func() {
		<-ctx.Done()
		server.Stop()
	}()
	return server, nil
}

func (s *Server) Stop() {
	if s == nil {
		return
	}
	s.once.Do(func() {
		s.grpc.GracefulStop()
		_ = s.listener.Close()
		_ = os.Remove(s.path)
	})
}

func (s *Server) Done() <-chan error { return s.done }
