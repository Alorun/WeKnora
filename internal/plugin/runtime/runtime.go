// Package runtime implements the formal external plugin runtime contract. The
// backend is injected explicitly; production without one remains unavailable.
package runtime

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	plugindatasource "github.com/Tencent/WeKnora/internal/plugin/datasource"
	pluginv1 "github.com/Tencent/WeKnora/pkg/plugin/proto/v1"
	pluginsdk "github.com/Tencent/WeKnora/pkg/plugin/sdk"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var ErrRuntimeNotAvailable = errors.New(pluginsdk.ErrorRuntimeNotAvailable)

type ArtifactReference struct {
	Digest    string
	EntryPath string
	AppPath   string
	HostPath  string
	Device    uint64
	Inode     uint64
}

type DirectoryGrantReference struct {
	ID         string
	Generation uint64
	AppPath    string
	HostPath   string
	Device     uint64
	Inode      uint64
}

type EffectivePermissions struct {
	Network    string
	Filesystem string
}

type ResourceLimits struct {
	MemoryBytes int64
	CPUQuota    float64
	PidsLimit   int64
}

type InstanceSpec struct {
	PluginID             string
	PluginVersion        string
	ExtensionID          string
	DataSourceID         string
	Generation           uint64
	ProtocolVersion      string
	ContractVersion      string
	ConfigJSON           []byte
	StartupNonce         []byte
	RequiredCapabilities []string
	Artifact             ArtifactReference
	Grant                *DirectoryGrantReference
	RuntimeHostPath      string
	RuntimeAppPath       string
	Permissions          EffectivePermissions
	Resources            ResourceLimits
}

func (s InstanceSpec) Validate() error {
	if s.PluginID == "" || s.PluginVersion == "" || s.ExtensionID == "" || s.DataSourceID == "" {
		return fmt.Errorf("plugin, extension and data source identity are required")
	}
	if s.Generation == 0 || len(s.StartupNonce) == 0 {
		return fmt.Errorf("positive generation and startup nonce are required")
	}
	if s.ProtocolVersion == "" || s.ContractVersion == "" {
		return fmt.Errorf("protocol and contract versions are required")
	}
	if len(s.ConfigJSON) > pluginsdk.MaxConfigBytes {
		return fmt.Errorf("configuration exceeds %d bytes", pluginsdk.MaxConfigBytes)
	}
	if s.Artifact.Digest == "" || s.Artifact.EntryPath == "" || s.RuntimeHostPath == "" {
		return fmt.Errorf("artifact and runtime path are required")
	}
	return nil
}

type BackendInstance struct {
	ID          string
	UDSHostPath string
	Metadata    map[string]string
}

type BackendInstanceState struct {
	Instance       BackendInstance
	Running        bool
	ExitCode       int
	OOMKilled      bool
	DiagnosticTail string
}

type PluginSandboxBackend interface {
	StartService(context.Context, InstanceSpec) (BackendInstance, error)
	List(context.Context, map[string]string) ([]BackendInstance, error)
	Inspect(context.Context, string) (BackendInstanceState, error)
	Stop(context.Context, string, time.Duration) error
}

type HealthResult struct {
	Status      pluginv1.HealthStatus
	ErrorCode   string
	SafeMessage string
	CheckedAt   time.Time
}

type RuntimeHandle interface {
	plugindatasource.RPCHandle
	DataSourceID() string
	Generation() uint64
	BackendInstance() BackendInstance
}

type Runtime interface {
	Start(context.Context, InstanceSpec) (RuntimeHandle, error)
	Health(context.Context, RuntimeHandle) (HealthResult, error)
	Stop(context.Context, RuntimeHandle, time.Duration) error
}

type PluginSandboxRuntime struct {
	backend      PluginSandboxBackend
	resolver     *plugindatasource.Resolver
	dialTimeout  time.Duration
	readyTimeout time.Duration
}

func New(backend PluginSandboxBackend, resolver *plugindatasource.Resolver) *PluginSandboxRuntime {
	return &PluginSandboxRuntime{backend: backend, resolver: resolver, dialTimeout: 5 * time.Second, readyTimeout: 15 * time.Second}
}

type handle struct {
	instance          BackendInstance
	dsID              string
	generation        uint64
	conn              *grpc.ClientConn
	control           pluginv1.PluginControlClient
	datasource        pluginv1.DataSourcePluginClient
	closeOnce         sync.Once
	stopMu            sync.Mutex
	shutdownAttempted bool
}

func (h *handle) InstanceID() string                                { return h.instance.ID }
func (h *handle) DataSourceID() string                              { return h.dsID }
func (h *handle) Generation() uint64                                { return h.generation }
func (h *handle) BackendInstance() BackendInstance                  { return h.instance }
func (h *handle) ControlClient() pluginv1.PluginControlClient       { return h.control }
func (h *handle) DataSourceClient() pluginv1.DataSourcePluginClient { return h.datasource }

func (r *PluginSandboxRuntime) Start(ctx context.Context, spec InstanceSpec) (RuntimeHandle, error) {
	if r == nil || r.backend == nil {
		return nil, ErrRuntimeNotAvailable
	}
	if r.resolver == nil {
		return nil, fmt.Errorf("datasource resolver is required")
	}
	if err := spec.Validate(); err != nil {
		return nil, err
	}
	instance, err := r.backend.StartService(ctx, spec)
	if err != nil {
		return nil, fmt.Errorf("start plugin service: %w", err)
	}
	cleanup := func(startErr error, conn *grpc.ClientConn) error {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		if conn != nil {
			startErr = errors.Join(startErr, conn.Close())
		}
		startErr = errors.Join(startErr, r.backend.Stop(cleanupCtx, instance.ID, 0))
		return startErr
	}
	if instance.ID == "" || instance.UDSHostPath == "" {
		return nil, cleanup(fmt.Errorf("backend returned incomplete instance metadata"), nil)
	}
	if err := waitForSocket(ctx, instance.UDSHostPath, r.readyTimeout); err != nil {
		return nil, cleanup(err, nil)
	}
	dialCtx, cancel := context.WithTimeout(ctx, r.dialTimeout)
	defer cancel()
	conn, err := grpc.NewClient("passthrough:///plugin", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(pluginsdk.MaxMessageBytes), grpc.MaxCallSendMsgSize(pluginsdk.MaxMessageBytes)),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", instance.UDSHostPath)
		}))
	if err != nil {
		return nil, cleanup(fmt.Errorf("create plugin UDS client: %w", err), nil)
	}
	controlClient := pluginv1.NewPluginControlClient(conn)
	handshake, err := controlClient.Handshake(dialCtx, &pluginv1.HandshakeRequest{
		PluginId: spec.PluginID, PluginVersion: spec.PluginVersion, ExtensionId: spec.ExtensionID,
		ExtensionType:   pluginv1.ExtensionType_EXTENSION_TYPE_DATASOURCE,
		ProtocolVersion: spec.ProtocolVersion, ContractVersion: spec.ContractVersion,
		StartupNonce: bytes.Clone(spec.StartupNonce), RequiredCapabilities: append([]string(nil), spec.RequiredCapabilities...),
	})
	if err != nil {
		return nil, cleanup(fmt.Errorf("plugin handshake: %w", err), conn)
	}
	if err := validateHandshake(spec, handshake); err != nil {
		return nil, cleanup(err, conn)
	}
	validated, err := controlClient.ValidateConfig(dialCtx, &pluginv1.ValidateConfigRequest{ConfigJson: bytes.Clone(spec.ConfigJSON)})
	if err != nil {
		return nil, cleanup(fmt.Errorf("validate plugin config: %w", err), conn)
	}
	if err := responseError(validated.GetError()); err != nil {
		return nil, cleanup(fmt.Errorf("validate plugin config: %w", err), conn)
	}
	health, err := controlClient.Health(dialCtx, &pluginv1.HealthRequest{})
	if err != nil {
		return nil, cleanup(fmt.Errorf("plugin health: %w", err), conn)
	}
	if err := responseError(health.GetError()); err != nil {
		return nil, cleanup(fmt.Errorf("plugin health: %w", err), conn)
	}
	if health.GetStatus() != pluginv1.HealthStatus_HEALTH_STATUS_READY {
		return nil, cleanup(fmt.Errorf("plugin health is %s", health.GetStatus()), conn)
	}
	result := &handle{instance: instance, dsID: spec.DataSourceID, generation: spec.Generation, conn: conn,
		control: controlClient, datasource: pluginv1.NewDataSourcePluginClient(conn)}
	// Publication is deliberately last: every validation above has completed.
	if err := ctx.Err(); err != nil {
		return nil, cleanup(err, conn)
	}
	if err := r.resolver.Publish(spec.DataSourceID, spec.Generation, result); err != nil {
		return nil, cleanup(fmt.Errorf("publish plugin handle: %w", err), conn)
	}
	return result, nil
}

func (r *PluginSandboxRuntime) Health(ctx context.Context, runtimeHandle RuntimeHandle) (HealthResult, error) {
	if runtimeHandle == nil {
		return HealthResult{}, fmt.Errorf("runtime handle is required")
	}
	resp, err := runtimeHandle.ControlClient().Health(ctx, &pluginv1.HealthRequest{})
	if err != nil {
		return HealthResult{}, err
	}
	result := HealthResult{Status: resp.GetStatus()}
	if resp.GetCheckedAt() != nil {
		result.CheckedAt = resp.GetCheckedAt().AsTime()
	}
	if pluginErr := resp.GetError(); pluginErr != nil {
		result.ErrorCode, result.SafeMessage = pluginErr.GetCode(), pluginErr.GetSafeMessage()
		return result, responseError(pluginErr)
	}
	return result, nil
}

func (r *PluginSandboxRuntime) Stop(ctx context.Context, runtimeHandle RuntimeHandle, grace time.Duration) error {
	if runtimeHandle == nil {
		return nil
	}
	concrete, isConcrete := runtimeHandle.(*handle)
	if isConcrete {
		concrete.stopMu.Lock()
		defer concrete.stopMu.Unlock()
	}
	var result error
	grace = min(maxDuration(grace, 0), 10*time.Second)
	cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cleanupCancel()
	if grace == 0 {
		result = errors.Join(result, r.resolver.Unpublish(runtimeHandle.DataSourceID(), runtimeHandle.Generation()))
	} else {
		drainCtx, drainCancel := context.WithTimeout(cleanupCtx, grace)
		result = errors.Join(result, r.resolver.UnpublishAndDrain(drainCtx, runtimeHandle.DataSourceID(), runtimeHandle.Generation()))
		drainCancel()
	}
	if !isConcrete || !concrete.shutdownAttempted {
		shutdownCtx, cancel := context.WithTimeout(cleanupCtx, maxDuration(grace, time.Second))
		_, shutdownErr := runtimeHandle.ControlClient().Shutdown(shutdownCtx, &pluginv1.ShutdownRequest{GraceMillis: grace.Milliseconds()})
		cancel()
		result = errors.Join(result, shutdownErr)
		if isConcrete {
			concrete.shutdownAttempted = true
		}
	}
	result = errors.Join(result, r.backend.Stop(cleanupCtx, runtimeHandle.InstanceID(), grace))
	if concrete, ok := runtimeHandle.(*handle); ok {
		concrete.closeOnce.Do(func() { result = errors.Join(result, concrete.conn.Close()) })
	}
	return result
}

func validateHandshake(spec InstanceSpec, resp *pluginv1.HandshakeResponse) error {
	if resp == nil {
		return fmt.Errorf("plugin returned an empty handshake")
	}
	if err := responseError(resp.GetError()); err != nil {
		return err
	}
	if resp.GetPluginId() != spec.PluginID || resp.GetPluginVersion() != spec.PluginVersion ||
		resp.GetExtensionId() != spec.ExtensionID || resp.GetExtensionType() != pluginv1.ExtensionType_EXTENSION_TYPE_DATASOURCE {
		return fmt.Errorf("%s: plugin identity response does not match", pluginsdk.ErrorIdentityMismatch)
	}
	if !bytes.Equal(resp.GetStartupNonce(), spec.StartupNonce) {
		return fmt.Errorf("%s: startup nonce response does not match", pluginsdk.ErrorNonceMismatch)
	}
	if !pluginsdk.CompatibleVersion(spec.ProtocolVersion, resp.GetProtocolVersion()) {
		return fmt.Errorf("%s: protocol %s", pluginsdk.ErrorIncompatibleProtocol, resp.GetProtocolVersion())
	}
	if !pluginsdk.CompatibleVersion(spec.ContractVersion, resp.GetContractVersion()) {
		return fmt.Errorf("%s: contract %s", pluginsdk.ErrorIncompatibleContract, resp.GetContractVersion())
	}
	supported := make(map[string]struct{}, len(resp.GetSupportedCapabilities()))
	for _, capability := range resp.GetSupportedCapabilities() {
		supported[capability] = struct{}{}
	}
	for _, required := range spec.RequiredCapabilities {
		if _, ok := supported[required]; !ok {
			return fmt.Errorf("%s: required capability %q", pluginsdk.ErrorUnsupportedCapability, required)
		}
	}
	return nil
}

func responseError(pluginErr *pluginv1.PluginError) error {
	if pluginErr == nil || pluginErr.GetCode() == "" {
		return nil
	}
	return fmt.Errorf("%s: %s", pluginErr.GetCode(), pluginErr.GetSafeMessage())
}

func waitForSocket(ctx context.Context, path string, timeout time.Duration) error {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		if info, err := os.Stat(path); err == nil && info.Mode()&os.ModeSocket != 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return fmt.Errorf("timed out waiting for plugin UDS")
		case <-ticker.C:
		}
	}
}

func maxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}
