package sdk

import (
	"bytes"
	"context"
	"fmt"
	"time"

	pluginv1 "github.com/Tencent/WeKnora/pkg/plugin/proto/v1"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Identity struct {
	PluginID              string
	PluginVersion         string
	ExtensionID           string
	ExtensionType         pluginv1.ExtensionType
	ProtocolVersion       string
	ContractVersion       string
	StartupNonce          []byte
	SupportedCapabilities []string
}

type ControlHooks struct {
	ValidateConfig func(context.Context, []byte) error
	Health         func(context.Context) (pluginv1.HealthStatus, error)
	Shutdown       func(context.Context, time.Duration) error
}

// ControlServer provides the repetitive V1 control-plane validation while the
// plugin supplies only its identity and business hooks.
type ControlServer struct {
	pluginv1.UnimplementedPluginControlServer
	identity Identity
	hooks    ControlHooks
}

func NewControlServer(identity Identity, hooks ControlHooks) (*ControlServer, error) {
	if identity.PluginID == "" || identity.PluginVersion == "" || identity.ExtensionID == "" {
		return nil, fmt.Errorf("plugin id, version and extension id are required")
	}
	if identity.ExtensionType != pluginv1.ExtensionType_EXTENSION_TYPE_DATASOURCE {
		return nil, fmt.Errorf("V1 SDK only supports datasource extensions")
	}
	if len(identity.StartupNonce) == 0 {
		return nil, fmt.Errorf("startup nonce is required")
	}
	if identity.ProtocolVersion == "" {
		identity.ProtocolVersion = ProtocolVersion
	}
	if identity.ContractVersion == "" {
		identity.ContractVersion = DataSourceContractVersion
	}
	identity.StartupNonce = bytes.Clone(identity.StartupNonce)
	identity.SupportedCapabilities = append([]string(nil), identity.SupportedCapabilities...)
	return &ControlServer{identity: identity, hooks: hooks}, nil
}

func (s *ControlServer) Handshake(_ context.Context, req *pluginv1.HandshakeRequest) (*pluginv1.HandshakeResponse, error) {
	resp := &pluginv1.HandshakeResponse{
		PluginId: s.identity.PluginID, PluginVersion: s.identity.PluginVersion,
		ExtensionId: s.identity.ExtensionID, ExtensionType: s.identity.ExtensionType,
		ProtocolVersion: s.identity.ProtocolVersion, ContractVersion: s.identity.ContractVersion,
		StartupNonce:          append([]byte(nil), s.identity.StartupNonce...),
		SupportedCapabilities: append([]string(nil), s.identity.SupportedCapabilities...),
	}
	if req.GetPluginId() != s.identity.PluginID || req.GetPluginVersion() != s.identity.PluginVersion ||
		req.GetExtensionId() != s.identity.ExtensionID || req.GetExtensionType() != s.identity.ExtensionType {
		resp.Error = pluginError(ErrorIdentityMismatch, "plugin identity does not match the runtime instance")
		return resp, nil
	}
	if !bytes.Equal(req.GetStartupNonce(), s.identity.StartupNonce) {
		resp.Error = pluginError(ErrorNonceMismatch, "startup nonce does not match the runtime instance")
		return resp, nil
	}
	if !CompatibleVersion(req.GetProtocolVersion(), s.identity.ProtocolVersion) {
		resp.Error = pluginError(ErrorIncompatibleProtocol, "protocol version is incompatible")
		return resp, nil
	}
	if !CompatibleVersion(req.GetContractVersion(), s.identity.ContractVersion) {
		resp.Error = pluginError(ErrorIncompatibleContract, "datasource contract version is incompatible")
		return resp, nil
	}
	supported := make(map[string]struct{}, len(s.identity.SupportedCapabilities))
	for _, capability := range s.identity.SupportedCapabilities {
		supported[capability] = struct{}{}
	}
	for _, required := range req.GetRequiredCapabilities() {
		if _, ok := supported[required]; !ok {
			resp.Error = pluginError(ErrorUnsupportedCapability, "a required capability is unsupported")
			return resp, nil
		}
	}
	return resp, nil
}

func (s *ControlServer) ValidateConfig(ctx context.Context, req *pluginv1.ValidateConfigRequest) (*pluginv1.ValidateConfigResponse, error) {
	if len(req.GetConfigJson()) > MaxConfigBytes {
		return &pluginv1.ValidateConfigResponse{Error: pluginError(ErrorMessageTooLarge, "configuration exceeds the V1 size limit")}, nil
	}
	if s.hooks.ValidateConfig != nil {
		if err := s.hooks.ValidateConfig(ctx, bytes.Clone(req.GetConfigJson())); err != nil {
			return &pluginv1.ValidateConfigResponse{Error: pluginError(ErrorInvalidArgument, err.Error())}, nil
		}
	}
	return &pluginv1.ValidateConfigResponse{}, nil
}

func (s *ControlServer) Health(ctx context.Context, _ *pluginv1.HealthRequest) (*pluginv1.HealthResponse, error) {
	status := pluginv1.HealthStatus_HEALTH_STATUS_READY
	var resultErr error
	if s.hooks.Health != nil {
		status, resultErr = s.hooks.Health(ctx)
	}
	resp := &pluginv1.HealthResponse{Status: status, CheckedAt: timestamppb.Now()}
	if resultErr != nil {
		resp.Error = pluginError(ErrorNotReady, resultErr.Error())
	}
	return resp, nil
}

func (s *ControlServer) Shutdown(ctx context.Context, req *pluginv1.ShutdownRequest) (*emptypb.Empty, error) {
	if s.hooks.Shutdown != nil {
		if err := s.hooks.Shutdown(ctx, time.Duration(req.GetGraceMillis())*time.Millisecond); err != nil {
			return nil, err
		}
	}
	return &emptypb.Empty{}, nil
}

func pluginError(code, message string) *pluginv1.PluginError {
	return &pluginv1.PluginError{Code: code, SafeMessage: message}
}
