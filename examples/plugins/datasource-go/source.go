package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"

	pb "github.com/Tencent/WeKnora/pkg/plugin/proto/v1"
	"github.com/Tencent/WeKnora/pkg/plugin/sdk"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const rootID = "grant_root"

type source struct {
	pb.UnimplementedDataSourcePluginServer
}

func strictJSON(data []byte, value any) error {
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(value); err != nil {
		return err
	}
	if d.Decode(new(any)) != io.EOF {
		return errors.New("trailing JSON")
	}
	return nil
}

func selection(ids []string) error {
	for _, id := range ids {
		if id != rootID {
			return status.Error(codes.InvalidArgument, "unknown resource")
		}
	}
	return nil
}

func validateConfig(data []byte) error {
	if len(data) > sdk.MaxConfigBytes {
		return status.Error(codes.ResourceExhausted, "configuration too large")
	}
	if len(bytes.TrimSpace(data)) == 0 {
		data = []byte(`{}`)
	}
	// configSchema describes settings; the RPC carries the Host envelope.
	var cfg struct {
		Type        string                     `json:"type"`
		Credentials map[string]json.RawMessage `json:"credentials"`
		ResourceIDs []string                   `json:"resource_ids"`
		Settings    struct{}                   `json:"settings"`
	}
	if strictJSON(data, &cfg) != nil || (cfg.Type != "" && cfg.Type != "hello") || len(cfg.Credentials) != 0 {
		return status.Error(codes.InvalidArgument, "expected hello with empty settings and credentials")
	}
	return selection(cfg.ResourceIDs)
}

func (*source) ListResources(ctx context.Context, req *pb.ListResourcesRequest) (*pb.ListResourcesResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if err := validateConfig(req.GetConfigJson()); err != nil {
		return nil, err
	}
	switch req.GetParentId() {
	case "":
		return &pb.ListResourcesResponse{Resources: []*pb.Resource{{ExternalId: rootID, Name: "已授权目录", Type: "directory"}}}, nil
	case rootID:
		return &pb.ListResourcesResponse{}, nil
	default:
		return nil, status.Error(codes.InvalidArgument, "unknown parent resource")
	}
}

func (*source) ResolveAncestors(ctx context.Context, req *pb.ResolveAncestorsRequest) (*pb.ResolveAncestorsResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if err := validateConfig(req.GetConfigJson()); err != nil {
		return nil, err
	}
	if err := selection(req.GetResourceIds()); err != nil {
		return nil, err
	}
	return &pb.ResolveAncestorsResponse{}, nil
}

type snapshot struct {
	Version  int    `json:"version"`
	Round    string `json:"round"`
	Digest   string `json:"digest"`
	Revision string `json:"revision"`
}

func (*source) Sync(req *pb.SyncRequest, stream grpc.ServerStreamingServer[pb.SyncEvent]) error {
	if err := validateConfig(req.GetConfigJson()); err != nil {
		return err
	}
	if len(req.GetCursorJson()) > sdk.MaxCursorBytes {
		return status.Error(codes.ResourceExhausted, "cursor too large")
	}
	var envelope struct {
		ConnectorCursor map[string]json.RawMessage `json:"connector_cursor"`
	}
	var old snapshot
	var round uint64
	if len(req.GetCursorJson()) > 0 {
		if json.Unmarshal(req.CursorJson, &envelope) != nil || strictJSON(envelope.ConnectorCursor["hello"], &old) != nil || old.Version != 1 || old.Revision == "" || old.Digest == "" {
			return status.Error(codes.InvalidArgument, "invalid or unsupported cursor")
		}
		var err error
		round, err = strconv.ParseUint(old.Round, 10, 64)
		if err != nil || round == ^uint64(0) {
			return status.Error(codes.InvalidArgument, "invalid cursor round")
		}
	}
	// Replace this fixed record with data from your authorized source. Never
	// parse documents here. Preserve file extensions and send original bytes.
	body := []byte("Hello from a WeKnora DataSource plugin.\n")
	digest := fmt.Sprintf("%x", sha256.Sum256(body))
	next := snapshot{Version: 1, Round: strconv.FormatUint(round+1, 10), Digest: digest, Revision: old.Revision}
	changed := old.Digest != digest
	if changed {
		next.Revision = fmt.Sprintf("%x", sha256.Sum256([]byte(next.Round+"\x00hello.txt\x00"+digest)))
	}
	if err := stream.Context().Err(); err != nil {
		return status.FromContextError(err).Err()
	}
	if changed || req.GetForceFull() {
		if err := stream.Send(&pb.SyncEvent{Event: &pb.SyncEvent_Upsert{Upsert: &pb.DocumentUpsert{
			ExternalId: "hello.txt", Revision: next.Revision, Title: "Hello", Content: body,
			ContentType: "text/plain", FileName: "hello.txt", SourceResourceId: rootID,
		}}}); err != nil {
			return err
		}
	}
	// This single-record example has no deletion. A real source may emit
	// Delete only after successful enumeration; every Send error must return.
	cursor, err := json.Marshal(map[string]any{"connector_cursor": map[string]any{"hello": next}})
	if err != nil {
		return err
	}
	if err := stream.Context().Err(); err != nil {
		return status.FromContextError(err).Err()
	}
	return stream.Send(&pb.SyncEvent{Event: &pb.SyncEvent_Checkpoint{Checkpoint: &pb.Checkpoint{CursorJson: cursor}}})
}
