package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	pb "github.com/Tencent/WeKnora/pkg/plugin/proto/v1"
	"github.com/Tencent/WeKnora/pkg/plugin/sdk"
	"github.com/Tencent/WeKnora/pkg/plugin/sdk/contracttest"
)

type syncResult struct {
	upserts []*pb.DocumentUpsert
	cursor  []byte
}

func receiveSync(ctx context.Context, client pb.DataSourcePluginClient, cursor []byte) (syncResult, error) {
	stream, err := client.Sync(ctx, &pb.SyncRequest{CursorJson: cursor})
	if err != nil {
		return syncResult{}, err
	}
	var result syncResult
	for {
		event, recvErr := stream.Recv()
		if recvErr == io.EOF {
			return result, nil
		}
		if recvErr != nil {
			return syncResult{}, recvErr
		}
		switch {
		case event.GetUpsert() != nil:
			result.upserts = append(result.upserts, event.GetUpsert())
		case event.GetCheckpoint() != nil:
			result.cursor = bytes.Clone(event.GetCheckpoint().GetCursorJson())
		default:
			return syncResult{}, errors.New("unexpected sync event")
		}
	}
}

func TestPublicSDKContract(t *testing.T) {
	id := identity([]byte("template-contract-nonce"))
	control, err := sdk.NewControlServer(id, sdk.ControlHooks{
		ValidateConfig: func(_ context.Context, data []byte) error { return validateConfig(data) },
		Health: func(context.Context) (pb.HealthStatus, error) {
			return pb.HealthStatus_HEALTH_STATUS_READY, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	contracttest.Run(t, contracttest.Options{
		Identity:             id,
		Services:             sdk.Services{Control: control, DataSource: &source{}},
		ConfigJSON:           []byte(`{"type":"hello","resource_ids":["grant_root"],"settings":{}}`),
		RequiredCapabilities: id.SupportedCapabilities,
		VerifyDataSource: func(ctx context.Context, client pb.DataSourcePluginClient) error {
			resources, err := client.ListResources(ctx, &pb.ListResourcesRequest{})
			if err != nil {
				return err
			}
			if len(resources.GetResources()) != 1 || resources.GetResources()[0].GetExternalId() != rootID {
				return errors.New("expected the grant_root resource")
			}
			if _, err = client.ResolveAncestors(ctx, &pb.ResolveAncestorsRequest{ResourceIds: []string{rootID}}); err != nil {
				return err
			}
			first, err := receiveSync(ctx, client, nil)
			if err != nil {
				return err
			}
			if len(first.upserts) != 1 || first.upserts[0].GetExternalId() != "hello.txt" ||
				first.upserts[0].GetRevision() == "" || len(first.cursor) == 0 {
				return errors.New("first sync did not return one stable record and checkpoint")
			}
			second, err := receiveSync(ctx, client, first.cursor)
			if err != nil {
				return err
			}
			if len(second.upserts) != 0 || len(second.cursor) == 0 {
				return errors.New("unchanged sync was not incremental")
			}
			return nil
		},
	})
}

func TestConfigurationIsStrict(t *testing.T) {
	for _, raw := range []string{
		`{"type":"hello","settings":{"unknown":true}}`,
		`{"type":"other","settings":{}}`,
		`{"type":"hello","credentials":{"token":"not-accepted"},"settings":{}}`,
		`{"type":"hello","resource_ids":["outside"],"settings":{}}`,
	} {
		if err := validateConfig([]byte(raw)); err == nil {
			t.Fatalf("accepted invalid configuration %s", raw)
		}
	}
}
