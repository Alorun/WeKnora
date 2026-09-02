//go:build integration

package prototype

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/moby/moby/client"
)

func TestPluginPrototype(t *testing.T) {
	specPath := os.Getenv("WEKNORA_PROTOTYPE_SPEC")
	if specPath == "" {
		t.Skip("WEKNORA_PROTOTYPE_SPEC is required")
	}
	data, err := os.ReadFile(specPath)
	if err != nil {
		t.Fatal(err)
	}
	var spec PrototypeInstanceSpec
	if err := json.Unmarshal(data, &spec); err != nil {
		t.Fatal(err)
	}
	controller, err := NewController()
	if err != nil {
		t.Fatal(err)
	}
	defer controller.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	evidence, err := controller.Run(ctx, spec)
	if err != nil {
		t.Fatalf("run: %v; evidence=%+v", err, evidence)
	}
	if evidence.PluginStartedAtGate || evidence.PluginStartedAtReady || !evidence.HealthServing || !evidence.Recovered || !evidence.SecondStopSucceeded {
		t.Fatalf("incomplete evidence: %+v", evidence)
	}
	t.Logf("evidence=%+v", evidence)

	assertInvalidRecoveryCleanup(t, controller, spec, "generation-mismatch", true, false)
	assertInvalidRecoveryCleanup(t, controller, spec, "policy-missing", false, false)
	assertInvalidRecoveryCleanup(t, controller, spec, "uds-bad", false, true)
}

func assertInvalidRecoveryCleanup(t *testing.T, controller *Controller, base PrototypeInstanceSpec, suffix string, generationMismatch, attachPolicy bool) {
	t.Helper()
	spec := base
	spec.RunID = base.RunID + "-" + suffix
	spec.RuntimeAppPath = filepath.Join(spec.RuntimeMapping.AppRoot, "runtime", spec.RunID, spec.DataSourceID, "1")
	var err error
	spec.RuntimeHostPath, err = spec.RuntimeMapping.HostPath(spec.RuntimeAppPath)
	if err != nil {
		t.Fatal(err)
	}
	spec.BPFPinRoot = filepath.Join("/sys/fs/bpf/weknora-plugin-prototype", spec.RunID)
	if err := PrepareRuntimeDir(spec.RuntimeAppPath, spec.ContainerUser); err != nil {
		t.Fatal(err)
	}
	digest, err := artifactDigest(spec.ArtifactAppPath)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	containerID, err := controller.createPlugin(ctx, spec, digest)
	if err != nil {
		t.Fatal(err)
	}
	defer controller.Stop(context.Background(), spec, containerID, attachPolicy)
	if _, err := controller.client.ContainerStart(ctx, containerID, client.ContainerStartOptions{}); err != nil {
		t.Fatal(err)
	}
	if attachPolicy {
		if _, err := controller.runNetworkHelper(ctx, spec, "attach", containerID, 0); err != nil {
			t.Fatal(err)
		}
	}
	expected := spec
	if generationMismatch {
		expected.Generation++
	}
	if recovered, err := controller.Recover(ctx, expected); err == nil || recovered {
		t.Fatalf("invalid recovery %s was accepted", suffix)
	}
	if _, err := controller.client.ContainerInspect(ctx, containerID, client.ContainerInspectOptions{}); !errdefs.IsNotFound(err) {
		t.Fatalf("invalid recovery %s left container: %v", suffix, err)
	}
	if _, err := os.Stat(spec.RuntimeAppPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("invalid recovery %s left runtime directory: %v", suffix, err)
	}
	if attachPolicy {
		if _, err := controller.runNetworkHelper(ctx, spec, "inspect", containerID, 0); err == nil {
			t.Fatalf("invalid recovery %s left BPF policy", suffix)
		}
	}
	t.Logf("invalid recovery %s stopped and removed exact-run resources", suffix)
}
