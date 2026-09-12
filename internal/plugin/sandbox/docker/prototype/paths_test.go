package prototype

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCreateGrantValidation(t *testing.T) {
	allow := filepath.Join(t.TempDir(), "allow")
	mustMkdir(t, filepath.Join(allow, "nested", "source"))

	if _, err := CreateGrant(allow, "nested/source", 1); err != nil {
		t.Fatalf("valid grant: %v", err)
	}
	for _, selected := range []string{"../outside", "/absolute"} {
		if _, err := CreateGrant(allow, selected, 1); err == nil {
			t.Fatalf("expected %q to be rejected", selected)
		}
	}

	outside := filepath.Join(t.TempDir(), "outside")
	mustMkdir(t, outside)
	if err := os.Symlink(outside, filepath.Join(allow, "link")); err != nil {
		t.Fatal(err)
	}
	if _, err := CreateGrant(allow, "link", 1); err == nil || !strings.Contains(err.Error(), "symbolic link") {
		t.Fatalf("expected symlink rejection, got %v", err)
	}
}

func TestRevalidateGrantRejectsInodeChangeAndRevocation(t *testing.T) {
	allow := filepath.Join(t.TempDir(), "allow")
	path := filepath.Join(allow, "source")
	mustMkdir(t, path)
	grant, err := CreateGrant(allow, "source", 1)
	if err != nil {
		t.Fatal(err)
	}
	grant.Status = "revoked"
	if err := RevalidateGrant(grant); err == nil {
		t.Fatal("revoked grant was accepted")
	}
	grant.Status = "active"
	wrongDevice := grant
	wrongDevice.Device++
	if err := RevalidateGrant(wrongDevice); err == nil || !strings.Contains(err.Error(), "device or inode changed") {
		t.Fatalf("expected device change rejection, got %v", err)
	}
	if err := os.Rename(path, path+"-old"); err != nil {
		t.Fatal(err)
	}
	mustMkdir(t, path)
	if err := RevalidateGrant(grant); err == nil || !strings.Contains(err.Error(), "inode changed") {
		t.Fatalf("expected inode change rejection, got %v", err)
	}
}

func TestPathMappingRequiresExplicitConsistentRoots(t *testing.T) {
	mapping := PathMapping{AppRoot: "/controller/plugin-root", HostRoot: "/host/plugin-root"}
	host, err := mapping.HostPath("/controller/plugin-root/grants/source")
	if err != nil {
		t.Fatal(err)
	}
	if host != "/host/plugin-root/grants/source" {
		t.Fatalf("unexpected host path %q", host)
	}
	if err := mapping.ValidatePair("/controller/plugin-root/grants/source", host); err != nil {
		t.Fatal(err)
	}
	if err := mapping.ValidatePair("/controller/plugin-root/grants/source", "/controller/plugin-root/grants/source"); err == nil {
		t.Fatal("controller path was incorrectly accepted as Docker host path")
	}
	if _, err := mapping.HostPath("/controller/outside"); err == nil {
		t.Fatal("outside app path was accepted")
	}
}

func TestCleanupRuntimeDirIsIdempotent(t *testing.T) {
	runtimeDir := filepath.Join(t.TempDir(), "runtime")
	mustMkdir(t, runtimeDir)
	if err := os.WriteFile(filepath.Join(runtimeDir, "release"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := cleanupRuntimeDir(runtimeDir); err != nil {
		t.Fatal(err)
	}
	if err := cleanupRuntimeDir(runtimeDir); err != nil {
		t.Fatalf("second cleanup failed: %v", err)
	}
}

func TestMetadataFiltering(t *testing.T) {
	spec := PrototypeInstanceSpec{RunID: "run-a", PluginID: "plugin-a", DataSourceID: "ds-a", Generation: 2}
	labels := labelsForSpec(spec, "sha256:test")
	if !matchesSpecLabels(spec, labels) {
		t.Fatal("matching metadata was rejected")
	}
	labels["prototype_run_id"] = "other-run"
	if matchesSpecLabels(spec, labels) {
		t.Fatal("other run metadata was accepted")
	}
}

func mustMkdir(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(path, 0o700); err != nil {
		t.Fatal(err)
	}
}
