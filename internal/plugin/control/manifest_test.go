package control

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const validManifest = `apiVersion: plugins.weknora.io/v1alpha1
kind: Plugin
metadata:
  id: community.local-files
  name: Local Files
  version: 0.1.0
spec:
  protocolVersion: "1.0"
  compatibility:
    weknora: ">=0.7.2 <0.8.0"
  extension:
    id: local_directory
    type: datasource
    contractVersion: "1.0"
    capabilities: [resource_listing, full_sync]
  runtime:
    kind: sandbox_service
    entrypoint: bin/plugin-linux-amd64
    transport: uds
  permissions:
    network: none
    filesystem: selected_directory_readonly
  resources:
    memoryMiB: 256
    cpuQuota: 0.5
    maxProcesses: 32
  configSchema:
    type: object
    additionalProperties: false
    properties:
      source_grant:
        type: string
`

func TestParseManifestValid(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "bin"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "bin", "plugin-linux-amd64"), []byte("binary"), 0o700); err != nil {
		t.Fatal(err)
	}
	manifest, err := ParseManifest([]byte(validManifest), ManifestValidationOptions{
		WeKnoraVersion: "0.7.3", ArtifactRoot: root,
	})
	if err != nil {
		t.Fatalf("parse valid manifest: %v", err)
	}
	if manifest.Definition().Source != SourceExternal {
		t.Fatal("manifest definition must be external")
	}
}

func TestParseManifestRejectsInvalidContracts(t *testing.T) {
	tests := []struct {
		name    string
		old     string
		new     string
		wantErr string
	}{
		{"version", "version: 0.1.0", "version: latest", "metadata.version"},
		{"absolute path", "entrypoint: bin/plugin-linux-amd64", "entrypoint: /bin/plugin", "package-relative"},
		{"parent path", "entrypoint: bin/plugin-linux-amd64", "entrypoint: ../plugin", "canonical"},
		{"network", "network: none", "network: outbound", "unsupported_permission"},
		{"filesystem", "filesystem: selected_directory_readonly", "filesystem: writable", "unsupported_permission"},
		{"extension", "type: datasource", "type: web_search", "only support datasource"},
		{"protocol", `protocolVersion: "1.0"`, `protocolVersion: "2.0"`, "unsupported protocolVersion"},
		{"schema", "    type: object", "    type: array", "configSchema.type"},
		{"resource", "memoryMiB: 256", "memoryMiB: 8192", "memoryMiB"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := ParseManifest([]byte(strings.Replace(validManifest, test.old, test.new, 1)), ManifestValidationOptions{})
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("error = %v, want substring %q", err, test.wantErr)
			}
		})
	}
}

func TestValidateArtifactEntrypointRejectsSymlink(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "real")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(target, "plugin"), []byte("binary"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(root, "bin")); err != nil {
		t.Fatal(err)
	}
	if err := ValidateArtifactEntrypoint(root, "bin/plugin"); err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("expected symlink rejection, got %v", err)
	}
}

func TestParseManifestRejectsBuiltinIDAndUnknownField(t *testing.T) {
	builtin := strings.Replace(validManifest, "community.local-files", "builtin.local_files", 1)
	if _, err := ParseManifest([]byte(builtin), ManifestValidationOptions{}); err == nil {
		t.Fatal("expected builtin namespace rejection")
	}
	unknown := strings.Replace(validManifest, "  protocolVersion:", "  mystery: true\n  protocolVersion:", 1)
	if _, err := ParseManifest([]byte(unknown), ManifestValidationOptions{}); err == nil {
		t.Fatal("expected strict YAML field rejection")
	}
}
