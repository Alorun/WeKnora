#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tool_dir="$(mktemp -d)"
trap 'rm -rf "$tool_dir"' EXIT

GOBIN="$tool_dir" go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.11
GOBIN="$tool_dir" go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.1

PATH="$tool_dir:$PATH" protoc \
  -I "$repo_root/api/proto" \
  -I /usr/include \
  --go_out="$repo_root" \
  --go_opt=module=github.com/Tencent/WeKnora \
  --go-grpc_out="$repo_root" \
  --go-grpc_opt=module=github.com/Tencent/WeKnora \
  "$repo_root/api/proto/plugin/v1/plugin.proto"
