#!/usr/bin/env bash
set -euo pipefail
plugin_root=$(cd "$(dirname "$0")/.." && pwd)
sdk_root=$(cd "${1:?usage: with-sdk.sh /path/to/WeKnora command [args...]}" && pwd)
shift
test -f "$sdk_root/pkg/plugin/sdk/contract.go"
workspace=$(mktemp -d /tmp/wk-example-sdk-XXXXXXXX)
trap 'rm -f "$workspace/go.work" "$workspace/go.work.sum"; rmdir "$workspace"' EXIT
export GOWORK="$workspace/go.work"
go work init "$plugin_root" "$sdk_root"
cd "$plugin_root"
"$@"
