#!/usr/bin/env bash
set -euo pipefail

# Only this test's directory receives mount propagation. SYS_ADMIN is needed
# for per-instance tmpfs mounts, not for BPF; no host-global policy is changed.
repo_root=$(git rev-parse --show-toplevel)
mode=${1:-backend}
case "$mode" in
  backend) ;;
  local-directory)
    : "${C2_ARTIFACT:?set C2_ARTIFACT to the independently built unpacked plugin directory}"
    test -f "$C2_ARTIFACT/plugin.yaml"
    C2_ARTIFACT=$(cd "$C2_ARTIFACT" && pwd)
    ;;
  *) echo "usage: $0 [backend|local-directory]" >&2; exit 1 ;;
esac
test_root=$(mktemp -d /tmp/wkc1-XXXXXXXX)
run_id="c1-$(date +%s)-$$"
image="weknora/plugin-c1-test:$run_id"
controller="weknora-c1-test-$run_id"
jieba_dir="$(go list -m -f '{{.Dir}}' github.com/yanyiwu/gojieba)/deps/cppjieba/dict"
docker_log_root="$(docker info --format '{{.DockerRootDir}}')/containers"
cleanup() {
  # The test binary does normal cleanup. On a test crash, run its cleanup-only
  # entry with the same formal backend, including pins and mounts, before rm.
  set +e
  if docker image inspect "$image" >/dev/null 2>&1 && test -x "$test_root/backend.test"; then
    run_controller cleanup -test.run '^TestDockerPluginCleanup$' -test.v
  fi
  docker image rm "$image" >/dev/null 2>&1
  # Never delete a directory which still contains a mount or owned resource.
  if mount | grep -F "$test_root/" >/dev/null || test -n "$(docker ps -aq --filter "label=deployment_id=$run_id" --filter label=workload_kind=plugin)"; then
    echo "cleanup incomplete; retained $test_root (deployment $run_id)" >&2
  else
    find "$test_root" -depth -delete
  fi
}
run_controller() {
  local suffix=$1; shift
  local controller_memory=512m
  # The race-instrumented Controller plus its recovery subprocess need more
  # memory. This does not change the plugin's 256/64 MiB test limits.
  if test "${C1_RACE:-0}" = 1; then controller_memory=1g; fi
  local bpf_mount=type=bind,src=/sys/fs/bpf,dst=/sys/fs/bpf
  if test "$suffix" = no-policy; then bpf_mount+=,readonly; fi
  docker run --rm --name "$controller-$suffix" --network none --read-only \
    --cap-drop ALL --cap-add BPF --cap-add NET_ADMIN --cap-add PERFMON \
    --cap-add SYS_ADMIN --cap-add DAC_OVERRIDE \
    --security-opt no-new-privileges --cgroupns host --memory "$controller_memory" --memory-swap "$controller_memory" --pids-limit 128 \
    --label workload_kind=plugin_c1_test --label "deployment_id=$run_id" \
    --mount "type=bind,src=$test_root,dst=/wk,bind-propagation=rshared" \
    --mount type=bind,src=/var/run/docker.sock,dst=/var/run/docker.sock \
    --mount type=bind,src=/sys/fs/cgroup,dst=/sys/fs/cgroup,readonly \
    --mount "$bpf_mount" \
    --mount "type=bind,src=$jieba_dir,dst=/opt/jieba,readonly" --env JIEBA_DICT_DIR=/opt/jieba \
    --mount "type=bind,src=$docker_log_root,dst=/docker-logs,readonly" \
    --env "C1_HOST_ROOT=$test_root" --env C1_APP_ROOT=/wk --env "C1_DEPLOYMENT=$run_id" \
    --env "C1_IMAGE=$image" --env "C1_ADMIN_UID=$(id -u)" \
    --env "C1_OPERATION=$suffix" --env "C2_ARTIFACT=${controller_artifact:-}" \
    "$image" /wk/backend.test "$@"
}
trap cleanup EXIT
cd "$repo_root"
chmod 755 "$test_root"
CGO_ENABLED=0 go build -trimpath -o "$test_root/gate" ./cmd/weknora-plugin-gate
test_tags=integration,netgo,osusergo
if test "$mode" = local-directory; then
  mkdir "$test_root/packages"
  cp -R -- "$C2_ARTIFACT" "$test_root/packages/local-directory"
  controller_artifact=/wk/packages/local-directory
  test_tags+=,localdirectory
else
  CGO_ENABLED=0 go build -trimpath -o "$test_root/probe" ./internal/plugin/sandbox/docker/testdata/runtime-probe
fi
# Existing Runtime imports utils/pg_query, whose parser requires CGO. Link the
# controller test statically; the SDK probe and gate remain CGO-free.
race_flags=()
if test "${C1_RACE:-0}" = 1; then race_flags=(-race); fi
CGO_ENABLED=1 go test "${race_flags[@]}" -tags="$test_tags" -c -ldflags '-linkmode external -extldflags -static' -o "$test_root/backend.test" ./internal/plugin/sandbox/docker
docker build -q -t "$image" -f internal/plugin/sandbox/docker/testdata/Dockerfile internal/plugin/sandbox/docker/testdata
if test "$mode" = local-directory; then
  run_controller run -test.run '^TestLocalDirectoryRuntime$' -test.v -test.timeout 3m
else
  run_controller run -test.run '^TestDockerPlugin' -test.v -test.timeout 5m
  run_controller no-policy -test.run '^TestDockerPluginPolicyUnavailable$' -test.v -test.timeout 1m
fi
