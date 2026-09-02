#!/usr/bin/env bash
set -euo pipefail

mode=${1:-all}
case "$mode" in
  host|mapped|integration|ebpf|all) ;;
  *) echo "usage: $0 [host|mapped|integration|ebpf|all]" >&2; exit 64 ;;
esac

for tool in go docker stat; do
  command -v "$tool" >/dev/null || { echo "missing tool: $tool" >&2; exit 69; }
done

repo_root=$(git rev-parse --show-toplevel)
prototype_root=$(mktemp -d /tmp/weknora-stage1-prototype-XXXXXXXX)
run_id="stage1-$(date +%s)-$$"
image_tag="weknora/plugin-prototype-base:${run_id}"
pin_root="/sys/fs/bpf/weknora-plugin-prototype/${run_id}"

cleanup() {
  set +e
  for id in $(docker ps -aq --filter "label=prototype_run_id=${run_id}"); do
    docker rm -f "$id" >/dev/null
  done
  if docker image inspect "$image_tag" >/dev/null 2>&1; then
    for pins in "$pin_root" "${pin_root}-network-test" "${pin_root}-generation-mismatch" \
      "${pin_root}-policy-missing" "${pin_root}-uds-bad"; do
      docker run --rm --network none --read-only --cap-drop ALL \
        --cap-add BPF --cap-add NET_ADMIN --cap-add PERFMON \
        --security-opt no-new-privileges --cgroupns host \
        --label managed=true --label workload_kind=plugin_prototype_controller \
        --label "prototype_run_id=${run_id}" \
        --mount "type=bind,src=${prototype_root}/controller-prototype,dst=/opt/controller/controller-prototype,readonly" \
        --mount type=bind,src=/sys/fs/cgroup,dst=/sys/fs/cgroup,readonly \
        --mount type=bind,src=/sys/fs/bpf,dst=/sys/fs/bpf \
        "$image_tag" /opt/controller/controller-prototype network-helper detach \
        -container-id 0000000000000000000000000000000000000000000000000000000000000000 \
        -pin-root "$pins" >/dev/null 2>&1
    done
    docker image rm "$image_tag" >/dev/null 2>&1
  fi
  case "$prototype_root" in
    /tmp/weknora-stage1-prototype-*) find "$prototype_root" -depth -delete ;;
  esac
}
trap cleanup EXIT INT TERM

mkdir -p "$prototype_root/artifact" "$prototype_root/allow/source"
printf 'allowed-by-directory-grant\n' > "$prototype_root/allow/source/allowed.txt"
printf 'outside-sentinel\n' > "$prototype_root/allow/outside-sentinel.txt"

cd "$repo_root"
CGO_ENABLED=0 go build -trimpath -o "$prototype_root/artifact/gate-prototype" ./cmd/weknora-plugin-gate-prototype
CGO_ENABLED=0 go build -trimpath -o "$prototype_root/artifact/fake-prototype" ./cmd/weknora-plugin-fake-prototype
CGO_ENABLED=0 go build -trimpath -o "$prototype_root/controller-prototype" ./cmd/weknora-plugin-controller-prototype
docker build -q -t "$image_tag" -f internal/plugin/sandbox/docker/prototype/Dockerfile internal/plugin/sandbox/docker/prototype >/dev/null

write_host_spec() {
  "$prototype_root/controller-prototype" spec \
    -run-id "$run_id" -image "$image_tag" \
    -app-root "$prototype_root" -host-root "$prototype_root" \
    -controller-host-path "$prototype_root/controller-prototype" > "$prototype_root/spec.json"
}

run_host() {
  write_host_spec
  echo "== host Controller =="
  "$prototype_root/controller-prototype" run -spec "$prototype_root/spec.json"
}

run_mapped() {
  local docker_group
  docker_group=$(stat -c '%g' /var/run/docker.sock)
  echo "== containerized Controller: explicit App/Host path mapping =="
  docker run --rm --network none --read-only --cap-drop ALL --security-opt no-new-privileges \
    --user 1000:1000 --group-add "$docker_group" \
    --label managed=true --label workload_kind=plugin_prototype_controller --label "prototype_run_id=${run_id}" \
    --mount "type=bind,src=${prototype_root},dst=/prototype-app" \
    --mount "type=bind,src=${prototype_root}/controller-prototype,dst=/opt/controller/controller-prototype,readonly" \
    --mount type=bind,src=/var/run/docker.sock,dst=/var/run/docker.sock \
    "$image_tag" /opt/controller/controller-prototype spec \
    -run-id "$run_id" -image "$image_tag" -app-root /prototype-app -host-root "$prototype_root" \
    -controller-host-path "$prototype_root/controller-prototype" > "$prototype_root/spec.json"
  docker run --rm --network none --read-only --cap-drop ALL --security-opt no-new-privileges \
    --user 1000:1000 --group-add "$docker_group" \
    --label managed=true --label workload_kind=plugin_prototype_controller --label "prototype_run_id=${run_id}" \
    --mount "type=bind,src=${prototype_root},dst=/prototype-app" \
    --mount "type=bind,src=${prototype_root}/controller-prototype,dst=/opt/controller/controller-prototype,readonly" \
    --mount type=bind,src=/var/run/docker.sock,dst=/var/run/docker.sock \
    "$image_tag" /opt/controller/controller-prototype run -spec /prototype-app/spec.json
}

run_integration() {
  write_host_spec
  echo "== tagged Docker integration test =="
  WEKNORA_PROTOTYPE_SPEC="$prototype_root/spec.json" \
    go test -tags=integration ./internal/plugin/sandbox/docker/... -run PluginPrototype -v
}

run_ebpf() {
  echo "== tagged eBPF integration test in controlled Controller container =="
  CGO_ENABLED=0 go test -tags=integration -c -o "$prototype_root/network.test" ./internal/plugin/sandbox/docker/network
  docker run --rm --network none --read-only --cap-drop ALL \
    --cap-add BPF --cap-add NET_ADMIN --cap-add PERFMON \
    --security-opt no-new-privileges --cgroupns host \
    --label managed=true --label workload_kind=plugin_prototype_controller --label "prototype_run_id=${run_id}" \
    --env WEKNORA_EBPF_INTEGRATION=1 --env "WEKNORA_PROTOTYPE_RUN_ID=${run_id}" \
    --mount "type=bind,src=${prototype_root}/network.test,dst=/opt/network.test,readonly" \
    --mount type=bind,src=/sys/fs/cgroup,dst=/sys/fs/cgroup,readonly \
    --mount type=bind,src=/sys/fs/bpf,dst=/sys/fs/bpf \
    "$image_tag" /opt/network.test -test.v -test.run CgroupDenyAudit
}

case "$mode" in
  host) run_host ;;
  mapped) run_mapped ;;
  integration) run_integration ;;
  ebpf) run_ebpf ;;
  all)
    run_host
    run_mapped
    run_ebpf
    ;;
esac
