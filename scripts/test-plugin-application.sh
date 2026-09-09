#!/usr/bin/env bash
set -euo pipefail
# Isolated C3 application test: the executable is cmd/server, not a test
# Controller. Existing services are only used for real model inference.
: "${C2_ARTIFACT:?set C2_ARTIFACT to an independently built unpacked plugin directory}"
: "${C3_MODEL:=llama2:latest}"
: "${OLLAMA_BASE_URL:=http://127.0.0.1:11434}"
repo_root=$(git rev-parse --show-toplevel)
C2_ARTIFACT=$(cd "$C2_ARTIFACT" && pwd)
test -x "$C2_ARTIFACT/bin/plugin-linux-amd64"
test_root=$(mktemp -d /tmp/wkc3-XXXXXXXX)
run_id="c3-$(date +%s)-$$"
image="weknora/plugin-c3-test:$run_id"
app="weknora-c3-$run_id"
export C3_ROOT="$test_root" C3_DEPLOYMENT="$run_id" C3_MODEL OLLAMA_BASE_URL
export C3_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])')
export C3_REDIS_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])')
jieba_dir="$(go list -m -f '{{.Dir}}' github.com/yanyiwu/gojieba)/deps/cppjieba/dict"
redis_pid=""
stop_app() {
  if docker container inspect "$app" >/dev/null 2>&1; then
    docker stop -t 70 "$app" >/dev/null
    docker logs "$app" >"$test_root/application-${1:-final}.log" 2>&1
    docker rm "$app" >/dev/null
  fi
}
cleanup() {
  local result=$?
  set +e
  stop_app final
  if test -n "$redis_pid"; then kill "$redis_pid"; wait "$redis_pid" 2>/dev/null; fi
  # Never erase failed-cleanup evidence or use broad container deletion.
  docker ps -a --filter "label=deployment_id=$run_id" --format '{{.ID}} {{.Names}}'
  if mount | grep -F "$test_root/" || test -n "$(docker ps -aq --filter label=workload_kind=plugin --filter "label=deployment_id=$run_id")"; then
    echo "INCOMPLETE CLEANUP: keep $test_root and deployment $run_id for formal Backend retry" >&2
  else
    echo "No containers/mounts remain for $run_id; evidence retained at $test_root"
    docker image rm "$image" >/dev/null 2>&1
  fi
  if test -n "$(docker ps -aq --filter "label=deployment_id=$run_id")" || mount | grep -F "$test_root/" >/dev/null; then
    result=1
  fi
  return "$result"
}
trap cleanup EXIT
chmod 755 "$test_root"
mkdir -p "$test_root/packages" "$test_root/artifacts" "$test_root/runtime" "$test_root/grants/source" "$test_root/storage"
cp -R "$repo_root/config" "$test_root/config"
cp -R "$repo_root/migrations" "$test_root/migrations"
cp -R "$C2_ARTIFACT" "$test_root/packages/local-directory"
python3 scripts/testdata/plugin_application.py prepare
CGO_ENABLED=0 go build -trimpath -o "$test_root/gate" ./cmd/weknora-plugin-gate
CGO_ENABLED=0 go build -o "$test_root/queue-test" ./scripts/testdata/plugin_queue
CGO_ENABLED=0 go build -trimpath -o "$test_root/packages/runtime-probe/probe" ./internal/plugin/sandbox/docker/testdata/runtime-probe
CGO_ENABLED=1 go build -tags=netgo,osusergo,sqlite_fts5 -ldflags '-linkmode external -extldflags -static' -o "$test_root/server" ./cmd/server
docker build -q -t "$image" -f internal/plugin/sandbox/docker/testdata/Dockerfile internal/plugin/sandbox/docker/testdata
redis-server --bind 127.0.0.1 --port "$C3_REDIS_PORT" --dir "$test_root" --save '' --appendonly no --logfile "$test_root/redis.log" &
redis_pid=$!
start_app() {
  local enabled=$1
  local restricted=${2:-false}
  export C3_ENABLED="$enabled" C3_IMAGE="$image"
  python3 scripts/testdata/plugin_application.py config
  local security=(--user "$(id -u):$(id -g)")
  if test "$enabled" = true && test "$restricted" = false; then
    security=(--cap-add BPF --cap-add NET_ADMIN --cap-add PERFMON --cap-add SYS_ADMIN --cap-add DAC_OVERRIDE
      --mount type=bind,src=/var/run/docker.sock,dst=/var/run/docker.sock
      --mount type=bind,src=/sys/fs/cgroup,dst=/sys/fs/cgroup,readonly
      --mount type=bind,src=/sys/fs/bpf,dst=/sys/fs/bpf)
  fi
  # Host network is for the trusted WeKnora application reaching local Redis
  # and Ollama. Backend still enforces network=none on every plugin container.
  docker run -d --name "$app" --network host --read-only --cap-drop ALL "${security[@]}" \
    --security-opt no-new-privileges --cgroupns host --memory 2g --memory-swap 2g --pids-limit 256 \
    --tmpfs /tmp:rw,nosuid,nodev,size=128m --workdir /wk \
    --label workload_kind=plugin_c3_application_test --label "deployment_id=$run_id" \
    --mount "type=bind,src=$test_root,dst=/wk,bind-propagation=rshared" \
    --mount "type=bind,src=$jieba_dir,dst=/opt/jieba,readonly" \
    --env JIEBA_DICT_DIR=/opt/jieba --env GIN_MODE=release --env DB_DRIVER=sqlite --env DB_PATH=/wk/application.db \
    --env RETRIEVE_DRIVER=sqlite --env STORAGE_TYPE=local --env LOCAL_STORAGE_BASE_DIR=/wk/storage \
    --env "REDIS_ADDR=127.0.0.1:$C3_REDIS_PORT" --env REDIS_DB=0 \
    --env "OLLAMA_BASE_URL=$OLLAMA_BASE_URL" --env WEKNORA_BOOTSTRAP_SYSTEM_ADMIN_EMAIL=c3-admin@example.test \
    --env WEKNORA_TENANT_ENABLE_RBAC=true --env JWT_SECRET="$(python3 scripts/testdata/plugin_application.py secret)" \
    "$image" /wk/server >/dev/null
  if test "$restricted" = true; then
    local exit_code
    exit_code=$(timeout 30 docker wait "$app")
    test "$exit_code" != 0
    docker logs "$app" >"$test_root/application-security-rejected.log" 2>&1
    grep -qi 'docker\|plugin' "$test_root/application-security-rejected.log"
    stop_app security-rejected
    echo "PASS explicitly enabled without mandatory Docker/BPF access: normal server exits with visible startup error"
    return
  fi
  python3 scripts/testdata/plugin_application.py wait
}
start_app false
python3 scripts/testdata/plugin_application.py bootstrap
stop_app bootstrap
start_app true true
start_app true
python3 scripts/testdata/plugin_application.py ingest
stop_app before-restart
python3 scripts/testdata/plugin_application.py lose-pending
start_app true
python3 scripts/testdata/plugin_application.py recovery
stop_app final
python3 scripts/testdata/plugin_application.py cleanup-check
docker run --rm --name "$app-pin-check" --network none --read-only --cap-drop ALL --cap-add DAC_OVERRIDE \
  --security-opt no-new-privileges --memory 64m --memory-swap 64m --pids-limit 32 \
  --label workload_kind=plugin_c3_application_test --label "deployment_id=$run_id" \
  --mount type=bind,src=/sys/fs/bpf,dst=/sys/fs/bpf,readonly \
  --mount "type=bind,src=$test_root/queue-test,dst=/check,readonly" \
  --env "C3_DEPLOYMENT=$run_id" --env "C3_REDIS_PORT=$C3_REDIS_PORT" --env GOMAXPROCS=2 \
  "$image" /check check-pins
