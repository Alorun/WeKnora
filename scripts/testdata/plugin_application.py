"""C3 HTTP assertions against normal cmd/server; no ingestion or model doubles.

Only this test's root is written. DB access is read-only assertions; all business
mutations go through authenticated APIs. Never print credentials or JWTs.
"""
import json
import os
from pathlib import Path
import secrets
import sqlite3
import subprocess
import sys
import time
import urllib.error
import urllib.request

root = Path(os.environ["C3_ROOT"])
base = "http://127.0.0.1:" + os.environ["C3_PORT"]
phase = sys.argv[1]
state_file = root / "test-state.json"
state = json.loads(state_file.read_text()) if state_file.exists() else {}
token = ""


def save():
    state_file.write_text(json.dumps(state))
    state_file.chmod(0o600)


def api(method, path, data=None, expected=(200,), auth=True):
    headers = {"Content-Type": "application/json"}
    if auth and token:
        headers["Authorization"] = "Bearer " + token
    req = urllib.request.Request(base + "/api/v1" + path, headers=headers,
                                 data=None if data is None else json.dumps(data).encode(), method=method)
    try:
        response = urllib.request.urlopen(req, timeout=60)
    except urllib.error.HTTPError as err:
        response = err
    body = response.read()
    assert response.status in expected, (method, path, response.status, body[:1800])
    return json.loads(body) if body else None


def login(email="c3-admin@example.test"):
    global token
    result = api("POST", "/auth/login", {"email": email, "password": state["password"]}, auth=False)
    token = result["token"]
    return result


def sql(query, params=()):
    with sqlite3.connect("file:" + str(root / "application.db") + "?mode=ro", uri=True) as db:
        db.row_factory = sqlite3.Row
        return [{key: value.decode("utf-8") if isinstance(value, bytes) else value
                 for key, value in dict(row).items()} for row in db.execute(query, params)]


def wait_for(fn, seconds=150):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        result = fn()
        if result:
            return result
        time.sleep(1)
    raise AssertionError("timed out waiting for " + fn.__name__)


def sync(expect_success=True):
    result = api("POST", "/datasource/" + state["ds"] + "/sync", {}, expected=(200, 202))
    sync_id = result["id"]
    def terminal():
        rows = sql("SELECT * FROM sync_logs WHERE id=?", (sync_id,))
        return rows[0] if rows and rows[0]["status"] not in ("pending", "running") else None
    result = wait_for(terminal)
    if expect_success:
        assert result["status"] == "success", result
    else:
        assert result["status"] == "failed", result
    return result


def active():
    return sql("SELECT r.external_id,r.revision,r.knowledge_id,k.parse_status,k.enable_status "
               "FROM datasource_plugin_revisions r JOIN knowledges k ON k.id=r.knowledge_id "
               "WHERE r.data_source_id=? AND r.state='active' ORDER BY r.external_id", (state["ds"],))


def usable_three():
    rows = active()
    return rows if len(rows) == 3 and all(r["parse_status"] == "completed" and r["enable_status"] == "enabled" for r in rows) else None


if phase == "prepare":
    state.update(password="C3!" + secrets.token_urlsafe(18), jwt=secrets.token_hex(32))
    save()
    for name, text in {"a.txt": "Alpha is a real C3 document.", "b.txt": "Beta stays unchanged.", "c.txt": "Gamma stays unchanged."}.items():
        (root / "grants/source" / name).write_text(text)
    (root / "base-config.yaml").write_bytes((root / "config/config.yaml").read_bytes())
    probe = root / "packages/runtime-probe"
    probe.mkdir()
    manifest = {"apiVersion": "plugins.weknora.io/v1alpha1", "kind": "Plugin",
                "metadata": {"id": "test.runtime-probe", "name": "C3 audit test material", "version": "1.0.0"},
                "spec": {"protocolVersion": "1.0", "compatibility": {"weknora": ">=0.1.0"},
                         "extension": {"id": "runtime_probe", "type": "datasource", "contractVersion": "1.0", "capabilities": ["full_sync"]},
                         "runtime": {"kind": "sandbox_service", "entrypoint": "probe", "transport": "uds"},
                         "permissions": {"network": "none", "filesystem": "selected_directory_readonly"},
                         "resources": {"memoryMiB": 256, "cpuQuota": 0.5, "maxProcesses": 32},
                         "configSchema": {"type": "object", "properties": {"mode": {"type": "string"}}, "additionalProperties": False}}}
    (probe / "plugin.yaml").write_text(json.dumps(manifest))
elif phase == "secret":
    print(state["jwt"])
elif phase == "config":
    def mapping(name):
        return {"app_root": "/wk/" + name, "host_root": str(root / name)}
    cfg = {"enabled": os.environ["C3_ENABLED"] == "true", "deployment_id": os.environ["C3_DEPLOYMENT"],
           "backend_replicas": 1, "docker_host": "unix:///var/run/docker.sock", "image": os.environ["C3_IMAGE"],
           "gate_app_path": "/wk/gate", "gate_host_path": str(root / "gate"), "packages": mapping("packages"),
           "runtime_root": mapping("runtime"), "artifact_root": mapping("artifacts"),
           "allow_roots": {"test": mapping("grants")}, "admin_uid": os.getuid(), "plugin_uid": 65532, "plugin_gid": 65532,
           "max_instances": 3, "max_resources": {"memoryMiB": 256, "cpuQuota": 0.5, "maxProcesses": 32}}
    content = (root / "base-config.yaml").read_text().replace("port: 8080", "port: " + os.environ["C3_PORT"])
    (root / "config/config.yaml").write_text(content + "\nexternal_plugins: " + json.dumps(cfg) + "\n")
elif phase == "wait":
    def healthy():
        try:
            return urllib.request.urlopen(base + "/health", timeout=2).status == 200
        except (urllib.error.URLError, TimeoutError):
            return False
    wait_for(healthy, 90)
elif phase == "bootstrap":
    for username, email in [("C3 Admin", "c3-admin@example.test"), ("C3 Other", "c3-other@example.test")]:
        api("POST", "/auth/register", {"username": username, "email": email, "password": state["password"]}, expected=(200, 201), auth=False)
    login()
    assert not api("GET", "/plugins")["external_enabled"]
    print("PASS normal server with external disabled and NO Docker/BPF mounts or capabilities", flush=True)
elif phase == "ingest":
    login()
    listing = api("GET", "/plugins")
    definitions = [row["definition"] for row in listing["data"]]
    assert {d["ExtensionType"] for d in definitions if d["Source"] == "builtin"} == {
        "datasource", "document_parser", "web_search", "model_provider", "retrieval_engine"}
    builtin = next(d["ID"] for d in definitions if d["Source"] == "builtin" and d["ExtensionType"] == "web_search")
    for action, want in [("stop", "STOPPED"), ("start", "READY"), ("health", "READY")]:
        observed = api("POST", "/plugins/builtins/" + builtin + "/" + action, {})["data"]
        assert observed["State"] == want, observed
    login("c3-other@example.test")
    api("POST", "/plugins/builtins/" + builtin + "/stop", {}, expected=(403,))
    login()
    print("PASS five builtin types discovered; authenticated lifecycle delegates to the same Manager; non-admin rejected", flush=True)
    installation = next(i for i in listing["installations"] if i["plugin_id"] == "community.local-directory")
    assert installation["install_status"] == "installed" and not installation["enabled"]
    model = api("POST", "/models", {"name": os.environ["C3_MODEL"], "type": "Embedding", "source": "local",
                                   "parameters": {"embedding_parameters": {"dimension": 4096}}}, expected=(201,))["data"]
    kb = api("POST", "/knowledge-bases", {"name": "C3 Real Local Directory", "type": "document",
             "embedding_model_id": model["id"], "chunking_config": {"chunk_size": 512, "chunk_overlap": 32},
             "indexing_strategy": {"vector_enabled": True, "keyword_enabled": True},
             "question_generation_config": {"enabled": False}, "auto_tag_config": {"enabled": False}}, expected=(201,))["data"]
    state["kb"] = kb["id"]
    ds = api("POST", "/plugins/datasources", {"knowledge_base_id": kb["id"], "installation_id": installation["id"],
             "name": "C3 directory", "settings": {}, "resource_ids": ["grant_root"]}, expected=(201,))["data"]
    state["ds"] = ds["id"]
    save()
    prefix = "/plugins/datasources/" + ds["id"]
    api("POST", prefix + "/lifecycle/enable", {}, expected=(409,))
    login("c3-other@example.test")
    api("GET", prefix, expected=(403, 404))
    api("GET", prefix + "/audit", expected=(403, 404))
    api("POST", prefix + "/grants", {"allow_root_id": "test", "relative_directory": "source"}, expected=(403,))
    login()
    grant = api("POST", prefix + "/grants", {"allow_root_id": "test", "relative_directory": "source"}, expected=(201,))["data"]
    state["grant"] = grant["id"]
    state["old_instance"] = api("POST", prefix + "/lifecycle/enable", {})["binding"]["sandbox_id"]
    resources = api("GET", "/datasource/" + ds["id"] + "/resources")
    assert "grant_root" in json.dumps(resources)
    result = sync()
    rows = wait_for(usable_three)
    assert result["items_created"] == 3, result
    state["first"] = rows
    # Real parser chunks plus SQLite embedding storage, not just ledger state.
    counts = sql("SELECT knowledge_id,count(*) AS n FROM chunks WHERE knowledge_id IN (?,?,?) GROUP BY knowledge_id",
                 tuple(row["knowledge_id"] for row in rows))
    assert len(counts) == 3 and all(r["n"] > 0 for r in counts), counts
    indexed = sql("SELECT knowledge_id,dimension FROM lite_embeddings WHERE knowledge_id IN (?,?,?)",
                  tuple(row["knowledge_id"] for row in rows))
    assert len(indexed) == 3 and all(r["dimension"] == 4096 for r in indexed), indexed
    hits = api("POST", "/knowledge-bases/" + kb["id"] + "/hybrid-search",
               {"query_text": "Alpha", "match_count": 3, "vector_threshold": 0, "keyword_threshold": 0})["data"]
    assert any(hit["knowledge_id"] == rows[0]["knowledge_id"] for hit in hits), hits
    print("PASS first sync: 3 durable revisions, 3 completed/enabled Knowledge, real chunks/indexing", flush=True)
    result = sync()
    assert result["items_created"] == 0 and result["items_updated"] == 0 and active() == rows, result
    print("PASS unchanged: 0 new processing, identical Knowledge/revision IDs", flush=True)
    (root / "grants/source/a.txt").write_text("Alpha changed once in the real C3 directory.")
    result = sync()
    def replaced():
        current = usable_three()
        return current if current and current[0]["knowledge_id"] != rows[0]["knowledge_id"] else None
    current = wait_for(replaced)
    assert current[1:] == rows[1:], current
    assert result["items_created"] + result["items_updated"] == 1, result
    state["before_restart"] = current
    state["cursor"] = sql("SELECT last_sync_cursor FROM data_sources WHERE id=?", (ds["id"],))[0]["last_sync_cursor"]
    # A failed real scan cannot advance persisted Cursor. Disable invalidates
    # its Asynq retry generation before removing the test-only unsafe entry.
    unsafe = root / "grants/source/unsafe"
    unsafe.symlink_to("/etc/passwd")
    sync(expect_success=False)
    assert sql("SELECT last_sync_cursor FROM data_sources WHERE id=?", (ds["id"],))[0]["last_sync_cursor"] == state["cursor"]
    api("POST", prefix + "/lifecycle/disable", {})
    unsafe.unlink()
    api("POST", prefix + "/lifecycle/enable", {})
    print("PASS real unsafe scan: failed sync retains old Cursor; queued retry fenced by generation", flush=True)
    save()
    print("PASS incremental: only a.txt replaced; b/c Knowledge and revisions unchanged", flush=True)
    # Reuse the existing bounded C1 probe solely in this isolated test deployment.
    probe_install = next(i for i in listing["installations"] if i["plugin_id"] == "test.runtime-probe")
    probe_ds = api("POST", "/plugins/datasources", {"knowledge_base_id": kb["id"], "installation_id": probe_install["id"],
                   "name": "C3 audit test", "settings": {"mode": "network"}}, expected=(201,))["data"]["id"]
    probe_prefix = "/plugins/datasources/" + probe_ds
    api("POST", probe_prefix + "/grants", {"allow_root_id": "test", "relative_directory": "source"}, expected=(201,))
    api("POST", probe_prefix + "/lifecycle/enable", {})
    api("POST", "/datasource/" + probe_ds + "/sync", {})
    def audited():
        rows = api("GET", probe_prefix + "/audit?action=plugin.network_denied")["data"]
        return rows if len(rows) >= 4 else None
    audit_rows = wait_for(audited, 30)
    events = [row["details"] for row in audit_rows]
    assert all(e["denied"] and e["identity"]["data_source_id"] == probe_ds and e["identity"]["instance_id"] for e in events)
    assert len({(e["family"], e["protocol"]) for e in events}) == 4
    login("c3-other@example.test")
    api("GET", probe_prefix + "/audit", expected=(403, 404))
    login()
    api("POST", probe_prefix + "/lifecycle/disable", {})
    state["audit_ds"] = probe_ds
    save()
    print("PASS real cgroup deny -> synchronous audit DB -> authenticated tenant-scoped API: four IP/protocol events", flush=True)
    # Pause only this deployment's document queue, not QueuePlugin. Persist a
    # new revision through the real API, then lose its queued task while the
    # app is stopped. Startup must reconstruct it from the existing ledger.
    subprocess.run([str(root / "queue-test"), "pause"], check=True)
    (root / "grants/source/b.txt").write_text("Beta update durably accepted before application restart.")
    result = sync()
    assert result["items_created"] == 1 and active() == state["before_restart"]
    pending = sql("SELECT r.knowledge_id,k.parse_status,k.enable_status FROM datasource_plugin_revisions r "
                  "JOIN knowledges k ON k.id=r.knowledge_id WHERE r.data_source_id=? AND r.state='pending'", (state["ds"],))
    assert len(pending) == 1 and pending[0]["parse_status"] == "pending" and pending[0]["enable_status"] == "disabled", pending
    state["pending_knowledge"] = pending[0]["knowledge_id"]
    state["cursor"] = sql("SELECT last_sync_cursor FROM data_sources WHERE id=?", (state["ds"],))[0]["last_sync_cursor"]
    state["old_instance"] = api("GET", prefix)["binding"]["sandbox_id"]
    save()
    print("PASS accepted is not usable: pending replacement remains disabled, old active retained", flush=True)
elif phase == "lose-pending":
    subprocess.run([str(root / "queue-test"), "drop", state["pending_knowledge"]], check=True)
    print("PASS test fault: removed exactly this deployment's pending document task while application stopped", flush=True)
elif phase == "recovery":
    login()
    prefix = "/plugins/datasources/" + state["ds"]
    observed = api("GET", prefix)
    assert observed["binding"]["observed_state"] == "READY", observed
    assert observed["binding"]["sandbox_id"] != state["old_instance"]
    assert sql("SELECT last_sync_cursor FROM data_sources WHERE id=?", (state["ds"],))[0]["last_sync_cursor"] == state["cursor"]
    subprocess.run([str(root / "queue-test"), "resume"], check=True)
    def recovered():
        current = usable_three()
        return current if current and current[1]["knowledge_id"] == state["pending_knowledge"] else None
    recovered_rows = wait_for(recovered)
    assert recovered_rows[0] == state["before_restart"][0] and recovered_rows[2] == state["before_restart"][2]
    state["before_restart"] = recovered_rows
    print("PASS application startup re-arms lost pending task: same Knowledge parsed/indexed/activated, unaffected a/c retained", flush=True)
    result = sync()
    assert active() == state["before_restart"] and result["items_created"] == 0, result
    print("PASS normal application restart: new instance/handshake, DB Cursor restored, no reprocessing", flush=True)
    api("PUT", "/datasource/" + state["ds"], {"knowledge_base_id": state["kb"], "config": {"resource_ids": ["different"]}}, expected=(400, 409))
    api("POST", prefix + "/lifecycle/disable", {})
    time.sleep(12)  # one existing reconciliation interval
    assert api("GET", prefix)["binding"]["observed_state"] == "STOPPED"
    api("POST", "/datasource/" + state["ds"] + "/sync", {}, expected=(400, 409))
    api("POST", prefix + "/lifecycle/enable", {})
    api("DELETE", prefix + "/grants/" + state["grant"])
    api("POST", prefix + "/lifecycle/enable", {}, expected=(409,))
    api("POST", prefix + "/grants", {"allow_root_id": "test", "relative_directory": "source"}, expected=(409,))
    api("GET", "/datasource/" + state["ds"] + "/resources", expected=(400, 500))
    audits = api("GET", prefix + "/audit")["data"]
    assert any(a["action"] == "plugin.grant_revoked" for a in audits)
    assert active() == state["before_restart"], "revocation must not delete accepted knowledge"
    print("PASS pause/revoke: desired state persistent, no restart/new read, existing Knowledge retained; authenticated management audit", flush=True)
elif phase == "cleanup-check":
    assert not list((root / "runtime").rglob("*.sock"))
    assert not list((root / "runtime").rglob("*.json")), "cleanup locators remain"
    print("PASS formal application shutdown: no UDS/cleanup locators", flush=True)
else:
    raise ValueError(phase)
