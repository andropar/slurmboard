"""End-to-end API tests against the fake Slurm CLI."""

from __future__ import annotations

import json
import time

from conftest import HEADERS, ME, make_config

from slurm_dashboard.app import create_app
from slurm_dashboard.services.logs import search_log, tail_events


def get(client, url):
    response = client.get(url)
    assert response.status_code == 200, response.get_data(as_text=True)
    return response.get_json()


def first_events(client, url, count=2):
    """Read the first SSE data events of a stream."""
    response = client.get(url, buffered=False)
    events = []
    for chunk in response.response:
        for frame in chunk.decode().split("\n\n"):
            if frame.startswith("data: "):
                events.append(json.loads(frame[6:]))
        if len(events) >= count:
            break
    response.close()
    return events


# ---------------------------------------------------------------- job listing


def test_jobs_come_from_slurm_and_are_scoped_to_the_user(client):
    data = get(client, "/api/jobs")
    assert data["errors"] == []
    running = {job["id"]: job for job in data["running"]}
    assert {"5001", "5002", "5003", "5010_1", "5010_2"} <= set(running)
    assert "6001" not in running  # alice's job
    assert running["5010_1"]["array_job_id"] == "5010"  # array tasks stay separate
    assert running["5005"]["expected_start"] and running["5005"]["reason"] == "Priority"
    assert all(job["user"] == ME for job in data["recent"] if job.get("user"))
    states = {job["state"] for job in data["recent"]}
    assert "COMPLETED" in states and "CANCELLED" in states  # "CANCELLED by 1000" normalized
    assert not {job["id"] for job in data["recent"]} & set(running)


def test_job_without_matching_log_pattern_is_still_listed(cluster, tmp_path):
    app = create_app(make_config(cluster, tmp_path, log_root=tmp_path / "nowhere"))
    data = get(app.test_client(), "/api/jobs")
    assert len(data["recent"]) >= 30


def test_log_only_jobs_are_listed_as_unknown(client):
    orphan = next(job for job in get(client, "/api/jobs")["recent"] if job["id"] == "1234")
    assert orphan["state"] == "UNKNOWN" and orphan["name"] == "legacy_run"


def test_all_users_mode_is_read_only_for_other_users(cluster, tmp_path):
    client = create_app(make_config(cluster, tmp_path, all_users=True)).test_client()
    data = get(client, "/api/jobs")
    running = {job["id"]: job for job in data["running"]}
    assert running["6001"]["user"] == "alice" and running["6001"]["owned"] is False
    assert running["5001"]["owned"] is True
    assert any(job["user"] == "alice" for job in data["recent"])

    response = client.post("/api/cancel/6001", headers=HEADERS)
    assert response.status_code == 400 and "alice" in response.get_json()["error"]
    assert cluster.job("6001")["state"] == "RUNNING"


def test_slurm_failure_is_reported_not_hidden(cluster, client):
    assert get(client, "/api/jobs")["errors"] == []
    cluster.update(fail=["squeue", "sacct"])
    time.sleep(0.1)
    client.application.extensions["slurm"].cache._values = {
        key: (0.0, value)
        for key, (_, value) in client.application.extensions["slurm"].cache._values.items()
    }
    data = get(client, "/api/jobs")
    assert any("squeue failed" in error for error in data["errors"])
    assert any("sacct failed" in error for error in data["errors"])
    assert len(data["running"]) > 0  # stale data is kept


def test_requests_share_cached_slurm_calls(cluster, client, monkeypatch):
    calls = []
    from slurm_dashboard.services import slurm as slurm_module

    real = slurm_module.run_command
    monkeypatch.setattr(
        slurm_module, "run_command", lambda cmd, **kw: calls.append(cmd[0]) or real(cmd, **kw)
    )
    for url in (
        "/api/jobs",
        "/api/jobs",
        "/api/job_history?days=3",
        "/api/job_dependencies",
        "/api/jobs",
    ):
        get(client, url)
    assert calls.count("squeue") == 1
    assert calls.count("sacct") == 2  # --helpformat probe + one history query


# -------------------------------------------------------------------- details


def test_details_include_efficiency_from_job_steps(client):
    details = get(client, "/api/job_details/4990")
    assert details["cpu_eff"].endswith("%") and details["mem_eff"].endswith("%")
    assert details["max_rss"] and details["exit_code"] is not None
    assert get(client, "/api/job_details/5001")["exit_code"] is None  # still running


def test_invalid_job_ids_are_rejected(client):
    assert client.get("/api/job_details/1;id").status_code == 400
    assert client.post("/api/cancel/abc", headers=HEADERS).status_code == 400


def test_script_is_looked_up_by_job_never_by_path(client):
    script = get(client, "/api/job_script/4990")
    assert script["source"] == "slurm" and script["content"].startswith("#!/bin/bash")
    assert client.get("/api/script_content?path=/etc/passwd").status_code == 404


def test_script_falls_back_to_file_on_disk(cluster, client):
    state = cluster.state
    for job in state["jobs"]:
        job.pop("script", None)
    cluster.state_path.write_text(json.dumps(state))
    script = get(client, "/api/job_script/4990")
    assert script["source"] == "file" and script["path"].endswith(".sh")


# -------------------------------------------------------------------- actions


def test_cancel_own_job(cluster, client):
    assert client.post("/api/cancel/5002", headers=HEADERS).get_json() == {"success": True}
    assert cluster.job("5002")["state"] == "CANCELLED"
    assert "5002" not in {job["id"] for job in get(client, "/api/jobs")["running"]}


def test_submit_script_text_goes_through_stdin(cluster, client, tmp_path):
    body = {"script_content": "echo hi", "job_name": "adhoc", "partition": "cpu", "cpus": 2}
    result = client.post("/api/submit", json=body, headers=HEADERS).get_json()
    assert result["success"] and result["job_id"] == "9000"
    job = cluster.job("9000")
    assert job["name"] == "adhoc" and job["cpus"] == 2
    assert job["script"] == "#!/bin/bash\necho hi\n"
    assert "9000" in {j["id"] for j in get(client, "/api/jobs")["running"]}


def test_submit_reports_sbatch_errors(client):
    response = client.post(
        "/api/submit", json={"script_path": "/no/such/script.sh"}, headers=HEADERS
    )
    assert response.status_code == 400 and "Script not found" in response.get_json()["error"]
    assert client.post("/api/submit", json={}, headers=HEADERS).status_code == 400


def test_resubmit_by_job_id_with_overrides(cluster, client):
    body = {"job_id": "4990", "memory": "64G"}
    result = client.post("/api/resubmit", json=body, headers=HEADERS).get_json()
    new_job = cluster.job(result["job_id"])
    assert new_job["req_mem"] == "64G" and new_job["command"].endswith(".sh")


def test_writes_need_the_csrf_header(cluster, client):
    assert client.post("/api/cancel/5002").status_code == 403
    cross_site = dict(HEADERS, Origin="http://evil.example")
    assert client.post("/api/cancel/5002", headers=cross_site).status_code == 403
    assert cluster.job("5002")["state"] == "RUNNING"


# ----------------------------------------------------------------------- auth


def test_token_auth_flow(cluster, tmp_path):
    client = create_app(
        make_config(cluster, tmp_path, auth=True), token="s3cret-token"
    ).test_client()
    assert client.get("/api/jobs").status_code == 401
    assert client.get("/").status_code == 401
    assert client.get("/static/js/dashboard.js").status_code == 401
    assert client.get("/?token=wrong").status_code == 401
    assert (
        client.get("/api/jobs", headers={"Authorization": "Bearer s3cret-token"}).status_code == 200
    )

    response = client.get("/?token=s3cret-token")
    assert response.status_code == 302 and "token" not in response.headers["Location"]
    assert (
        "HttpOnly" in response.headers["Set-Cookie"]
        and "SameSite=Strict" in response.headers["Set-Cookie"]
    )
    assert client.get("/api/jobs").status_code == 200  # cookie is now set
    assert client.get("/").status_code == 200


def test_token_is_persisted_with_private_permissions(cluster, tmp_path):
    config = make_config(cluster, tmp_path, auth=True)
    first = create_app(config).extensions["token"]
    token_file = config.state_dir / "token"
    assert token_file.read_text().strip() == first
    assert token_file.stat().st_mode & 0o077 == 0
    assert create_app(config).extensions["token"] == first


# ----------------------------------------------------------------------- logs


def test_log_stream_for_running_job_uses_scheduler_path(client):
    events = first_events(client, "/stream_log?log_key=5001&kind=stdout")
    assert events[0]["path"].endswith("train_resnet/job.out.5001")
    assert "UserWarning: synthetic warning" in events[1]["snapshot"]


def test_log_found_from_accounting_without_log_pattern(cluster, tmp_path):
    client = create_app(make_config(cluster, tmp_path, log_root=tmp_path / "nowhere")).test_client()
    events = first_events(client, "/stream_log?log_key=4990&kind=stdout")
    assert events[0]["path"].endswith("job.out.4990") and "loss=" in events[1]["snapshot"]


def test_missing_log_explains_where_it_looked(client):
    events = first_events(client, "/stream_log?log_key=5005&kind=stdout", count=1)
    assert "No stdout file found for job 5005" in events[0]["error"]
    assert "job.out.5005" in events[0]["error"] and "not started yet" in events[0]["error"]
    unknown = first_events(client, "/stream_log?log_key=424242&kind=stdout", count=1)
    assert "unknown to Slurm" in unknown[0]["error"]


def test_log_stream_rejects_path_tricks(client):
    events = first_events(client, "/stream_log?log_key=../../etc/passwd::1&kind=stdout", count=1)
    assert events[0]["error"] == "Invalid job ID"


def test_tail_follows_appends_and_truncation(tmp_path):
    log = tmp_path / "job.out"
    log.write_text("one\n")
    stream = tail_events(log, poll=0.01, heartbeat=0.03)
    assert next(stream)["snapshot"] == "one\n"
    with log.open("a") as handle:
        handle.write("two\nthree\n")
    assert next(stream) == {"append": "two\nthree\n"}
    assert next(stream) == {"ping": True}
    log.write_text("new\n")
    assert next(stream) == {"reset": True}
    assert next(stream) == {"append": "new\n"}


def test_search_log_streams_with_context(tmp_path):
    log = tmp_path / "job.out"
    log.write_text(
        "\n".join(f"line {i}" if i % 10 else f"ERROR at {i}" for i in range(1, 101)) + "\n"
    )
    result = search_log(log, "error", context_lines=1)
    assert result["total_matches"] == 10
    first = result["matches"][0]
    assert first["line_number"] == 10
    assert [c["text"] for c in first["context_before"]] == ["line 9"]
    assert [c["text"] for c in first["context_after"]] == ["line 11"]
    assert "error" in search_log(log, "(", use_regex=True)
    assert search_log(log, "(", use_regex=False)["total_matches"] == 0


def test_search_endpoint(client):
    data = get(client, "/api/search_log?log_key=5001&kind=stdout&q=UserWarning")
    assert data["total_matches"] == 1


# ------------------------------------------------------------------ analytics


def test_timeline_history(client):
    jobs = get(client, "/api/job_history?days=7")["jobs"]
    assert jobs and all(job["start"] for job in jobs)
    assert {job["state_category"] for job in jobs} <= {
        "completed",
        "running",
        "pending",
        "failed",
        "timeout",
        "unknown",
    }


def test_pipeline_graph(client):
    graph = get(client, "/api/job_dependencies")
    edges = {(edge["from"], edge["to"]) for edge in graph["edges"]}
    assert {("5001", "5003"), ("5003", "5004")} <= edges
    pipeline = next(p for p in graph["pipelines"] if "5001" in p["job_ids"])
    assert pipeline["job_ids"] == ["5001", "5003", "5004"] and pipeline["total"] == 3
    array_node = next(node for node in graph["nodes"] if node["id"] == "5010")
    assert array_node["tasks"] == 2 and array_node["state_category"] == "running"


def test_insights_usage_and_heatmap(client):
    insights = get(client, "/api/insights?days=30")
    stats = insights["job_stats"]
    assert (
        stats["total_jobs"]
        == stats["completed"] + stats["failed"] + stats["cancelled"] + stats["timeout"]
    )
    assert insights["memory_insights"]["sample_count"] > 0  # needs MaxRSS from steps
    assert insights["efficiency_score"]["cpu_efficiency"] is not None

    usage = get(client, "/api/usage?days=30")
    assert usage["total_gpu_hours"] > 0 and usage["by_partition"]
    assert "total_sus" not in usage

    heat = get(client, "/api/heatmap?days=30")
    assert len(heat["hourly"]) == 168 and heat["total_jobs"] == sum(
        d["total"] for d in heat["daily"]
    )


def test_removed_heuristic_endpoints_are_gone(client):
    for url in ("/api/predict/5001", "/api/queue_info", "/api/cost"):
        assert client.get(url).status_code == 404


def test_partitions(client):
    assert get(client, "/api/partitions")["partitions"] == [
        {"name": "gpu", "default": True},
        {"name": "cpu", "default": False},
    ]


# ------------------------------------------------------------------ templates


def test_templates_survive_a_restart(cluster, tmp_path):
    config = make_config(cluster, tmp_path)
    client = create_app(config).test_client()
    body = {
        "name": "GPU Train",
        "script_content": "#!/bin/bash\necho",
        "partition": "gpu",
        "cpus": 4,
    }
    saved = client.post("/api/templates", json=body, headers=HEADERS).get_json()["template"]
    assert saved["id"] == "gpu_train"

    restarted = create_app(config).test_client()
    assert [t["name"] for t in get(restarted, "/api/templates")["templates"]] == ["GPU Train"]
    assert restarted.delete("/api/templates/gpu_train", headers=HEADERS).status_code == 200
    assert get(restarted, "/api/templates")["templates"] == []
    assert restarted.delete("/api/templates/gpu_train", headers=HEADERS).status_code == 404
