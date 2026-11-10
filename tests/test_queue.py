import subprocess
from unittest.mock import Mock

import pytest

from slurm_dashboard.app import create_app
from slurm_dashboard.config import Config, LogPattern
from slurm_dashboard.routes.api import cached_queue
from slurm_dashboard.services.logs import collect_recent_jobs, safe_log_path
from slurm_dashboard.services.queue import get_queue


def output(text="", code=0, error=""):
    return subprocess.CompletedProcess([], code, text, error)


@pytest.fixture
def config(tmp_path):
    return Config(user="alice", log_root=tmp_path)


def test_queue_merges_live_accounting_and_log_identity(monkeypatch, config):
    name = "long_full_slurm_name_" * 30
    log_dir = config.log_root / "different_log_directory"
    log_dir.mkdir()
    (log_dir / "job.out.123").write_text("output")
    (config.log_root / "old").mkdir()
    (config.log_root / "old/job.err.99").write_text("error")
    run = Mock(side_effect=[
        output(
            f"123|{name}|COMPLETED|2026-11-09T01:00:00|2026-11-09T01:01:00|"
            "2026-11-09T01:05:00|gpu|00:04:00|0:0\n"
            "124_1|a|CANCELLED by 1|2026-11-09T01:00:00|Unknown|"
            "2026-11-09T01:05:00|gpu|00:00:00|0:15\n"
            "124_2|b|FAILED|2026-11-09T01:00:00|2026-11-09T01:01:00|"
            "2026-11-09T01:05:00|gpu|00:04:00|1:0\n"
        ),
        output(
            f"123|{name}|RUNNING|2026-11-09T01:00:00|2026-11-09T01:01:00|gpu|node1\n"
            "125|pending|PENDING|2026-11-09T01:00:00|2026-11-11T04:00:00|gpu|Resources\n"
        ),
    ])
    monkeypatch.setattr("slurm_dashboard.services.queue.subprocess.run", run)
    data = get_queue(config, 7)
    jobs = {job["id"]: job for job in data["jobs"]}
    assert len(jobs) == 5
    assert jobs["123"]["name"] == name
    assert jobs["123"]["status"] == "RUNNING"
    assert jobs["123"]["ended"] is None
    assert jobs["123"]["queued"] == "2026-11-09T01:00:00Z"
    assert jobs["123"]["log_key"] == "different_log_directory::123"
    assert jobs["124_1"]["status"] == "CANCELLED"
    assert jobs["124_1"]["started"] is None
    assert jobs["124_2"]["exit_code"] == "1:0"
    assert jobs["125"]["started"] is None  # The scheduler's estimate is not a start.
    assert jobs["99"]["status"] == "UNKNOWN"
    assert jobs["99"]["queued"] is jobs["99"]["started"] is jobs["99"]["ended"] is None
    assert data["errors"] == []
    assert run.call_args_list[0].kwargs["env"]["TZ"] == "UTC"
    assert run.call_args_list[0].kwargs["env"]["SLURM_TIME_FORMAT"] == "standard"
    assert "--array" in run.call_args_list[1].args[0]
    fields = (
        "--format=JobID%64,JobName%1024,State%64,Submit,Start,End,"
        "Partition%128,Elapsed,ExitCode"
    )
    assert fields in run.call_args_list[0].args[0]


@pytest.mark.parametrize("failure", [
    FileNotFoundError("missing command"),
    PermissionError("not executable"),
    subprocess.TimeoutExpired("sacct", 30),
    output(code=1, error="Slurm unavailable"),
])
def test_source_failure_is_visible_not_an_empty_success(monkeypatch, config, failure):
    run = Mock(side_effect=[failure, output("1|running|RUNNING|Unknown|Unknown|gpu|node1")])
    monkeypatch.setattr("slurm_dashboard.services.queue.subprocess.run", run)
    data = get_queue(config, 7)
    assert len(data["jobs"]) == 1
    assert data["errors"]
    assert "Slurm accounting" in data["errors"][0]


def test_malformed_output_is_explicit(monkeypatch, config):
    monkeypatch.setattr("slurm_dashboard.services.queue.subprocess.run", Mock(
        side_effect=[output("wrong|fields"), output("wrong|fields")]
    ))
    data = get_queue(config, 7)
    assert len(data["errors"]) == 2
    assert not data["jobs"]


def test_job_log_ids_include_array_tasks_and_stay_under_root(config):
    for job_id in ["12", "12_3", "12+1"]:
        directory = config.log_root / "name"
        directory.mkdir(exist_ok=True)
        path = directory / f"job.out.{job_id}"
        path.write_text("test")
        resolved = safe_log_path(f"name::{job_id}", "stdout", config.log_root, config.log_pattern)
        assert resolved == path
    assert len(collect_recent_jobs(config.log_root, config.log_pattern, limit=None)) == 3
    assert safe_log_path("../escape::12", "stdout", config.log_root, config.log_pattern) is None
    assert safe_log_path("name::12;echo", "stdout", config.log_root, config.log_pattern) is None
    assert safe_log_path("name::１２", "stdout", config.log_root, config.log_pattern) is None


def test_flat_log_pattern_uses_discovered_key(config, monkeypatch):
    config.log_pattern = LogPattern("slurm-{id}.{stream}")
    (config.log_root / "slurm-123_2.out").write_text("test")
    monkeypatch.setattr("slurm_dashboard.services.queue.subprocess.run", Mock(side_effect=[
        output("123_2|real_name|COMPLETED|Unknown|Unknown|Unknown|gpu|01:00:00|0:0"),
        output(),
    ]))
    job = get_queue(config, 7)["jobs"][0]
    assert job["log_key"] == "123_2::123_2"
    assert job["name"] == "real_name"


def test_queue_endpoint_full_dataset_and_configuration_cache(config, monkeypatch):
    cached_queue.cache_clear()
    get = Mock(return_value={
        "jobs": [{"id": str(i)} for i in range(1201)],
        "errors": [], "days": 7,
    })
    monkeypatch.setattr("slurm_dashboard.routes.api.get_queue", get)
    app = create_app(config)
    client = app.test_client()
    first = client.get("/api/queue?days=7")
    assert len(first.json["jobs"]) == 1201  # No server cutoff before global sort/filter.
    assert first.headers["Cache-Control"] == "no-store"
    client.get("/api/queue?days=7")
    assert get.call_count == 1
    client.get("/api/queue?days=30")
    assert get.call_count == 2
    for invalid in ["0", "-1", "10000", "banana", "1.5"]:
        assert client.get(f"/api/queue?days={invalid}").status_code == 400
    assert get.call_count == 2
    assert b"js/queue.js" in client.get("/").data
    assert b"js/dashboard.js" in client.get("/advanced").data
    cached_queue.cache_clear()
