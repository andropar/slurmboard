import subprocess
from unittest.mock import Mock

import pytest
from flask import Flask

from slurm_dashboard.config import Config, set_config
from slurm_dashboard.routes.api import api
from slurm_dashboard.services.slurm import get_submitted_script


def result(content="", error="", code=0):
    return subprocess.CompletedProcess([], code, content, error)


@pytest.fixture
def run(monkeypatch):
    mocked = Mock()
    monkeypatch.setattr("slurm_dashboard.services.slurm.subprocess.run", mocked)
    return mocked


def test_controller_copy_is_not_mutable_source(run, tmp_path):
    source = tmp_path / "job.sh"
    source.write_text("#!/bin/bash\necho changed\n")
    submitted = "#!/bin/bash\necho original\n"
    run.return_value = result(submitted)
    data = get_submitted_script("123", "alice")
    assert data["content"] == submitted
    assert data["source"] == "controller"
    assert data["truncated"] is False
    assert run.call_args.args[0] == ["scontrol", "write", "batch_script", "123", "-"]
    assert run.call_count == 1


def test_accounting_fallback_for_purged_job(run):
    run.side_effect = [
        result(error="Invalid job id specified", code=1),
        result("Batch Script for Job 123\n#!/bin/bash\necho archived\n"),
    ]
    data = get_submitted_script("123", "alice")
    assert data["source"] == "accounting"
    assert data["content"] == "#!/bin/bash\necho archived\n"
    assert run.call_args.args[0] == [
        "sacct", "--batch-script", "-j", "123", "-u", "alice", "--noheader",
    ]


@pytest.mark.parametrize("content", ["", "NONE\n", "Batch Script for Job 123\nNONE\n"])
def test_no_stored_script_is_unavailable(run, content):
    run.side_effect = [result(error="Invalid job id specified", code=1), result(content)]
    data = get_submitted_script("123", "alice")
    assert data["status"] == "unavailable"
    assert "content" not in data
    assert len(data["attempts"]) == 2


@pytest.mark.parametrize("failure,expected", [
    (result(error="Access/permission denied", code=1), "forbidden"),
    (result(error="unrecognized option '--batch-script'", code=1), "unsupported"),
    (FileNotFoundError(), "unsupported"),
    (subprocess.TimeoutExpired("scontrol", 10), "timeout"),
    (result(error="Unable to contact slurm controller", code=1), "error"),
    (PermissionError("Cannot execute Slurm client"), "error"),
])
def test_failures_are_explicit(run, failure, expected):
    run.side_effect = [failure, failure]
    data = get_submitted_script("123", "alice")
    assert data["status"] == expected
    assert data["error"]
    assert "content" not in data
    assert all(attempt["message"] for attempt in data["attempts"])


def test_array_task_id_is_passed_unchanged(run):
    run.return_value = result("#!/bin/bash\necho array\n")
    assert get_submitted_script("123_4", "alice")["status"] == "available"
    assert run.call_args.args[0][-2:] == ["123_4", "-"]


@pytest.mark.parametrize("job_id", ["--help", "123;echo bad", "１２３", "123.batch", "123_[1-5]"])
def test_invalid_id_never_runs_commands(run, job_id):
    assert get_submitted_script(job_id, "alice")["status"] == "invalid"
    run.assert_not_called()


@pytest.mark.parametrize("count,truncated", [(199, False), (200, False), (201, True)])
def test_line_limit_is_exact(run, count, truncated):
    run.return_value = result("#!/bin/bash\n" + "echo test\n" * (count - 1))
    data = get_submitted_script("123", "alice")
    assert len(data["content"].splitlines()) == min(count, 200)
    assert data["truncated"] is truncated


def test_byte_limit_is_labelled(run):
    run.return_value = result("#!/bin/bash\n" + "x" * (1024 * 1024))
    data = get_submitted_script("123", "alice")
    assert len(data["content"].encode()) == 1024 * 1024
    assert data["truncated"] is True


@pytest.mark.parametrize("status,code", [
    ("available", 200), ("forbidden", 403), ("unavailable", 404),
    ("unsupported", 501), ("timeout", 504), ("error", 502), ("invalid", 400),
])
def test_api_statuses_and_no_cache(monkeypatch, status, code):
    set_config(Config(user="alice"))
    mocked = Mock(return_value={"status": status})
    monkeypatch.setattr("slurm_dashboard.routes.api.get_submitted_script", mocked)
    app = Flask(__name__)
    app.register_blueprint(api)
    response = app.test_client().get("/api/submitted_script/123")
    assert response.status_code == code
    assert response.headers["Cache-Control"] == "no-store"
    mocked.assert_called_once_with("123", "alice")


def test_api_rejects_invalid_id_before_command(run):
    set_config(Config(user="alice"))
    app = Flask(__name__)
    app.register_blueprint(api)
    assert app.test_client().get("/api/submitted_script/--help").status_code == 400
    run.assert_not_called()
