"""Shared fixtures: a fake Slurm installation and an app wired to it."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pytest

FAKE_DIR = Path(__file__).parent / "fake_slurm"
sys.path.insert(0, str(FAKE_DIR))

import demo  # noqa: E402

from slurm_dashboard.app import create_app  # noqa: E402
from slurm_dashboard.config import Config, LogPattern  # noqa: E402

ME = "tester"
HEADERS = {"X-Slurm-Dashboard": "1"}


class Cluster:
    """Handle on the fake cluster's state file."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.state_path = demo.build(root, me=ME)

    @property
    def state(self) -> dict:
        return json.loads(self.state_path.read_text())

    def update(self, **changes) -> None:
        state = self.state
        state.update(changes)
        self.state_path.write_text(json.dumps(state))

    def job(self, job_id: str) -> dict:
        return next(j for j in self.state["jobs"] if j["id"] == job_id)


@pytest.fixture()
def cluster(tmp_path, monkeypatch) -> Cluster:
    fake = Cluster(tmp_path / "cluster")
    monkeypatch.setenv("PATH", f"{FAKE_DIR}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("FAKE_SLURM_STATE", str(fake.state_path))
    monkeypatch.setenv("FAKE_SLURM_PYTHON", sys.executable)
    monkeypatch.setattr("getpass.getuser", lambda: ME)
    return fake


def make_config(cluster: Cluster, tmp_path: Path, **overrides) -> Config:
    settings = dict(
        log_root=cluster.root / "logs",
        user=ME,
        log_pattern=LogPattern("{name}/job.{stream}.{id}"),
        auth=False,
        state_dir=tmp_path / "state",
        refresh_cache=5,
    )
    settings.update(overrides)
    return Config(**settings)


@pytest.fixture()
def app(cluster, tmp_path):
    return create_app(make_config(cluster, tmp_path))


@pytest.fixture()
def client(app):
    return app.test_client()
