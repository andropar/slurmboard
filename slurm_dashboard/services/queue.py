"""Job-first queue data, with explicit source failures and UTC lifecycle times."""
from __future__ import annotations

import os
import re
import subprocess

from slurm_dashboard.config import Config
from slurm_dashboard.services.logs import collect_recent_jobs

JOB_ID = re.compile(r"[0-9]+(?:_[0-9]+)?(?:\+[0-9]+)?", re.ASCII)
PENDING_STATES = {"PENDING", "REQUEUED", "REQUEUE_FED", "REQUEUE_HOLD"}
ACTIVE_STATES = PENDING_STATES | {
    "RUNNING", "CONFIGURING", "COMPLETING", "SUSPENDED", "RESIZING", "STAGE_OUT",
}


def _timestamp(value: str) -> str | None:
    if not value or value in {"Unknown", "N/A", "None", "0"}:
        return None
    return value + "Z" if not value.endswith("Z") else value


def _query(command: list[str], source: str, errors: list[str]) -> list[str]:
    try:
        result = subprocess.run(
            command, capture_output=True, text=True, timeout=30, check=False,
            env={**os.environ, "TZ": "UTC", "SLURM_TIME_FORMAT": "standard"},
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        errors.append(f"{source} unavailable: {exc}")
        return []
    if result.returncode:
        errors.append(f"{source} failed: {result.stderr.strip() or 'command failed'}")
        return []
    if result.stderr.strip():
        errors.append(f"{source}: {result.stderr.strip()}")
    return result.stdout.strip().splitlines()


def get_queue(config: Config, days: int) -> dict:
    """Merge accounting, live queue, and discovered logs without using log mtimes as dates."""
    errors: list[str] = []
    jobs: dict[str, dict] = {}
    accounting = _query([
        "sacct", "-u", config.user, "--starttime", f"now-{days}days",
        "--noheader", "--parsable2", "-X",
        "--format=JobID%64,JobName%1024,State%64,Submit,Start,End,"
        "Partition%128,Elapsed,ExitCode",
    ], "Slurm accounting", errors)
    for line in accounting:
        parts = [part.strip() for part in line.split("|")]
        if len(parts) != 9:
            errors.append("Slurm accounting returned a malformed job row.")
            continue
        job_id, name, state, queued, started, ended, partition, elapsed, exit_code = parts
        if not JOB_ID.fullmatch(job_id):
            continue
        state = state.split()[0].rstrip("+") if state else "UNKNOWN"
        jobs[job_id] = {
            "id": job_id, "name": name, "status": state, "queued": _timestamp(queued),
            "started": None if state in PENDING_STATES else _timestamp(started),
            "ended": None if state in ACTIVE_STATES else _timestamp(ended),
            "partition": partition, "elapsed": elapsed, "exit_code": exit_code,
        }

    live = _query([
        "squeue", "-u", config.user, "--noheader", "--array",
        "--format=%i|%j|%T|%V|%S|%P|%R",
    ], "Live Slurm queue", errors)
    for line in live:
        parts = [part.strip() for part in line.split("|")]
        if len(parts) != 7:
            errors.append("Live Slurm queue returned a malformed job row.")
            continue
        job_id, name, state, queued, started, partition, reason = parts
        if not JOB_ID.fullmatch(job_id):
            continue
        job = jobs.setdefault(job_id, {"id": job_id})
        job.update({
            "name": name, "status": state, "queued": _timestamp(queued),
            # squeue reports an estimated start for pending jobs, not an actual start.
            "started": None if state in PENDING_STATES else _timestamp(started),
            "ended": None, "partition": partition,
            "reason": reason if state in PENDING_STATES else "",
        })

    try:
        logs = collect_recent_jobs(config.log_root, config.log_pattern, limit=None)
    except OSError as exc:
        errors.append(f"Log discovery failed: {exc}")
        logs = []
    for log in logs:
        job = jobs.setdefault(log["id"], {
            "id": log["id"], "name": log["name"], "status": "UNKNOWN",
            "queued": None, "started": None, "ended": None,
            "reason": "No accounting record in the selected history window",
        })
        # A log directory may differ from Slurm's job name; retain its actual lookup key.
        job.setdefault("log_key", log["log_key"])
    for job in jobs.values():
        job.setdefault("log_key", f"{job['name']}::{job['id']}")

    return {
        "jobs": list(jobs.values()), "errors": list(dict.fromkeys(errors)),
        "user": config.user, "days": days,
        "scope": f"Live queue + {days} days of Slurm accounting + discovered logs",
    }
