"""Slurm client: one cached source of truth for queue and accounting data.

Every view of the dashboard reads from two datasets:

* the live queue (one ``squeue`` call), and
* accounting history (one ``sacct`` call per time window).

Both are cached and shared between requests, so the number of Slurm commands
does not grow with the number of open tabs or views.
"""

from __future__ import annotations

import getpass
import logging
import re
import shlex
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from slurm_dashboard.services.parsing import (
    array_parts,
    format_elapsed,
    is_active_state,
    normalize_state,
    parse_exit_code,
    parse_gpu_count,
    parse_memory,
    parse_scontrol_job,
    parse_time_to_seconds,
    parse_timestamp,
    requested_memory_bytes,
    split_job_id,
    state_category,
    timestamp_epoch,
)
from slurm_dashboard.services.runner import SlurmError, TTLCache, run_command

# Re-exported for backwards compatibility with 0.1.x imports.
__all__ = [
    "Slurm",
    "SlurmError",
    "parse_gpu_count",
    "parse_memory",
    "parse_time_to_seconds",
]

logger = logging.getLogger(__name__)

# Unit separator: cannot appear in job names, paths or command lines, unlike "|".
SEP = "\x1f"

QUEUE_FIELDS = [
    ("id", "%i"),
    ("name", "%j"),
    ("user", "%u"),
    ("state", "%T"),
    ("runtime", "%M"),
    ("limit", "%l"),
    ("nodes", "%D"),
    ("reason", "%r"),
    ("nodelist", "%N"),
    ("partition", "%P"),
    ("start", "%S"),
    ("submit", "%V"),
    ("dependency", "%E"),
    ("gres", "%b"),
    ("cpus", "%C"),
    ("array_job_id", "%F"),
    ("array_task_id", "%K"),
    ("priority", "%Q"),
]

ACCT_FIELDS = [
    "JobID",
    "JobIDRaw",
    "JobName",
    "User",
    "State",
    "ExitCode",
    "Submit",
    "Start",
    "End",
    "ElapsedRaw",
    "TimelimitRaw",
    "Partition",
    "AllocCPUS",
    "NNodes",
    "ReqMem",
    "MaxRSS",
    "TotalCPU",
    "NodeList",
    "WorkDir",
]
# Only requested when this Slurm version knows them (an unknown field makes sacct fail).
ACCT_OPTIONAL_FIELDS = ["AllocTRES", "AllocGRES", "SubmitLine", "StdOut", "StdErr"]

JOB_ID_RE = re.compile(r"^\d+(?:_(?:\d+|\[[\d,\-:%]+\]))?(?:\+\d+)?$")

QUEUE_TTL = 5
SCONTROL_TTL = 10
STATIC_TTL = 600
MAX_SCRIPT_BYTES = 1024 * 1024


def valid_job_id(job_id: str) -> bool:
    """True for plain, array ("12_3", "12_[1-5%2]") and het ("12+1") job IDs."""
    return bool(job_id) and len(job_id) < 128 and bool(JOB_ID_RE.match(job_id))


class Slurm:
    """Cached access to Slurm for one dashboard instance.

    Args:
        user: Whose jobs to show. None shows every user's jobs (read-only).
        accounting_ttl: Seconds an sacct result is reused.
    """

    def __init__(self, user: Optional[str], accounting_ttl: int = 30) -> None:
        self.user = user
        self.accounting_ttl = max(5, accounting_ttl)
        self.os_user = getpass.getuser()
        self.cache = TTLCache()

    # ------------------------------------------------------------------ queue

    def queue(self) -> Tuple[List[dict], Optional[str]]:
        """Jobs the scheduler currently tracks. Returns (jobs, error)."""
        jobs, error = self.cache.get(("queue", self.user), QUEUE_TTL, self._load_queue)
        return jobs or [], error

    def _load_queue(self) -> List[dict]:
        cmd = ["squeue", "--noheader", "-o", SEP.join(spec for _, spec in QUEUE_FIELDS)]
        if self.user:
            cmd += ["-u", self.user]
        out = run_command(cmd, timeout=15)
        names = [name for name, _ in QUEUE_FIELDS]
        jobs = []
        for line in out.splitlines():
            parts = line.split(SEP)
            if len(parts) != len(names):
                continue
            row = {name: value.strip() for name, value in zip(names, parts)}
            state = normalize_state(row["state"])
            category = state_category(state)
            start = parse_timestamp(row["start"])
            array_task = row["array_task_id"] if row["array_task_id"] not in {"N/A", ""} else None
            jobs.append(
                {
                    "id": row["id"],
                    "name": row["name"],
                    "user": row["user"],
                    "state": state,
                    "state_category": category,
                    "runtime": row["runtime"],
                    "limit": row["limit"],
                    "nodes": row["nodes"],
                    "reason": row["reason"] if row["reason"] not in {"None", "(null)"} else "",
                    "nodelist": row["nodelist"],
                    "partition": row["partition"],
                    # For pending jobs squeue reports the scheduler's planned start.
                    "start": start if category != "pending" else None,
                    "expected_start": start if category == "pending" else None,
                    "submit": parse_timestamp(row["submit"]),
                    "dependency": row["dependency"] if row["dependency"] != "(null)" else "",
                    "gres": row["gres"] if row["gres"] not in {"N/A", "(null)"} else "",
                    "cpus": row["cpus"],
                    "array_job_id": row["array_job_id"] if array_task else None,
                    "array_task_id": array_task,
                    "priority": row["priority"],
                    "log_key": row["id"],
                    "owned": row["user"] == self.os_user,
                }
            )
        return jobs

    # ------------------------------------------------------------- accounting

    def _acct_fields(self) -> List[str]:
        """Fields to request from sacct, adapted to the installed version."""

        def load() -> List[str]:
            try:
                supported = set(run_command(["sacct", "--helpformat"], timeout=10).split())
            except SlurmError:
                return ACCT_FIELDS + ["AllocTRES"]
            fields = [f for f in ACCT_FIELDS if not supported or f in supported]
            optional = [f for f in ACCT_OPTIONAL_FIELDS if f in supported]
            if "AllocTRES" in optional and "AllocGRES" in optional:
                optional.remove("AllocGRES")
            return fields + optional

        fields, _ = self.cache.get(("acct_fields",), STATIC_TTL * 100, load)
        return fields or ACCT_FIELDS

    def accounting(self, days: int) -> Tuple[List[dict], Optional[str]]:
        """Jobs from the accounting database for the last `days` days.

        A fresh result for a longer window is reused instead of querying again.
        """
        cutoff = time.time() - days * 86400
        for key in self.cache.keys():
            if isinstance(key, tuple) and key[:2] == ("acct", self.user) and key[2] > days:
                wider = self.cache.peek(key, self.accounting_ttl)
                if wider is not None:
                    return [j for j in wider if _in_window(j, cutoff)], None
        jobs, error = self.cache.get(
            ("acct", self.user, days), self.accounting_ttl, lambda: self._load_accounting(days)
        )
        return jobs or [], error

    def _load_accounting(self, days: int, job_id: Optional[str] = None) -> List[dict]:
        fields = self._acct_fields()
        cmd = [
            "sacct",
            "--noheader",
            "--parsable2",
            f"--delimiter={SEP}",
            f"--format={','.join(fields)}",
        ]
        if job_id:
            cmd += ["-j", job_id]
        else:
            cmd += ["--starttime", f"now-{days}days"]
            cmd += ["-u", self.user] if self.user else ["--allusers"]
        out = run_command(cmd, timeout=90 if not job_id else 20)
        return self._parse_accounting(out, fields)

    def _parse_accounting(self, out: str, fields: List[str]) -> List[dict]:
        jobs: Dict[str, dict] = {}
        steps: List[Tuple[str, Dict[str, str]]] = []
        for line in out.splitlines():
            parts = line.split(SEP)
            if len(parts) != len(fields):
                continue
            row = dict(zip(fields, parts))
            parent_id, step = split_job_id(row["JobID"].strip())
            if step is not None:
                steps.append((parent_id, row))
            else:
                jobs[parent_id] = self._acct_job(row)

        # Memory usage is only recorded on steps; fold it into the parent job.
        for parent_id, row in steps:
            job = jobs.get(parent_id)
            if job is None:
                continue
            job["max_rss_bytes"] = max(job["max_rss_bytes"], parse_memory_kb(row.get("MaxRSS", "")))
        for job in jobs.values():
            _add_efficiency(job)
        return sorted(jobs.values(), key=_recency, reverse=True)

    def _acct_job(self, row: Dict[str, str]) -> dict:
        job_id = row["JobID"].strip()
        state = normalize_state(row.get("State", ""))
        cpus = _to_int(row.get("AllocCPUS"))
        nodes = _to_int(row.get("NNodes"))
        elapsed = _to_int(row.get("ElapsedRaw"))
        limit_raw = row.get("TimelimitRaw", "").strip()
        tres = (row.get("AllocTRES") or row.get("AllocGRES") or "").strip()
        base, task = array_parts(job_id)
        user = row.get("User", "").strip()
        return {
            "id": job_id,
            "raw_id": row.get("JobIDRaw", "").strip() or job_id,
            "name": row.get("JobName", "").strip(),
            "user": user,
            "state": state,
            "state_category": state_category(state),
            "exit_code": parse_exit_code(row.get("ExitCode", "").strip()),
            "submit": parse_timestamp(row.get("Submit", "")),
            "start": parse_timestamp(row.get("Start", "")),
            "end": parse_timestamp(row.get("End", "")),
            "elapsed_seconds": elapsed,
            "elapsed": format_elapsed(elapsed),
            "timelimit_seconds": _to_int(limit_raw) * 60 if limit_raw.isdigit() else 0,
            "partition": row.get("Partition", "").strip(),
            "cpus": cpus,
            "nodes": nodes,
            "req_mem": row.get("ReqMem", "").strip(),
            "req_mem_bytes": requested_memory_bytes(row.get("ReqMem", ""), cpus, nodes),
            "max_rss_bytes": parse_memory_kb(row.get("MaxRSS", "")),
            "total_cpu_seconds": parse_time_to_seconds(row.get("TotalCPU", "")),
            "gres": tres,
            "gpus": parse_gpu_count(tres),
            "nodelist": row.get("NodeList", "").strip(),
            "work_dir": row.get("WorkDir", "").strip(),
            "submit_line": row.get("SubmitLine", "").strip(),
            "stdout_pattern": row.get("StdOut", "").strip(),
            "stderr_pattern": row.get("StdErr", "").strip(),
            "array_job_id": base if task else None,
            "array_task_id": task,
            "log_key": job_id,
            "owned": user == self.os_user,
        }

    # ----------------------------------------------------------------- lookup

    def find_job(self, job_id: str, history_days: int = 7) -> Optional[dict]:
        """Find a job by ID in the queue, cached history, or by a direct query.

        The returned dict always has an ``active`` flag; accounting fields are
        merged in when both sources know the job.
        """
        if not valid_job_id(job_id):
            return None
        queued = next((j for j in self.queue()[0] if j["id"] == job_id), None)
        record = next((j for j in self.accounting(history_days)[0] if j["id"] == job_id), None)
        if record is None:
            single, _ = self.cache.get(
                ("acct_job", job_id),
                self.accounting_ttl,
                lambda: self._load_accounting(0, job_id=job_id),
            )
            record = next((j for j in single or [] if j["id"] == job_id), None)
        if queued is None and record is None:
            return None
        if self.user and (queued or record or {}).get("user") not in {self.user, ""}:
            return None
        merged: Dict[str, Any] = dict(record or {})
        if queued:
            merged.update({k: v for k, v in queued.items() if v not in (None, "")})
        merged["active"] = queued is not None or is_active_state(merged.get("state", ""))
        return merged

    def scontrol_job(self, job_id: str) -> Dict[str, str]:
        """`scontrol show job` fields for a job the scheduler still tracks."""
        info, _ = self.cache.get(
            ("scontrol", job_id),
            SCONTROL_TTL,
            lambda: parse_scontrol_job(
                run_command(["scontrol", "show", "job", job_id], timeout=10)
            ),
        )
        return info or {}

    def job_details(self, job_id: str, history_days: int = 7) -> dict:
        """State, timing and efficiency for one job."""
        job = self.find_job(job_id, history_days)
        if job is None:
            return {"error": "Job not found in Slurm"}
        max_rss = job.get("max_rss_bytes") or 0
        return {
            "id": job["id"],
            "name": job.get("name"),
            "user": job.get("user"),
            "state": job.get("state"),
            "exit_code": None if job["active"] else job.get("exit_code"),
            "cpu_eff": _percent(job.get("cpu_eff")),
            "mem_eff": _percent(job.get("mem_eff")),
            "submit_time": job.get("submit"),
            "start_time": job.get("start"),
            "end_time": job.get("end") or "N/A",
            "elapsed": job.get("elapsed") or job.get("runtime"),
            "partition": job.get("partition"),
            "nodelist": job.get("nodelist"),
            "cpus": job.get("cpus"),
            "gpus": job.get("gpus"),
            "req_mem": job.get("req_mem"),
            "max_rss": f"{max_rss / 1024**2:.0f}M" if max_rss else None,
            "work_dir": job.get("work_dir"),
            "reason": job.get("reason"),
        }

    def submit_info(self, job_id: str, history_days: int = 7) -> dict:
        """How a job was submitted: script, working directory, resources."""
        job = self.find_job(job_id, history_days)
        if job is None:
            return {"error": "Job not found in Slurm"}
        info = {
            "job_id": job["id"],
            "submit_line": job.get("submit_line") or None,
            "work_dir": job.get("work_dir") or None,
            "job_name": job.get("name") or None,
            "partition": job.get("partition") or None,
            "timelimit": None,
            "req_mem": job.get("req_mem") or None,
            "req_cpus": job.get("cpus") or None,
            "script_path": None,
            "owned": bool(job.get("owned")),
        }
        if job.get("timelimit_seconds"):
            info["timelimit"] = format_elapsed(job["timelimit_seconds"])
        if job["active"]:
            ctl = self.scontrol_job(job["id"])
            info["work_dir"] = ctl.get("WorkDir") or info["work_dir"]
            info["timelimit"] = ctl.get("TimeLimit") or info["timelimit"]
            command = ctl.get("Command", "")
            if command and command != "(null)":
                info["submit_line"] = info["submit_line"] or command
                info["script_path"] = command.split()[0]
        if not info["script_path"]:
            info["script_path"] = _script_from_submit_line(info["submit_line"], info["work_dir"])
        return info

    def job_script(self, job_id: str, history_days: int = 7) -> dict:
        """The batch script of a job.

        Prefers the copy Slurm retained at submission; falls back to the script
        file on disk, whose path is taken from Slurm, never from the client.
        """
        job = self.find_job(job_id, history_days)
        if job is None:
            return {"error": "Job not found in Slurm"}
        raw_id = job.get("raw_id") or job["id"]
        try:
            if job["active"]:
                content = run_command(
                    ["scontrol", "write", "batch_script", job["id"], "-"], timeout=10
                )
            else:
                content = _strip_sacct_script_header(
                    run_command(["sacct", "--batch-script", "-j", raw_id], timeout=15)
                )
            if content.strip():
                return {"content": content, "source": "slurm", "path": None, "truncated": False}
        except SlurmError as exc:
            logger.debug("No retained script for %s: %s", job_id, exc)

        path = self.submit_info(job_id, history_days).get("script_path")
        if not path:
            return {"error": "Slurm kept no copy of this script and its path is unknown"}
        script = Path(path)
        try:
            if not script.is_file():
                return {"error": f"Script file no longer exists: {path}"}
            if script.stat().st_size > MAX_SCRIPT_BYTES:
                return {"error": "Script file is larger than 1 MB"}
            content = script.read_text(errors="replace")
        except OSError as exc:
            return {"error": f"Cannot read script: {exc.strerror or exc}"}
        return {"content": content, "source": "file", "path": str(script), "truncated": False}

    def job_resources(self, job_id: str) -> dict:
        """Live memory/CPU usage of a running job from sstat."""
        if not valid_job_id(job_id):
            return {"error": "Invalid job ID"}
        fields = ["JobID", "AveCPU", "AveRSS", "MaxRSS", "MaxVMSize", "NTasks"]
        try:
            out = run_command(
                [
                    "sstat",
                    "-j",
                    job_id,
                    "--allsteps",
                    "--noheader",
                    "--parsable2",
                    f"--format={','.join(fields)}",
                ],
                timeout=10,
            )
        except SlurmError as exc:
            return {"error": str(exc)}
        rows = [
            dict(zip(fields, line.split("|"))) for line in out.splitlines() if line.count("|") == 5
        ]
        rows = [r for r in rows if not r["JobID"].endswith(".extern")]
        if not rows:
            return {"error": "No resource data yet (the job has no running steps)"}
        peak = max(rows, key=lambda r: parse_memory_kb(r["MaxRSS"]))
        return {
            "ave_cpu": peak["AveCPU"],
            "ave_rss": peak["AveRSS"],
            "max_rss": peak["MaxRSS"],
            "max_vm": peak["MaxVMSize"],
            "ntasks": peak["NTasks"],
        }

    def partitions(self) -> Tuple[List[dict], Optional[str]]:
        """Partitions of the cluster as [{"name", "default"}]."""

        def load() -> List[dict]:
            out = run_command(["sinfo", "--noheader", "-o", "%P"], timeout=10)
            seen: Dict[str, dict] = {}
            for line in out.splitlines():
                name = line.strip()
                if name:
                    seen.setdefault(
                        name.rstrip("*"), {"name": name.rstrip("*"), "default": name.endswith("*")}
                    )
            return list(seen.values())

        parts, error = self.cache.get(("partitions",), STATIC_TTL, load)
        return parts or [], error

    # ---------------------------------------------------------------- actions

    def _require_owned(self, job_id: str, history_days: int = 7) -> dict:
        job = self.find_job(job_id, history_days)
        if job is None:
            raise SlurmError(f"Job {job_id} not found in Slurm")
        if not job.get("owned"):
            raise SlurmError(
                f"Job {job_id} belongs to {job.get('user') or 'another user'}; "
                "the dashboard only changes your own jobs"
            )
        return job

    def _invalidate_jobs(self) -> None:
        """Forget cached job lists so the next poll reflects a change at once."""
        for prefix in ("queue", "acct", "acct_job", "scontrol"):
            self.cache.invalidate(prefix)

    def cancel(self, job_id: str) -> None:
        """Cancel one of the current user's jobs."""
        job = self._require_owned(job_id)
        if not job["active"]:
            raise SlurmError(f"Job {job_id} is not running or pending")
        run_command(["scancel", job["id"]], timeout=15)
        self._invalidate_jobs()

    def submit(
        self,
        options: Dict[str, Any],
        script_path: Optional[str] = None,
        script_content: Optional[str] = None,
    ) -> dict:
        """Submit a batch job from a script file or from script text.

        Script text is passed to sbatch on stdin, so nothing is written to disk.
        """
        cmd = ["sbatch", "--parsable"]
        flag_names = [
            ("job_name", "--job-name"),
            ("partition", "--partition"),
            ("time_limit", "--time"),
            ("memory", "--mem"),
            ("cpus", "--cpus-per-task"),
            ("gpus", "--gpus"),
            ("work_dir", "--chdir"),
            ("output_file", "--output"),
            ("error_file", "--error"),
            ("dependency", "--dependency"),
        ]
        for key, flag in flag_names:
            value = options.get(key)
            if value in (None, "", 0):
                continue
            value = str(value).strip()
            if "\n" in value or "\x00" in value:
                raise SlurmError(f"Invalid value for {key}")
            cmd.append(f"{flag}={value}")

        work_dir = str(options.get("work_dir") or "").strip() or None
        if work_dir:
            work_dir = str(Path(work_dir).expanduser())
        stdin = None
        if script_path:
            script = Path(str(script_path)).expanduser()
            if not script.is_absolute() and work_dir:
                script = Path(work_dir) / script
            if not script.is_file():
                raise SlurmError(f"Script not found: {script}")
            cmd.append(str(script))
        elif script_content and script_content.strip():
            stdin = script_content if script_content.endswith("\n") else script_content + "\n"
            if not stdin.startswith("#!"):
                stdin = "#!/bin/bash\n" + stdin
        else:
            raise SlurmError("Provide a script path or script content")

        out = run_command(cmd, timeout=30, stdin=stdin, cwd=work_dir)
        new_id = out.strip().split(";")[0].strip()
        if not new_id.isdigit():
            raise SlurmError(f"sbatch gave an unexpected reply: {out.strip()[:200]}")
        self._invalidate_jobs()
        return {"success": True, "job_id": new_id, "message": f"Submitted batch job {new_id}"}

    def resubmit(self, job_id: str, overrides: Dict[str, Any], history_days: int = 7) -> dict:
        """Submit an earlier job again, optionally changing resources.

        Uses the script file on disk when it still exists (so a fixed script is
        picked up), otherwise the copy Slurm retained.
        """
        self._require_owned(job_id, history_days)
        info = self.submit_info(job_id, history_days)
        options = {k: v for k, v in overrides.items() if v not in (None, "")}
        options.setdefault("work_dir", info.get("work_dir"))
        path = info.get("script_path")
        if path and Path(path).is_file():
            return self.submit(options, script_path=path)
        script = self.job_script(job_id, history_days)
        if script.get("error"):
            raise SlurmError(f"Cannot resubmit: {script['error']}")
        options.setdefault("job_name", info.get("job_name"))
        return self.submit(options, script_content=script["content"])


# -------------------------------------------------------------------- helpers


def parse_memory_kb(value: str) -> float:
    """Parse a usage value such as MaxRSS, where a bare number is kilobytes."""
    text = (value or "").strip()
    if not text:
        return 0.0
    if text[-1].isdigit():
        try:
            return float(text) * 1024
        except ValueError:
            return 0.0
    return parse_memory(text)


def _to_int(value: Optional[str]) -> int:
    try:
        return int(float((value or "").strip()))
    except ValueError:
        return 0


def _percent(value: Optional[float]) -> str:
    return f"{value:.1f}%" if value is not None else "N/A"


def _recency(job: dict) -> float:
    for key in ("end", "start", "submit"):
        epoch = timestamp_epoch(job.get(key))
        if epoch:
            return epoch
    return 0.0


def _in_window(job: dict, cutoff: float) -> bool:
    end = timestamp_epoch(job.get("end"))
    return end is None or end >= cutoff


def _add_efficiency(job: dict) -> None:
    """Attach cpu_eff / mem_eff percentages (None when not measurable)."""
    job["cpu_eff"] = None
    job["mem_eff"] = None
    core_seconds = job["elapsed_seconds"] * job["cpus"]
    if core_seconds > 0 and job["total_cpu_seconds"] > 0:
        job["cpu_eff"] = round(min(100.0, job["total_cpu_seconds"] / core_seconds * 100), 1)
    if job["req_mem_bytes"] > 0 and job["max_rss_bytes"] > 0:
        job["mem_eff"] = round(min(100.0, job["max_rss_bytes"] / job["req_mem_bytes"] * 100), 1)


def _script_from_submit_line(submit_line: Optional[str], work_dir: Optional[str]) -> Optional[str]:
    """Find the script in an sbatch command line: the first argument that is a file."""
    if not submit_line:
        return None
    try:
        tokens = shlex.split(submit_line)
    except ValueError:
        tokens = submit_line.split()
    for token in tokens[1:]:
        if token.startswith("-"):
            continue
        candidate = Path(token).expanduser()
        if not candidate.is_absolute() and work_dir:
            candidate = Path(work_dir) / candidate
        try:
            if candidate.is_file():
                return str(candidate)
        except OSError:
            continue
    return None


def _strip_sacct_script_header(output: str) -> str:
    """Remove the "Batch Script for <id>" banner sacct prints before the script."""
    lines = output.splitlines(keepends=True)
    if lines and lines[0].startswith("Batch Script for"):
        lines = lines[1:]
        if lines and set(lines[0].strip()) <= {"-"}:
            lines = lines[1:]
    return "".join(lines)
